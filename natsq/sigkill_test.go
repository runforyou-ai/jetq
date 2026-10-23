package natsq

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

// The SIGKILL tests run a worker in a child process (this test binary,
// re-executed) against the parent's NATS server. The child reports on stdout
// when it reaches a durable boundary and then blocks; the parent kills it
// with SIGKILL, waits for it, and recovers with a worker of its own. The
// hooks only mark the boundary: the crash is a real process kill.

const (
	sigkillURL   = "JETQ_SIGKILL_URL"
	sigkillNS    = "JETQ_SIGKILL_NS"
	sigkillPhase = "JETQ_SIGKILL_PHASE"
)

func sigkillTask() jetq.Task[int] {
	return jetq.MustTask[int]("crash.task", jetq.Policy{MaxAttempts: 2, Timeout: jetq.TimeoutOf(time.Minute)})
}

// TestSigkillChild is the child process body; it is skipped unless the
// parent started it.
func TestSigkillChild(t *testing.T) {
	url := os.Getenv(sigkillURL)
	if url == "" {
		t.Skip("child process only")
	}
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	d, err := Open(context.Background(), js, sigkillConfig(os.Getenv(sigkillNS)))
	if err != nil {
		t.Fatal(err)
	}
	phase := os.Getenv(sigkillPhase)
	boundary := func(what string) {
		fmt.Printf("BOUNDARY %s\n", what)
		_ = os.Stdout.Sync()
		select {} // wait for SIGKILL
	}
	switch phase {
	case "admit":
		d.hks.afterAdmit = func(string) { boundary("admit") }
	case "decision":
		d.hks.afterDecisionWrite = func(jetq.Decision) error { boundary("decision"); return nil }
	}
	client, err := jetq.NewClient(d)
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(jetq.Queue{Name: "q", Concurrency: 1})
	jetq.Handle(w, sigkillTask(), func(context.Context, int) error {
		fmt.Println("EXECUTED")
		if phase == "snooze" {
			// Snooze, then die once its decision is durable.
			d.hks.afterDecisionWrite = func(jetq.Decision) error { boundary("snooze"); return nil }
			return jetq.Snooze(time.Millisecond)
		}
		return nil
	})
	_ = w.Run(context.Background())
}

func sigkillConfig(ns string) Config {
	return Config{Namespace: ns, Memory: true, ClaimLease: 400 * time.Millisecond, ScanInterval: 50 * time.Millisecond, JobRetention: -1}
}

// killAtBoundary runs a child worker for phase and SIGKILLs it once it reports
// the boundary. It returns whether the child ran the handler.
func killAtBoundary(t *testing.T, s *testServer, ns, phase string) bool {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSigkillChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), sigkillURL+"="+s.srv.ClientURL(), sigkillNS+"="+ns, sigkillPhase+"="+phase)
	cmd.Dir = t.TempDir()
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	executed := false
	timeout := time.After(20 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				_ = cmd.Wait()
				t.Fatalf("child %s exited before its boundary", phase)
			}
			if line == "EXECUTED" {
				executed = true
			}
			if strings.HasPrefix(line, "BOUNDARY ") {
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				state, _ := cmd.Process.Wait()
				if state == nil || state.Success() {
					t.Fatalf("child %s was not killed: %v", phase, state)
				}
				go func() {
					for range lines {
					}
				}()
				return executed
			}
		case <-timeout:
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("child %s never reached its boundary", phase)
		}
	}
}

// recoverWith runs a worker in this process until the job is terminal and
// returns how often its handler ran.
func recoverWith(t *testing.T, d *Driver, id string) int {
	t.Helper()
	client, err := jetq.NewClient(d)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	w := client.NewWorker(jetq.Queue{Name: "q", Concurrency: 1})
	jetq.Handle(w, sigkillTask(), func(context.Context, int) error {
		calls.Add(1)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		snap, ok, err := d.GetJob(context.Background(), id)
		if err == nil && ok && snap.State.Terminal() {
			return int(calls.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s never finished after recovery", id)
	return 0
}

func sigkillSetup(t *testing.T, ns string) (*testServer, *Driver, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("spawns processes")
	}
	s := startServer(t)
	cfg := sigkillConfig(ns)
	d := openDriver(t, s, cfg)
	client, err := jetq.NewClient(d)
	if err != nil {
		t.Fatal(err)
	}
	res, err := jetq.SubmitTask(context.Background(), client, sigkillTask(), 1, jetq.Submission{Queue: "q", JobID: "crash-job"})
	if err != nil {
		t.Fatal(err)
	}
	return s, d, res.JobID
}

// TestSigkillAfterAdmission: a worker killed after its admission is durable
// has spent budget; repeated kills never exceed MaxAttempts, and recovery
// fails the job without running the handler again.
func TestSigkillAfterAdmission(t *testing.T) {
	s, d, id := sigkillSetup(t, "kill-admit")
	for range 2 {
		if killAtBoundary(t, s, "kill-admit", "admit") {
			t.Fatal("handler ran before the admission boundary")
		}
	}
	if calls := recoverWith(t, d, id); calls != 0 {
		t.Fatalf("handler ran %d times past an exhausted budget", calls)
	}
	snap, _, _ := d.GetJob(context.Background(), id)
	if snap.State != jetq.StateFailed || snap.Budget != 2 {
		t.Fatalf("after two killed admissions: %+v", snap)
	}
}

// TestSigkillAfterDecision: a worker killed after its success decision is
// durable does not have its handler run again.
func TestSigkillAfterDecision(t *testing.T) {
	s, d, id := sigkillSetup(t, "kill-decision")
	if !killAtBoundary(t, s, "kill-decision", "decision") {
		t.Fatal("child did not run the handler")
	}
	if calls := recoverWith(t, d, id); calls != 0 {
		t.Fatalf("handler re-ran %d times after a durable decision", calls)
	}
	snap, _, _ := d.GetJob(context.Background(), id)
	if snap.State != jetq.StateSucceeded || snap.Budget != 1 {
		t.Fatalf("after killed decision: %+v", snap)
	}
}

// TestSigkillAfterSnooze: a worker killed after its snooze decision is
// durable gets the refund exactly once; the job then runs and succeeds.
func TestSigkillAfterSnooze(t *testing.T) {
	s, d, id := sigkillSetup(t, "kill-snooze")
	if !killAtBoundary(t, s, "kill-snooze", "snooze") {
		t.Fatal("child did not run the handler")
	}
	if calls := recoverWith(t, d, id); calls != 1 {
		t.Fatalf("handler ran %d times after the snooze, want 1", calls)
	}
	snap, _, _ := d.GetJob(context.Background(), id)
	if snap.State != jetq.StateSucceeded || snap.Snoozes != 1 || snap.Budget != 1 {
		t.Fatalf("after killed snooze: %+v", snap)
	}
}
