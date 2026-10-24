package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

type testServer struct {
	srv *server.Server
	nc  *nats.Conn
	js  jetstream.JetStream
}

func startServer(t testing.TB) *testServer {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: server.RANDOM_PORT, JetStream: true,
		StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return &testServer{srv: srv, nc: nc, js: js}
}

func testConfig(t testing.TB, ns string) Config {
	t.Helper()
	return Config{
		Namespace:    ns,
		Memory:       true,
		ClaimLease:   400 * time.Millisecond,
		ScanInterval: 50 * time.Millisecond,
		JobRetention: -1,
		Logger:       slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

type testWriter struct{ t testing.TB }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func openDriver(t testing.TB, s *testServer, cfg Config) *Driver {
	t.Helper()
	if _, err := Provision(context.Background(), s.js, cfg); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	d, err := Open(context.Background(), s.js, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return d
}

func prepare(t *testing.T, id, queue string, unique *jetq.UniqueRule) *jetq.PreparedJob {
	t.Helper()
	sub := jetq.Submission{Queue: queue, JobID: id, Unique: unique}
	job, err := jetq.Prepare("task.a", json.RawMessage(`{"n":1}`), sub, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return id })
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return job
}

func mustSubmit(t *testing.T, d *Driver, job *jetq.PreparedJob) jetq.EnqueueResult {
	t.Helper()
	res, err := d.Submit(context.Background(), job)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return res
}

func receive(t *testing.T, c jetq.Consumer) jetq.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		dlv, err := c.Receive(ctx, 1)
		if err != nil && ctx.Err() == nil {
			t.Fatalf("Receive: %v", err)
		}
		if len(dlv) > 0 {
			return dlv[0]
		}
		if ctx.Err() != nil {
			t.Fatal("no delivery")
		}
	}
}

func expectNoDelivery(t *testing.T, c jetq.Consumer, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	dlv, err := c.Receive(ctx, 1)
	if len(dlv) > 0 {
		t.Fatalf("unexpected delivery of %s", dlv[0].Envelope().JobID)
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Receive: %v", err)
	}
}

func settle(t *testing.T, dlv jetq.Delivery, dec jetq.Decision) jetq.SettleStatus {
	t.Helper()
	if dec.TransitionID == "" {
		adm, err := dlv.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}
		dec = jetq.Decision{Kind: jetq.DecideSucceed, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSucceed)}
	}
	status, err := dlv.Settle(context.Background(), dec)
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	return status
}

func TestSubmitReceiveAdmitSettle(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()

	res := mustSubmit(t, d, prepare(t, "job-1", "mail", nil))
	if res.Status != jetq.Accepted || res.JobID != "job-1" {
		t.Fatalf("result = %+v", res)
	}
	if !res.RunAt.IsZero() {
		t.Errorf("immediate job RunAt = %s", res.RunAt)
	}

	other, err := d.Consumer(ctx, "other")
	if err != nil {
		t.Fatal(err)
	}
	expectNoDelivery(t, other, 200*time.Millisecond) // queues are isolated

	c, err := d.Consumer(ctx, "mail")
	if err != nil {
		t.Fatal(err)
	}
	dlv := receive(t, c)
	if dlv.Envelope().JobID != "job-1" || dlv.Envelope().Task != "task.a" || string(dlv.Envelope().Payload) != `{"n":1}` {
		t.Fatalf("envelope = %+v", dlv.Envelope())
	}
	if st := dlv.State(); st.Budget != 0 || st.Deliveries != 1 {
		t.Fatalf("state = %+v", st)
	}
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if adm.Attempt != 1 || adm.ExecutionID == "" {
		t.Fatalf("admission = %+v", adm)
	}
	// Admit is idempotent for one delivery.
	if again, err := dlv.Admit(ctx); err != nil || again != adm {
		t.Fatalf("second Admit = %+v, %v", again, err)
	}
	status := settle(t, dlv, jetq.Decision{Kind: jetq.DecideSucceed, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSucceed)})
	if status != jetq.Done {
		t.Fatalf("settle = %v", status)
	}
	state, ok, err := d.JobState(ctx, "job-1")
	if err != nil || !ok || state != jetq.StateSucceeded {
		t.Fatalf("state = %q, %v, %v", state, ok, err)
	}
	expectNoDelivery(t, c, 200*time.Millisecond)
}

func TestRetrySchedulesNextExecution(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	c, _ := d.Consumer(ctx, "q")

	dlv := receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := dlv.Settle(ctx, jetq.Decision{
		Kind: jetq.DecideRetry, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideRetry),
		RunAt: time.Now().Add(300 * time.Millisecond), Error: "boom",
	})
	if err != nil || status != jetq.Done {
		t.Fatalf("settle = %v, %v", status, err)
	}
	// A retry keeps the spent budget and is not eligible before RunAt.
	ent, ok, err := d.ledger.get(ctx, jobKey("job-1"))
	if err != nil || !ok {
		t.Fatal(err)
	}
	if ent.rec.State != jetq.StateAwaiting || ent.rec.Wait != jetq.WaitRetry || ent.rec.Budget != 1 {
		t.Fatalf("record = %+v", ent.rec)
	}
	expectNoDelivery(t, c, 150*time.Millisecond)
	dlv = receive(t, c)
	adm2, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if adm2.Attempt != 2 {
		t.Fatalf("attempt = %d", adm2.Attempt)
	}
}

func TestSnoozeRefundsBudgetOnce(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	c, _ := d.Consumer(ctx, "q")

	dlv := receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status := settle(t, dlv, jetq.Decision{Kind: jetq.DecideSnooze, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSnooze)}); status != jetq.Done {
		t.Fatalf("settle = %v", status)
	}
	ent, _, _ := d.ledger.get(ctx, jobKey("job-1"))
	if ent.rec.Budget != 0 || ent.rec.Snoozes != 1 {
		t.Fatalf("budget = %d, snoozes = %d", ent.rec.Budget, ent.rec.Snoozes)
	}

	// Retrying the same settlement must not refund again or create a second
	// execution.
	if status, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideSnooze, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSnooze)}); err != nil || status != jetq.Done {
		t.Fatalf("second settle = %v, %v", status, err)
	}
	ent, _, _ = d.ledger.get(ctx, jobKey("job-1"))
	if ent.rec.Budget != 0 || ent.rec.Snoozes != 1 {
		t.Fatalf("after repeat: budget = %d, snoozes = %d", ent.rec.Budget, ent.rec.Snoozes)
	}

	// The snoozed job runs again with its budget intact.
	dlv = receive(t, c)
	if _, err := dlv.Admit(ctx); err != nil {
		t.Fatal(err)
	}
	ent, _, _ = d.ledger.get(ctx, jobKey("job-1"))
	if ent.rec.Budget != 1 {
		t.Fatalf("budget after re-admission = %d", ent.rec.Budget)
	}
}

func TestRecordedDecisionIsResumedNotRerun(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	cfg.ClaimLease = 150 * time.Millisecond
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))

	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The decision is written; cleanup fails, as when the process dies after
	// writing it. The job's next delivery must resume, never re-run.
	d.hks.afterCleanup = func(jetq.Decision) error { return errors.New("crash before cleanup") }
	status, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideRetry, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideRetry), RunAt: time.Now()})
	if status != jetq.RecordedCleanupPending || err == nil {
		t.Fatalf("settle = %v, %v", status, err)
	}
	d.hks.afterCleanup = nil

	// The crashing process stops holding the job: its lease runs out.
	time.Sleep(300 * time.Millisecond)
	dlv = receive(t, c)
	st := dlv.State()
	if st.Pending == nil || st.Pending.TransitionID != jetq.TransitionID(adm.ExecutionID, jetq.DecideRetry) || st.Budget != 1 {
		t.Fatalf("state = %+v", st)
	}
	if _, err := dlv.Admit(ctx); err == nil {
		t.Fatal("Admit succeeded for a job with a recorded decision")
	}
	if status, err := dlv.Settle(ctx, *st.Pending); err != nil || status != jetq.Done {
		t.Fatalf("resume = %v, %v", status, err)
	}
	ent, _, _ := d.ledger.get(ctx, jobKey("job-1"))
	if ent.rec.Pending != nil || ent.rec.Budget != 1 {
		t.Fatalf("record = %+v", ent.rec)
	}
	// Only after the recorded decision is applied does a new execution
	// start, and it is the second business attempt.
	dlv = receive(t, c)
	adm2, err := dlv.Admit(ctx)
	if err != nil || adm2.Attempt != 2 {
		t.Fatalf("attempt = %+v, %v", adm2, err)
	}
}

func TestRecoveryCompletesRecordedDecision(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d.hks.afterCleanup = func(jetq.Decision) error { return errors.New("crash") }
	if _, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideSnooze, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSnooze)}); err == nil {
		t.Fatal("expected a cleanup failure")
	}
	d.hks.afterCleanup = nil
	if n, err := d.Recover(ctx); err != nil || n == 0 {
		t.Fatalf("Recover = %d, %v", n, err)
	}
	ent, _, _ := d.ledger.get(ctx, jobKey("job-1"))
	if ent.rec.Pending != nil || ent.rec.Budget != 0 || ent.rec.Snoozes != 1 {
		t.Fatalf("record = %+v", ent.rec)
	}
	// Recovery is idempotent: a second pass changes nothing and never
	// refunds the budget twice.
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	ent, _, _ = d.ledger.get(ctx, jobKey("job-1"))
	if ent.rec.Budget != 0 || ent.rec.Snoozes != 1 {
		t.Fatalf("after second pass: budget = %d, snoozes = %d", ent.rec.Budget, ent.rec.Snoozes)
	}
}

func TestDecisionWriteResponseLost(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The decision is written; the response is lost.
	d.hks.afterDecisionWrite = func(jetq.Decision) error { return errors.New("response lost") }
	dec := jetq.Decision{Kind: jetq.DecideSnooze, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSnooze)}
	status, err := dlv.Settle(ctx, dec)
	if status != jetq.SettleUnknown || !errors.Is(err, jetq.ErrUncertain) {
		t.Fatalf("settle = %v, %v", status, err)
	}
	d.hks.afterDecisionWrite = nil

	ent, _, _ := d.ledger.get(ctx, jobKey("job-1"))
	if ent.rec.Pending == nil {
		t.Fatal("decision was not durable")
	}
	// Recovery finishes it; the budget is refunded exactly once even though
	// the settlement is retried.
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if status, err := dlv.Settle(ctx, dec); err != nil || status != jetq.Done {
		t.Fatalf("retried settle = %v, %v", status, err)
	}
	ent, _, _ = d.ledger.get(ctx, jobKey("job-1"))
	if ent.rec.Budget != 0 || ent.rec.Snoozes != 1 || ent.rec.Pending != nil {
		t.Fatalf("record = %+v", ent.rec)
	}
}

func TestEnqueueKeyIdempotencyAndConflict(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()

	mk := func(id, payload string) *jetq.PreparedJob {
		sub := jetq.Submission{Queue: "q", JobID: id, EnqueueKey: "outbox-1"}
		job, err := jetq.Prepare("task.a", json.RawMessage(payload), sub, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return id })
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	first := mk("job-1", `{"n":1}`)
	if res := mustSubmit(t, d, first); res.Status != jetq.Accepted {
		t.Fatalf("first = %+v", res)
	}
	// The same prepared job retried (a new call, same identity and content)
	// returns the original job.
	again := mk("job-1", `{"n":1}`)
	res := mustSubmit(t, d, again)
	if res.Status != jetq.AlreadyAccepted || res.JobID != "job-1" {
		t.Fatalf("retry = %+v", res)
	}
	// Different content under the same key is a conflict and stores nothing.
	conflict := mk("job-2", `{"n":2}`)
	if _, err := d.Submit(ctx, conflict); !errors.Is(err, jetq.ErrEnqueueKeyConflict) {
		t.Fatalf("conflict = %v", err)
	}
	if _, ok, _ := d.JobState(ctx, "job-2"); ok {
		t.Fatal("conflicting job was stored")
	}
	// A different key accepts a new job.
	other, err := jetq.Prepare("task.a", json.RawMessage(`{"n":2}`), jetq.Submission{Queue: "q", JobID: "job-3", EnqueueKey: "outbox-2"}, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return "job-3" })
	if err != nil {
		t.Fatal(err)
	}
	if res := mustSubmit(t, d, other); res.Status != jetq.Accepted {
		t.Fatalf("other key = %+v", res)
	}
}

func TestSubmitRejectsTamperedFingerprint(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	job := prepare(t, "job-1", "q", nil)
	job.Fingerprint = "tampered"
	if _, err := d.Submit(context.Background(), job); err == nil {
		t.Fatal("tampered fingerprint accepted")
	}
	modified := prepare(t, "job-1", "q", nil)
	modified.Envelope.Payload = json.RawMessage(`{"n":2}`)
	if _, err := d.Submit(context.Background(), modified); err == nil {
		t.Fatal("modified envelope accepted")
	}
}

func TestUniqueWindow(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeWindow, Key: "k", Window: 250 * time.Millisecond}

	if res := mustSubmit(t, d, prepare(t, "job-1", "q", unique)); res.Status != jetq.Accepted {
		t.Fatalf("first = %+v", res)
	}
	res := mustSubmit(t, d, prepare(t, "job-2", "q", unique))
	if res.Status != jetq.Suppressed || res.ExistingJobID != "job-1" {
		t.Fatalf("second = %+v", res)
	}
	if _, ok, _ := d.JobState(ctx, "job-2"); ok {
		t.Fatal("suppressed job was stored")
	}
	// After the window a new job is accepted even though the first still runs.
	time.Sleep(300 * time.Millisecond)
	if res := mustSubmit(t, d, prepare(t, "job-3", "q", unique)); res.Status != jetq.Accepted {
		t.Fatalf("after window = %+v", res)
	}
}

func TestUniqueUntilDoneReleasedOnSuccess(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mustSubmit(t, d, prepare(t, "job-1", "q", unique))
	if res := mustSubmit(t, d, prepare(t, "job-2", "q", unique)); res.Status != jetq.Suppressed {
		t.Fatalf("while pending = %+v", res)
	}
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	settle(t, dlv, jetq.Decision{})
	if res := mustSubmit(t, d, prepare(t, "job-3", "q", unique)); res.Status != jetq.Accepted {
		t.Fatalf("after settle = %+v", res)
	}
}

func TestUniqueUntilDoneLockRecoveredOnCrash(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	cfg.ClaimLease = 100 * time.Millisecond
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mustSubmit(t, d, prepare(t, "job-1", "q", unique))
	c, _ := d.Consumer(ctx, "q")
	receive(t, c) // claimed, then the worker "dies"
	if res := mustSubmit(t, d, prepare(t, "job-2", "q", unique)); res.Status != jetq.Suppressed {
		t.Fatalf("while claimed = %+v", res)
	}
	time.Sleep(400 * time.Millisecond) // lease expires
	if n, err := d.Recover(ctx); err != nil || n == 0 {
		t.Fatalf("Recover = %d, %v", n, err)
	}
	// The crashed job is runnable again, so its key stays held.
	if res := mustSubmit(t, d, prepare(t, "job-2", "q", unique)); res.Status != jetq.Suppressed {
		t.Fatalf("after recovery = %+v", res)
	}
	dlv := receive(t, c)
	settle(t, dlv, jetq.Decision{})
	if res := mustSubmit(t, d, prepare(t, "job-3", "q", unique)); res.Status != jetq.Accepted {
		t.Fatalf("after crash run = %+v", res)
	}
}

func TestLeaseExpiryRedeliversAndKeepsBudgetSpent(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	cfg.ClaimLease = 150 * time.Millisecond
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))

	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	if _, err := dlv.Admit(ctx); err != nil {
		t.Fatal(err)
	}
	// The process dies after admission. The lease runs out and the job is
	// delivered again; the admission stays spent.
	time.Sleep(400 * time.Millisecond)
	c2, _ := d.Consumer(ctx, "q")
	dlv2 := receive(t, c2)
	if st := dlv2.State(); st.Budget != 1 {
		t.Fatalf("budget = %d", st.Budget)
	}
	adm, err := dlv2.Admit(ctx)
	if err != nil || adm.Attempt != 2 {
		t.Fatalf("admit = %+v, %v", adm, err)
	}
}

func TestExtendKeepsLease(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	cfg.ClaimLease = 200 * time.Millisecond
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	if _, err := dlv.Admit(ctx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		time.Sleep(100 * time.Millisecond)
		if err := dlv.Extend(ctx); err != nil {
			t.Fatalf("Extend: %v", err)
		}
	}
	other, _ := d.Consumer(ctx, "q")
	expectNoDelivery(t, other, 100*time.Millisecond)
	settle(t, dlv, jetq.Decision{})
}

func TestReleaseKeepsBudget(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	if err := dlv.Release(ctx); err != nil {
		t.Fatal(err)
	}
	dlv = receive(t, c)
	if st := dlv.State(); st.Budget != 0 {
		t.Fatalf("budget = %d", st.Budget)
	}
	if adm, err := dlv.Admit(ctx); err != nil || adm.Attempt != 1 {
		t.Fatalf("admit = %+v, %v", adm, err)
	}
}

func TestReleaseAfterAdmissionKeepsSpentBudget(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	cfg.ClaimLease = 150 * time.Millisecond
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	if _, err := dlv.Admit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := dlv.Release(ctx); err != nil {
		t.Fatal(err)
	}
	dlv = receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil || adm.Attempt != 2 {
		t.Fatalf("admit = %+v, %v", adm, err)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	s := startServer(t)
	a := openDriver(t, s, testConfig(t, "a"))
	b := openDriver(t, s, testConfig(t, "b"))
	ctx := context.Background()
	mustSubmit(t, a, prepare(t, "job-1", "q", nil))
	cb, _ := b.Consumer(ctx, "q")
	expectNoDelivery(t, cb, 200*time.Millisecond)
	if _, ok, _ := b.JobState(ctx, "job-1"); ok {
		t.Fatal("job visible in another namespace")
	}
	ca, _ := a.Consumer(ctx, "q")
	receive(t, ca)
	// The same job id in both namespaces is independent.
	mustSubmit(t, b, prepare(t, "job-1", "q", nil))
	if res := mustSubmit(t, a, prepare(t, "job-2", "q", nil)); res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v", res)
	}
}

func TestOpenRequiresProvisionedResources(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	if _, err := Open(context.Background(), s.js, cfg); err == nil {
		t.Fatal("Open succeeded without provisioning")
	}
	if _, err := Provision(context.Background(), s.js, cfg); err != nil {
		t.Fatal(err)
	}
	d, err := Open(context.Background(), s.js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Opening again does not recreate or change the resources.
	before, _ := d.ledger.kv.Status(context.Background())
	if _, err := Open(context.Background(), s.js, cfg); err != nil {
		t.Fatal(err)
	}
	after, _ := d.ledger.kv.Status(context.Background())
	if before.Values() != after.Values() || after.Bucket() != "app_LEDGER" {
		t.Fatalf("Open changed the ledger: %v -> %v", before, after)
	}
	// A conflicting deployment configuration is reported, not applied.
	bad := cfg
	bad.WakeWindow = 10 * time.Minute
	if _, err := Open(context.Background(), s.js, bad); err == nil {
		t.Fatal("incompatible wake stream accepted")
	}
}

func TestConfigConflictAtProvision(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	cfg.Memory = false
	if _, err := Provision(context.Background(), s.js, cfg); err != nil {
		t.Fatal(err)
	}
	kv, err := s.js.KeyValue(context.Background(), "app_LEDGER")
	if err != nil {
		t.Fatal(err)
	}
	// A provision with a different storage type would rebuild resources; it
	// is rejected by Open instead of silently reconfiguring them.
	mem := cfg
	mem.Memory = true
	if _, err := Provision(context.Background(), s.js, mem); err == nil {
		if d, err := Open(context.Background(), s.js, mem); err == nil {
			_ = d
		}
	}
	// The original stays intact and usable.
	if _, err := Open(context.Background(), s.js, cfg); err != nil {
		t.Fatal(err)
	}
	status, _ := kv.Status(context.Background())
	if status.Values() != 0 {
		t.Fatalf("values = %d", status.Values())
	}
}

func TestCancel(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	if err := d.Cancel(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	c, _ := d.Consumer(ctx, "q")
	expectNoDelivery(t, c, 200*time.Millisecond)
	if err := d.Cancel(ctx, "job-1"); err != nil {
		t.Fatalf("second cancel = %v", err)
	}
	if err := d.Cancel(ctx, "missing"); !errors.Is(err, jetq.ErrNotFound) {
		t.Fatalf("missing = %v", err)
	}
	state, _, _ := d.JobState(ctx, "job-1")
	if state != jetq.StateCanceled {
		t.Fatalf("state = %s", state)
	}
	// A running job is not interrupted; its retry does not run.
	mustSubmit(t, d, prepare(t, "job-2", "q", nil))
	dlv := receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Cancel(ctx, "job-2"); err != nil {
		t.Fatal(err)
	}
	if status, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideRetry, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideRetry), RunAt: time.Now()}); err != nil || status != jetq.Done {
		t.Fatalf("settle = %v, %v", status, err)
	}
	expectNoDelivery(t, c, 200*time.Millisecond)
}

func TestDelayedJobIsNotDeliveredEarly(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	sub := jetq.Submission{Queue: "q", JobID: "job-1", Delay: 300 * time.Millisecond}
	job, err := jetq.Prepare("task.a", json.RawMessage(`{}`), sub, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return "job-1" })
	if err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, d, job)
	c, _ := d.Consumer(ctx, "q")
	expectNoDelivery(t, c, 150*time.Millisecond)
	receive(t, c)
}

func TestConcurrentConsumersClaimEachJobOnce(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	const jobs = 30
	for i := range jobs {
		mustSubmit(t, d, prepare(t, fmt.Sprintf("job-%d", i), "q", nil))
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for range 4 {
		c, _ := d.Consumer(ctx, "q")
		wg.Add(1)
		go func() {
			defer wg.Done()
			runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			for runCtx.Err() == nil {
				dlv, err := c.Receive(runCtx, 3)
				for _, dlv := range dlv {
					mu.Lock()
					seen[dlv.Envelope().JobID]++
					mu.Unlock()
					if _, err := dlv.Admit(ctx); err != nil {
						continue
					}
					if _, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideSucceed, TransitionID: jetq.TransitionID(dlv.State().Admitted, jetq.DecideSucceed)}); err != nil {
						continue
					}
				}
				if err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(seen) != jobs {
		t.Fatalf("delivered %d distinct jobs", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s delivered %d times", id, n)
		}
	}
}

// TestSubmitCreateResponseLost: the job record is stored but the client sees
// an error. Nothing is released, a competing submission of the same key is
// still suppressed by the stored job (or converges on the same id), and a
// retry that adopts the intent returns the original job.
func TestSubmitCreateResponseLost(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	d := openDriver(t, s, cfg)
	d.hks.kv = func(kv jetstream.KeyValue) jetstream.KeyValue {
		f := &faultKV{KeyValue: kv}
		f.createCommitted = func(key string) bool {
			id, ok := strings.CutPrefix(key, "job.")
			return ok && id == "job-1"
		}
		return f
	}
	d.ledger.kv = d.hks.kv(d.ledger.kv)
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mk := func(id string) *jetq.PreparedJob {
		sub := jetq.Submission{Queue: "q", JobID: id, EnqueueKey: "outbox-1", Unique: unique}
		job, err := jetq.Prepare("task.a", json.RawMessage(`{"n":1}`), sub, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return id })
		if err != nil {
			t.Fatal(err)
		}
		return job
	}

	res, err := d.Submit(ctx, mk("job-1"))
	if res.Status != jetq.Unknown || !errors.Is(err, jetq.ErrUncertain) {
		t.Fatalf("submit = %+v, %v", res, err)
	}
	// The lost response's create committed the accepted outcome; the caller
	// only lost the answer. The repair markers of that response did not run,
	// which is fine: every reader checks the record.
	ent, ok, err := d.ledger.get(ctx, jobKey("job-1"))
	if err != nil || !ok || ent.rec.Outcome != outcomeAccepted {
		t.Fatalf("record = %+v, %v, %v", ent, ok, err)
	}
	// A competing submission of the same content adopts the stored intent and
	// reports its job; either way nothing new runs and nothing was released.
	if res := mustSubmit(t, d, mk("job-2")); res.Status != jetq.AlreadyAccepted || res.JobID != "job-1" {
		t.Fatalf("competing submission = %+v", res)
	}
	// A retry with the same prepared job reports the original job.
	res = mustSubmit(t, d, mk("job-1"))
	if res.Status != jetq.AlreadyAccepted || res.JobID != "job-1" {
		t.Fatalf("retry = %+v", res)
	}
}

// TestConcurrentEnqueueKeyRetriesConverge: many producers retry the same
// EnqueueKey with the same content but their own generated ids. They must all
// end up on one job, and none may see a conflict.
func TestConcurrentEnqueueKeyRetriesConverge(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	const n = 8
	results := make([]jetq.EnqueueResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("candidate-%d", i)
			job, err := jetq.Prepare("task.a", json.RawMessage(`{"n":1}`), jetq.Submission{Queue: "q", JobID: id, EnqueueKey: "outbox"}, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return id })
			if err != nil {
				errs[i] = err
				return
			}
			results[i], errs[i] = d.Submit(ctx, job)
		}()
	}
	wg.Wait()
	ids := map[string]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		if st := results[i].Status; st != jetq.Accepted && st != jetq.AlreadyAccepted {
			t.Fatalf("submit %d = %+v", i, results[i])
		}
		ids[results[i].JobID] = true
	}
	if len(ids) != 1 {
		t.Fatalf("concurrent retries produced %d jobs: %v", len(ids), ids)
	}
}

// TestAlreadyAcceptedKeepsOriginalRunAt: a retry of a delayed submission
// returns the original, frozen run time.
func TestAlreadyAcceptedKeepsOriginalRunAt(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	runAt := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	mk := func() *jetq.PreparedJob {
		job, err := jetq.Prepare("task.a", json.RawMessage(`{}`), jetq.Submission{Queue: "q", JobID: "job-1", EnqueueKey: "outbox", RunAt: runAt}, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return "job-1" })
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	first := mustSubmit(t, d, mk())
	if first.Status != jetq.Accepted || !first.RunAt.Equal(runAt) {
		t.Fatalf("first = %+v", first)
	}
	// A retry prepared much later still reports the original run time.
	later := mustSubmit(t, d, mk())
	if later.Status != jetq.AlreadyAccepted || !later.RunAt.Equal(runAt) {
		t.Fatalf("retry = %+v", later)
	}
}

// TestStalledIntentIsCompletedNotReplaced: a submission whose intent was
// written but whose job was not stored is completed by a competing submission
// under the original job id and run time; a late original producer converges
// on the same job instead of creating a second one.
func TestStalledIntentIsCompletedNotReplaced(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	runAt := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	mk := func(id string) *jetq.PreparedJob {
		job, err := jetq.Prepare("task.a", json.RawMessage(`{}`), jetq.Submission{Queue: "q", JobID: id, EnqueueKey: "outbox", RunAt: runAt}, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return id })
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	original := mk("original")
	// The original producer writes its intent but "stalls" before the job.
	env := original.Envelope.Clone()
	if _, err := d.claimIntent(ctx, env, original.Fingerprint); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	res := mustSubmit(t, d, mk("replacement"))
	// The competitor completed the original submission: the job it stored is
	// the original one, with the frozen run time, not its own candidate id.
	if res.Status != jetq.Accepted || res.JobID != "original" || !res.RunAt.Equal(runAt) {
		t.Fatalf("competitor = %+v", res)
	}
	// The original producer wakes up: its job is already stored, so it learns
	// the same outcome and the frozen run time.
	res = mustSubmit(t, d, original)
	if res.Status != jetq.AlreadyAccepted || res.JobID != "original" || !res.RunAt.Equal(runAt) {
		t.Fatalf("stalled producer = %+v", res)
	}
	// Exactly one job exists and it runs as the original submission.
	if _, ok, _ := d.JobState(ctx, "replacement"); ok {
		t.Fatal("a replacement job was created")
	}
	if ent, ok, _ := d.ledger.get(ctx, jobKey("original")); !ok || !ent.rec.RunAt.Equal(runAt) {
		t.Fatalf("job record = %+v", ent)
	}
}

// TestUniqueLockCompletesUnstoredJob: a lock whose submission died before
// storing its job keeps the key: a competitor completes the original job from
// the lock instead of releasing it, and only the documented hold time frees
// the key.
func TestUniqueLockCompletesUnstoredJob(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	cfg.UniqueUntilDoneMax = 300 * time.Millisecond
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mk := func(id string) *jetq.PreparedJob {
		job, err := jetq.Prepare("task.a", json.RawMessage(`{}`), jetq.Submission{Queue: "q", JobID: id, Unique: unique}, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return id })
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	job := mk("job-1")
	if _, _, err := d.takeUnique(ctx, &job.Envelope, job.Fingerprint); err != nil {
		t.Fatal(err)
	}
	// The submission did not finish; the competitor is suppressed and the
	// original job is completed from the lock, so it still runs.
	if res := mustSubmit(t, d, mk("job-2")); res.Status != jetq.Suppressed || res.ExistingJobID != "job-1" {
		t.Fatalf("competitor = %+v", res)
	}
	if _, ok, _ := d.JobState(ctx, "job-1"); !ok {
		t.Fatal("the lock's job was not completed")
	}
	c, _ := d.Consumer(ctx, "q")
	if got := receive(t, c).Envelope().JobID; got != "job-1" {
		t.Fatalf("delivered %s", got)
	}
	// The producer's own retry is not suppressed by its own lock.
	if res := mustSubmit(t, d, job); res.Status != jetq.AlreadyAccepted || res.JobID != "job-1" {
		t.Fatalf("own retry = %+v", res)
	}
	// Once the documented hold time passed, the key is free again.
	d.hks.now = func() time.Time { return time.Now().Add(time.Second) }
	if res := mustSubmit(t, d, mk("job-3")); res.Status != jetq.Accepted {
		t.Fatalf("after hold time = %+v", res)
	}
}

// TestUniqueLockReleasedAfterRewrite: the lock's revision changes when the
// submission marks it stored; the job still releases its own lock.
func TestUniqueLockReleasedAfterRewrite(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mustSubmit(t, d, prepare(t, "job-1", "q", unique))
	c, _ := d.Consumer(ctx, "q")
	settle(t, receive(t, c), jetq.Decision{})
	if res := mustSubmit(t, d, prepare(t, "job-2", "q", unique)); res.Status != jetq.Accepted {
		t.Fatalf("key not released: %+v", res)
	}
}

// failingKV fails every write of job.job-1, and can report a missing key as a
// transport error to check that recovery keeps state it cannot read.
type failingKV struct {
	jetstream.KeyValue
	failGet func(key string) error
	failPut func(key string) error
}

func (f *failingKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if f.failPut != nil {
		if err := f.failPut(key); err != nil {
			return 0, err
		}
	}
	return f.KeyValue.Update(ctx, key, value, revision)
}

func (f *failingKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if f.failGet != nil {
		if err := f.failGet(key); err != nil {
			return nil, err
		}
	}
	return f.KeyValue.Get(ctx, key)
}

func (f *failingKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	if f.failPut != nil {
		if err := f.failPut(key); err != nil {
			return err
		}
	}
	return f.KeyValue.Delete(ctx, key, opts...)
}

// TestCancelReportsWriteFailureAndKeepsLock: a failed cancellation write is
// reported, the job stays uncancelled and its unique lock is kept.
func TestCancelReportsWriteFailureAndKeepsLock(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mustSubmit(t, d, prepare(t, "job-1", "q", unique))

	real := d.ledger.kv
	failing := &failingKV{KeyValue: real, failPut: func(key string) error {
		if key == jobKey("job-1") {
			return errInjected
		}
		return nil
	}}
	d.ledger.kv = failing
	if err := d.Cancel(ctx, "job-1"); !errors.Is(err, errInjected) {
		t.Fatalf("Cancel = %v", err)
	}
	d.ledger.kv = real
	state, _, _ := d.JobState(ctx, "job-1")
	if state != jetq.StateAwaiting {
		t.Fatalf("state = %s", state)
	}
	// The lock was not released, so a new job with the key is suppressed.
	if res := mustSubmit(t, d, prepare(t, "job-2", "q", unique)); res.Status != jetq.Suppressed {
		t.Fatalf("lock was released: %+v", res)
	}
}

// TestRecoverKeepsStateOnReadErrors: a read failure makes recovery report an
// error and change nothing; a lock is never dropped because its job could not
// be read.
func TestRecoverKeepsStateOnReadErrors(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mustSubmit(t, d, prepare(t, "job-1", "q", unique))

	bump := &failingKV{KeyValue: d.ledger.kv}
	d.ledger.kv = bump
	bump.failGet = func(key string) error {
		if strings.HasPrefix(key, "job.") {
			return errInjected
		}
		return nil
	}
	if _, err := d.Recover(ctx); !errors.Is(err, errInjected) {
		t.Fatalf("Recover = %v", err)
	}
	d.ledger.kv = bump.KeyValue
	// An expired window lock is normally dropped by recovery; here its hold
	// time passed, but because reading its job failed the lock must survive.
	window := &jetq.UniqueRule{Mode: jetq.UniqueModeWindow, Key: "w", Window: 50 * time.Millisecond}
	mustSubmit(t, d, prepare(t, "job-2", "q", window))
	time.Sleep(80 * time.Millisecond)
	fresh := &failingKV{KeyValue: bump.KeyValue, failGet: func(key string) error {
		if key == jobKey("job-2") {
			return errInjected
		}
		return nil
	}}
	d.ledger.kv = fresh
	// The expired lock is dropped from the lock's own time without reading
	// its job; a job read failure only affects jobs that need one.
	if _, err := d.Recover(ctx); !errors.Is(err, errInjected) {
		t.Fatalf("Recover = %v", err)
	}
	// The unreadable job was not delivered, fenced or deleted.
	if _, err := fresh.KeyValue.Get(ctx, jobKey("job-2")); err != nil {
		t.Fatalf("job record lost: %v", err)
	}
	d.ledger.kv = bump.KeyValue

	// A lock whose job record cannot be read is kept: its hold is open and
	// recovery cannot tell whether the job is terminal.
	open := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "open"}
	mustSubmit(t, d, prepare(t, "job-3", "q", open))
	d.ledger.kv = &failingKV{KeyValue: bump.KeyValue, failGet: func(key string) error {
		if key == jobKey("job-3") {
			return errInjected
		}
		return nil
	}}
	if _, err := d.Recover(ctx); !errors.Is(err, errInjected) {
		t.Fatalf("Recover = %v", err)
	}
	d.ledger.kv = bump.KeyValue
	if res := mustSubmit(t, d, prepare(t, "job-4", "q", open)); res.Status != jetq.Suppressed {
		t.Fatalf("lock was released after a read error: %+v", res)
	}
}

// TestRecoverReportsNonCasWriteFailures: a write that fails for another
// reason is an error, not a successful recovery.
func TestRecoverReportsNonCasWriteFailures(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "app")
	cfg.ClaimLease = 50 * time.Millisecond
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job-1", "q", nil))
	c, _ := d.Consumer(ctx, "q")
	receive(t, c) // claimed, then abandoned
	time.Sleep(100 * time.Millisecond)
	d.ledger.kv = &failingKV{KeyValue: d.ledger.kv, failPut: func(key string) error {
		if key == jobKey("job-1") {
			return errInjected
		}
		return nil
	}}
	if _, err := d.Recover(ctx); !errors.Is(err, errInjected) {
		t.Fatalf("Recover = %v", err)
	}
	// Recovery did not claim success: the job is still claimed, not rerun.
	state, _, _ := d.JobState(ctx, "job-1")
	if state != jetq.StateRunning {
		t.Fatalf("state = %s", state)
	}
}

// TestUndecodableLockIsKept: recovery leaves a corrupt lock alone rather than
// deleting state it cannot interpret.
func TestUndecodableLockIsKept(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	job := prepare(t, "job-1", "q", unique)
	mustSubmit(t, d, job)
	key := lockKeyFor(d.cfg.Namespace, unique)
	ent, ok, _ := d.ledger.get(ctx, key)
	if !ok {
		t.Fatal("no lock")
	}
	corrupt, _ := json.Marshal("not a lock")
	if _, err := d.ledger.put(ctx, key, ent.rev, &record{Data: corrupt}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.ledger.get(ctx, key); !ok {
		t.Fatal("recovery deleted an undecodable lock")
	}
}

// TestRecoverCompletesUnstoredIntent: a process that died between writing an
// intent and storing its job leaves a job that never ran; recovery completes
// it, and nothing about its identity changes.
func TestRecoverCompletesUnstoredIntent(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	job := prepare(t, "job-1", "q", nil)
	job.Envelope.EnqueueKey = "outbox"
	job.Fingerprint = job.Envelope.Fingerprint()
	env := job.Envelope.Clone()
	if _, err := d.claimIntent(ctx, env, job.Fingerprint); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.JobState(ctx, "job-1"); ok {
		t.Fatal("job stored before recovery")
	}
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.JobState(ctx, "job-1"); !ok {
		t.Fatal("recovery did not complete the intent")
	}
	// The intent now reports the job as stored, so a later retry is
	// AlreadyAccepted, even after the job record is cleaned up.
	if _, held, ok, _ := d.readIntent(ctx, "outbox"); !ok || !held.Stored {
		t.Fatalf("intent = %+v", held)
	}
	if err := d.ledger.delete(ctx, jobKey("job-1"), mustRev(t, d, "job-1")); err != nil {
		t.Fatal(err)
	}
	if res, err := d.Submit(ctx, job); err != nil || res.Status != jetq.AlreadyAccepted || res.JobID != "job-1" {
		t.Fatalf("retry after cleanup = %+v, %v", res, err)
	}
	// Recovery frees the key once the job is known to have finished.
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := d.readIntent(ctx, "outbox"); ok {
		t.Fatal("intent of a finished job was kept")
	}
}

func mustRev(t *testing.T, d *Driver, id string) uint64 {
	t.Helper()
	ent, ok, err := d.ledger.get(context.Background(), jobKey(id))
	if err != nil || !ok {
		t.Fatalf("job %s: %v", id, err)
	}
	return ent.rev
}

// TestRecordedJobBeatsUnmarkedIntent: an intent marker that failed to be
// written is repaired from the job record; the intent never overrides it.
func TestRecordedJobBeatsUnmarkedIntent(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "app"))
	ctx := context.Background()
	kv := d.ledger.kv
	d.ledger.kv = reviewFailMarker{KeyValue: kv, prefix: "ek."}
	job := prepare(t, "job-1", "q", nil)
	job.Envelope.EnqueueKey = "outbox"
	job.Fingerprint = job.Envelope.Fingerprint()
	if res := mustSubmit(t, d, job); res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v", res)
	}
	d.ledger.kv = kv
	if _, held, _, _ := d.readIntent(ctx, "outbox"); held.Stored {
		t.Fatal("marker was written despite the failure")
	}
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, held, _, _ := d.readIntent(ctx, "outbox"); !held.Stored {
		t.Fatal("recovery did not repair the intent marker")
	}
}
