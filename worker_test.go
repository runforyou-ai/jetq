package jobq_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/memq"
)

type email struct {
	To string `json:"to"`
}

func (email) JobName() string { return "email" }

func newClient(t *testing.T) *jobq.Client {
	t.Helper()
	d := memq.New()
	t.Cleanup(func() { _ = d.Close() })
	return jobq.New(d)
}

// start runs w until the test ends.
func start(t *testing.T, w *jobq.Worker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

func wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out")
		panic("unreachable")
	}
}

func deadLetters(t *testing.T, c *jobq.Client) []jobq.DeadLetter {
	t.Helper()
	page, err := c.DeadLetters(context.Background(), jobq.DeadLetterQuery{})
	if err != nil {
		t.Fatal(err)
	}
	return page.DeadLetters
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestInfoAndHeaders(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "mail", MaxAttempts: 4, Timeout: time.Minute})
	got := make(chan jobq.Info, 1)
	jobq.Handle(w, func(ctx context.Context, e email) error {
		info, _ := jobq.JobInfo(ctx)
		got <- info
		return nil
	})
	start(t, w)
	id, err := c.Enqueue(context.Background(), email{To: "a"}, jobq.OnQueue("mail"), jobq.WithHeader("traceparent", "x"), jobq.Timeout(0))
	if err != nil {
		t.Fatal(err)
	}
	info := wait(t, got)
	if info.ID != id || info.Name != "email" || info.Queue != "mail" || info.Attempt != 1 || info.MaxAttempts != 4 ||
		info.Timeout != 0 || info.Header["traceparent"] != "x" || info.EnqueuedAt.IsZero() {
		t.Fatalf("info = %+v", info)
	}
}

func TestEnqueueValidation(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := context.Background()
	for name, opts := range map[string][]jobq.EnqueueOption{
		"queue":    {jobq.OnQueue("a.b")},
		"id":       {jobq.JobID("x y")},
		"header":   {jobq.WithHeader("Jobq-Id", "x")},
		"window":   {jobq.UniqueFor("k", 0)},
		"attempts": {jobq.MaxAttempts(-1)},
	} {
		if _, err := c.Enqueue(ctx, email{}, opts...); err == nil {
			t.Errorf("%s: enqueue succeeded", name)
		}
	}
	if _, err := c.EnqueueNamed(ctx, "raw", []byte("{bad")); err == nil {
		t.Error("invalid raw JSON accepted")
	}
	if _, err := c.Enqueue(ctx, email{}, jobq.Unique("k")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, email{}, jobq.Unique("k")); !errors.Is(err, jobq.ErrDuplicate) {
		t.Fatalf("duplicate = %v", err)
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default", MaxAttempts: 3, Backoff: jobq.Constant(0)})
	var attempts atomic.Int32
	typed := make(chan error, 1)
	global := make(chan jobq.Info, 1)
	jobq.Handle(w, func(ctx context.Context, e email) error {
		attempts.Add(1)
		return errors.New("smtp down")
	}, jobq.OnFailure(func(_ context.Context, e email, err error) { typed <- err }))
	w.OnFailed(func(_ context.Context, info jobq.Info, _ []byte, _ error) { global <- info })
	start(t, w)
	if _, err := c.Enqueue(context.Background(), email{To: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, typed); err.Error() != "smtp down" {
		t.Fatalf("failure error = %v", err)
	}
	if info := wait(t, global); info.Attempt != 3 {
		t.Fatalf("failed at attempt %d", info.Attempt)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("attempts = %d", n)
	}
	if dl := deadLetters(t, c); len(dl) != 1 || dl[0].Attempts != 3 || dl[0].Error != "smtp down" {
		t.Fatalf("dead letters = %+v", dl)
	}
}

func TestPermanentAndDecodeErrors(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default", MaxAttempts: 5})
	var attempts atomic.Int32
	jobq.Handle(w, func(ctx context.Context, e email) error {
		attempts.Add(1)
		return jobq.Permanent(errors.New("bad address"))
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), email{To: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnqueueNamed(context.Background(), "email", []byte(`{"to": 5}`)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return len(deadLetters(t, c)) == 2 })
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d", attempts.Load())
	}
	for _, dl := range deadLetters(t, c) {
		if dl.Attempts != 1 {
			t.Fatalf("dead letter after %d attempts", dl.Attempts)
		}
	}
}

func TestSnoozeKeepsAttempt(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default", MaxAttempts: 1})
	infos := make(chan jobq.Info, 3)
	jobq.Handle(w, func(ctx context.Context, e email) error {
		info, _ := jobq.JobInfo(ctx)
		infos <- info
		if info.Snoozes < 2 {
			return jobq.Snooze(10 * time.Millisecond)
		}
		return nil
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), email{}); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		info := wait(t, infos)
		if info.Attempt != 1 || info.Snoozes != i {
			t.Fatalf("run %d: attempt %d snoozes %d", i, info.Attempt, info.Snoozes)
		}
	}
}

func TestRetryAfterOverridesBackoff(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default", Backoff: jobq.Constant(time.Hour)})
	runs := make(chan time.Time, 2)
	jobq.Handle(w, func(ctx context.Context, e email) error {
		runs <- time.Now()
		if info, _ := jobq.JobInfo(ctx); info.Attempt == 1 {
			return jobq.RetryAfter(50*time.Millisecond, errors.New("rate limited"))
		}
		return nil
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), email{}); err != nil {
		t.Fatal(err)
	}
	first := wait(t, runs)
	if gap := wait(t, runs).Sub(first); gap < 50*time.Millisecond {
		t.Fatalf("retried after %s", gap)
	}
}

func TestTimeoutFailsAttempt(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default", MaxAttempts: 1, Timeout: 50 * time.Millisecond})
	cause := make(chan error, 1)
	jobq.Handle(w, func(ctx context.Context, e email) error {
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return jobq.Snooze(time.Hour) // ignored after a timeout
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), email{}); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, cause); !errors.Is(err, jobq.ErrTimeout) {
		t.Fatalf("cause = %v", err)
	}
	eventually(t, func() bool { return len(deadLetters(t, c)) == 1 })
	if e := deadLetters(t, c)[0].Error; !strings.HasPrefix(e, jobq.ErrTimeout.Error()) {
		t.Fatalf("error = %q", e)
	}
}

func TestPanicBecomesError(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default", MaxAttempts: 1})
	jobq.Handle(w, func(ctx context.Context, e email) error { panic("kaboom") })
	start(t, w)
	if _, err := c.Enqueue(context.Background(), email{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return len(deadLetters(t, c)) == 1 })
	if e := deadLetters(t, c)[0].Error; !strings.Contains(e, "kaboom") {
		t.Fatalf("error = %q", e)
	}
}

func TestMiddlewareOrder(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default"})
	var trace []string
	done := make(chan struct{})
	for _, name := range []string{"outer", "inner"} {
		w.Use(func(ctx context.Context, next func(context.Context) error) error {
			trace = append(trace, name)
			return next(ctx)
		})
	}
	jobq.Handle(w, func(ctx context.Context, e email) error {
		trace = append(trace, "handler")
		close(done)
		return nil
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), email{}); err != nil {
		t.Fatal(err)
	}
	wait(t, done)
	if strings.Join(trace, ",") != "outer,inner,handler" {
		t.Fatalf("trace = %v", trace)
	}
}

func TestShutdownPutsJobBackWithoutAttempt(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jobq.Queue{Name: "default", MaxAttempts: 1})
	w.SetShutdownTimeout(50 * time.Millisecond)
	started := make(chan struct{})
	jobq.Handle(w, func(ctx context.Context, e email) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	if _, err := c.Enqueue(ctx, email{}); err != nil {
		t.Fatal(err)
	}
	wait(t, started)
	stop()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}

	// Another worker gets the job as attempt 1 again.
	w2 := c.NewWorker(jobq.Queue{Name: "default", MaxAttempts: 1})
	attempt := make(chan int, 1)
	jobq.Handle(w2, func(ctx context.Context, e email) error {
		info, _ := jobq.JobInfo(ctx)
		attempt <- info.Attempt
		return nil
	})
	start(t, w2)
	if n := wait(t, attempt); n != 1 {
		t.Fatalf("attempt after shutdown = %d", n)
	}
}

func TestUnknownJobIsPutBackThenDeadLettered(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default"})
	w.SetUnknownJobTimeout(100 * time.Millisecond)
	failed := make(chan jobq.Info, 1)
	w.OnFailed(func(_ context.Context, info jobq.Info, _ []byte, _ error) { failed <- info })
	start(t, w)
	if _, err := c.EnqueueNamed(context.Background(), "mystery", nil); err != nil {
		t.Fatal(err)
	}
	info := wait(t, failed)
	if info.Name != "mystery" || info.Attempt != 1 {
		t.Fatalf("failed info = %+v", info)
	}
}

func TestRawJobs(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default"})
	got := make(chan string, 1)
	w.HandleRaw("bridge", func(ctx context.Context, payload json.RawMessage) error {
		got <- string(payload)
		return nil
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), jobq.RawJob{Name: "bridge", Payload: []byte(`{"z":1,  "a":2}`)}); err != nil {
		t.Fatal(err)
	}
	if p := wait(t, got); p != `{"z":1,  "a":2}` {
		t.Fatalf("payload = %s", p)
	}
}

// lostDriver reports every lease as lost on its first extension.
type lostDriver struct{ *memq.Driver }

func (d lostDriver) Claim(ctx context.Context, queue string, limit int, lease time.Duration) ([]jobq.Lease, error) {
	leases, err := d.Driver.Claim(ctx, queue, limit, lease)
	for i, l := range leases {
		leases[i] = lostLease{l}
	}
	return leases, err
}

type lostLease struct{ jobq.Lease }

func (lostLease) Extend(context.Context) error { return jobq.ErrLeaseLost }

func TestLostLeaseCancelsHandler(t *testing.T) {
	t.Parallel()
	d := memq.New()
	t.Cleanup(func() { _ = d.Close() })
	c := jobq.New(lostDriver{d})
	w := c.NewWorker(jobq.Queue{Name: "default", Lease: 150 * time.Millisecond})
	cause := make(chan error, 1)
	jobq.Handle(w, func(ctx context.Context, e email) error {
		<-ctx.Done()
		select {
		case cause <- context.Cause(ctx):
		default:
		}
		return ctx.Err()
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), email{}); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, cause); !errors.Is(err, jobq.ErrLeaseLost) {
		t.Fatalf("cause = %v", err)
	}
}

func TestCancelDelayedJob(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := context.Background()
	id, err := c.Enqueue(ctx, email{}, jobq.Delay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(ctx, id); !errors.Is(err, jobq.ErrNotFound) {
		t.Fatalf("second cancel = %v", err)
	}
}

func TestSchedules(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "cron"})
	ids := make(chan string, 4)
	jobq.Handle(w, func(ctx context.Context, e email) error {
		info, _ := jobq.JobInfo(ctx)
		ids <- info.ID
		return nil
	})
	start(t, w)
	err := c.SyncSchedules(context.Background(), jobq.Schedule{Key: "tick", Cron: "@every 1s", Job: email{To: "ops"}, Options: []jobq.EnqueueOption{jobq.OnQueue("cron")}})
	if err != nil {
		t.Fatal(err)
	}
	if id := wait(t, ids); !strings.HasPrefix(id, "tick-") {
		t.Fatalf("id = %s", id)
	}
	if err := c.SyncSchedules(context.Background(), jobq.Schedule{Key: "bad", Cron: "@every 1s", Job: email{}, Options: []jobq.EnqueueOption{jobq.Delay(time.Second)}}); err == nil {
		t.Fatal("schedule with Delay accepted")
	}
}

type bare struct{ jobq.Driver }

func TestOptionalCapabilities(t *testing.T) {
	t.Parallel()
	d := memq.New()
	t.Cleanup(func() { _ = d.Close() })
	c := jobq.New(bare{d})
	if _, err := c.Stats(context.Background()); !errors.Is(err, jobq.ErrUnsupported) {
		t.Fatalf("Stats = %v", err)
	}
	if err := c.SyncSchedules(context.Background()); !errors.Is(err, jobq.ErrUnsupported) {
		t.Fatalf("SyncSchedules = %v", err)
	}
}

func TestNewIDSortsByTime(t *testing.T) {
	t.Parallel()
	a := jobq.NewID()
	time.Sleep(2 * time.Millisecond)
	b := jobq.NewID()
	if len(a) != 26 || !jobq.ValidName(a) || a >= b {
		t.Fatalf("ids %s, %s", a, b)
	}
}
