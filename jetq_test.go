package jetq_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
	"github.com/runforyou-ai/jetq/jetqtest"
)

type sendEmail struct {
	To string `json:"to"`
}

func (sendEmail) JobName() string { return "send-email" }

type report struct {
	Day string `json:"day"`
}

func (report) JobName() string { return "report" }

func newClient(t *testing.T) *jetq.Client {
	t.Helper()
	srv := jetqtest.Start(t)
	c, err := jetq.New(context.Background(), srv.JetStream, jetq.WithMemoryStorage())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// start runs w until the test ends.
func start(t *testing.T, w *jetq.Worker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	})
}

func wait[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timed out after %s", timeout)
		var zero T
		return zero
	}
}

func expectNothing[T any](t *testing.T, ch <-chan T, d time.Duration) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected value %v", v)
	case <-time.After(d):
	}
}

func TestEnqueueAndProcess(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "mail"})
	got := make(chan jetq.Info, 1)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		info, _ := jetq.JobInfo(ctx)
		if job.To != "a@example.com" {
			t.Errorf("job.To = %q", job.To)
		}
		got <- info
		return nil
	})
	start(t, w)

	id, err := c.Enqueue(context.Background(), sendEmail{To: "a@example.com"}, jetq.OnQueue("mail"), jetq.WithHeader("traceparent", "00-abc-def-01"))
	if err != nil {
		t.Fatal(err)
	}
	info := wait(t, got, 5*time.Second)
	if info.ID != id || info.Name != "send-email" || info.Queue != "mail" || info.Attempt != 1 || info.MaxAttempts != 3 {
		t.Fatalf("info = %+v, id %s", info, id)
	}
	if info.EnqueuedAt.IsZero() || info.Header.Get("traceparent") != "00-abc-def-01" {
		t.Fatalf("info = %+v", info)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", Backoff: jetq.Constant(50 * time.Millisecond)})
	attempts := make(chan int, 5)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		info, _ := jetq.JobInfo(ctx)
		attempts <- info.Attempt
		if info.Attempt < 3 {
			return errors.New("smtp down")
		}
		return nil
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), sendEmail{}); err != nil {
		t.Fatal(err)
	}
	for want := 1; want <= 3; want++ {
		if got := wait(t, attempts, 5*time.Second); got != want {
			t.Fatalf("attempt = %d, want %d", got, want)
		}
	}
	expectNothing(t, attempts, 300*time.Millisecond)
}

func TestRetryAfterOverridesBackoff(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", Backoff: jetq.Constant(time.Hour)})
	attempts := make(chan time.Time, 2)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		attempts <- time.Now()
		if info, _ := jetq.JobInfo(ctx); info.Attempt == 1 {
			return jetq.RetryAfter(200*time.Millisecond, errors.New("rate limited"))
		}
		return nil
	})
	start(t, w)
	if _, err := c.Enqueue(context.Background(), sendEmail{}); err != nil {
		t.Fatal(err)
	}
	first := wait(t, attempts, 5*time.Second)
	second := wait(t, attempts, 5*time.Second)
	if gap := second.Sub(first); gap < 150*time.Millisecond {
		t.Fatalf("retried after %s", gap)
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 2, Backoff: jetq.Constant(10 * time.Millisecond)})
	typed := make(chan string, 1)
	generic := make(chan jetq.Info, 1)
	var calls atomic.Int32
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		calls.Add(1)
		return errors.New("boom")
	}, jetq.OnFailure(func(ctx context.Context, job sendEmail, err error) {
		typed <- job.To + ": " + err.Error()
	}))
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { generic <- info })
	start(t, w)

	id, err := c.Enqueue(context.Background(), sendEmail{To: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if got := wait(t, typed, 5*time.Second); got != "x: boom" {
		t.Fatalf("typed failure = %q", got)
	}
	if info := wait(t, generic, 5*time.Second); info.ID != id || info.Attempt != 2 {
		t.Fatalf("failed info = %+v", info)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d", calls.Load())
	}
	assertDeadLetter(t, c, "default", id, "boom", "2")
}

func assertDeadLetter(t *testing.T, c *jetq.Client, queue, id, errText, attempts string) {
	t.Helper()
	srvJS := c.JetStream()
	stream, err := srvJS.Stream(context.Background(), c.DeadLetterStreamName())
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.GetLastMsgForSubject(context.Background(), "jetq.dead."+queue)
	if err != nil {
		t.Fatal(err)
	}
	h := msg.Header
	if (id != "" && h.Get(jetq.HeaderID) != id) || h.Get(jetq.HeaderError) != errText || h.Get(jetq.HeaderAttempts) != attempts || h.Get(jetq.HeaderQueue) != queue {
		t.Fatalf("dead letter headers = %v", h)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "mail", MaxAttempts: 5})
	failed := make(chan jetq.Info, 1)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		return jetq.Permanent(errors.New("invalid address"))
	})
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- info })
	start(t, w)
	id, _ := c.Enqueue(context.Background(), sendEmail{}, jetq.OnQueue("mail"))
	if info := wait(t, failed, 5*time.Second); info.Attempt != 1 {
		t.Fatalf("attempt = %d", info.Attempt)
	}
	assertDeadLetter(t, c, "mail", id, "invalid address", "1")
}

func TestPanicIsRetried(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", Backoff: jetq.Constant(10 * time.Millisecond)})
	done := make(chan int, 1)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		info, _ := jetq.JobInfo(ctx)
		if info.Attempt == 1 {
			panic("nil map")
		}
		done <- info.Attempt
		return nil
	})
	start(t, w)
	_, _ = c.Enqueue(context.Background(), sendEmail{})
	if got := wait(t, done, 5*time.Second); got != 2 {
		t.Fatalf("attempt = %d", got)
	}
}

func TestDelayAndCancel(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default"})
	got := make(chan string, 2)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		got <- job.To
		return nil
	})
	start(t, w)

	ctx := context.Background()
	enqueued := time.Now()
	if _, err := c.Enqueue(ctx, sendEmail{To: "later"}, jetq.Delay(1500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	cancelled, err := c.Enqueue(ctx, sendEmail{To: "cancelled"}, jetq.At(time.Now().Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(ctx, cancelled); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := c.Cancel(ctx, cancelled); !errors.Is(err, jetq.ErrNotFound) {
		t.Fatalf("second Cancel = %v", err)
	}

	if to := wait(t, got, 10*time.Second); to != "later" {
		t.Fatalf("ran %q", to)
	}
	if elapsed := time.Since(enqueued); elapsed < 1400*time.Millisecond {
		t.Fatalf("delayed job ran after %s", elapsed)
	}
	expectNothing(t, got, 1500*time.Millisecond)
}

func TestUnique(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, sendEmail{}, jetq.Unique("welcome-42")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, sendEmail{}, jetq.Unique("welcome-42")); !errors.Is(err, jetq.ErrDuplicate) {
		t.Fatalf("second Enqueue = %v", err)
	}
	if _, err := c.Enqueue(ctx, sendEmail{}, jetq.Unique("welcome-43")); err != nil {
		t.Fatal(err)
	}
}

func TestReservedHeaderRejected(t *testing.T) {
	c := newClient(t)
	if _, err := c.Enqueue(context.Background(), sendEmail{}, jetq.WithHeader("Nats-Schedule", "@every 1s")); err == nil {
		t.Fatal("expected error")
	}
	if _, err := c.Enqueue(context.Background(), sendEmail{}, jetq.OnQueue("bad.queue")); err == nil {
		t.Fatal("expected error")
	}
}

func TestSchedules(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jetq.Queue{Name: "reports"})
	got := make(chan jetq.Info, 10)
	jetq.Handle(w, func(ctx context.Context, job report) error {
		info, _ := jetq.JobInfo(ctx)
		if job.Day != "today" {
			t.Errorf("job = %+v", job)
		}
		got <- info
		return nil
	})
	start(t, w)

	schedule := jetq.Cron("daily", "@every 1s", report{Day: "today"}, jetq.OnQueue("reports"))
	if err := c.SyncSchedules(ctx, schedule); err != nil {
		t.Fatal(err)
	}
	stream, err := c.JetStream().Stream(ctx, c.StreamName())
	if err != nil {
		t.Fatal(err)
	}
	before, err := stream.GetLastMsgForSubject(ctx, "jetq.cron.daily")
	if err != nil {
		t.Fatal(err)
	}
	// Re-syncing an unchanged schedule must not replace it.
	if err := c.SyncSchedules(ctx, schedule); err != nil {
		t.Fatal(err)
	}
	after, err := stream.GetLastMsgForSubject(ctx, "jetq.cron.daily")
	if err != nil {
		t.Fatal(err)
	}
	if before.Sequence != after.Sequence {
		t.Fatalf("unchanged schedule was replaced: %d -> %d", before.Sequence, after.Sequence)
	}

	first := wait(t, got, 5*time.Second)
	second := wait(t, got, 5*time.Second)
	if first.ID == second.ID || first.Queue != "reports" || first.Attempt != 1 {
		t.Fatalf("runs = %+v, %+v", first, second)
	}

	// Removing the schedule from the set deletes it.
	if err := c.SyncSchedules(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetLastMsgForSubject(ctx, "jetq.cron.daily"); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("schedule still present: %v", err)
	}
	time.Sleep(1200 * time.Millisecond)
	for len(got) > 0 {
		<-got
	}
	expectNothing(t, got, 1500*time.Millisecond)
}

func TestScheduleValidation(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	cases := []jetq.Schedule{
		jetq.Cron("bad key", "@hourly", report{}),
		jetq.Cron("k", "* * *", report{}),
		jetq.Cron("k", "@at 2030-01-01T00:00:00Z", report{}),
		jetq.Cron("k", "@hourly", report{}, jetq.Delay(time.Second)),
		jetq.Cron("k", "0 2 * * *", report{}).In("Mars/Olympus"),
		jetq.Cron("k", "@every 1m", report{}).In("Asia/Shanghai"),
		jetq.Cron("k", "@every 500ms", report{}),
		jetq.Cron("k", "@fortnightly", report{}),
	}
	for _, s := range cases {
		if err := c.SyncSchedules(ctx, s); err == nil {
			t.Errorf("expected error for %+v", s)
		}
	}
	if err := c.SyncSchedules(ctx, jetq.Cron("a", "@hourly", report{}), jetq.Cron("a", "@daily", report{})); err == nil {
		t.Error("expected duplicate key error")
	}
	if err := c.SyncSchedules(ctx, jetq.Cron("nightly", "0 2 * * *", report{}).In("Asia/Shanghai")); err != nil {
		t.Errorf("five-field cron: %v", err)
	}
}

func TestSnoozeDoesNotCountAttempt(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	runs := make(chan jetq.Info, 3)
	var snoozed atomic.Bool
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		info, _ := jetq.JobInfo(ctx)
		runs <- info
		if snoozed.CompareAndSwap(false, true) {
			return jetq.Snooze(500 * time.Millisecond)
		}
		return nil
	})
	start(t, w)
	id, _ := c.Enqueue(context.Background(), sendEmail{})
	first := wait(t, runs, 5*time.Second)
	second := wait(t, runs, 5*time.Second)
	if first.Attempt != 1 || second.Attempt != 1 || second.ID != id {
		t.Fatalf("runs = %+v, %+v", first, second)
	}
}

func TestKeepAliveLongJob(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", AckWait: time.Second})
	var calls atomic.Int32
	done := make(chan struct{}, 1)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		calls.Add(1)
		time.Sleep(2500 * time.Millisecond)
		done <- struct{}{}
		return nil
	})
	start(t, w)
	_, _ = c.Enqueue(context.Background(), sendEmail{})
	wait(t, done, 10*time.Second)
	time.Sleep(500 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, job was redelivered while running", calls.Load())
	}
}

func TestConcurrencyAndMiddleware(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", Concurrency: 3})
	var mu sync.Mutex
	chains := map[string][]string{}
	record := func(name string) jetq.Middleware {
		return func(ctx context.Context, next func(context.Context) error) error {
			info, _ := jetq.JobInfo(ctx)
			mu.Lock()
			chains[info.ID] = append(chains[info.ID], name)
			mu.Unlock()
			return next(ctx)
		}
	}
	w.Use(record("outer"), record("inner"))
	var active, peak atomic.Int32
	done := make(chan struct{}, 6)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
		active.Add(-1)
		done <- struct{}{}
		return nil
	})
	start(t, w)
	for range 6 {
		if _, err := c.Enqueue(context.Background(), sendEmail{}); err != nil {
			t.Fatal(err)
		}
	}
	for range 6 {
		wait(t, done, 10*time.Second)
	}
	if p := peak.Load(); p != 3 {
		t.Fatalf("peak concurrency = %d", p)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(chains) != 6 {
		t.Fatalf("chains = %v", chains)
	}
	for id, chain := range chains {
		if len(chain) != 2 || chain[0] != "outer" || chain[1] != "inner" {
			t.Fatalf("middleware order for %s = %v", id, chain)
		}
	}
}

func TestShutdownWaitsForRunningJob(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default"})
	started := make(chan struct{})
	var finished atomic.Bool
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		close(started)
		time.Sleep(500 * time.Millisecond)
		finished.Store(true)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	_, _ = c.Enqueue(context.Background(), sendEmail{})
	wait(t, started, 5*time.Second)
	cancel()
	if err := wait(t, done, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("Run returned before the running job finished")
	}
}

func TestUnknownJobIsRetried(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	failed := make(chan error, 1)
	jetq.Handle(w, func(ctx context.Context, job report) error { return nil })
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- err })
	start(t, w)
	_, _ = c.Enqueue(context.Background(), sendEmail{})
	if err := wait(t, failed, 5*time.Second); err == nil {
		t.Fatal("expected error")
	}
}

func TestSlowFailureCallbackIsNotRedelivered(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1, AckWait: 500 * time.Millisecond, Concurrency: 2})
	var handled, failed atomic.Int32
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		handled.Add(1)
		return errors.New("boom")
	})
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) {
		failed.Add(1)
		time.Sleep(2 * time.Second)
	})
	start(t, w)
	_, _ = c.Enqueue(context.Background(), sendEmail{})
	time.Sleep(3500 * time.Millisecond)
	if handled.Load() != 1 || failed.Load() != 1 {
		t.Fatalf("handled = %d, failed = %d", handled.Load(), failed.Load())
	}
	stream, err := c.JetStream().Stream(context.Background(), c.DeadLetterStreamName())
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("dead letters = %d", info.State.Msgs)
	}
}

func TestFailureCallbackPanicIsRecovered(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	ok := make(chan string, 1)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		if job.To == "bad" {
			return errors.New("boom")
		}
		ok <- job.To
		return nil
	}, jetq.OnFailure(func(ctx context.Context, job sendEmail, err error) { panic("callback bug") }))
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { panic("callback bug") })
	start(t, w)
	_, _ = c.Enqueue(context.Background(), sendEmail{To: "bad"})
	_, _ = c.Enqueue(context.Background(), sendEmail{To: "good"})
	if got := wait(t, ok, 5*time.Second); got != "good" {
		t.Fatalf("got %q", got)
	}
	assertDeadLetter(t, c, "default", "", "boom", "1")
}

func TestCrashRedeliveryRespectsMaxAttempts(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, sendEmail{}); err != nil {
		t.Fatal(err)
	}
	// Simulate a worker that took the job on its last attempt and crashed.
	consumer, err := c.JetStream().CreateOrUpdateConsumer(ctx, c.StreamName(), jetstream.ConsumerConfig{
		Durable: "jetq-default", FilterSubject: "jetq.q.default", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: time.Second, MaxDeliver: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(1)
	if err != nil {
		t.Fatal(err)
	}
	for range batch.Messages() {
	}

	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1, AckWait: time.Second})
	var handled atomic.Int32
	failed := make(chan jetq.Info, 1)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		handled.Add(1)
		return nil
	})
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- info })
	start(t, w)
	if info := wait(t, failed, 10*time.Second); info.Attempt != 2 {
		t.Fatalf("attempt = %d", info.Attempt)
	}
	if handled.Load() != 0 {
		t.Fatal("handler ran beyond the attempt limit")
	}
}

func TestDeadLetterDuringShutdown(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default"})
	started := make(chan struct{})
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		return jetq.Permanent(errors.New("invalid"))
	})
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	id, _ := c.Enqueue(context.Background(), sendEmail{})
	wait(t, started, 5*time.Second)
	cancel()
	if err := wait(t, done, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	assertDeadLetter(t, c, "default", id, "invalid", "1")
}

func TestBackoff(t *testing.T) {
	b := jetq.Exponential(time.Second, time.Duration(math.MaxInt64))
	if d := b(200); d <= 0 {
		t.Fatalf("overflowed: %s", d)
	}
	b = jetq.Exponential(0, 0)
	if d := b(3); d != time.Second {
		t.Fatalf("d = %s", d)
	}
	b = jetq.Exponential(time.Second, 10*time.Second)
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 5: 10 * time.Second, 50: 10 * time.Second} {
		if d := b(attempt); d != want {
			t.Errorf("attempt %d: %s, want %s", attempt, d, want)
		}
	}
}

func TestSettleTimeoutStartsAfterHandler(t *testing.T) {
	defer jetq.SetSettleTimeout(time.Second)()
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		time.Sleep(1500 * time.Millisecond)
		return jetq.Permanent(errors.New("slow failure"))
	})
	failed := make(chan jetq.Info, 1)
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- info })
	start(t, w)
	id, _ := c.Enqueue(context.Background(), sendEmail{})
	wait(t, failed, 10*time.Second)
	assertDeadLetter(t, c, "default", id, "slow failure", "1")
}
