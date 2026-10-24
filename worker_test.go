package jetq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTypedAndRawUseTheSamePath: typed and raw submissions produce the same
// envelope shape and policy resolution through one Prepare/Submit path.
func TestTypedAndRawUseTheSamePath(t *testing.T) {
	d := newFakeDriver()
	c, err := NewClient(d, WithDefaultQueue("producer-q"), WithDefaultPolicy(Policy{MaxAttempts: 7}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	type invoice struct{ Total int }
	task := MustTask[invoice]("billing.invoice", Policy{MaxAttempts: 5})

	if _, err := SubmitTask(ctx, c, task, invoice{Total: 3}, Submission{}); err != nil {
		t.Fatal(err)
	}
	raw := Raw("billing.invoice")
	if _, err := c.Submit(ctx, raw.Name(), json.RawMessage(`{"Total":4}`), Submission{Queue: "other"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(ctx, "dynamic.task", map[string]int{"x": 1}, Submission{Queue: "q3"}); err != nil {
		t.Fatal(err)
	}
	prepared, err := c.Prepare("plain.task", struct{ A int }{1}, Submission{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubmitPrepared(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubmitPrepared(ctx, prepared); err != nil { // repeatable
		t.Fatal(err)
	}
	if len(d.submits) != 5 {
		t.Fatalf("submits = %d", len(d.submits))
	}
	first, second, third, fourth := d.submits[0].Envelope, d.submits[1].Envelope, d.submits[2].Envelope, d.submits[3].Envelope
	// Task defaults (5) beat producer defaults (7).
	if first.Policy.MaxAttempts != 5 || first.Queue != "producer-q" || first.Task != "billing.invoice" {
		t.Fatalf("typed = %+v", first)
	}
	// The same task goes to another queue; the queue is not part of the task.
	if second.Queue != "other" || second.Policy.MaxAttempts != 7 || string(second.Payload) != `{"Total":4}` {
		t.Fatalf("raw = %+v", second)
	}
	if third.Task != "dynamic.task" || string(third.Payload) != `{"x":1}` || third.Queue != "q3" {
		t.Fatalf("dynamic = %+v", third)
	}
	// A repeated prepared submission keeps its identity and content.
	if d.submits[4].Envelope.JobID != fourth.JobID || d.submits[4].Fingerprint != d.submits[3].Fingerprint {
		t.Fatal("prepared job changed on repeat")
	}
}

// TestPolicyPriority: submission beats task beats producer.
func TestPolicyPriority(t *testing.T) {
	c, err := NewClient(newFakeDriver(), WithDefaultPolicy(Policy{MaxAttempts: 2, Timeout: TimeoutOf(time.Minute)}))
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return time.Unix(1700000000, 0) }
	job, err := c.Prepare("t", 1, Submission{})
	if err != nil || job.Envelope.Policy.MaxAttempts != 2 || job.Envelope.Policy.Timeout != time.Minute {
		t.Fatalf("producer only: %+v, %v", job, err)
	}
	job, err = c.Prepare("t", 1, Submission{Policy: Policy{MaxAttempts: 9, Timeout: NoTimeout()}})
	if err != nil || job.Envelope.Policy.MaxAttempts != 9 || job.Envelope.Policy.Timeout != 0 {
		t.Fatalf("submission: %+v, %v", job, err)
	}
	// A relative delay becomes an absolute time once, at preparation.
	job, _ = c.Prepare("t", 1, Submission{Delay: time.Minute})
	if !job.Envelope.RunAt.Equal(time.Unix(1700000060, 0)) {
		t.Fatalf("run at = %s", job.Envelope.RunAt)
	}
	if _, err := c.Prepare("t", 1, Submission{Delay: time.Minute, RunAt: time.Now()}); err == nil {
		t.Fatal("both Delay and RunAt accepted")
	}
}

// TestHandlersByTaskNameOnly: consumers find handlers by task name without a
// shared Go declaration; concurrent handlers and middleware run.
func TestHandlersByTaskNameOnly(t *testing.T) {
	d := newFakeDriver()
	client, err := NewClient(d, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(Queue{Name: "q", Concurrency: 3})
	var mu sync.Mutex
	var order []string
	var inFlight, maxInFlight atomic.Int32
	w.Use(func(ctx context.Context, next func(context.Context) error) error {
		mu.Lock()
		order = append(order, "outer-in")
		mu.Unlock()
		err := next(ctx)
		mu.Lock()
		order = append(order, "outer-out")
		mu.Unlock()
		return err
	})
	HandleTask(w, "orders.placed", func(ctx context.Context, job map[string]int) error {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(40 * time.Millisecond)
		if job["id"] == 0 {
			return fmt.Errorf("id must not be zero")
		}
		return nil
	})

	for i := 1; i <= 3; i++ {
		env := testEnvelope(t, fmt.Sprintf("job-%d", i), "orders.placed", testPolicy(3))
		env.Payload = json.RawMessage(fmt.Sprintf(`{"id":%d}`, i))
		d.add(env, ExecutionState{})
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	d.push("job-1", "job-2", "job-3")
	waitFor(t, 5*time.Second, "all jobs settled", func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.jobs["job-1"].settled+d.jobs["job-2"].settled+d.jobs["job-3"].settled == 3
	})
	for i := 1; i <= 3; i++ {
		if dec := d.decision(fmt.Sprintf("job-%d", i)); dec == nil || dec.Kind != DecideSucceed {
			t.Fatalf("job-%d decision = %+v", i, dec)
		}
	}
	if maxInFlight.Load() < 2 {
		t.Fatalf("concurrency was %d", maxInFlight.Load())
	}
	mu.Lock()
	if len(order) == 0 || order[0] != "outer-in" {
		t.Fatalf("middleware order = %v", order)
	}
	mu.Unlock()
}

// TestRetryExhaustionAndPanic: ordinary errors retry with the envelope's
// backoff until the budget is spent, then fail; panics are ordinary failures.
func TestRetryExhaustionAndPanic(t *testing.T) {
	d := newFakeDriver()
	w := newWorker(t, d, Queue{Name: "q"})
	var runs atomic.Int32
	var failures atomic.Int32
	w.OnFailed(func(ctx context.Context, info Info, payload json.RawMessage, err error) {
		failures.Add(1)
	})
	HandleTask(w, "flaky", func(ctx context.Context, v int) error {
		if runs.Add(1) == 3 {
			panic("kaboom")
		}
		return errBoom
	})
	env := testEnvelope(t, "job-1", "flaky", ExecutionPolicy{MaxAttempts: 3, Backoff: ConstantBackoff(10 * time.Millisecond)})
	d.add(env, ExecutionState{})
	startWorker(t, w)
	for range 3 {
		d.push("job-1")
		waitFor(t, 5*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
		// Simulate the driver waking the retry after the recorded decision.
		if dec := d.decision("job-1"); dec.Kind == DecideRetry {
			time.Sleep(30 * time.Millisecond)
			continue
		}
		break
	}
	dec := d.decision("job-1")
	if dec.Kind != DecideFail || dec.Error == "" {
		t.Fatalf("decision = %+v", dec)
	}
	if n := runs.Load(); n != 3 {
		t.Fatalf("handler ran %d times", n)
	}
	waitFor(t, time.Second, "failure observer", func() bool { return failures.Load() == 1 })
	// The observer runs after the decision is durable.
	if d.jobs["job-1"].decision == nil {
		t.Fatal("observer ran before the decision")
	}
}

// TestSnoozeRefundsBudget: Snooze delays the job and does not consume budget.
func TestSnoozeRefundsBudget(t *testing.T) {
	d := newFakeDriver()
	w := newWorker(t, d, Queue{Name: "q"})
	HandleTask(w, "paused", func(ctx context.Context, v int) error { return Snooze(time.Hour) })
	d.add(testEnvelope(t, "job-1", "paused", testPolicy(1)), ExecutionState{})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 5*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	dec := d.decision("job-1")
	if dec.Kind != DecideSnooze || !dec.RunAt.After(time.Now()) {
		t.Fatalf("decision = %+v", dec)
	}
	if st := d.state(); st.Budget != 0 {
		t.Fatalf("budget after refund = %d", st.Budget)
	}
}

// TestTimeoutThenNilIsTimeout: a handler that returns nil after its timeout
// still fails as a timeout.
func TestTimeoutThenNilIsTimeout(t *testing.T) {
	d := newFakeDriver()
	w := newWorker(t, d, Queue{Name: "q"})
	HandleTask(w, "slow", func(ctx context.Context, v int) error {
		<-ctx.Done()
		time.Sleep(20 * time.Millisecond) // returns after the timeout, with nil
		return nil
	})
	d.add(testEnvelope(t, "job-1", "slow", ExecutionPolicy{MaxAttempts: 1, Timeout: 50 * time.Millisecond, Backoff: ConstantBackoff(0)}), ExecutionState{})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 5*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	dec := d.decision("job-1")
	if dec.Kind != DecideFail || !containsText(dec.Error, "timed out") {
		t.Fatalf("decision = %+v", dec)
	}
}

// TestUnknownTaskDefersWithoutBudget: a job without a handler is deferred
// without spending budget, observed, and left runnable.
func TestUnknownTaskDefersWithoutBudget(t *testing.T) {
	d := newFakeDriver()
	w := newWorker(t, d, Queue{Name: "q"})
	w.SetUnknownTaskTimeout(time.Hour)
	d.add(testEnvelope(t, "job-1", "not.here", testPolicy(1)), ExecutionState{})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 5*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	dec := d.decision("job-1")
	if dec.Kind != DecideDefer || dec.Reason != WaitUnknown || dec.UnknownSince.IsZero() || dec.RunAt.IsZero() {
		t.Fatalf("decision = %+v", dec)
	}
	if st := d.state(); st.Budget != 0 {
		t.Fatalf("unknown task spent budget %d", st.Budget)
	}
}

// TestUnknownTaskFailsAfterAge: once the unknown window passed, the job
// fails instead of being deferred forever.
func TestUnknownTaskFailsAfterAge(t *testing.T) {
	d := newFakeDriver()
	w := newWorker(t, d, Queue{Name: "q"})
	w.SetUnknownTaskTimeout(time.Minute)
	env := testEnvelope(t, "job-1", "not.here", testPolicy(3))
	d.add(env, ExecutionState{UnknownSince: time.Now().Add(-2 * time.Minute)})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 5*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	if dec := d.decision("job-1"); dec.Kind != DecideFail {
		t.Fatalf("decision = %+v", dec)
	}
}

// TestExhaustedBudgetAfterRedeliveryDoesNotRun: a delivery that arrives with
// the budget already spent fails without running the handler.
func TestExhaustedBudgetAfterRedeliveryDoesNotRun(t *testing.T) {
	d := newFakeDriver()
	w := newWorker(t, d, Queue{Name: "q"})
	var runs atomic.Int32
	HandleTask(w, "t", func(ctx context.Context, v int) error { runs.Add(1); return nil })
	d.add(testEnvelope(t, "job-1", "t", testPolicy(2)), ExecutionState{Budget: 2})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 5*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	if dec := d.decision("job-1"); dec.Kind != DecideFail || runs.Load() != 0 {
		t.Fatalf("decision = %+v, runs = %d", dec, runs.Load())
	}
}

// TestPendingDecisionResumesWithoutHandler: a delivery with a recorded
// decision resumes its settlement and never runs the handler.
func TestPendingDecisionResumesWithoutHandler(t *testing.T) {
	d := newFakeDriver()
	w := newWorker(t, d, Queue{Name: "q"})
	var runs atomic.Int32
	HandleTask(w, "t", func(ctx context.Context, v int) error { runs.Add(1); return nil })
	pending := Decision{Kind: DecideRetry, TransitionID: "exec-1.retry", ExecutionID: "exec-1", RunAt: time.Now()}
	d.add(testEnvelope(t, "job-1", "t", testPolicy(3)), ExecutionState{Budget: 1, Pending: &pending})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 5*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	if dec := d.decision("job-1"); dec.TransitionID != pending.TransitionID || runs.Load() != 0 {
		t.Fatalf("decision = %+v, runs = %d", dec, runs.Load())
	}
}

// TestCanceledBeforeAdmissionSkipsHandler: a recorded cancellation settles
// the delivery without running the handler.
func TestCanceledBeforeAdmissionSkipsHandler(t *testing.T) {
	d := newFakeDriver()
	w := newWorker(t, d, Queue{Name: "q"})
	var runs atomic.Int32
	HandleTask(w, "t", func(ctx context.Context, v int) error { runs.Add(1); return nil })
	d.add(testEnvelope(t, "job-1", "t", testPolicy(3)), ExecutionState{Canceled: true})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 5*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	if dec := d.decision("job-1"); dec.Kind != DecideCancel || runs.Load() != 0 {
		t.Fatalf("decision = %+v, runs = %d", dec, runs.Load())
	}
}

// TestLongJobIsKeptAlive: a long execution renews its lease repeatedly.
func TestLongJobIsKeptAlive(t *testing.T) {
	d := newFakeDriver()
	d.lease = 300 * time.Millisecond
	w := newWorker(t, d, Queue{Name: "q"})
	HandleTask(w, "long", func(ctx context.Context, v int) error {
		time.Sleep(1100 * time.Millisecond)
		return nil
	})
	d.add(testEnvelope(t, "job-1", "long", testPolicy(1)), ExecutionState{})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 10*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	d.mu.Lock()
	extends := d.extend
	d.mu.Unlock()
	if extends < 2 {
		t.Fatalf("lease extended %d times", extends)
	}
}

// TestHandlerIgnoringCancelIsAbandoned: Run returns even if a handler never
// returns after cancellation, within the bounded abandon window.
func TestHandlerIgnoringCancelIsAbandoned(t *testing.T) {
	d := newFakeDriver()
	client, err := NewClient(d, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(Queue{Name: "q"})
	w.abandonAfter = 200 * time.Millisecond
	w.SetShutdownTimeout(100 * time.Millisecond)
	release := make(chan struct{})
	defer close(release)
	HandleTask(w, "stuck", func(ctx context.Context, v int) error {
		<-release // ignores ctx entirely
		return nil
	})
	d.add(testEnvelope(t, "job-1", "stuck", testPolicy(1)), ExecutionState{})
	d.push("job-1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitFor(t, 5*time.Second, "admission", func() bool { return d.state().Budget == 1 })
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("Run took %s", time.Since(start))
	}
	waitFor(t, time.Second, "release decision", func() bool { return d.decision("job-1") != nil })
	if dec := d.decision("job-1"); dec.Kind != DecideRelease {
		t.Fatalf("decision = %+v", dec)
	}
}

// TestPartialReceiveProcessesAll: deliveries returned together with an error
// are all processed.
func TestPartialReceiveProcessesAll(t *testing.T) {
	d := newFakeDriver()
	client, err := NewClient(d, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(Queue{Name: "q"})
	var runs atomic.Int32
	HandleTask(w, "t", func(ctx context.Context, v int) error { runs.Add(1); return nil })
	consumer := &partialConsumer{d: d}
	w.client.driver = &partialDriver{fakeDriver: d, consumer: consumer}
	d.add(testEnvelope(t, "job-1", "t", testPolicy(1)), ExecutionState{})
	d.add(testEnvelope(t, "job-2", "t", testPolicy(1)), ExecutionState{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()
	waitFor(t, 5*time.Second, "both processed", func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.jobs["job-1"].settled == 1 && d.jobs["job-2"].settled == 1
	})
	cancel()
	<-done
	if runs.Load() != 2 {
		t.Fatalf("runs = %d", runs.Load())
	}
}

type partialDriver struct {
	*fakeDriver
	consumer Consumer
}

func (p *partialDriver) Consumer(ctx context.Context, queue string) (Consumer, error) {
	return p.consumer, nil
}

// partialConsumer returns two deliveries together with an error, once.
type partialConsumer struct {
	d    *fakeDriver
	once sync.Once
}

func (p *partialConsumer) Receive(ctx context.Context, max int) ([]Delivery, error) {
	var out []Delivery
	p.once.Do(func() {
		p.d.mu.Lock()
		out = []Delivery{&fakeDelivery{d: p.d, job: p.d.jobs["job-1"]}, &fakeDelivery{d: p.d, job: p.d.jobs["job-2"]}}
		p.d.mu.Unlock()
	})
	if len(out) > 0 {
		return out, errors.New("connection reset after two deliveries")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return nil, nil
	}
}

func (p *partialConsumer) Close() error { return nil }

// TestSettlementRetriedWhenNotRecorded: a settlement that was not recorded is
// retried with the same transition id.
func TestSettlementRetriedWhenNotRecorded(t *testing.T) {
	d := newFakeDriver()
	d.settleErr = errors.New("transient")
	client, err := NewClient(d, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(Queue{Name: "q"})
	w.settleTimeout = 2 * time.Second
	HandleTask(w, "t", func(ctx context.Context, v int) error { return nil })
	d.add(testEnvelope(t, "job-1", "t", testPolicy(1)), ExecutionState{})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 5*time.Second, "settle attempts", func() bool { return d.settleCount() >= 4 })
	if d.settleCount() != 4 {
		t.Fatalf("settle attempts = %d", d.settleCount())
	}
}

// TestShutdownReleasesUnadmittedDelivery: a delivery taken while the worker
// is stopping is released without running the handler or spending budget.
func TestShutdownReleasesUnadmittedDelivery(t *testing.T) {
	d := newFakeDriver()
	client, err := NewClient(d, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(Queue{Name: "q"})
	var runs atomic.Int32
	HandleTask(w, "t", func(ctx context.Context, v int) error { runs.Add(1); return nil })
	d.add(testEnvelope(t, "job-1", "t", testPolicy(1)), ExecutionState{Deliveries: 1})
	startWorker(t, w)
	stop := &stopper{}
	stop.stopped.Store(true)
	w.process(context.Background(), context.Background(), stop, Queue{Name: "q"}, &fakeDelivery{d: d, job: d.jobs["job-1"]})
	if runs.Load() != 0 {
		t.Fatalf("handler ran %d times", runs.Load())
	}
	if d.releases != 1 {
		t.Fatalf("releases = %d", d.releases)
	}
	if st := d.state(); st.Budget != 0 {
		t.Fatalf("budget = %d", st.Budget)
	}
}

// TestShutdownGraceCompletesRunningJob: a job that finishes inside the grace
// period succeeds, and the worker returns nil.
func TestShutdownGraceCompletesRunningJob(t *testing.T) {
	d := newFakeDriver()
	client, err := NewClient(d, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(Queue{Name: "q"})
	w.SetShutdownTimeout(5 * time.Second)
	started := make(chan struct{})
	HandleTask(w, "t", func(ctx context.Context, v int) error {
		close(started)
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	d.add(testEnvelope(t, "job-1", "t", testPolicy(1)), ExecutionState{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	d.push("job-1")
	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if dec := d.decision("job-1"); dec == nil || dec.Kind != DecideSucceed {
		t.Fatalf("decision = %+v", dec)
	}
}

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testLog{t}, &slog.HandlerOptions{Level: slog.LevelError + 100}))
}

func containsText(s, sub string) bool { return strings.Contains(s, sub) }

// newWorker returns a worker on the fake driver whose handlers can still be
// registered.
func newWorker(t *testing.T, d *fakeDriver, queues ...Queue) *Worker {
	t.Helper()
	client, err := NewClient(d, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(queues...)
	w.SetShutdownTimeout(2 * time.Second)
	return w
}

// startWorker runs the worker until the test ends.
func startWorker(t *testing.T, w *Worker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(ctx); err != nil {
			t.Errorf("worker: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
	})
}

// TestSlowLogContextKeepsLease: a slow log context function does not let the
// delivery's lease expire; the lease is renewed meanwhile.
func TestSlowLogContextKeepsLease(t *testing.T) {
	d := newFakeDriver()
	d.lease = 300 * time.Millisecond
	w := newWorker(t, d, Queue{Name: "q"})
	w.SetLogContext(func(ctx context.Context, info Info) context.Context {
		time.Sleep(600 * time.Millisecond)
		return ctx
	})
	HandleTask(w, "t", func(ctx context.Context, v int) error { return nil })
	d.add(testEnvelope(t, "job-1", "t", testPolicy(1)), ExecutionState{})
	startWorker(t, w)
	d.push("job-1")
	waitFor(t, 10*time.Second, "decision", func() bool { return d.decision("job-1") != nil })
	d.mu.Lock()
	extends := d.extend
	d.mu.Unlock()
	if extends < 1 {
		t.Fatalf("lease was not renewed during a slow log context (extends=%d)", extends)
	}
}

// TestObserverDoesNotBlockSettlementOrShutdown: a failure observer that
// returns late never blocks the decision, and shutdown returns in bounded
// time even if it ignores cancellation.
func TestObserverDoesNotBlockSettlementOrShutdown(t *testing.T) {
	d := newFakeDriver()
	client, err := NewClient(d, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(Queue{Name: "q"})
	w.callbackTimeout = 30 * time.Millisecond
	w.SetShutdownTimeout(30 * time.Millisecond)
	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{})
	w.OnFailed(func(context.Context, Info, json.RawMessage, error) {
		close(entered)
		<-release // ignores ctx
	})
	HandleTask(w, "t", func(context.Context, int) error { return Permanent(errBoom) })
	d.add(testEnvelope(t, "job-1", "t", testPolicy(1)), ExecutionState{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	d.push("job-1")
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("observer did not start")
	}
	// The decision is durable before the observer runs.
	if dec := d.decision("job-1"); dec == nil || dec.Kind != DecideFail {
		t.Fatalf("decision = %+v", dec)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run hung on a blocking observer")
	}
}
