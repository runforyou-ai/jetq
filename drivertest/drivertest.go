// Package drivertest holds the behaviour every jetq driver must have. A
// backend package runs it with a factory that returns a clean driver on a
// fresh namespace, so the same suite runs against NATS today and against
// other backends later. Nothing in this package imports a backend.
package drivertest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
)

// Factory returns a driver on a fresh, empty namespace. The returned cleanup
// runs when the test ends. Two calls with the same name must yield the same
// namespace only if the backend needs it; names are unique per test.
type Factory func(t *testing.T, name string) (driver jetq.Driver, cleanup func())

// Options tunes the suite for a backend whose timings differ.
type Options struct {
	// ClaimTimeout bounds how long the suite waits for a claim to appear or
	// expire. Default 5s.
	ClaimTimeout time.Duration
}

// Run runs the whole conformance suite.
func Run(t *testing.T, f Factory, opts Options) {
	t.Helper()
	if opts.ClaimTimeout <= 0 {
		opts.ClaimTimeout = 5 * time.Second
	}
	s := suite{f: f, opts: opts}
	t.Run("SubmitIdempotency", s.submitIdempotency)
	t.Run("SubmitContentConflict", s.submitContentConflict)
	t.Run("SubmitJobIDInUse", s.submitJobIDInUse)
	t.Run("UniqueUntilDone", s.uniqueUntilDone)
	t.Run("UniqueWindow", s.uniqueWindow)
	t.Run("AdmitBudgetAndLease", s.admitBudgetAndLease)
	t.Run("SnoozeRefundsBudget", s.snoozeRefundsBudget)
	t.Run("CancelAwaitingJob", s.cancelAwaitingJob)
	t.Run("Inspection", s.inspection)
}

type suite struct {
	f    Factory
	opts Options
}

const (
	taskName = "conformance.task"
	queue    = "conformance"
)

func (s suite) driver(t *testing.T) jetq.Driver {
	t.Helper()
	d, cleanup := s.f(t, t.Name())
	if cleanup != nil {
		t.Cleanup(cleanup)
	}
	return d
}

func (s suite) prepare(t *testing.T, id string, sub jetq.Submission) *jetq.PreparedJob {
	t.Helper()
	sub.JobID = id
	if sub.Queue == "" {
		sub.Queue = queue
	}
	job, err := jetq.Prepare(taskName, json.RawMessage(`{"n":1}`), sub, jetq.Policy{}, jetq.Policy{}, time.Now(), func() string { return id })
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return job
}

func (s suite) receive(t *testing.T, c jetq.Consumer) jetq.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.ClaimTimeout)
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
			t.Fatalf("no delivery within %s", s.opts.ClaimTimeout)
		}
	}
}

// submitIdempotency: the same EnqueueKey and content returns the original job
// and its original run time, whatever id the retry generated.
func (s suite) submitIdempotency(t *testing.T) {
	d := s.driver(t)
	ctx := context.Background()
	runAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	first, err := d.Submit(ctx, s.prepare(t, "idem-1", jetq.Submission{EnqueueKey: "k1", RunAt: runAt}))
	if err != nil || first.Status != jetq.Accepted || !first.RunAt.Equal(runAt) {
		t.Fatalf("first = %+v, %v", first, err)
	}
	second, err := d.Submit(ctx, s.prepare(t, "idem-2", jetq.Submission{EnqueueKey: "k1", RunAt: runAt}))
	if err != nil || second.Status != jetq.AlreadyAccepted || second.JobID != "idem-1" || !second.RunAt.Equal(runAt) {
		t.Fatalf("retry = %+v, %v", second, err)
	}
}

// submitContentConflict: the same EnqueueKey with different content fails and
// keeps the original job.
func (s suite) submitContentConflict(t *testing.T) {
	d := s.driver(t)
	ctx := context.Background()
	if res, err := d.Submit(ctx, s.prepare(t, "conf-1", jetq.Submission{EnqueueKey: "k1"})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("first = %+v, %v", res, err)
	}
	conflict := s.prepare(t, "conf-2", jetq.Submission{EnqueueKey: "k1"})
	conflict.Envelope.Payload = json.RawMessage(`{"n":2}`)
	conflict.Fingerprint = conflict.Envelope.Fingerprint()
	if _, err := d.Submit(ctx, conflict); !errors.Is(err, jetq.ErrEnqueueKeyConflict) {
		t.Fatalf("conflict = %v", err)
	}
	if insp, ok := d.(jetq.Inspector); ok {
		if _, exists, err := insp.GetJob(ctx, "conf-2"); err != nil || exists {
			t.Fatalf("conflicting job was stored: %v, %v", exists, err)
		}
	}
}

// submitJobIDInUse: an explicit JobID that already holds different content,
// with no EnqueueKey, is a definite conflict: a classified error, no result
// status, and the original job untouched.
func (s suite) submitJobIDInUse(t *testing.T) {
	d := s.driver(t)
	ctx := context.Background()
	first := s.prepare(t, "dup-1", jetq.Submission{})
	if res, err := d.Submit(ctx, first); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("first = %+v, %v", res, err)
	}
	again := s.prepare(t, "dup-1", jetq.Submission{})
	again.Envelope.Payload = json.RawMessage(`{"n":2}`)
	again.Fingerprint = again.Envelope.Fingerprint()
	res, err := d.Submit(ctx, again)
	if !errors.Is(err, jetq.ErrJobIDInUse) || errors.Is(err, jetq.ErrUncertain) {
		t.Fatalf("conflicting job id = %v, want ErrJobIDInUse without ErrUncertain", err)
	}
	if res.Status != 0 || res.JobID != "" {
		t.Fatalf("conflict reported a result: %+v", res)
	}
	if insp, ok := d.(jetq.Inspector); ok {
		snap, found, err := insp.GetJob(ctx, "dup-1")
		if err != nil || !found || string(snap.Envelope.Payload) != `{"n":1}` {
			t.Fatalf("original job changed: %+v, %v, %v", snap.Envelope, found, err)
		}
	}
	// The unchanged content is still the original job.
	if res, err := d.Submit(ctx, s.prepare(t, "dup-1", jetq.Submission{})); err != nil || res.Status != jetq.AlreadyAccepted {
		t.Fatalf("unchanged resubmission = %+v, %v", res, err)
	}
}

// uniqueUntilDone: a second job with the key is suppressed while the first is
// unfinished; the key is free again once the first succeeds.
func (s suite) uniqueUntilDone(t *testing.T) {
	d := s.driver(t)
	ctx := context.Background()
	u := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "account"}
	if res, err := d.Submit(ctx, s.prepare(t, "uniq-1", jetq.Submission{Unique: u})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("first = %+v, %v", res, err)
	}
	res, err := d.Submit(ctx, s.prepare(t, "uniq-2", jetq.Submission{Unique: u}))
	if err != nil || res.Status != jetq.Suppressed || res.ExistingJobID != "uniq-1" {
		t.Fatalf("second = %+v, %v", res, err)
	}
	c, err := d.Consumer(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	settleSucceed(t, s, c)
	if res, err := d.Submit(ctx, s.prepare(t, "uniq-3", jetq.Submission{Unique: u})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("after settle = %+v, %v", res, err)
	}
}

// uniqueWindow: the key suppresses for its window only.
func (s suite) uniqueWindow(t *testing.T) {
	d := s.driver(t)
	ctx := context.Background()
	u := &jetq.UniqueRule{Mode: jetq.UniqueModeWindow, Key: "win", Window: 150 * time.Millisecond}
	if res, err := d.Submit(ctx, s.prepare(t, "win-1", jetq.Submission{Unique: u})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("first = %+v, %v", res, err)
	}
	if res, err := d.Submit(ctx, s.prepare(t, "win-2", jetq.Submission{Unique: u})); err != nil || res.Status != jetq.Suppressed {
		t.Fatalf("inside window = %+v, %v", res, err)
	}
	time.Sleep(200 * time.Millisecond)
	if res, err := d.Submit(ctx, s.prepare(t, "win-3", jetq.Submission{Unique: u})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("after window = %+v, %v", res, err)
	}
}

// admitBudgetAndLease: an admission spends one unit of budget, the execution
// id is stable for the delivery, and an extended lease keeps another consumer
// from taking the job.
func (s suite) admitBudgetAndLease(t *testing.T) {
	d := s.driver(t)
	ctx := context.Background()
	if res, err := d.Submit(ctx, s.prepare(t, "adm-1", jetq.Submission{})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v, %v", res, err)
	}
	c, err := d.Consumer(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	dlv := s.receive(t, c)
	if st := dlv.State(); st.Budget != 0 || st.Deliveries != 1 {
		t.Fatalf("state = %+v", st)
	}
	adm, err := dlv.Admit(ctx)
	if err != nil || adm.Attempt != 1 || adm.ExecutionID == "" {
		t.Fatalf("admit = %+v, %v", adm, err)
	}
	if again, err := dlv.Admit(ctx); err != nil || again != adm {
		t.Fatalf("second admit = %+v, %v", again, err)
	}
	if st := dlv.State(); st.Budget != 1 {
		t.Fatalf("state after admit = %+v", st)
	}
	// Extending keeps the lease while the job runs.
	lease := dlv.LeaseDuration()
	if lease <= 0 {
		t.Fatalf("lease = %s", lease)
	}
	for i := 0; i < 3; i++ {
		time.Sleep(lease / 3)
		if err := dlv.Extend(ctx); err != nil {
			t.Fatalf("Extend: %v", err)
		}
	}
	other, err := d.Consumer(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if dlv, err := other.Receive(watchCtx, 1); len(dlv) > 0 {
		t.Fatalf("job was delivered to another consumer during a live lease: %v, %v", dlv[0].Envelope().JobID, err)
	}
	dec := jetq.Decision{Kind: jetq.DecideSucceed, ExecutionID: adm.ExecutionID, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSucceed)}
	if status, err := dlv.Settle(ctx, dec); err != nil || status != jetq.Done {
		t.Fatalf("settle = %v, %v", status, err)
	}
}

// snoozeRefundsBudget: a snoozed execution is refunded and runs again with
// the same attempt number; repeating the settlement changes nothing.
func (s suite) snoozeRefundsBudget(t *testing.T) {
	d := s.driver(t)
	ctx := context.Background()
	if res, err := d.Submit(ctx, s.prepare(t, "snz-1", jetq.Submission{})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v, %v", res, err)
	}
	c, err := d.Consumer(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	dlv := s.receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dec := jetq.Decision{Kind: jetq.DecideSnooze, ExecutionID: adm.ExecutionID, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSnooze), RunAt: time.Now().Add(20 * time.Millisecond)}
	if status, err := dlv.Settle(ctx, dec); err != nil || status != jetq.Done {
		t.Fatalf("snooze settle = %v, %v", status, err)
	}
	if status, err := dlv.Settle(ctx, dec); err != nil || status != jetq.Done {
		t.Fatalf("repeated settle = %v, %v", status, err)
	}
	dlv = s.receive(t, c)
	if st := dlv.State(); st.Budget != 0 || st.Snoozes != 1 {
		t.Fatalf("state after snooze = %+v", st)
	}
	adm2, err := dlv.Admit(ctx)
	if err != nil || adm2.Attempt != 1 {
		t.Fatalf("second admit = %+v, %v", adm2, err)
	}
}

// cancelAwaitingJob: a canceled awaiting job is never delivered.
func (s suite) cancelAwaitingJob(t *testing.T) {
	d := s.driver(t)
	ctx := context.Background()
	canceler, ok := d.(jetq.Canceler)
	if !ok {
		t.Skip("driver does not implement jetq.Canceler")
	}
	if res, err := d.Submit(ctx, s.prepare(t, "can-1", jetq.Submission{})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v, %v", res, err)
	}
	if err := canceler.Cancel(ctx, "can-1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	c, err := d.Consumer(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if dlv, _ := c.Receive(watchCtx, 1); len(dlv) > 0 {
		t.Fatalf("canceled job was delivered: %s", dlv[0].Envelope().JobID)
	}
	if err := canceler.Cancel(ctx, "missing"); err == nil {
		t.Fatal("cancel of a missing job succeeded")
	}
}

// inspection: snapshots report the logical state without backend ids.
func (s suite) inspection(t *testing.T) {
	d := s.driver(t)
	insp, ok := d.(jetq.Inspector)
	if !ok {
		t.Skip("driver does not implement jetq.Inspector")
	}
	ctx := context.Background()
	if res, err := d.Submit(ctx, s.prepare(t, "insp-1", jetq.Submission{})); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v, %v", res, err)
	}
	snap, found, err := insp.GetJob(ctx, "insp-1")
	if err != nil || !found {
		t.Fatalf("GetJob = %v, %v", found, err)
	}
	if snap.State != jetq.StateAwaiting || snap.Envelope.JobID != "insp-1" || snap.Envelope.Task != taskName {
		t.Fatalf("snapshot = %+v", snap)
	}
	if _, found, _ := insp.GetJob(ctx, "missing"); found {
		t.Fatal("GetJob found a missing job")
	}
	page, err := insp.List(ctx, jetq.JobQuery{Queue: queue})
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].Envelope.JobID != "insp-1" {
		t.Fatalf("List = %+v, %v", page, err)
	}
	stats, err := insp.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	for _, q := range stats.Queues {
		if q.Queue == queue && q.Ready != 1 {
			t.Fatalf("queue stats = %+v", q)
		}
	}
}

// settleSucceed admits and succeeds the next delivery of the queue.
func settleSucceed(t *testing.T, s suite, c jetq.Consumer) {
	t.Helper()
	dlv := s.receive(t, c)
	adm, err := dlv.Admit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dec := jetq.Decision{Kind: jetq.DecideSucceed, ExecutionID: adm.ExecutionID, TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideSucceed)}
	if status, err := dlv.Settle(context.Background(), dec); err != nil || status != jetq.Done {
		t.Fatalf("settle = %v, %v", status, err)
	}
}

// Redrive runs the redrive behaviour against a driver that implements
// [jetq.Inspector]. Every backend runs it with the rest of the suite when it
// can inspect jobs.
func Redrive(t *testing.T, f Factory, opts Options) {
	t.Run("Redrive", func(t *testing.T) {
		s := suite{f: f, opts: opts}
		d := s.driver(t)
		insp, ok := d.(jetq.Inspector)
		if !ok {
			t.Skip("driver does not implement jetq.Inspector")
		}
		ctx := context.Background()
		if res, err := d.Submit(ctx, s.prepare(t, "redrive-parent", jetq.Submission{})); err != nil || res.Status != jetq.Accepted {
			t.Fatalf("submit = %+v, %v", res, err)
		}
		c, err := d.Consumer(ctx, queue)
		if err != nil {
			t.Fatal(err)
		}
		dlv := s.receive(t, c)
		adm, err := dlv.Admit(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideFail, ExecutionID: adm.ExecutionID,
			TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideFail), Error: "boom"}); err != nil {
			t.Fatal(err)
		}
		client, err := jetq.NewClient(d)
		if err != nil {
			t.Fatal(err)
		}
		runAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
		res, err := client.Redrive(ctx, "redrive-parent", jetq.RedriveEnqueueKey("redrive-key"), jetq.RedriveRunAt(runAt))
		if err != nil || res.Status != jetq.Accepted || res.JobID == "redrive-parent" {
			t.Fatalf("redrive = %+v, %v", res, err)
		}
		again, err := client.Redrive(ctx, "redrive-parent", jetq.RedriveEnqueueKey("redrive-key"), jetq.RedriveRunAt(runAt))
		if err != nil || again.Status != jetq.AlreadyAccepted || again.JobID != res.JobID {
			t.Fatalf("retried redrive = %+v, %v", again, err)
		}
		child, found, err := insp.GetJob(ctx, res.JobID)
		if err != nil || !found || child.ParentJobID != "redrive-parent" || !child.Envelope.RunAt.Equal(runAt) {
			t.Fatalf("replacement = %+v, %v, %v", child, found, err)
		}
		// A relative delay is fixed by preparing once; repeating the prepared
		// value returns the same replacement at the same time.
		prepared, err := client.PrepareRedrive(ctx, "redrive-parent", jetq.RedriveEnqueueKey("redrive-delay"), jetq.RedriveDelay(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		first, err := client.SubmitPrepared(ctx, prepared)
		if err != nil || first.Status != jetq.Accepted {
			t.Fatalf("prepared redrive = %+v, %v", first, err)
		}
		retry, err := client.SubmitPrepared(ctx, prepared)
		if err != nil || retry.Status != jetq.AlreadyAccepted || retry.JobID != first.JobID || !retry.RunAt.Equal(first.RunAt) {
			t.Fatalf("retried prepared redrive = %+v, %v", retry, err)
		}
		parent, found, err := insp.GetJob(ctx, "redrive-parent")
		if err != nil || !found || parent.State != jetq.StateFailed {
			t.Fatalf("parent = %+v, %v, %v", parent, found, err)
		}
	})
}
