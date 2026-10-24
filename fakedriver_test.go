package jetq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeDriver is a controllable in-memory driver for engine tests. It records
// what the engine asked for and lets a test script the durable state a
// delivery starts with.
type fakeDriver struct {
	mu       sync.Mutex
	jobs     map[string]*fakeJob
	order    []string
	submits  []*PreparedJob
	releases int
	extend   int
	fail     error // Submit returns this error

	// admitErr, when set, makes the next Admit fail.
	admitErr error
	// settleErr overrides what Settle reports.
	settleErr error
	settles   int
	// lease is the lease length reported by deliveries.
	lease time.Duration
	// queue receives claimed job ids.
	notify chan string
}

type fakeJob struct {
	env      *Envelope
	state    ExecutionState
	decision *Decision
	settled  int
	released bool
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{jobs: map[string]*fakeJob{}, notify: make(chan string, 64), lease: time.Minute}
}

func (f *fakeDriver) Submit(ctx context.Context, job *PreparedJob) (EnqueueResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return EnqueueResult{Status: Unknown, JobID: job.Envelope.JobID}, f.fail
	}
	f.submits = append(f.submits, job)
	if existing, ok := f.jobs[job.Envelope.JobID]; ok {
		return EnqueueResult{Status: AlreadyAccepted, JobID: existing.env.JobID, RunAt: existing.env.RunAt}, nil
	}
	f.jobs[job.Envelope.JobID] = &fakeJob{env: job.Envelope.Clone()}
	f.order = append(f.order, job.Envelope.JobID)
	return EnqueueResult{Status: Accepted, JobID: job.Envelope.JobID, RunAt: job.Envelope.RunAt}, nil
}

func (f *fakeDriver) Consumer(ctx context.Context, queue string) (Consumer, error) {
	return &fakeConsumer{d: f, queue: queue}, nil
}

func (f *fakeDriver) add(env *Envelope, st ExecutionState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs[env.JobID] = &fakeJob{env: env.Clone(), state: st}
	f.order = append(f.order, env.JobID)
}

type fakeConsumer struct {
	d     *fakeDriver
	queue string
}

func (c *fakeConsumer) Receive(ctx context.Context, max int) ([]Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case id := <-c.d.notify:
		c.d.mu.Lock()
		j := c.d.jobs[id]
		c.d.mu.Unlock()
		return []Delivery{&fakeDelivery{d: c.d, job: j}}, nil
	}
}

func (c *fakeConsumer) Close() error { return nil }

type fakeDelivery struct {
	d   *fakeDriver
	job *fakeJob
	mu  sync.Mutex
}

func (d *fakeDelivery) Envelope() *Envelope {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.job.env
}

func (d *fakeDelivery) State() ExecutionState {
	d.mu.Lock()
	defer d.mu.Unlock()
	// A copy: the engine may hold on to it after the delivery moved on.
	return d.job.state
}

func (d *fakeDelivery) Admit(ctx context.Context) (Admission, error) {
	d.d.mu.Lock()
	defer d.d.mu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.d.admitErr; err != nil {
		return Admission{}, err
	}
	d.job.state.Budget++
	d.job.state.Deliveries++
	return Admission{ExecutionID: "exec-" + d.job.env.JobID, Attempt: d.job.state.Budget}, nil
}

func (d *fakeDelivery) Extend(ctx context.Context) error {
	d.d.mu.Lock()
	defer d.d.mu.Unlock()
	d.d.extend++
	return nil
}

func (d *fakeDelivery) Settle(ctx context.Context, dec Decision) (SettleStatus, error) {
	d.d.mu.Lock()
	defer d.d.mu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.d.settles++
	if err := d.d.settleErr; err != nil {
		return NotRecorded, err
	}
	d.job.decision = &dec
	d.job.settled++
	if dec.Kind.Refunds() {
		d.job.state.Budget--
	}
	return Done, nil
}

func (d *fakeDelivery) Release(ctx context.Context) error {
	d.d.mu.Lock()
	defer d.d.mu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.d.releases++
	d.job.released = true
	return nil
}

func (d *fakeDelivery) LeaseDuration() time.Duration { return d.d.lease }

// decisions returns the recorded decision of a job.
func (f *fakeDriver) decision(id string) *Decision {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobs[id].decision
}

func (f *fakeDriver) settleCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.settles
}

// state reports the execution state of the single test job, job-1.
func (f *fakeDriver) state() ExecutionState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobs["job-1"].state
}

func (f *fakeDriver) push(ids ...string) {
	for _, id := range ids {
		f.notify <- id
	}
}

// testEnvelope builds a stored envelope with a fixed id and task name.
func testEnvelope(t *testing.T, id, task string, policy ExecutionPolicy) *Envelope {
	t.Helper()
	env := &Envelope{
		JobID: id, Task: task, Queue: "q", Payload: json.RawMessage(`null`),
		Policy: policy, CreatedAt: time.Now().UTC(),
	}
	if err := env.Validate(); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	return env
}

func testPolicy(max int) ExecutionPolicy {
	return ExecutionPolicy{MaxAttempts: max, Backoff: ConstantBackoff(0)}
}

type testLog struct{ t *testing.T }

func (l testLog) Write(p []byte) (int, error) {
	l.t.Log(string(p))
	return len(p), nil
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

var errBoom = errors.New("boom")
