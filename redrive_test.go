package jetq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// inspectingFake adds GetJob to the fake driver so redrive can read parents.
type inspectingFake struct {
	*fakeDriver
	failed map[string]bool
}

func (f *inspectingFake) GetJob(ctx context.Context, id string) (JobSnapshot, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return JobSnapshot{}, false, nil
	}
	state := StateAwaiting
	if f.failed[id] {
		state = StateFailed
	}
	return JobSnapshot{Envelope: *j.env.Clone(), State: state, ParentJobID: j.env.ParentJobID}, true, nil
}

func (f *inspectingFake) List(context.Context, JobQuery) (JobPage, error) { return JobPage{}, nil }
func (f *inspectingFake) Stats(context.Context) (Stats, error)            { return Stats{}, nil }

func newRedriveFixture(t *testing.T) (*inspectingFake, *Client, time.Time) {
	t.Helper()
	d := &inspectingFake{fakeDriver: newFakeDriver(), failed: map[string]bool{}}
	c, err := NewClient(d)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0).UTC()
	c.now = func() time.Time { return now }
	task := MustTask[int]("billing", Policy{MaxAttempts: 7, Timeout: TimeoutOf(time.Minute)})
	if _, err := SubmitTask(context.Background(), c, task, 5, Submission{JobID: "orig", EnqueueKey: "outbox-1", Metadata: Metadata{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	d.failed["orig"] = true
	return d, c, now
}

// TestPrepareRedriveFixesDelayAcrossRetries: a redrive with a relative delay
// is prepared once; repeating SubmitPrepared after the clock moved returns the
// original replacement at the original run time.
func TestPrepareRedriveFixesDelayAcrossRetries(t *testing.T) {
	d, c, now := newRedriveFixture(t)
	ctx := context.Background()
	job, err := c.PrepareRedrive(ctx, "orig", RedriveDelay(time.Hour), RedriveEnqueueKey("redrive-1"))
	if err != nil {
		t.Fatal(err)
	}
	if job.Envelope.JobID == "orig" || job.Envelope.ParentJobID != "orig" {
		t.Fatalf("replacement identity = %+v", job.Envelope)
	}
	if !job.Envelope.RunAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("run at = %s", job.Envelope.RunAt)
	}
	if job.Envelope.Policy.MaxAttempts != 7 || job.Envelope.Policy.Timeout != time.Minute {
		t.Fatalf("typed task policy lost: %+v", job.Envelope.Policy)
	}
	if job.Envelope.EnqueueKey != "redrive-1" || string(job.Envelope.Payload) != "5" || job.Envelope.Metadata["k"] != "v" {
		t.Fatalf("replacement content = %+v", job.Envelope)
	}
	first, err := c.SubmitPrepared(ctx, job)
	if err != nil || first.Status != Accepted {
		t.Fatalf("first = %+v, %v", first, err)
	}
	c.now = func() time.Time { return now.Add(10 * time.Minute) }
	again, err := c.SubmitPrepared(ctx, job)
	if err != nil || again.Status != AlreadyAccepted || again.JobID != first.JobID || !again.RunAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("retry = %+v, %v", again, err)
	}
	orig, _, _ := d.GetJob(ctx, "orig")
	if orig.State != StateFailed || orig.Envelope.EnqueueKey != "outbox-1" || orig.ParentJobID != "" {
		t.Fatalf("original changed: %+v", orig)
	}
}

func TestPrepareRedriveRejects(t *testing.T) {
	_, c, _ := newRedriveFixture(t)
	ctx := context.Background()
	if _, err := c.PrepareRedrive(ctx, "orig", RedriveEnqueueKey("outbox-1")); err == nil || !strings.Contains(err.Error(), "inherit") {
		t.Fatalf("inherited key = %v", err)
	}
	if _, err := c.PrepareRedrive(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing parent = %v", err)
	}
	if _, err := c.Submit(ctx, "other", json.RawMessage("1"), Submission{JobID: "live"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PrepareRedrive(ctx, "live"); !errors.Is(err, ErrNotRedrivable) {
		t.Fatalf("live parent = %v", err)
	}
	plain, err := NewClient(newFakeDriver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.PrepareRedrive(ctx, "orig"); err == nil {
		t.Fatal("redrive without an inspector succeeded")
	}
}

// TestMaxPayloadBytes: every submission path enforces the producer limit
// before the driver sees the job.
func TestMaxPayloadBytes(t *testing.T) {
	if _, err := NewClient(newFakeDriver(), WithMaxPayloadBytes(-1)); err == nil {
		t.Fatal("negative limit accepted")
	}
	d := newFakeDriver()
	c, err := NewClient(d, WithMaxPayloadBytes(5))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	exact := json.RawMessage(`"abc"`) // 5 bytes
	over := json.RawMessage(`"abcd"`) // 6 bytes
	typed := MustTask[string]("typed", Policy{})
	if _, err := c.SubmitRaw(ctx, Raw("raw"), exact, Submission{}); err != nil {
		t.Fatalf("payload at the limit rejected: %v", err)
	}
	checks := map[string]func() error{
		"typed": func() error { _, err := SubmitTask(ctx, c, typed, "abcd", Submission{}); return err },
		"raw":   func() error { _, err := c.SubmitRaw(ctx, Raw("raw"), over, Submission{}); return err },
		"dynamic": func() error {
			_, err := c.Submit(ctx, "dyn", "abcd", Submission{})
			return err
		},
		"prepare": func() error { _, err := c.Prepare("dyn", over, Submission{}); return err },
		"prepare task": func() error {
			_, err := PrepareTask(c, typed, "abcd", Submission{})
			return err
		},
		"submit prepared": func() error {
			job, err := Prepare("dyn", over, Submission{JobID: "big"}, Policy{}, Policy{}, time.Now(), nil)
			if err != nil {
				return err
			}
			_, err = c.SubmitPrepared(ctx, job)
			return err
		},
	}
	for name, fn := range checks {
		if err := fn(); !errors.Is(err, ErrPayloadTooLarge) {
			t.Errorf("%s: %v, want ErrPayloadTooLarge", name, err)
		}
	}
	if len(d.submits) != 1 {
		t.Fatalf("driver saw %d submissions, want only the one at the limit", len(d.submits))
	}
}
