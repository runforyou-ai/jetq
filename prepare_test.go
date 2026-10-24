package jetq

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestPrepareTaskFixesIdentityAndDelay: a prepared task keeps the task's
// default policy, fixes a relative delay once, and can be submitted again
// without changing identity or time.
func TestPrepareTaskFixesIdentityAndDelay(t *testing.T) {
	d := newFakeDriver()
	c, err := NewClient(d, WithDefaultPolicy(Policy{MaxAttempts: 9}))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0)
	c.now = func() time.Time { return now }
	task := MustTask[map[string]int]("billing", Policy{MaxAttempts: 4, Timeout: TimeoutOf(time.Minute)})
	job, err := PrepareTask(c, task, map[string]int{"id": 1}, Submission{Delay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if job.Envelope.Policy.MaxAttempts != 4 || job.Envelope.Policy.Timeout != time.Minute {
		t.Fatalf("task policy lost: %+v", job.Envelope.Policy)
	}
	if !job.Envelope.RunAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("run at = %s", job.Envelope.RunAt)
	}
	if _, err := c.SubmitPrepared(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	// A later retry uses the same prepared value: same id, same run time.
	c.now = func() time.Time { return now.Add(30 * time.Second) }
	res, err := c.SubmitPrepared(context.Background(), job)
	if err != nil || res.Status != AlreadyAccepted || res.JobID != job.Envelope.JobID {
		t.Fatalf("retry = %+v, %v", res, err)
	}
	if !res.RunAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("retry moved the run time to %s", res.RunAt)
	}
}

// TestSubmitRawUsesTaskDefaults: a raw task's defaults apply exactly as a
// typed task's.
func TestSubmitRawUsesTaskDefaults(t *testing.T) {
	d := newFakeDriver()
	c, err := NewClient(d)
	if err != nil {
		t.Fatal(err)
	}
	raw := Raw("runtime.task")
	raw.policy = Policy{MaxAttempts: 6, Timeout: NoTimeout()}
	if _, err := c.SubmitRaw(context.Background(), raw, json.RawMessage(`{"a":1}`), Submission{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(context.Background(), "plain", 1, Submission{}); err != nil {
		t.Fatal(err)
	}
	if got := d.submits[0].Envelope.Policy; got.MaxAttempts != 6 || got.Timeout != 0 {
		t.Fatalf("raw policy = %+v", got)
	}
	if got := d.submits[1].Envelope.Policy.MaxAttempts; got != DefaultMaxAttempts {
		t.Fatalf("plain policy = %d", got)
	}
	if string(d.submits[0].Envelope.Payload) != `{"a":1}` {
		t.Fatalf("raw payload = %s", d.submits[0].Envelope.Payload)
	}
}

// TestPrepareExplainsMissingIdentity: a submission without a job id and
// without a generator fails with an error instead of panicking.
func TestPrepareExplainsMissingIdentity(t *testing.T) {
	if _, err := Prepare("t", json.RawMessage(`1`), Submission{}, Policy{}, Policy{}, time.Now(), nil); err == nil {
		t.Fatal("missing id and generator accepted")
	}
	if _, err := Prepare("t", json.RawMessage(`1`), Submission{}, Policy{}, Policy{}, time.Now(), func() string { return "" }); err == nil {
		t.Fatal("empty generated id accepted")
	}
	job, err := Prepare("t", json.RawMessage(`1`), Submission{JobID: "fixed"}, Policy{}, Policy{}, time.Now(), nil)
	if err != nil || job.Envelope.JobID != "fixed" {
		t.Fatalf("explicit id = %+v, %v", job, err)
	}
}
