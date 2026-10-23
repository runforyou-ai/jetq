package jetq

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBackoffValidate(t *testing.T) {
	tests := []struct {
		name string
		spec BackoffSpec
		ok   bool
	}{
		{"constant", ConstantBackoff(time.Second), true},
		{"constant zero", ConstantBackoff(0), true},
		{"constant negative", ConstantBackoff(-time.Second), false},
		{"constant with limit", BackoffSpec{Kind: BackoffConstant, Base: 1, Limit: 2}, false},
		{"exponential", ExponentialBackoff(time.Second, time.Minute), true},
		{"exponential equal", ExponentialBackoff(time.Second, time.Second), true},
		{"exponential zero base", ExponentialBackoff(0, time.Minute), false},
		{"exponential limit below base", ExponentialBackoff(time.Minute, time.Second), false},
		{"empty kind", BackoffSpec{Base: time.Second}, false},
		{"unknown kind", BackoffSpec{Kind: "linear", Base: time.Second}, false},
	}
	for _, tt := range tests {
		if err := tt.spec.Validate(); (err == nil) != tt.ok {
			t.Errorf("%s: Validate() = %v", tt.name, err)
		}
	}
}

func TestBackoffDelay(t *testing.T) {
	exp := ExponentialBackoff(5*time.Second, 10*time.Minute)
	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second,
		80 * time.Second, 160 * time.Second, 320 * time.Second, 10 * time.Minute, 10 * time.Minute}
	for i, w := range want {
		got, err := exp.Delay(i + 1)
		if err != nil || got != w {
			t.Errorf("attempt %d: %v, %v; want %v", i+1, got, err, w)
		}
	}
	if got, _ := ConstantBackoff(3 * time.Second).Delay(7); got != 3*time.Second {
		t.Errorf("constant = %v", got)
	}
	if got, _ := ExponentialBackoff(time.Second, time.Hour).Delay(1000); got != time.Hour {
		t.Errorf("large attempt = %v", got)
	}
	if _, err := exp.Delay(0); err == nil {
		t.Error("attempt 0 accepted")
	}
	if _, err := (BackoffSpec{Kind: "custom"}).Delay(1); err == nil {
		t.Error("unknown kind computed a delay")
	}
}

func TestBackoffJSON(t *testing.T) {
	spec := ExponentialBackoff(time.Second, time.Minute)
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var back BackoffSpec
	if err := json.Unmarshal(data, &back); err != nil || back != spec {
		t.Fatalf("round trip = %+v, %v", back, err)
	}
	// An unknown stored rule is an error, never a silent fallback.
	if err := json.Unmarshal([]byte(`{"kind":"fibonacci","base":1}`), &back); err == nil {
		t.Fatal("unknown kind decoded")
	}
}

func TestResolvePolicyPriority(t *testing.T) {
	producer := Policy{MaxAttempts: 2, Timeout: TimeoutOf(time.Minute), Backoff: &BackoffSpec{Kind: BackoffConstant, Base: time.Second}}
	task := Policy{MaxAttempts: 5, Backoff: &BackoffSpec{Kind: BackoffConstant, Base: 2 * time.Second}}

	got, err := ResolvePolicy(Policy{}, Policy{}, Policy{})
	if err != nil || got.MaxAttempts != DefaultMaxAttempts || got.Timeout != 0 || got.Backoff != defaultBackoff() {
		t.Fatalf("defaults = %+v, %v", got, err)
	}

	got, err = ResolvePolicy(Policy{}, task, producer)
	if err != nil || got.MaxAttempts != 5 || got.Timeout != time.Minute || got.Backoff.Base != 2*time.Second {
		t.Fatalf("task over producer = %+v, %v", got, err)
	}

	sub := Policy{MaxAttempts: 9, Timeout: NoTimeout(), Backoff: &BackoffSpec{Kind: BackoffExponential, Base: time.Second, Limit: time.Hour}}
	got, err = ResolvePolicy(sub, task, producer)
	if err != nil || got.MaxAttempts != 9 || got.Timeout != 0 || got.Backoff.Kind != BackoffExponential {
		t.Fatalf("submission wins = %+v, %v", got, err)
	}
}

func TestResolvePolicyZeroTimeoutIsNotInherit(t *testing.T) {
	task := Policy{Timeout: TimeoutOf(30 * time.Second)}
	if got, _ := ResolvePolicy(Policy{}, task, Policy{}); got.Timeout != 30*time.Second {
		t.Fatalf("nil inherits: %v", got.Timeout)
	}
	if got, _ := ResolvePolicy(Policy{Timeout: NoTimeout()}, task, Policy{}); got.Timeout != 0 {
		t.Fatalf("explicit zero: %v", got.Timeout)
	}
}

func TestResolvePolicyRejectsInvalidLayers(t *testing.T) {
	bad := []Policy{
		{MaxAttempts: -1},
		{Timeout: TimeoutOf(-time.Second)},
		{Backoff: &BackoffSpec{Kind: "nope"}},
	}
	for _, p := range bad {
		// Invalid values are reported in any layer, even when overridden.
		for _, layers := range [][3]Policy{{p, {}, {}}, {{MaxAttempts: 1}, p, {}}, {{}, {}, p}} {
			if _, err := ResolvePolicy(layers[0], layers[1], layers[2]); err == nil {
				t.Errorf("accepted %+v", layers)
			}
		}
	}
}

type invoice struct {
	ID int `json:"id"`
}

func TestTaskDescriptor(t *testing.T) {
	task, err := NewTask[invoice]("billing.invoice-send", Policy{MaxAttempts: 4, Timeout: TimeoutOf(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if task.Name() != "billing.invoice-send" {
		t.Fatal(task.Name())
	}
	// Defaults are copied out: mutating them does not change the task.
	d := task.Defaults()
	*d.Timeout = time.Hour
	if *task.Defaults().Timeout != time.Second {
		t.Fatal("task is mutable through Defaults")
	}
	data, err := task.Encode(invoice{ID: 7})
	if err != nil || string(data) != `{"id":7}` {
		t.Fatalf("Encode = %s, %v", data, err)
	}
	for _, name := range []string{"", ".x", "a b", strings.Repeat("a", 201)} {
		if _, err := NewTask[invoice](name, Policy{}); err == nil {
			t.Errorf("name %q accepted", name)
		}
	}
	if _, err := NewTask[invoice]("x", Policy{MaxAttempts: -2}); err == nil {
		t.Error("invalid default accepted")
	}
}

func TestEncodeRawPayload(t *testing.T) {
	raw, err := EncodePayload("t", json.RawMessage(`{ "a" : 1 }`))
	if err != nil || string(raw) != `{ "a" : 1 }` {
		t.Fatalf("raw kept byte for byte: %s, %v", raw, err)
	}
	if raw, _ := EncodePayload("t", json.RawMessage(nil)); string(raw) != "null" {
		t.Fatalf("empty raw = %s", raw)
	}
	if _, err := EncodePayload("t", json.RawMessage(`{`)); err == nil {
		t.Fatal("invalid raw accepted")
	}
}

func fixedID() string { return "job-1" }

func TestPrepareFixesRunAtAndFingerprint(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sub := Submission{Queue: "mail", Delay: time.Minute, EnqueueKey: "outbox-1", Metadata: Metadata{"traceparent": "00-1"}}
	a, err := Prepare("t", json.RawMessage(`{}`), sub, Policy{}, Policy{}, now, fixedID)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Envelope.RunAt.Equal(now.Add(time.Minute)) || a.Envelope.JobID != "job-1" {
		t.Fatalf("envelope = %+v", a.Envelope)
	}
	// Re-preparing later with the absolute time gives the same fingerprint;
	// CreatedAt and JobID do not take part.
	sub2 := Submission{Queue: "mail", RunAt: now.Add(time.Minute), EnqueueKey: "outbox-1", JobID: "other", Metadata: Metadata{"traceparent": "00-1"}}
	b, err := Prepare("t", json.RawMessage(`{}`), sub2, Policy{}, Policy{}, now.Add(time.Hour), fixedID)
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint != b.Fingerprint {
		t.Fatal("fingerprint depends on generated fields")
	}
	// Content changes change the fingerprint.
	for _, mutate := range []func(*Envelope){
		func(e *Envelope) { e.Payload = json.RawMessage(`{"x":1}`) },
		func(e *Envelope) { e.Queue = "other" },
		func(e *Envelope) { e.Task = "u" },
		func(e *Envelope) { e.RunAt = e.RunAt.Add(time.Second) },
		func(e *Envelope) { e.Policy.MaxAttempts++ },
		func(e *Envelope) { e.Metadata = Metadata{"traceparent": "00-2"} },
	} {
		e := a.Envelope.Clone()
		mutate(e)
		if e.Fingerprint() == a.Fingerprint {
			t.Errorf("fingerprint ignores a content change: %+v", e)
		}
	}
}

func TestPrepareValidation(t *testing.T) {
	now := time.Now()
	cases := []Submission{
		{RunAt: now, Delay: time.Second},
		{Delay: -time.Second},
		{Queue: "bad.queue"},
		{JobID: "bad id"},
		{Metadata: Metadata{"Jetq-Attempt": "1"}},
		{Unique: &UniqueRule{Mode: UniqueModeWindow, Key: "k"}},
		{Unique: &UniqueRule{Mode: "sometimes", Key: "k"}},
		{EnqueueKey: "a\nb"},
	}
	for _, sub := range cases {
		if _, err := Prepare("t", json.RawMessage(`1`), sub, Policy{}, Policy{}, now, fixedID); err == nil {
			t.Errorf("accepted %+v", sub)
		}
	}
	if _, err := Prepare("t", json.RawMessage(`1`), Submission{Unique: &UniqueRule{Mode: UniqueModeUntilDone, Key: "k"}}, Policy{}, Policy{}, now, fixedID); err != nil {
		t.Errorf("until-done rejected: %v", err)
	}
}

func TestEnvelopeCloneIsDeep(t *testing.T) {
	e := &Envelope{Payload: json.RawMessage(`1`), Metadata: Metadata{"a": "1"}, Unique: &UniqueRule{Key: "k"}}
	c := e.Clone()
	c.Payload[0] = '2'
	c.Metadata["a"] = "2"
	c.Unique.Key = "j"
	if string(e.Payload) != "1" || e.Metadata["a"] != "1" || e.Unique.Key != "k" {
		t.Fatal("clone shares state")
	}
}

func TestDecisionKinds(t *testing.T) {
	for kind, refunds := range map[DecisionKind]bool{DecideSnooze: true, DecideRelease: true, DecideRetry: false, DecideFail: false, DecideDefer: false, DecideSucceed: false} {
		if kind.Refunds() != refunds {
			t.Errorf("%s refunds = %v", kind, kind.Refunds())
		}
	}
	if a, b := TransitionID("e1", DecideRetry), TransitionID("e1", DecideRetry); a != b || TransitionID("e1", DecideRetry) == TransitionID("e1", DecideFail) {
		t.Fatal("transition ids not stable per execution and kind")
	}
	if !StateCanceled.Terminal() || StateAwaiting.Terminal() {
		t.Fatal("terminal states")
	}
	if Accepted.String() != "accepted" || SettleUnknown.String() != "unknown" {
		t.Fatal("status strings")
	}
	if !errors.Is(ErrEnqueueKeyConflict, ErrEnqueueKeyConflict) {
		t.Fatal()
	}
}

func TestPrepareCopiesPayloadAndChecksParent(t *testing.T) {
	payload := json.RawMessage(`{"a":1}`)
	job, err := Prepare("t", payload, Submission{}, Policy{}, Policy{}, time.Now(), fixedID)
	if err != nil {
		t.Fatal(err)
	}
	payload[6] = '2' // {"a":2}
	if string(job.Envelope.Payload) != `{"a":1}` || job.Envelope.Fingerprint() != job.Fingerprint {
		t.Fatal("envelope shares the caller's payload")
	}
	for _, parent := range []string{"job-1", "bad id"} {
		if _, err := Prepare("t", json.RawMessage(`1`), Submission{ParentJobID: parent}, Policy{}, Policy{}, time.Now(), fixedID); err == nil {
			t.Errorf("parent %q accepted", parent)
		}
	}
	// A redriven job carries its own submission identity (for a retried
	// redrive); [Client.Redrive] rejects an inherited key, not Prepare.
	redrive, err := Prepare("t", json.RawMessage(`1`), Submission{ParentJobID: "dead-1", EnqueueKey: "k"}, Policy{}, Policy{}, time.Now(), fixedID)
	if err != nil || redrive.Envelope.ParentJobID != "dead-1" || redrive.Envelope.EnqueueKey != "k" {
		t.Errorf("redrive with its own enqueue key: %+v, %v", redrive, err)
	}
	if _, err := Prepare("t", json.RawMessage(`1`), Submission{ParentJobID: "dead-1"}, Policy{}, Policy{}, time.Now(), fixedID); err != nil {
		t.Errorf("redrive rejected: %v", err)
	}
}
