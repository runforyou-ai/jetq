package jetq

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// Metadata is application metadata carried with a job, for example a W3C
// traceparent. Keys are case-sensitive; keys starting with "jetq-" (any case)
// are reserved. A Metadata value handed to jetq is copied, and the copies
// jetq hands out are owned by the receiver, so no value is shared between
// goroutines. Metadata itself is not safe for concurrent mutation.
type Metadata map[string]string

// Clone returns an independent copy; nil stays nil.
func (m Metadata) Clone() Metadata {
	if m == nil {
		return nil
	}
	return maps.Clone(m)
}

// Get returns the value for key.
func (m Metadata) Get(key string) string { return m[key] }

func (m Metadata) validate() error {
	for key := range m {
		if key == "" || strings.ContainsAny(key, "\r\n:") {
			return fmt.Errorf("jetq: invalid metadata key %q", key)
		}
		if strings.HasPrefix(strings.ToLower(key), "jetq-") {
			return fmt.Errorf("jetq: metadata key %q is reserved", key)
		}
		if strings.ContainsAny(m[key], "\r\n") {
			return fmt.Errorf("jetq: metadata value of %q contains a newline", key)
		}
	}
	return nil
}

// Envelope is the durable, driver-neutral description of one logical job.
// It is fixed at submission: retries, deferrals and snoozes keep the same
// Envelope and JobID. Drivers store it as an opaque document.
type Envelope struct {
	// JobID identifies the logical job.
	JobID string `json:"job_id"`
	// Task is the stable task name used to find the handler.
	Task string `json:"task"`
	// Queue is the queue the job runs on.
	Queue string `json:"queue"`
	// Payload is the JSON payload, byte for byte.
	Payload json.RawMessage `json:"payload"`
	// Policy is the resolved execution policy.
	Policy ExecutionPolicy `json:"policy"`
	// Metadata is application metadata.
	Metadata Metadata `json:"metadata,omitempty"`
	// RunAt is the absolute time of the first execution; zero means now.
	RunAt time.Time `json:"run_at,omitzero"`
	// EnqueueKey is the caller's submission idempotency key, if any.
	EnqueueKey string `json:"enqueue_key,omitempty"`
	// Unique is the suppression rule, if any.
	Unique *UniqueRule `json:"unique,omitempty"`
	// ParentJobID is set on a job created by redrive.
	ParentJobID string `json:"parent_job_id,omitempty"`
	// CreatedAt is when the submission was prepared, by the producer's clock.
	// It is excluded from the fingerprint.
	CreatedAt time.Time `json:"created_at"`
}

// Validate checks an envelope before it is stored or executed.
func (e *Envelope) Validate() error {
	if err := ValidateID("job id", e.JobID); err != nil {
		return err
	}
	if err := ValidateTaskName(e.Task); err != nil {
		return err
	}
	if err := ValidateID("queue", e.Queue); err != nil {
		return err
	}
	if !json.Valid(e.Payload) {
		return fmt.Errorf("jetq: job %s: payload is not valid JSON", e.JobID)
	}
	if e.EnqueueKey != "" {
		if err := ValidateKey("enqueue key", e.EnqueueKey); err != nil {
			return err
		}
	}
	if e.Unique != nil {
		if err := e.Unique.validate(); err != nil {
			return err
		}
	}
	if err := e.Metadata.validate(); err != nil {
		return err
	}
	return e.Policy.Validate()
}

// Clone returns a deep copy.
func (e *Envelope) Clone() *Envelope {
	c := *e
	c.Payload = slices.Clone(e.Payload)
	c.Metadata = e.Metadata.Clone()
	if e.Unique != nil {
		u := *e.Unique
		c.Unique = &u
	}
	return &c
}

// Fingerprint identifies the content of a submission for [EnqueueKey]
// conflict detection. It covers the task name, queue, payload bytes, the
// absolute run time, the resolved policy, metadata and the unique rule. It
// excludes JobID and CreatedAt, which are generated per preparation.
func (e *Envelope) Fingerprint() string {
	h := sha256.New()
	write := func(parts ...string) {
		for _, p := range parts {
			_, _ = fmt.Fprintf(h, "%d:%s;", len(p), p)
		}
	}
	runAt := ""
	if !e.RunAt.IsZero() {
		runAt = e.RunAt.UTC().Format(time.RFC3339Nano)
	}
	policy, _ := json.Marshal(e.Policy)
	write(e.Task, e.Queue, string(e.Payload), runAt, string(policy), e.ParentJobID)
	for _, key := range slices.Sorted(maps.Keys(e.Metadata)) {
		write(key, e.Metadata[key])
	}
	if e.Unique != nil {
		write(string(e.Unique.Mode), e.Unique.Key, e.Unique.Window.String())
	}
	return hex.EncodeToString(h.Sum(nil))
}

// UniqueMode selects a suppression rule.
type UniqueMode string

// Unique modes.
const (
	// UniqueModeWindow suppresses submissions with the same key for Window after
	// the first accepted one, whatever happens to that job.
	UniqueModeWindow UniqueMode = "window"
	// UniqueModeUntilDone suppresses submissions with the same key while a job
	// holding the key has not reached a terminal state, for at most the
	// driver's documented maximum hold time.
	UniqueModeUntilDone UniqueMode = "until_done"
)

// UniqueRule is a business suppression rule. It is independent of the job's
// identity and of submission idempotency.
type UniqueRule struct {
	Mode UniqueMode `json:"mode"`
	Key  string     `json:"key"`
	// Window applies to UniqueModeWindow.
	Window time.Duration `json:"window,omitempty"`
}

func (u *UniqueRule) validate() error {
	if err := ValidateKey("unique key", u.Key); err != nil {
		return err
	}
	switch u.Mode {
	case UniqueModeWindow:
		if u.Window <= 0 {
			return errors.New("jetq: unique window must be positive")
		}
	case UniqueModeUntilDone:
		if u.Window != 0 {
			return errors.New("jetq: until-done uniqueness has no window")
		}
	default:
		return fmt.Errorf("jetq: unknown unique mode %q", u.Mode)
	}
	return nil
}

// EnqueueStatus classifies a submission outcome.
type EnqueueStatus int

// Submission outcomes.
const (
	// Accepted: a new job was durably stored.
	Accepted EnqueueStatus = iota + 1
	// AlreadyAccepted: the same EnqueueKey and content was stored before;
	// the original JobID is returned.
	AlreadyAccepted
	// Suppressed: a unique rule held; no job was stored. ExistingJobID names
	// the holder when the driver knows it.
	Suppressed
	// Unknown: the job may or may not be stored; the error wraps
	// ErrUncertain and JobID is the candidate to trace. A definite rejection,
	// such as an occupied JobID (ErrJobIDInUse), is an error with no status.
	Unknown
)

func (s EnqueueStatus) String() string {
	switch s {
	case Accepted:
		return "accepted"
	case AlreadyAccepted:
		return "already_accepted"
	case Suppressed:
		return "suppressed"
	case Unknown:
		return "unknown"
	}
	return fmt.Sprintf("EnqueueStatus(%d)", int(s))
}

// EnqueueResult reports a submission.
type EnqueueResult struct {
	Status EnqueueStatus
	// JobID is the stored job, the original job for AlreadyAccepted, or the
	// candidate for Unknown. Empty for Suppressed.
	JobID string
	// ExistingJobID is the job holding the unique key, for Suppressed.
	ExistingJobID string
	// RunAt is the job's absolute first run time (zero: immediate).
	RunAt time.Time
}

// Submission errors.
var (
	// ErrUncertain is wrapped by errors of an operation whose outcome is
	// unknown: the job may or may not be stored. Retry an idempotent
	// submission, or trace the candidate JobID.
	ErrUncertain = errors.New("jetq: outcome unknown")
	// ErrEnqueueKeyConflict: the EnqueueKey was used for different content
	// within its retention; the original job is kept.
	ErrEnqueueKeyConflict = errors.New("jetq: enqueue key reused with different content")
	// ErrJobIDInUse: the explicitly chosen JobID already holds different
	// content. The submission is definitely not stored and the existing job
	// is left untouched; no result status is reported.
	ErrJobIDInUse = errors.New("jetq: job id already in use with different content")
	// ErrNotCancelable: the job has already started or finished.
	ErrNotCancelable = errors.New("jetq: job can no longer be canceled")
	// ErrNotFound: no job has this id.
	ErrNotFound = errors.New("jetq: job not found")
)

// ValidateID checks ids and queue names: letters, digits, '-' and '_'.
func ValidateID(kind, id string) error {
	if id == "" || strings.ContainsAny(id, ". \t\r\n") || strings.Contains(id, "/") {
		return fmt.Errorf("jetq: invalid %s %q: use letters, digits, '-' and '_'", kind, id)
	}
	for _, r := range id {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return fmt.Errorf("jetq: invalid %s %q: use letters, digits, '-' and '_'", kind, id)
		}
	}
	if len(id) > 128 {
		return fmt.Errorf("jetq: invalid %s %q: use up to 128 bytes", kind, id)
	}
	return nil
}

// JobState is the logical state of a job.
type JobState string

// Job states.
const (
	StateAwaiting  JobState = "awaiting"
	StateRunning   JobState = "running"
	StateSucceeded JobState = "succeeded"
	StateFailed    JobState = "failed"
	StateCanceled  JobState = "canceled"
)

// Terminal reports whether no further execution follows.
func (s JobState) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCanceled
}

// WaitReason says why an awaiting job waits.
type WaitReason string

// Wait reasons.
const (
	WaitScheduled WaitReason = "scheduled" // first run, possibly delayed
	WaitRetry     WaitReason = "retry"
	WaitSnoozed   WaitReason = "snoozed"
	WaitReleased  WaitReason = "released" // shutdown interruption
	WaitUnknown   WaitReason = "unknown_task"
)

// DecisionKind is the outcome of one delivery, decided by the engine.
type DecisionKind string

// Decision kinds.
const (
	// DecideSucceed completes the job.
	DecideSucceed DecisionKind = "succeed"
	// DecideRetry schedules the next execution at RunAt; the budget spent by
	// this execution stays spent.
	DecideRetry DecisionKind = "retry"
	// DecideSnooze schedules the next execution at RunAt and refunds the
	// budget of this execution.
	DecideSnooze DecisionKind = "snooze"
	// DecideRelease puts an interrupted execution back at RunAt and refunds
	// its budget.
	DecideRelease DecisionKind = "release"
	// DecideFail ends the job as failed.
	DecideFail DecisionKind = "fail"
	// DecideDefer puts back a delivery that was never admitted (unknown
	// task, shutdown before admission) without touching the budget.
	DecideDefer DecisionKind = "defer"
	// DecideCancel settles a delivery of a job whose cancellation was
	// recorded before admission. The handler does not run.
	DecideCancel DecisionKind = "cancel"
)

// Refunds reports whether the decision gives back the execution's budget.
func (k DecisionKind) Refunds() bool { return k == DecideSnooze || k == DecideRelease }

// Reschedules reports whether the job runs again.
func (k DecisionKind) Reschedules() bool {
	return k == DecideRetry || k == DecideSnooze || k == DecideRelease || k == DecideDefer
}

// Decision is the engine's verdict for one delivery. Drivers persist it
// keyed by TransitionID before any cleanup.
type Decision struct {
	Kind DecisionKind `json:"kind"`
	// TransitionID is stable for a given execution and kind: retrying a
	// settlement reuses it, so drivers can apply it idempotently.
	TransitionID string `json:"transition_id"`
	// ExecutionID is the admitted execution, empty for DecideDefer before
	// admission.
	ExecutionID string `json:"execution_id,omitempty"`
	// RunAt is the next execution time for rescheduling kinds.
	RunAt time.Time `json:"run_at,omitzero"`
	// Error is the failure text for retry and fail.
	Error string `json:"error,omitempty"`
	// Reason is the wait reason for rescheduling kinds.
	Reason WaitReason `json:"reason,omitempty"`
	// UnknownSince is when a worker first found no handler, for DecideDefer.
	UnknownSince time.Time `json:"unknown_since,omitzero"`
	// DecidedAt is the engine's clock at decision time.
	DecidedAt time.Time `json:"decided_at"`
}

// TransitionID derives the stable transition id of an execution's decision.
func TransitionID(executionID string, kind DecisionKind) string {
	return executionID + "." + string(kind)
}

// SettleStatus is the result of persisting a decision.
type SettleStatus int

// Settlement results.
const (
	// NotRecorded: the decision was certainly not stored. The delivery will
	// be offered again; nothing of the decision applies.
	NotRecorded SettleStatus = iota + 1
	// RecordedCleanupPending: the decision is durable; follow-up work
	// (next execution, unique release, transport ack) is incomplete and will
	// be finished by a later delivery or recovery.
	RecordedCleanupPending
	// Done: the decision and all follow-up work are complete.
	Done
	// SettleUnknown: whether the decision was stored is unknown; recovery
	// resolves it before any further execution.
	SettleUnknown
)

func (s SettleStatus) String() string {
	switch s {
	case NotRecorded:
		return "not_recorded"
	case RecordedCleanupPending:
		return "recorded_cleanup_pending"
	case Done:
		return "done"
	case SettleUnknown:
		return "unknown"
	}
	return fmt.Sprintf("SettleStatus(%d)", int(s))
}

// ValidateKey checks caller-provided keys: non-empty printable text.
func ValidateKey(kind, key string) error {
	if key == "" || len(key) > 1024 || strings.ContainsAny(key, "\r\n\x00") {
		return fmt.Errorf("jetq: invalid %s: use 1 to 1024 bytes without newlines", kind)
	}
	return nil
}

// envelopeWire is the persisted form of an Envelope. The payload is stored as
// opaque bytes (base64 in JSON), so persisting and restoring an envelope keeps
// the payload byte for byte: whitespace, key order and characters such as <
// are not rewritten, and the fingerprint computed from the original bytes
// stays valid.
type envelopeWire struct {
	JobID       string          `json:"job_id"`
	Task        string          `json:"task"`
	Queue       string          `json:"queue"`
	Payload     []byte          `json:"payload_bytes"`
	Policy      ExecutionPolicy `json:"policy"`
	Metadata    Metadata        `json:"metadata,omitempty"`
	RunAt       time.Time       `json:"run_at,omitzero"`
	EnqueueKey  string          `json:"enqueue_key,omitempty"`
	Unique      *UniqueRule     `json:"unique,omitempty"`
	ParentJobID string          `json:"parent_job_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

// MarshalJSON encodes the envelope with its payload as opaque bytes. It has a
// value receiver so values and pointers encode the same way.
func (e Envelope) MarshalJSON() ([]byte, error) {
	return json.Marshal(envelopeWire{
		JobID: e.JobID, Task: e.Task, Queue: e.Queue, Payload: []byte(e.Payload),
		Policy: e.Policy, Metadata: e.Metadata, RunAt: e.RunAt, EnqueueKey: e.EnqueueKey,
		Unique: e.Unique, ParentJobID: e.ParentJobID, CreatedAt: e.CreatedAt,
	})
}

// UnmarshalJSON restores an envelope encoded by MarshalJSON.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	var w envelopeWire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*e = Envelope{
		JobID: w.JobID, Task: w.Task, Queue: w.Queue, Payload: json.RawMessage(w.Payload),
		Policy: w.Policy, Metadata: w.Metadata, RunAt: w.RunAt, EnqueueKey: w.EnqueueKey,
		Unique: w.Unique, ParentJobID: w.ParentJobID, CreatedAt: w.CreatedAt,
	}
	return nil
}
