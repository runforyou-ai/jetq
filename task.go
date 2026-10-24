package jetq

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"
)

// taskNameRE allows dotted names such as "billing.invoice-send".
var taskNameRE = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,199}$`)

// ValidateTaskName reports whether name can be used as a stable task name.
func ValidateTaskName(name string) error {
	if !taskNameRE.MatchString(name) {
		return fmt.Errorf("jetq: invalid task name %q: use up to 200 letters, digits, '_', '-' and '.'", name)
	}
	return nil
}

// Task is an immutable task descriptor: a stable name and the producer-side
// default policy for payloads of type T. It is not bound to a queue,
// connection or driver; the same Task can be submitted to several queues.
// T is an ordinary JSON-encodable type and needs no methods. Use
// json.RawMessage for payloads passed through byte for byte.
type Task[T any] struct {
	name   string
	policy Policy
}

// NewTask declares a task. It returns an error for an invalid name or policy;
// [MustTask] panics instead, for package-level declarations.
func NewTask[T any](name string, defaults Policy) (Task[T], error) {
	if err := ValidateTaskName(name); err != nil {
		return Task[T]{}, err
	}
	if err := defaults.validate(); err != nil {
		return Task[T]{}, fmt.Errorf("jetq: task %s: %w", name, err)
	}
	return Task[T]{name: name, policy: clonePolicy(defaults)}, nil
}

// MustTask is like [NewTask] but panics on error.
func MustTask[T any](name string, defaults Policy) Task[T] {
	t, err := NewTask[T](name, defaults)
	if err != nil {
		panic(err)
	}
	return t
}

// Name returns the stable task name.
func (t Task[T]) Name() string { return t.name }

// Defaults returns a copy of the task's default policy.
func (t Task[T]) Defaults() Policy { return clonePolicy(t.policy) }

// Encode encodes a payload the way submissions store it. A json.RawMessage
// is validated and kept byte for byte (empty means null); other values follow
// encoding/json.
func (t Task[T]) Encode(payload T) (json.RawMessage, error) {
	return EncodePayload(t.name, payload)
}

// EncodePayload encodes payload for task name; see [Task.Encode].
func EncodePayload(name string, payload any) (json.RawMessage, error) {
	if raw, ok := payload.(json.RawMessage); ok {
		if len(raw) == 0 {
			return json.RawMessage("null"), nil
		}
		if !json.Valid(raw) {
			return nil, fmt.Errorf("jetq: task %s: payload is not valid JSON", name)
		}
		return append(json.RawMessage(nil), raw...), nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("jetq: encode task %s: %w", name, err)
	}
	return data, nil
}

func clonePolicy(p Policy) Policy {
	if p.Timeout != nil {
		d := *p.Timeout
		p.Timeout = &d
	}
	if p.Backoff != nil {
		b := *p.Backoff
		p.Backoff = &b
	}
	return p
}

// PrepareTask prepares payload for task with the task's default policy, so
// the caller can fix a delay or job id once and then submit the same
// [PreparedJob] any number of times.
func PrepareTask[T any](c *Client, task Task[T], payload T, sub Submission) (*PreparedJob, error) {
	data, err := task.Encode(payload)
	if err != nil {
		return nil, err
	}
	return c.prepare(task.name, task.policy, data, sub)
}

// Submission holds per-call submission choices for [Prepare].
type Submission struct {
	// Queue defaults to DefaultQueue.
	Queue string
	// JobID defaults to a generated id.
	JobID string
	// Policy overrides the task and producer defaults.
	Policy Policy
	// RunAt is an absolute first run time; Delay is relative and is turned
	// into an absolute time once, at preparation. Setting both is an error.
	RunAt time.Time
	Delay time.Duration
	// EnqueueKey makes the submission idempotent within the driver's
	// documented retention.
	EnqueueKey string
	// Unique is an optional suppression rule.
	Unique   *UniqueRule
	Metadata Metadata
	// ParentJobID is set by redrive.
	ParentJobID string
}

// Prepare validates a submission and fixes everything a retry must keep:
// the JobID, the absolute run time and the resolved policy. now is the
// producer's clock; newID generates job ids.
func Prepare(task string, payload json.RawMessage, sub Submission, taskDefaults, producerDefaults Policy, now time.Time, newID func() string) (*PreparedJob, error) {
	if sub.Queue == "" {
		sub.Queue = DefaultQueue
	}
	if sub.JobID == "" {
		if newID == nil {
			return nil, errors.New("jetq: submission has no job id and no id generator")
		}
		sub.JobID = newID()
		if sub.JobID == "" {
			return nil, errors.New("jetq: the id generator returned an empty job id")
		}
	}
	if !sub.RunAt.IsZero() && sub.Delay != 0 {
		return nil, errors.New("jetq: set RunAt or Delay, not both")
	}
	if sub.Delay < 0 {
		return nil, errors.New("jetq: delay must not be negative")
	}
	runAt := sub.RunAt
	if sub.Delay > 0 {
		runAt = now.Add(sub.Delay)
	}
	policy, err := ResolvePolicy(sub.Policy, taskDefaults, producerDefaults)
	if err != nil {
		return nil, err
	}
	env := Envelope{
		JobID: sub.JobID, Task: task, Queue: sub.Queue, Payload: slices.Clone(payload), Policy: policy,
		Metadata: sub.Metadata.Clone(), RunAt: runAt.UTC(), EnqueueKey: sub.EnqueueKey,
		ParentJobID: sub.ParentJobID, CreatedAt: now.UTC(),
	}
	if runAt.IsZero() {
		env.RunAt = time.Time{}
	}
	if sub.Unique != nil {
		u := *sub.Unique
		env.Unique = &u
	}
	if env.Unique != nil && env.ParentJobID != "" {
		// A redriven job's unique rule belongs to the replacement's own
		// content, not to the parent's identity.
		if err := env.Unique.validate(); err != nil {
			return nil, err
		}
	}
	if env.ParentJobID != "" {
		if err := ValidateID("parent job id", env.ParentJobID); err != nil {
			return nil, err
		}
		// A redriven job is a new logical job. It may carry its own submission
		// identity, so a retried redrive is idempotent, but it never inherits
		// the parent's: [Client.Redrive] rejects an inherited key.
		if env.ParentJobID == env.JobID {
			return nil, fmt.Errorf("jetq: job %s is its own parent", env.JobID)
		}
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	return &PreparedJob{Envelope: env, Fingerprint: env.Fingerprint()}, nil
}
