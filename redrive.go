package jetq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RedriveOption adjusts a redrive. A redrive creates a new job that repeats a failed one. The original job is
// left untouched; the new job has a new JobID and records the original as its
// ParentJobID. It never inherits the original's EnqueueKey: the redrive needs
// its own retryable submission identity.
//
// The payload and task are always the original's. Policy, queue, metadata
// and unique rule default to the original job's; override them with options.
//
// RedriveOption configures a redrive.
type RedriveOption func(*Submission)

// RedriveQueue overrides the replacement's queue.
func RedriveQueue(queue string) RedriveOption {
	return func(s *Submission) { s.Queue = queue }
}

// RedrivePolicy overrides the replacement's policy layers. Unset fields still
// resolve against the original job's resolved policy.
func RedrivePolicy(p Policy) RedriveOption {
	return func(s *Submission) { s.Policy = p }
}

// RedriveRunAt sets the replacement's absolute first run time.
func RedriveRunAt(t time.Time) RedriveOption {
	return func(s *Submission) { s.RunAt = t }
}

// RedriveDelay sets the replacement's delay relative to preparation. The
// delay becomes an absolute run time when [Client.PrepareRedrive] runs; a
// second PrepareRedrive or Redrive call computes a new time, and with the same
// enqueue key that is a content conflict. To retry a relative-delay redrive,
// prepare it once and repeat [Client.SubmitPrepared] with the same value.
func RedriveDelay(d time.Duration) RedriveOption {
	return func(s *Submission) { s.Delay = d }
}

// RedriveMetadata replaces the replacement's metadata. The original's
// metadata is used when this option is not given.
func RedriveMetadata(m Metadata) RedriveOption {
	return func(s *Submission) { s.Metadata = m.Clone() }
}

// RedriveEnqueueKey sets the replacement's own submission identity, for a
// caller that retries the redrive. The original's key is never inherited.
func RedriveEnqueueKey(key string) RedriveOption {
	return func(s *Submission) { s.EnqueueKey = key }
}

// RedriveJobID fixes the replacement's job id, for a caller that generates
// it. By default a fresh id is generated at preparation.
func RedriveJobID(id string) RedriveOption {
	return func(s *Submission) { s.JobID = id }
}

// PrepareRedrive prepares the replacement for the failed job parentID
// without storing it. The result fixes the replacement's job id, run time
// (including a RedriveDelay) and policy, so repeating [Client.SubmitPrepared]
// with it returns the same replacement: Accepted the first time,
// AlreadyAccepted on a retry.
//
// The original job must exist and be failed; a missing parent is ErrNotFound
// and a parent in another state ErrNotRedrivable. Reusing the parent's
// enqueue key is rejected.
func (c *Client) PrepareRedrive(ctx context.Context, parentID string, opts ...RedriveOption) (*PreparedJob, error) {
	inspector, err := c.Inspector()
	if err != nil {
		return nil, err
	}
	parent, ok, err := inspector.GetJob(ctx, parentID)
	if err != nil {
		return nil, fmt.Errorf("jetq: redrive %s: %w", parentID, err)
	}
	if !ok {
		return nil, fmt.Errorf("jetq: redrive %s: %w", parentID, ErrNotFound)
	}
	if parent.State != StateFailed {
		return nil, fmt.Errorf("jetq: redrive %s: %w: job is %s", parentID, ErrNotRedrivable, parent.State)
	}
	sub := Submission{
		Queue:    parent.Envelope.Queue,
		Metadata: parent.Envelope.Metadata.Clone(),
	}
	for _, opt := range opts {
		opt(&sub)
	}
	if sub.EnqueueKey != "" && sub.EnqueueKey == parent.Envelope.EnqueueKey {
		return nil, errors.New("jetq: a redrive must not inherit the parent's enqueue key; use RedriveEnqueueKey with a new key")
	}
	// The replacement keeps the parent's resolved policy as its defaults, so
	// a caller that overrides only some fields still gets the rest unchanged.
	env := parent.Envelope
	sub.ParentJobID = parentID
	if sub.Unique == nil {
		sub.Unique = env.Unique
	}
	if err := c.checkPayload(env.Task, env.Payload); err != nil {
		return nil, err
	}
	prepared, err := Prepare(env.Task, env.Payload, sub, Policy{
		MaxAttempts: env.Policy.MaxAttempts,
		Timeout:     TimeoutOf(env.Policy.Timeout),
		Backoff:     &BackoffSpec{Kind: env.Policy.Backoff.Kind, Base: env.Policy.Backoff.Base, Limit: env.Policy.Backoff.Limit},
	}, c.defaults, c.now(), c.newID)
	if err != nil {
		return nil, err
	}
	prepared.Fingerprint = prepared.Envelope.Fingerprint()
	if err := prepared.Envelope.Validate(); err != nil {
		return nil, err
	}
	return prepared, nil
}

// Redrive prepares and submits a replacement once; see
// [Client.PrepareRedrive]. A retry of Redrive with a fixed RedriveRunAt and
// the same RedriveEnqueueKey finds the replacement it created; a redrive with
// RedriveDelay must be retried by repeating SubmitPrepared on one prepared
// value instead.
func (c *Client) Redrive(ctx context.Context, parentID string, opts ...RedriveOption) (EnqueueResult, error) {
	prepared, err := c.PrepareRedrive(ctx, parentID, opts...)
	if err != nil {
		return EnqueueResult{}, err
	}
	return c.SubmitPrepared(ctx, prepared)
}

// ErrNotRedrivable is returned by [Client.Redrive] when the job is not in the
// failed state.
var ErrNotRedrivable = errors.New("jetq: job is not failed")
