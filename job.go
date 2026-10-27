package jobq

import (
	"context"
	"encoding/json"
	"time"
)

// Job is a unit of background work whose type names itself. Its exported
// fields are encoded as JSON.
//
// JobName must return a stable name that identifies the job type across
// deploys; renaming it orphans jobs already in the queue. It is called on the
// zero value, so it must not depend on field values. Types without the method
// can be used through [Client.EnqueueNamed] and [HandleNamed].
type Job interface {
	JobName() string
}

// RawJob is a job whose name and JSON payload are only known at runtime, for
// example when bridging an existing task system. Enqueue it like any other job
// and handle it with [Worker.HandleRaw]. Payload is stored and delivered byte
// for byte; empty means null.
type RawJob struct {
	Name    string
	Payload json.RawMessage
}

// JobName returns the job name.
func (j RawJob) JobName() string { return j.Name }

// MarshalJSON returns the payload ("null" when empty).
func (j RawJob) MarshalJSON() ([]byte, error) {
	if len(j.Payload) == 0 {
		return []byte("null"), nil
	}
	return j.Payload, nil
}

// Info describes the job being processed. Retrieve it with [JobInfo].
type Info struct {
	// ID identifies the job. Jobs fired by a recurring schedule get
	// "<schedule key>-<suffix>".
	ID string
	// Name is the stable job name.
	Name string
	// Queue is the queue the job was taken from.
	Queue string
	// Attempt is the 1-based attempt, including claims whose lease expired
	// after a worker crash.
	Attempt int
	// MaxAttempts is the number of attempts after which the job is dead-lettered.
	MaxAttempts int
	// Timeout is how long this attempt may run (zero: no limit).
	Timeout time.Duration
	// Snoozes is how many times the job was snoozed with [Snooze].
	Snoozes int
	// EnqueuedAt is when the job was enqueued, by the producer's clock. It is
	// zero for jobs fired by a recurring schedule.
	EnqueuedAt time.Time
	// Header carries the headers set with [WithHeader]. Do not modify it.
	Header map[string]string
}

type infoKey struct{}

// JobInfo returns the [Info] of the job being processed by the handler that
// received ctx, or false outside a handler.
func JobInfo(ctx context.Context) (Info, bool) {
	info, ok := ctx.Value(infoKey{}).(Info)
	return info, ok
}
