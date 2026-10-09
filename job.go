package jetq

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// Job is a unit of background work. Its exported fields are encoded as JSON.
//
// JobName must return a stable name that identifies the job type across
// deploys; renaming it orphans jobs already in the queue. It is called on the
// zero value, so it must not depend on field values.
type Job interface {
	JobName() string
}

// Info describes the job being processed. Retrieve it with [JobInfo].
type Info struct {
	// ID is assigned at enqueue time. Jobs fired by a recurring schedule get
	// "<schedule key>-<stream sequence>".
	ID string
	// Name is the job name returned by [Job.JobName].
	Name string
	// Queue is the queue the job was taken from.
	Queue string
	// Attempt is the 1-based delivery attempt, including redeliveries after a worker crash.
	Attempt int
	// MaxAttempts is the number of attempts after which the job is dead-lettered.
	MaxAttempts int
	// Timeout is how long this attempt may run (zero: no limit), from
	// [Queue.Timeout] or [Timeout].
	Timeout time.Duration
	// Snoozes is how many times the job was snoozed with [Snooze], for example
	// to give up after a number of snoozes.
	Snoozes int
	// EnqueuedAt is when [Client.Enqueue] was called, by the producer's clock;
	// for a delayed job that is before it became available. It is zero for jobs
	// fired by a recurring schedule.
	EnqueuedAt time.Time
	// Header carries the message headers, including headers set with [WithHeader].
	Header nats.Header
}

type infoKey struct{}

// JobInfo returns the [Info] of the job being processed by the handler that
// received ctx, or false outside a handler.
func JobInfo(ctx context.Context) (Info, bool) {
	info, ok := ctx.Value(infoKey{}).(Info)
	return info, ok
}
