package jetq

import (
	"context"
	"time"
)

// DefaultQueue is the queue used when neither the producer nor the
// submission names one.
const DefaultQueue = "default"

// Info describes an execution a handler runs. Retrieve it with [JobInfo].
type Info struct {
	// ID is the logical job id, stable across retries and snoozes.
	ID string
	// Name is the task name used to find the handler.
	Name string
	// Queue is the queue the job was taken from.
	Queue string
	// Attempt is the 1-based business execution number, including this one.
	Attempt int
	// MaxAttempts is the execution budget fixed at submission.
	MaxAttempts int
	// Timeout is how long this execution may run (zero: no limit).
	Timeout time.Duration
	// Snoozes is how many times the job was snoozed.
	Snoozes int
	// Deliveries is the number of physical deliveries of the job, including
	// this one.
	Deliveries int
	// EnqueuedAt is when the submission was prepared, by the producer's clock.
	EnqueuedAt time.Time
	// RunAt is the first execution time fixed at submission; zero means the
	// job was due immediately.
	RunAt time.Time
	// Metadata is a copy of the job's metadata owned by the handler.
	Metadata Metadata
	// ParentJobID is set on a job created by redrive.
	ParentJobID string
}

type infoKey struct{}

// JobInfo returns the [Info] of the job being processed by the handler that
// received ctx, or false outside a handler.
func JobInfo(ctx context.Context) (Info, bool) {
	info, ok := ctx.Value(infoKey{}).(Info)
	return info, ok
}
