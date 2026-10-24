package jetq

import (
	"context"
	"errors"
	"time"
)

// JobSnapshot is a point-in-time, driver-neutral view of one logical job. It
// never contains backend identifiers such as stream sequences or KV
// revisions: JobID is the only job identity.
type JobSnapshot struct {
	// Envelope is a copy of the job's submission-time envelope.
	Envelope Envelope
	State    JobState
	// Wait says why an awaiting job waits.
	Wait WaitReason
	// RunAt is the next (or first) execution time; zero for an immediately
	// due job. Terminal jobs have no run time.
	RunAt time.Time
	// Budget is the number of admitted executions minus refunds.
	Budget int
	// Attempts is the number of business executions the budget allowed.
	Attempts int
	// Deliveries counts physical deliveries, including this one.
	Deliveries int
	Snoozes    int
	// LastError is the failure text of the most recent failed execution.
	LastError string
	// PendingDecision is the recorded decision whose follow-up work is not
	// complete yet, if any.
	PendingDecision *Decision
	// Canceled reports that a cancellation was recorded.
	Canceled bool
	// UnknownSince is when a worker first found no handler; zero otherwise.
	UnknownSince time.Time
	// ParentJobID is set on jobs created by redrive.
	ParentJobID string
	// CreatedAt is when the job was submitted, by the producer's clock.
	CreatedAt time.Time
	// UpdatedAt is when the driver last changed the record.
	UpdatedAt time.Time
}

// JobQuery selects jobs for [Inspector.List].
type JobQuery struct {
	// Queue limits the result to one queue. Empty means all queues.
	Queue string
	// State limits the result to one logical state. Empty means all states.
	State JobState
	// FailedOnly selects jobs in the failed state.
	FailedOnly bool
	// After is the JobID after which the page starts (exclusive); use the
	// LastJobID of a page to fetch the next one.
	After string
	// Limit is the page size; zero means the driver's default.
	Limit int
}

// JobPage is one page of [Inspector.List], ordered by JobID.
type JobPage struct {
	Jobs []JobSnapshot
	// LastJobID is the JobID to pass as JobQuery.After for the next page; it
	// is empty when the page is the last one.
	LastJobID string
}

// QueueStats is a count of jobs waiting in one queue.
type QueueStats struct {
	Queue string
	// Ready counts jobs that are due now.
	Ready int
	// Delayed counts jobs awaiting a retry, snooze, release or schedule.
	Delayed int
	// Running counts jobs whose execution is in progress.
	Running int
	// Failed counts failed jobs that are still retained.
	Failed int
}

// Stats is a snapshot of the queue system.
type Stats struct {
	// Queues has one entry per queue with jobs, sorted by name.
	Queues []QueueStats
	// Succeeded and Canceled count retained terminal jobs.
	Succeeded int
	Canceled  int
}

// Inspector is the optional, driver-neutral inspection extension. Drivers
// implement it when they can read job state; an application checks for it
// rather than assuming it.
type Inspector interface {
	// GetJob returns the job with the given id, or false when no such job is
	// retained.
	GetJob(ctx context.Context, jobID string) (JobSnapshot, bool, error)
	// List returns one page of jobs matching q.
	List(ctx context.Context, q JobQuery) (JobPage, error)
	// Stats returns counts per queue and per terminal state.
	Stats(ctx context.Context) (Stats, error)
}

// ErrNotSupported is returned by an optional capability a driver does not
// implement.
var ErrNotSupported = errors.New("jetq: capability not supported by this driver")

// Inspector returns the driver's inspection extension, or [ErrNotSupported]
// when the driver does not implement it.
func (c *Client) Inspector() (Inspector, error) {
	if i, ok := c.driver.(Inspector); ok {
		return i, nil
	}
	return nil, ErrNotSupported
}

// Inspect returns a snapshot of one job through the driver's [Inspector].
func (c *Client) Inspect(ctx context.Context, jobID string) (JobSnapshot, bool, error) {
	i, err := c.Inspector()
	if err != nil {
		return JobSnapshot{}, false, err
	}
	return i.GetJob(ctx, jobID)
}

// ListJobs returns one page of jobs through the driver's [Inspector].
func (c *Client) ListJobs(ctx context.Context, q JobQuery) (JobPage, error) {
	i, err := c.Inspector()
	if err != nil {
		return JobPage{}, err
	}
	return i.List(ctx, q)
}

// Stat returns counters through the driver's [Inspector].
func (c *Client) Stat(ctx context.Context) (Stats, error) {
	i, err := c.Inspector()
	if err != nil {
		return Stats{}, err
	}
	return i.Stats(ctx)
}
