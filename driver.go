package jobq

import (
	"context"
	"time"
)

// Driver stores jobs and hands them out to workers. Driver packages such as
// jobq/natsq and jobq/pgq implement it; this package never imports a driver.
//
// A driver only records state transitions. Retry decisions, backoff,
// timeouts, snoozes and shutdown are made by the [Worker], so they behave the
// same on every driver. jobq/drivertest checks the contract.
//
// Optional capabilities are separate interfaces detected with a type
// assertion: [Scheduler] and [Inspector].
type Driver interface {
	// Enqueue durably stores msg. It returns ErrJobIDInUse when an
	// unfinished job has msg.ID, and ErrDuplicate when msg.UniqueKey is held
	// by another job (see [Message.UniqueKey]). An error wrapping
	// ErrUncertain means msg may have been stored; any other error means it
	// was not. Enqueue must not modify msg.
	Enqueue(ctx context.Context, msg *Message) error

	// Claim leases up to max jobs of queue that are due, for lease. It may
	// wait for jobs to become due, but returns within a few seconds even
	// when none are, with an empty slice and a nil error; it returns
	// ctx.Err() once ctx is done. A job whose lease expired without being
	// settled is due again; claiming it counts as a new attempt.
	Claim(ctx context.Context, queue string, max int, lease time.Duration) ([]Lease, error)

	// Cancel removes the job with id so that it does not run again: a job
	// waiting for its run time, a retry or a snooze. Drivers may also
	// support cancelling ready jobs. A running job finishes its current
	// attempt but is not retried. Cancel releases the job's unique key and
	// returns ErrNotFound when no cancelable job has id.
	Cancel(ctx context.Context, id string) error

	// Close releases the driver's resources. It does not close connections
	// passed in by the application.
	Close() error
}

// Lease is one claim of a job by a worker. Every method is fenced on this
// claim: once the lease expired and the job may be claimed again, methods
// return ErrLeaseLost when the driver can tell, and in no case alter a newer
// claim. Settling (Complete, Retry, Release, Fail) ends the lease; later
// calls return ErrLeaseLost. Methods may be called concurrently with Extend.
type Lease interface {
	// Message returns the claimed job. The caller must not modify it.
	Message() *Message
	// Attempt is the 1-based attempt of this claim: earlier claims that were
	// retried or whose lease expired count, released ones do not.
	Attempt() int
	// Extend renews the lease for the duration given to Claim.
	Extend(ctx context.Context) error
	// Complete marks the job succeeded and releases its unique key, unless
	// the key was taken with a window ([Message.UniqueFor]).
	Complete(ctx context.Context) error
	// Retry makes the job due again at at, keeping this attempt counted and
	// recording reason as its last error.
	Retry(ctx context.Context, at time.Time, reason string) error
	// Release makes the job due again without counting this attempt.
	Release(ctx context.Context, r Release) error
	// Fail moves the job to the dead letters with reason as its error and
	// releases its unique key.
	Fail(ctx context.Context, reason string) error
}

// Release describes how a job is put back by [Lease.Release].
type Release struct {
	// At is when the job is due again; zero or past means now.
	At time.Time
	// Snooze increments [Message.Snoozes].
	Snooze bool
	// UnknownSince replaces [Message.UnknownSince].
	UnknownSince time.Time
}

// NoTimeout as [Message.Timeout] runs the job without a time limit.
const NoTimeout time.Duration = -1

// Message is a job as stored by a driver. Fields marked as state are
// maintained by the driver; [Client.Enqueue] leaves them zero.
type Message struct {
	// ID identifies the job. Drivers may assume [ValidName].
	ID string
	// Name is the stable job name used to find its handler.
	Name string
	// Queue is the queue the job runs on, a [ValidName].
	Queue string
	// Payload is the encoded job, stored byte for byte.
	Payload []byte
	// Header carries application metadata such as trace context.
	Header map[string]string
	// RunAt is when the job becomes due; zero means at once. Drivers may
	// round it to their scheduling resolution.
	RunAt time.Time
	// EnqueuedAt is when the job was enqueued, by the producer's clock. It
	// is zero for jobs fired by a recurring schedule.
	EnqueuedAt time.Time
	// MaxAttempts overrides [Queue.MaxAttempts] when positive.
	MaxAttempts int
	// Timeout overrides [Queue.Timeout] when non-zero; [NoTimeout] means no
	// limit.
	Timeout time.Duration
	// UniqueKey, when set, is held by this job: enqueueing another job with
	// the same key returns ErrDuplicate. With UniqueFor zero, the key is
	// held until the job succeeds, is dead-lettered or is cancelled.
	UniqueKey string
	// UniqueFor, when positive, holds UniqueKey for this long after
	// EnqueuedAt instead, whatever happens to the job.
	UniqueFor time.Duration

	// Snoozes counts snoozes (state).
	Snoozes int
	// UnknownSince is when a worker first found no handler for the job (state).
	UnknownSince time.Time
}

// Clone returns a deep copy of m.
func (m *Message) Clone() *Message {
	c := *m
	c.Payload = append([]byte(nil), m.Payload...)
	if m.Header != nil {
		c.Header = make(map[string]string, len(m.Header))
		for k, v := range m.Header {
			c.Header[k] = v
		}
	}
	return &c
}

// Scheduler is the optional capability of drivers that fire recurring
// schedules. A schedule fires each occurrence once however many processes
// run, and does not replay occurrences missed while nothing ran.
type Scheduler interface {
	// SyncSchedules makes the stored schedules match schedules exactly.
	// Unchanged schedules keep their timing. Jobs fired by schedule key get
	// the id "<key>-<suffix>", unique per occurrence.
	SyncSchedules(ctx context.Context, schedules []ScheduleSpec) error
}

// ScheduleSpec is a validated recurring schedule passed to a [Scheduler].
type ScheduleSpec struct {
	// Key identifies the schedule, a [ValidName].
	Key string
	// Cron is a six-field cron expression (with seconds), a descriptor such
	// as "@daily" or "@every <duration>", as parsed by [ParseCron].
	Cron string
	// Timezone is an IANA time zone name; empty means UTC.
	Timezone string
	// Message is the job each occurrence enqueues. ID, RunAt and EnqueuedAt
	// are zero.
	Message *Message
}

// Inspector is the optional capability of drivers that report queue state
// and manage dead letters.
type Inspector interface {
	// Stats counts jobs per queue. Counts may be approximate.
	Stats(ctx context.Context) (Stats, error)
	// DeadLetters lists dead-lettered jobs, newest first.
	DeadLetters(ctx context.Context, q DeadLetterQuery) (DeadLetterPage, error)
	// RetryDead moves the dead-lettered job id back to its queue as a new
	// job with the same id and fresh attempts. It returns ErrNotFound when
	// no dead letter has id, and ErrJobIDInUse when an unfinished job has it.
	RetryDead(ctx context.Context, id string) error
	// DeleteDead removes the dead-lettered job id, or ErrNotFound.
	DeleteDead(ctx context.Context, id string) error
}
