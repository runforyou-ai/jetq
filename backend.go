package jetq

import (
	"context"
	"errors"
	"time"
)

// Driver is the storage contract implemented by backend packages such as
// jetq/natsq. The root package defines it and never imports a driver.
type Driver interface {
	// Submit durably stores a prepared job, applying EnqueueKey idempotency
	// and the unique rule. On an uncertain outcome it returns Status Unknown,
	// the candidate JobID and an error wrapping ErrUncertain.
	Submit(ctx context.Context, job *PreparedJob) (EnqueueResult, error)
	// Consumer attaches a local consumer of one queue. Closing it never
	// deletes shared durable resources.
	Consumer(ctx context.Context, queue string) (Consumer, error)
}

// Consumer receives deliveries for one queue on behalf of one worker.
type Consumer interface {
	// Receive waits for up to max deliveries. It may return deliveries
	// together with an error; the caller owns every returned delivery and
	// must settle or release each one.
	Receive(ctx context.Context, max int) ([]Delivery, error)
	// Close stops local delivery. It does not delete shared resources nor
	// close connections passed in by the application.
	Close() error
}

// Delivery is one physical delivery of a job. Its claim credentials are
// internal to the driver.
type Delivery interface {
	// Envelope returns the job. The caller must not modify it.
	Envelope() *Envelope
	// State returns the durable execution state as recorded by the driver
	// when the delivery was received.
	State() ExecutionState
	// Admit durably records the start of a business execution and spends one
	// unit of budget, before the handler runs. It returns the recorded
	// ExecutionID. When the delivery already carries a recorded decision
	// (State().Pending != nil), Admit must not be called.
	Admit(ctx context.Context) (Admission, error)
	// Extend keeps the claim alive. ErrClaimLost means the driver knows the
	// claim is gone; other errors are transient.
	Extend(ctx context.Context) error
	// Settle persists the decision idempotently by TransitionID, then
	// performs follow-up work. See [SettleStatus].
	Settle(ctx context.Context, d Decision) (SettleStatus, error)
	// Release gives the delivery back without a decision, for deliveries
	// taken during shutdown before admission. It never spends budget.
	Release(ctx context.Context) error
	// LeaseDuration is how long the claim lasts without Extend.
	LeaseDuration() time.Duration
}

// ExecutionState is the durable per-job execution record visible to the
// engine. Drivers may store it in any layout.
type ExecutionState struct {
	// Budget is the number of admitted executions minus refunds.
	Budget int
	// Snoozes is the number of recorded snoozes.
	Snoozes int
	// Deliveries counts physical deliveries, including this one.
	Deliveries int
	// Admitted is the execution admitted by an earlier delivery whose
	// decision was never recorded (crash after admission). The engine counts
	// it as spent and admits a new execution.
	Admitted string
	// Pending is a recorded decision whose follow-up work is incomplete.
	// The engine resumes it with Settle and does not run the handler.
	Pending *Decision
	// Canceled means a cancellation was recorded; the engine settles the
	// delivery as canceled without running the handler.
	Canceled bool
	// UnknownSince is when a worker first found no handler, zero otherwise.
	UnknownSince time.Time
}

// Admission is the result of a successful [Delivery.Admit].
type Admission struct {
	ExecutionID string
	// Attempt is the budget used including this execution.
	Attempt int
}

// ErrClaimLost is returned when the driver can establish that the delivery
// no longer holds its claim. Drivers report it only when they can confirm
// that; a transient error must not be reported as a lost claim.
var ErrClaimLost = errors.New("jetq: delivery claim lost")

// PreparedJob is a validated submission ready for a driver. Prepare it once
// and pass the same value to every retry of an idempotent submission, so the
// absolute run time and generated JobID stay unchanged.
type PreparedJob struct {
	Envelope Envelope
	// Fingerprint is Envelope.Fingerprint(), computed at preparation.
	Fingerprint string
}

// Canceler is the optional cancellation extension.
type Canceler interface {
	// Cancel prevents future executions of an awaiting job. A running
	// execution is not interrupted; its later retries do not run.
	Cancel(ctx context.Context, jobID string) error
}

// Maintainer is the optional driver extension for maintenance that must run
// in every process that serves a queue, for example repairing state left by a
// crashed process or by a lost wake-up. A [Worker] starts it automatically
// and stops it with its own context, so an application never has to call a
// driver's recovery loop to get correct behaviour.
type Maintainer interface {
	// Maintain repairs durable state until ctx is cancelled. Returning
	// ctx.Err() on cancellation is expected; any other error is logged and
	// maintenance is retried.
	Maintain(ctx context.Context) error
}
