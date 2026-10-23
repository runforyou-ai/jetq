package jetq

import (
	"errors"
	"time"
)

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not retryable: the job is dead-lettered immediately.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was marked with [Permanent].
func IsPermanent(err error) bool {
	var target *permanentError
	return errors.As(err, &target)
}

type retryAfterError struct {
	err   error
	delay time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

type snoozeError struct{ delay time.Duration }

func (e *snoozeError) Error() string { return "jetq: job snoozed for " + e.delay.String() }

// Snooze puts the job back on its queue to run again after delay without
// counting the current attempt, for example while a tenant is paused. The
// snoozed job keeps its id and can be cancelled with [Client.Cancel];
// [Info.Snoozes] counts how often it was snoozed, so a handler can give up.
// If the updated execution state cannot be recorded, the delivery is offered
// again after delay instead and that counts as an attempt.
func Snooze(delay time.Duration) error { return &snoozeError{delay: delay} }

// RetryAfter fails the attempt and asks for the next attempt after delay
// instead of the queue's backoff. The attempt still counts towards the limit.
// Like every retry, the job waits as a delayed job and can be cancelled with
// [Client.Cancel].
func RetryAfter(delay time.Duration, err error) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, delay: delay}
}
