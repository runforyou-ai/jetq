package jetq

import (
	"errors"
	"time"
)

// Backoff returns how long to wait before the next attempt after attempt
// (1-based) failed.
type Backoff func(attempt int) time.Duration

// Exponential doubles the delay after every failed attempt, starting at base
// and capped at limit. A non-positive base means one second; a limit below
// base means base.
func Exponential(base, limit time.Duration) Backoff {
	if base <= 0 {
		base = time.Second
	}
	limit = max(limit, base)
	return func(attempt int) time.Duration {
		delay := base
		for i := 1; i < attempt && delay < limit; i++ {
			if delay > limit/2 {
				delay = limit
			} else {
				delay *= 2
			}
		}
		return delay
	}
}

// Constant waits the same delay after every failed attempt. A negative delay means zero.
func Constant(delay time.Duration) Backoff {
	delay = max(delay, 0)
	return func(int) time.Duration { return delay }
}

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
// snoozed job keeps its id and can be cancelled with [Client.Cancel].
func Snooze(delay time.Duration) error { return &snoozeError{delay: delay} }

// RetryAfter fails the attempt and asks for the next attempt after delay
// instead of the queue's backoff. The attempt still counts towards the limit.
func RetryAfter(delay time.Duration, err error) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, delay: delay}
}
