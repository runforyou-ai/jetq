package jetq

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// SetSettleTimeout overrides the settlement timeout for a test and returns a restore function.
func SetSettleTimeout(d time.Duration) func() {
	previous := settleTimeout
	settleTimeout = d
	return func() { settleTimeout = previous }
}

// SetAbandonAfter overrides how long a cancelled handler may take to return for a test.
func SetAbandonAfter(d time.Duration) func() {
	previous := abandonAfter
	abandonAfter = d
	return func() { abandonAfter = previous }
}

// SetUnknownJobRetry overrides the delay before a job without a handler is
// offered again for a test.
func SetUnknownJobRetry(d time.Duration) func() {
	previous := unknownJobRetry
	unknownJobRetry = d
	return func() { unknownJobRetry = previous }
}

// CancelledCopy reports whether a copy of delayed job id fired by its schedule
// with the given enqueue time would be skipped as cancelled.
func CancelledCopy(c *Client, id, enqueuedAt string) bool {
	header := nats.Header{}
	header.Set(headerScheduler, c.delaySubject(id))
	header.Set(HeaderEnqueuedAt, enqueuedAt)
	cancelled, err := c.cancelled(context.Background(), header, id, false)
	return err == nil && cancelled
}

// StateLeaderReadsDirect reports whether the leader handle of the state bucket
// still uses direct gets, which would make leader reads fall back to followers.
func StateLeaderReadsDirect(c *Client) (bool, error) {
	state, err := c.stateBucket(context.Background(), true)
	if err != nil {
		return false, err
	}
	stream, err := c.stateLeaderStream(context.Background(), state.Bucket())
	if err != nil {
		return false, err
	}
	return stream.CachedInfo().Config.AllowDirect, nil
}
