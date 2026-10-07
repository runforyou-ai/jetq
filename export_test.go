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

// CancelledCopy reports whether a copy of delayed job id fired by its schedule
// with the given enqueue time would be skipped as cancelled.
func CancelledCopy(c *Client, id, enqueuedAt string) bool {
	header := nats.Header{}
	header.Set(headerScheduler, c.delaySubject(id))
	header.Set(HeaderEnqueuedAt, enqueuedAt)
	cancelled, err := c.cancelled(context.Background(), header, id)
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
