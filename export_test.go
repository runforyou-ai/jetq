package jetq

import "time"

// SetSettleTimeout overrides the settlement timeout for a test and returns a restore function.
func SetSettleTimeout(d time.Duration) func() {
	previous := settleTimeout
	settleTimeout = d
	return func() { settleTimeout = previous }
}
