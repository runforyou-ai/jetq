package jobq

import "time"

func ReviewSettleTimeout(d time.Duration) func() {
	old := settleTimeout
	settleTimeout = d
	return func() { settleTimeout = old }
}
