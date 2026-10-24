package jetq

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

type reviewThirtyMillisecondLease struct{ *reviewDlv }

func (d *reviewThirtyMillisecondLease) LeaseDuration() time.Duration { return 30 * time.Millisecond }
func TestReviewNoTimeoutJobRenewsPastShutdownBudget(t *testing.T) {
	c, _ := reviewClient(t)
	w := c.NewWorker(Queue{Name: "q"})
	w.SetShutdownTimeout(5 * time.Millisecond)
	w.abandonAfter = 5 * time.Millisecond
	w.settleTimeout = 5 * time.Millisecond
	w.callbackTimeout = 5 * time.Millisecond
	d := &reviewThirtyMillisecondLease{&reviewDlv{env: &Envelope{JobID: "long-running", Task: "review", Queue: "q", Payload: json.RawMessage("1"), Policy: ExecutionPolicy{MaxAttempts: 3, Backoff: ConstantBackoff(time.Hour)}, CreatedAt: time.Now()}, state: ExecutionState{Deliveries: 1}}}
	var halfway atomic.Int32
	Handle(w, MustTask[int]("review", Policy{}), func(context.Context, int) error {
		time.Sleep(100 * time.Millisecond)
		halfway.Store(d.extends.Load())
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	w.process(context.Background(), context.Background(), &stopper{}, Queue{Name: "q"}, d)
	if d.extends.Load() <= halfway.Load() {
		t.Fatalf("healthy job with no timeout stopped renewing at shutdown budget: halfway=%d final=%d", halfway.Load(), d.extends.Load())
	}
}
