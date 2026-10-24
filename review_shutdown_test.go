package jetq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestReviewBlockingFailureObserverDoesNotHangShutdown(t *testing.T) {
	d := newFakeDriver()
	c, e := NewClient(d, WithLogger(testLogger(t)))
	if e != nil {
		t.Fatal(e)
	}
	w := c.NewWorker(Queue{Name: "q"})
	w.SetShutdownTimeout(10 * time.Millisecond)
	w.abandonAfter = 10 * time.Millisecond
	w.settleTimeout = 20 * time.Millisecond
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	w.OnFailed(func(_ context.Context, _ Info, _ json.RawMessage, _ error) { close(entered); <-release })
	Handle(w, MustTask[int]("t", Policy{}), func(context.Context, int) error { return Permanent(errors.New("bad")) })
	d.add(testEnvelope(t, "review-failure", "t", testPolicy(1)), ExecutionState{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	d.push("review-failure")
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("failure observer did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Error("Worker.Run hung after shutdown because failure observer ignored cancellation")
	}
}
