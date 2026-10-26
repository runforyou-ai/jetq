package jobq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
)

func TestReviewBlockedFailureCallback(t *testing.T) {
	restore := jobq.ReviewSettleTimeout(50 * time.Millisecond)
	defer restore()
	c := newClient(t)
	w := c.NewWorker(jobq.Queue{Name: "default"})
	w.SetShutdownTimeout(0)
	entered := make(chan struct{})
	unblock := make(chan struct{})
	defer close(unblock)
	jobq.Handle(w, func(context.Context, email) error { return jobq.Permanent(errors.New("failed")) }, jobq.OnFailure(func(context.Context, email, error) { close(entered); <-unblock }))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	if _, err := c.Enqueue(ctx, email{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("callback not called")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run blocked by failure callback despite shutdown cancellation")
	}
}
