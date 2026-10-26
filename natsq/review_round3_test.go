package natsq_test

import (
	"context"
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/natsq"
)

func TestReviewR3ScheduleRetryLostAck(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	m.ID = "s-retry-complete"
	if err := natsq.ReviewPublishOccurrence(ctx, d, m); err != nil {
		t.Fatal(err)
	}
	first := claim(t, d, 2*time.Second)
	natsq.ReviewDropAck(first)
	if err := first.Retry(ctx, time.Now(), "temporary"); err != nil {
		t.Fatal(err)
	}
	second := claim(t, d, 2*time.Second)
	if second.Attempt() != 2 {
		t.Fatalf("retry attempt=%d", second.Attempt())
	}
	if err := second.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2200 * time.Millisecond)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ls, err := d.Claim(c, "q", 1, time.Second)
	if len(ls) > 0 {
		t.Fatalf("completed occurrence resurrected after retry: id=%s attempt=%d err=%v", ls[0].Message().ID, ls[0].Attempt(), err)
	}
}

func TestReviewR3ExtendedLeaseEarlyRedelivery(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, m); err != nil {
		t.Fatal(err)
	}
	short := claim(t, d, time.Second)
	if err := short.Release(ctx, jobq.Release{}); err != nil {
		t.Fatal(err)
	}
	long := claim(t, d, 3*time.Second)
	time.Sleep(2 * time.Second)
	if err := long.Extend(ctx); err != nil {
		t.Fatal(err)
	}
	// The KV lease is valid until t=5s; JetStream may redeliver at t=3s.
	time.Sleep(1200 * time.Millisecond)
	c, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	ls, err := d.Claim(c, "q", 1, time.Second)
	if len(ls) > 0 {
		t.Fatalf("active extended lease claimed twice: attempt=%d err=%v", ls[0].Attempt(), err)
	}
	if err := long.Complete(ctx); err != nil {
		t.Fatal(err)
	}
}
