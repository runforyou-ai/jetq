package natsq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/natsq"
)

func TestReviewR4UniqueWindowCannotBeShortened(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	a, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"), jobq.UniqueFor("shared", time.Minute))
	if err := d.Enqueue(ctx, a); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	b, _ := jobq.NewMessage("x", 2, jobq.OnQueue("q"), jobq.UniqueFor("shared", time.Second))
	if err := d.Enqueue(ctx, b); !errors.Is(err, jobq.ErrDuplicate) {
		t.Fatalf("1-minute unique window stolen after 1.2s by shorter incoming window: err=%v", err)
	}
}

func TestReviewR4CancelPendingScheduledRetryLostAck(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	m.ID = "s-canceled"
	if err := natsq.ReviewPublishOccurrence(ctx, d, m); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Second)
	natsq.ReviewDropAck(l)
	if err := l.Retry(ctx, time.Now().Add(time.Hour), "temporary"); err != nil {
		t.Fatal(err)
	}
	if err := d.Cancel(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ls, err := d.Claim(c, "q", 1, time.Second)
	if len(ls) > 0 {
		t.Fatalf("canceled occurrence resurrected: id=%s attempt=%d err=%v", ls[0].Message().ID, ls[0].Attempt(), err)
	}
}
