package natsq_test

import (
	"context"
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/natsq"
)

func TestReviewR2RequeueAckAfterConcurrentClaim(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, m); err != nil {
		t.Fatal(err)
	}
	old := claim(t, d, time.Second)
	entered, resume, restore := natsq.ReviewPausePublish(d)
	done := make(chan error, 1)
	go func() { done <- old.Retry(ctx, time.Now(), "retry") }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("publish not reached")
	}
	time.Sleep(1200 * time.Millisecond)
	newer := claim(t, d, time.Second)
	t.Logf("old delivery claimed again, attempt=%d", newer.Attempt())
	resume()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	restore()
	// Newer worker now crashes. The old delivery must remain recoverable.
	time.Sleep(1200 * time.Millisecond)
	c, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	ls, err := d.Claim(c, "q", 1, time.Second)
	if len(ls) == 0 {
		t.Fatalf("newer worker crashed but task disappeared: requeue ACK removed its delivery, new generation discarded; err=%v", err)
	}
}
func TestReviewR2FailedScheduleLostAck(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	m.ID = "s-failed"
	if err := natsq.ReviewPublishOccurrence(ctx, d, m); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Second)
	natsq.ReviewDropAck(l)
	if err := l.Fail(ctx, "permanent"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ls, _ := d.Claim(c, "q", 1, time.Second)
	if len(ls) > 0 {
		t.Fatalf("dead-lettered occurrence resurrected: id=%s attempt=%d", ls[0].Message().ID, ls[0].Attempt())
	}
}
