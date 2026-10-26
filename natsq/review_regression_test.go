package natsq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/natsq"
)

func TestReviewScheduleLostAck(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	m.ID = "s-123"
	if err := natsq.ReviewPublishOccurrence(ctx, d, m); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Second)
	natsq.ReviewDropAck(l)
	if err := l.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ls, _ := d.Claim(c, "q", 1, time.Second)
	for _, v := range ls {
		if v.Message().ID == m.ID {
			t.Fatalf("completed occurrence executed again: id=%s attempt=%d", v.Message().ID, v.Attempt())
		}
	}
}
func TestReviewCrashDuringRequeue(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, m); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Second)
	// Simulate exact durable state after requeue CAS, before publish.
	if err := natsq.ReviewStageRequeue(ctx, l); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ls, _ := d.Claim(c, "q", 1, time.Second)
	if len(ls) == 0 {
		t.Fatal("accepted job disappeared after requeue CAS; original delivery discarded as stale")
	}
}
func TestReviewExpiredUniqueOwner(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	natsq.ReviewUniqueTTL(d, time.Second)
	a, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"), jobq.Unique("key"))
	if err := d.Enqueue(ctx, a); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Minute)
	time.Sleep(2200 * time.Millisecond)
	b, _ := jobq.NewMessage("x", 2, jobq.OnQueue("q"), jobq.Unique("key"))
	if err := d.Enqueue(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := l.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := jobq.NewMessage("x", 3, jobq.OnQueue("q"), jobq.Unique("key"))
	if err := d.Enqueue(ctx, c); !errors.Is(err, jobq.ErrDuplicate) {
		t.Fatalf("old owner deleted new owner's lock: enqueue=%v", err)
	}
}
func TestReviewUncertainRetryPublish(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, m); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Second)
	err := natsq.ReviewRetryPublishTimeout(ctx, d, l)
	t.Logf("Retry returned %v", err)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ls, _ := d.Claim(c, "q", 1, time.Second)
	if len(ls) == 0 {
		t.Fatal("retry publish timed out before storing, but old message acked and job lost")
	}
}

// A requeue that recorded its intent but never published: the old delivery
// comes back and must count as exactly one more attempt.
func TestRequeuePendingCountsOneAttempt(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, m); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Second)
	if err := natsq.ReviewStageRequeue(ctx, l); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ls, _ := d.Claim(c, "q", 1, time.Minute)
	if len(ls) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(ls))
	}
	if ls[0].Attempt() != 2 {
		t.Fatalf("attempt = %d, want 2", ls[0].Attempt())
	}
}

// A requeue that published the new copy but crashed before switching the
// record: the job runs once, however the two copies are delivered.
func TestRequeueCrashAfterPublishRunsOnce(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, m); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Second)
	if err := natsq.ReviewStageRequeuePublished(ctx, l); err != nil {
		t.Fatal(err)
	}
	first := claim(t, d, time.Minute)
	// While the first claim runs, the other copy must not be handed out.
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for c.Err() == nil {
		if ls, _ := d.Claim(c, "q", 1, time.Minute); len(ls) > 0 {
			t.Fatalf("second copy claimed while the first runs: attempt %d", ls[0].Attempt())
		}
	}
	if err := first.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	// After completion nothing is left to run.
	c2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()
	for c2.Err() == nil {
		if ls, _ := d.Claim(c2, "q", 1, time.Minute); len(ls) > 0 {
			t.Fatalf("job ran again after completion: attempt %d", ls[0].Attempt())
		}
	}
}
