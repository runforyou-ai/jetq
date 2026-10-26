package natsq_test

import (
	"errors"
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/natsq"
)

func TestReviewR5ReusedIDFencesOldLease(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	a, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"), jobq.JobID("reused"))
	if err := d.Enqueue(ctx, a); err != nil {
		t.Fatal(err)
	}
	old := claim(t, d, time.Second)
	time.Sleep(1200 * time.Millisecond)
	recovered := claim(t, d, time.Minute)
	if err := recovered.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	b, _ := jobq.NewMessage("x", 2, jobq.OnQueue("q"), jobq.JobID(a.ID))
	if err := d.Enqueue(ctx, b); err != nil {
		t.Fatal(err)
	}
	fresh := claim(t, d, time.Minute)
	err := old.Complete(ctx)
	if !errors.Is(err, jobq.ErrLeaseLost) {
		t.Errorf("old lease completed replacement job: err=%v", err)
	}
	if err := fresh.Complete(ctx); err != nil {
		t.Errorf("replacement lease corrupted: %v", err)
	}
}
func TestReviewR5RedriveFailureStillHasDeadLetter(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	m, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := claim(t, d, time.Minute).Fail(ctx, "first failure"); err != nil {
		t.Fatal(err)
	}
	if err := d.RetryDead(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	if err := claim(t, d, time.Minute).Fail(ctx, "second failure"); err != nil {
		t.Fatal(err)
	}
	page, err := d.DeadLetters(ctx, jobq.DeadLetterQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.DeadLetters) != 1 {
		t.Fatalf("redriven job failed but dead letter missing: count=%d", len(page.DeadLetters))
	}
	if page.DeadLetters[0].Error != "second failure" {
		t.Fatalf("wrong failure: %s", page.DeadLetters[0].Error)
	}
}
func TestReviewR5DeleteDeadBeyondScanWindow(t *testing.T) {
	d := newDriver(t, startServer(t))
	ctx := t.Context()
	id, err := natsq.ReviewR5SeedDead(ctx, d, 1001)
	if err != nil {
		t.Fatal(err)
	}
	page, err := d.DeadLetters(ctx, jobq.DeadLetterQuery{Limit: 1100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.DeadLetters) != 1001 {
		t.Fatalf("seed count=%d", len(page.DeadLetters))
	}
	if err := d.DeleteDead(ctx, id); err != nil {
		t.Fatalf("existing dead letter beyond newest 1000 cannot be deleted: %v", err)
	}
}
