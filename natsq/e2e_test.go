package natsq_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
	"github.com/runforyou-ai/jetq/jetqtest"
)

type invoice struct {
	ID     int    `json:"id"`
	Amount string `json:"amount"`
}

// TestEndToEndTypedProducerAndWorker is the real broker end-to-end test: a
// typed producer, a worker without the producer's task declaration, retry,
// snooze, unknown task and cancellation all on a live server.
func TestEndToEndTypedProducerAndWorker(t *testing.T) {
	s := jetqtest.Start(t, "e2e")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer := s.Client(t, jetq.WithDefaultQueue("billing"))
	task := jetq.MustTask[invoice]("invoice.paid", jetq.Policy{MaxAttempts: 3, Backoff: ptr(jetq.ConstantBackoff(10 * time.Millisecond))})
	res, err := jetq.SubmitTask(ctx, producer, task, invoice{ID: 7, Amount: "10.00"}, jetq.Submission{})
	if err != nil || res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v, %v", res, err)
	}

	// The consumer only knows the task name and the payload type.
	client := s.Client(t)
	worker := client.NewWorker(jetq.Queue{Name: "billing", Concurrency: 2})
	var attempts atomic.Int32
	var snoozed atomic.Bool
	var got invoice
	jetq.HandleTask(worker, "invoice.paid", func(ctx context.Context, job invoice) error {
		attempts.Add(1)
		got = job
		if !snoozed.Load() {
			snoozed.Store(true)
			return jetq.Snooze(20 * time.Millisecond)
		}
		return nil
	})
	// A task this worker does not know stays deferred, not failed.
	raw := jetq.Raw("invoice.unknown")
	if _, err := producer.SubmitRaw(ctx, raw, json.RawMessage(`{"x":1}`), jetq.Submission{Queue: "billing"}); err != nil {
		t.Fatal(err)
	}

	worker.SetUnknownTaskTimeout(time.Minute)
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		state, ok, err := s.Driver.JobState(ctx, res.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if ok && state == jetq.StateSucceeded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("worker: %v", err)
	}
	// The worker's context is now cancelled, but the job already succeeded;
	// read it on a live context.
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	state, ok, err := s.Driver.JobState(readCtx, res.JobID)
	if err != nil || !ok || state != jetq.StateSucceeded {
		t.Fatalf("state = %s, %v, %v", state, ok, err)
	}
	if got != (invoice{ID: 7, Amount: "10.00"}) {
		t.Fatalf("payload = %+v", got)
	}
	if n := attempts.Load(); n != 2 {
		t.Fatalf("attempts = %d (snooze then success)", n)
	}
	// The snooze refunded the first execution, so only one attempt was spent;
	// the neutral snapshot reports it.
	snap, found, err := s.Driver.GetJob(readCtx, res.JobID)
	if err != nil || !found {
		t.Fatalf("snapshot: %v, %v", found, err)
	}
	if snap.Budget != 1 || snap.Snoozes != 1 || snap.Attempts != 3 {
		t.Fatalf("snapshot = %+v", snap)
	}
}

// TestEndToEndCancelAndUnique: a cancel prevents a delayed job from running,
// and an until-done unique key is released by a success.
func TestEndToEndCancelAndUnique(t *testing.T) {
	s := jetqtest.Start(t, "e2e2")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer := s.Client(t)
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "account-1"}
	mk := func(id string, delay time.Duration) *jetq.PreparedJob {
		job, err := producer.Prepare("report.build", json.RawMessage(`{}`), jetq.Submission{
			Queue: "reports", JobID: id, Delay: delay, Unique: unique,
		})
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	first := mk("job-1", time.Hour)
	if res, err := producer.SubmitPrepared(ctx, first); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v, %v", res, err)
	}
	// The same prepared job submitted again is the same job, not a duplicate.
	if res, err := producer.SubmitPrepared(ctx, first); err != nil {
		t.Fatal(err)
	} else if res.Status != jetq.AlreadyAccepted || res.JobID != "job-1" {
		t.Fatalf("repeat = %+v", res)
	}
	if res, err := producer.SubmitPrepared(ctx, mk("job-2", time.Hour)); err != nil {
		t.Fatal(err)
	} else if res.Status != jetq.Suppressed || res.ExistingJobID != "job-1" {
		t.Fatalf("unique = %+v", res)
	}
	if err := s.Driver.Cancel(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	c := s.Client(t)
	w := c.NewWorker(jetq.Queue{Name: "reports"})
	var runs atomic.Int32
	jetq.HandleTask(w, "report.build", func(ctx context.Context, v json.RawMessage) error {
		runs.Add(1)
		return nil
	})
	wctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	// The cancelled job is not delivered, and its key is free again.
	var replacement string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		res, err := producer.SubmitPrepared(ctx, mk("job-3", 0))
		if err != nil {
			t.Fatal(err)
		}
		if res.Status == jetq.Accepted {
			replacement = res.JobID
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if replacement == "" {
		t.Fatal("unique key was not released by the cancellation")
	}
	for time.Now().Before(deadline) {
		if state, ok, _ := s.Driver.JobState(ctx, replacement); ok && state == jetq.StateSucceeded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if runs.Load() != 1 {
		t.Fatalf("runs = %d, want only the replacement job", runs.Load())
	}
	if state, _, _ := s.Driver.JobState(context.Background(), "job-1"); state != jetq.StateCanceled {
		t.Fatalf("canceled job state = %s", state)
	}
}

func ptr(b jetq.BackoffSpec) *jetq.BackoffSpec { return &b }
