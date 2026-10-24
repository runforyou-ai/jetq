package jetq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// observerRecords collects the Info a failure observer sees.
type observerRecords struct {
	infos chan Info
	runs  atomic.Int32
}

func newObserverWorker(t *testing.T, d *fakeDriver, handler func() error) (*Worker, *observerRecords) {
	t.Helper()
	w := newWorker(t, d, Queue{Name: "q"})
	rec := &observerRecords{infos: make(chan Info, 4)}
	HandleTask(w, "t", func(ctx context.Context, v int) error {
		rec.runs.Add(1)
		return handler()
	}, OnFailure(func(ctx context.Context, job int, err error) {
		info, _ := JobInfo(ctx)
		rec.infos <- info
	}))
	return w, rec
}

// TestFailureObserverSeesBudgetOnExhaustedPath: a delivery whose budget is
// already spent fails without running the handler, and its failure observer
// sees the recorded attempt count, not zero.
func TestFailureObserverSeesBudgetOnExhaustedPath(t *testing.T) {
	d := newFakeDriver()
	w, rec := newObserverWorker(t, d, func() error { return nil })
	d.add(testEnvelope(t, "job-1", "t", testPolicy(2)), ExecutionState{Budget: 2, Deliveries: 3})
	startWorker(t, w)
	d.push("job-1")
	select {
	case info := <-rec.infos:
		if info.Attempt != 2 || info.MaxAttempts != 2 {
			t.Fatalf("observer attempt = %d of %d, want 2 of 2", info.Attempt, info.MaxAttempts)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure observer did not run")
	}
	if rec.runs.Load() != 0 {
		t.Fatalf("handler ran %d times with an exhausted budget", rec.runs.Load())
	}
}

// TestFailureObserverSeesBudgetOnPendingFail: a recorded fail decision is
// resumed without running the handler, and its failure observer sees the
// attempt that failed.
func TestFailureObserverSeesBudgetOnPendingFail(t *testing.T) {
	d := newFakeDriver()
	w, rec := newObserverWorker(t, d, func() error { return nil })
	pending := Decision{Kind: DecideFail, TransitionID: "exec-1.fail", ExecutionID: "exec-1", Error: "boom"}
	d.add(testEnvelope(t, "job-1", "t", testPolicy(2)), ExecutionState{Budget: 2, Pending: &pending})
	startWorker(t, w)
	d.push("job-1")
	select {
	case info := <-rec.infos:
		if info.Attempt != 2 {
			t.Fatalf("observer attempt = %d, want 2", info.Attempt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure observer did not run")
	}
	if rec.runs.Load() != 0 {
		t.Fatalf("handler re-ran %d times for a recorded decision", rec.runs.Load())
	}
}

// TestObserverAttemptAfterAdmission: the admitting path still reports the
// admission's attempt.
func TestObserverAttemptAfterAdmission(t *testing.T) {
	d := newFakeDriver()
	w, rec := newObserverWorker(t, d, func() error { return errBoom })
	d.add(testEnvelope(t, "job-1", "t", testPolicy(1)), ExecutionState{})
	startWorker(t, w)
	d.push("job-1")
	select {
	case info := <-rec.infos:
		if rec.runs.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", rec.runs.Load())
		}
		if info.Attempt != 1 {
			t.Fatalf("observer attempt = %d, want 1", info.Attempt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure observer did not run")
	}
}
