package jetq

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

type reviewCapture struct{ job *PreparedJob }

func (d *reviewCapture) Submit(_ context.Context, j *PreparedJob) (EnqueueResult, error) {
	d.job = j
	return EnqueueResult{Status: Accepted, JobID: j.Envelope.JobID}, nil
}
func (d *reviewCapture) Consumer(context.Context, string) (Consumer, error) {
	return nil, errors.New("unused")
}
func reviewClient(t *testing.T) (*Client, *reviewCapture) {
	t.Helper()
	d := &reviewCapture{}
	c, e := NewClient(d, WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if e != nil {
		t.Fatal(e)
	}
	return c, d
}
func TestReviewSubmitUsesTaskDefaults(t *testing.T) {
	c, d := reviewClient(t)
	task := MustTask[int]("review", Policy{MaxAttempts: 7, Timeout: TimeoutOf(time.Hour)})
	if _, e := SubmitTask(context.Background(), c, task, 1, Submission{}); e != nil {
		t.Fatal(e)
	}
	if p := d.job.Envelope.Policy; p.MaxAttempts != 7 || p.Timeout != time.Hour {
		t.Fatalf("task policy lost in real submit path: %+v", p)
	}
}

type reviewDlv struct {
	env        *Envelope
	state      ExecutionState
	decision   Decision
	settleWait time.Duration
	extends    atomic.Int32
}

func (d *reviewDlv) Envelope() *Envelope   { return d.env }
func (d *reviewDlv) State() ExecutionState { return d.state }
func (d *reviewDlv) Admit(context.Context) (Admission, error) {
	d.state.Admitted = "execution-review"
	d.state.Budget = 1
	return Admission{ExecutionID: "execution-review", Attempt: 1}, nil
}
func (d *reviewDlv) Extend(context.Context) error { d.extends.Add(1); return nil }
func (d *reviewDlv) Settle(_ context.Context, x Decision) (SettleStatus, error) {
	d.decision = x
	time.Sleep(d.settleWait)
	return Done, nil
}
func (d *reviewDlv) Release(context.Context) error { return nil }
func (d *reviewDlv) LeaseDuration() time.Duration  { return 300 * time.Millisecond }
func reviewRun(t *testing.T, timeout time.Duration, fn func(context.Context, int) error, configure func(*Worker), wait time.Duration) *reviewDlv {
	t.Helper()
	c, _ := reviewClient(t)
	w := c.NewWorker(Queue{Name: "q"})
	task := MustTask[int]("review", Policy{})
	Handle(w, task, fn)
	if configure != nil {
		configure(w)
	}
	d := &reviewDlv{env: &Envelope{JobID: "review-job", Task: "review", Queue: "q", Payload: json.RawMessage("1"), Policy: ExecutionPolicy{MaxAttempts: 3, Timeout: timeout, Backoff: ConstantBackoff(time.Hour)}, CreatedAt: time.Now()}, state: ExecutionState{Deliveries: 1}, settleWait: wait}
	w.process(context.Background(), context.Background(), &stopper{}, Queue{Name: "q"}, d)
	return d
}
func TestReviewWorkerUsesFrozenBackoff(t *testing.T) {
	before := time.Now()
	d := reviewRun(t, 0, func(context.Context, int) error { return errors.New("temporary") }, nil, 0)
	if d.decision.Kind != DecideRetry || d.decision.RunAt.Sub(before) < 59*time.Minute {
		t.Fatalf("frozen backoff lost: %+v", d.decision)
	}
}
func TestReviewTimeoutOverridesSpecialErrors(t *testing.T) {
	for _, kind := range []string{"snooze", "permanent"} {
		t.Run(kind, func(t *testing.T) {
			d := reviewRun(t, 10*time.Millisecond, func(ctx context.Context, _ int) error {
				<-ctx.Done()
				if kind == "snooze" {
					return Snooze(time.Hour)
				}
				return Permanent(errors.New("late permanent"))
			}, nil, 0)
			if d.decision.Kind != DecideRetry {
				t.Fatalf("timeout misclassified as %s", d.decision.Kind)
			}
		})
	}
}
func TestReviewFailureObserverReceivesPayload(t *testing.T) {
	var payload json.RawMessage
	reviewRun(t, 0, func(context.Context, int) error { return Permanent(errors.New("bad")) }, func(w *Worker) {
		w.OnFailed(func(_ context.Context, _ Info, p json.RawMessage, _ error) {
			payload = append(json.RawMessage(nil), p...)
		})
	}, 0)
	if string(payload) != "1" {
		t.Fatalf("observer payload=%q want 1", payload)
	}
}
func TestReviewSettlementKeepsLease(t *testing.T) {
	d := reviewRun(t, 0, func(context.Context, int) error { return nil }, nil, 350*time.Millisecond)
	if d.extends.Load() == 0 {
		t.Fatal("lease was not renewed while settlement blocked beyond lease")
	}
}
