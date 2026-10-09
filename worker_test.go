package jetq_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

func TestRetryingJobsDoNotHoldUpQueue(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	// More jobs than the consumer's default MaxAckPending (1000) wait for a retry.
	const failing = 1050
	for range failing {
		if _, err := c.Enqueue(ctx, sendEmail{To: "down"}); err != nil {
			t.Fatal(err)
		}
	}
	w := c.NewWorker(jetq.Queue{Name: "default", Concurrency: 50, MaxAttempts: 5})
	var failed atomic.Int32
	healthy := make(chan struct{}, 1)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		if job.To == "down" {
			failed.Add(1)
			return jetq.RetryAfter(time.Hour, errors.New("downstream down"))
		}
		healthy <- struct{}{}
		return nil
	})
	start(t, w)
	deadline := time.Now().Add(60 * time.Second)
	for failed.Load() < failing {
		if time.Now().After(deadline) {
			t.Fatalf("failed attempts = %d", failed.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := c.Enqueue(ctx, sendEmail{To: "ok"}); err != nil {
		t.Fatal(err)
	}
	wait(t, healthy, 10*time.Second)
	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if q := stats.Queues[0]; q.InFlight != 0 || stats.Delayed != failing {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestShutdownInterruptionKeepsAttempt(t *testing.T) {
	c := newClient(t)
	attempts := make(chan int, 4)
	var interrupted atomic.Bool
	newWorker := func() *jetq.Worker {
		w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
		w.SetShutdownTimeout(200 * time.Millisecond)
		jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
			info, _ := jetq.JobInfo(ctx)
			attempts <- info.Attempt
			if interrupted.CompareAndSwap(false, true) {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		})
		return w
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- newWorker().Run(runCtx) }()
	if _, err := c.Enqueue(context.Background(), sendEmail{}); err != nil {
		t.Fatal(err)
	}
	if got := wait(t, attempts, 5*time.Second); got != 1 {
		t.Fatalf("first attempt = %d", got)
	}
	cancel()
	if err := wait(t, done, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	// The interrupted last attempt runs again instead of being dead-lettered.
	second := newWorker()
	failed := make(chan error, 1)
	second.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- err })
	start(t, second)
	if got := wait(t, attempts, 5*time.Second); got != 1 {
		t.Fatalf("attempt after shutdown = %d", got)
	}
	expectNothing(t, failed, 300*time.Millisecond)
}

func TestRunReturnsWhenHandlerIgnoresCancellation(t *testing.T) {
	defer jetq.SetAbandonAfter(200 * time.Millisecond)()
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default"})
	w.SetShutdownTimeout(100 * time.Millisecond)
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	defer close(block)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		started <- struct{}{}
		<-block
		return nil
	})
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	if _, err := c.Enqueue(context.Background(), sendEmail{}); err != nil {
		t.Fatal(err)
	}
	wait(t, started, 5*time.Second)
	cancel()
	if err := wait(t, done, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	// The abandoned job was put back for another worker.
	next := c.NewWorker(jetq.Queue{Name: "default"})
	attempts := make(chan int, 1)
	jetq.Handle(next, func(ctx context.Context, job sendEmail) error {
		info, _ := jetq.JobInfo(ctx)
		attempts <- info.Attempt
		return nil
	})
	start(t, next)
	if got := wait(t, attempts, 5*time.Second); got != 1 {
		t.Fatalf("attempt = %d", got)
	}
}

func TestTimeout(t *testing.T) {
	defer jetq.SetAbandonAfter(200 * time.Millisecond)()
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1, Timeout: 100 * time.Millisecond})
	causes := make(chan error, 3)
	block := make(chan struct{})
	defer close(block)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		switch job.To {
		case "ignore":
			<-block
			return nil
		case "long":
			// Per-job timeout above the queue's.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(300 * time.Millisecond):
				return nil
			}
		}
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return ctx.Err()
	})
	failed := make(chan string, 3)
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) {
		if !errors.Is(err, jetq.ErrTimeout) {
			t.Errorf("failure = %v", err)
		}
		failed <- info.ID
	})
	ran := make(chan struct{}, 1)
	w.Use(func(ctx context.Context, next func(context.Context) error) error {
		err := next(ctx)
		if info, _ := jetq.JobInfo(ctx); err == nil && info.Timeout == time.Second {
			ran <- struct{}{}
		}
		return err
	})
	start(t, w)

	honours, _ := c.Enqueue(context.Background(), sendEmail{})
	if got := wait(t, causes, 5*time.Second); !errors.Is(got, jetq.ErrTimeout) {
		t.Fatalf("cause = %v", got)
	}
	if got := wait(t, failed, 5*time.Second); got != honours {
		t.Fatalf("failed %s", got)
	}
	// A handler ignoring its timeout is abandoned and the attempt fails.
	ignores, _ := c.Enqueue(context.Background(), sendEmail{To: "ignore"})
	if got := wait(t, failed, 5*time.Second); got != ignores {
		t.Fatalf("failed %s", got)
	}
	if _, err := c.Enqueue(context.Background(), sendEmail{To: "long"}, jetq.Timeout(time.Second)); err != nil {
		t.Fatal(err)
	}
	wait(t, ran, 5*time.Second)
}

func TestUnknownJobIsPutBack(t *testing.T) {
	defer jetq.SetUnknownJobRetry(200 * time.Millisecond)()
	c := newClient(t)
	ctx := context.Background()
	// An older worker without the handler puts the job back without using up
	// an attempt, so a newer worker runs it.
	old := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	failed := make(chan error, 1)
	old.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- err })
	jetq.Handle(old, func(ctx context.Context, job report) error { return nil })
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- old.Run(runCtx) }()
	if _, err := c.Enqueue(ctx, sendEmail{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		stats, err := c.Stats(ctx)
		return err == nil && stats.Delayed == 1
	})
	cancel()
	<-done
	select {
	case err := <-failed:
		t.Fatalf("dead-lettered: %v", err)
	default:
	}

	newer := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	attempts := make(chan int, 1)
	jetq.Handle(newer, func(ctx context.Context, job sendEmail) error {
		info, _ := jetq.JobInfo(ctx)
		attempts <- info.Attempt
		return nil
	})
	start(t, newer)
	if got := wait(t, attempts, 5*time.Second); got != 1 {
		t.Fatalf("attempt = %d", got)
	}
}

func TestUnknownJobDeadLetteredAfterTimeout(t *testing.T) {
	defer jetq.SetUnknownJobRetry(100 * time.Millisecond)()
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	w.SetUnknownJobTimeout(500 * time.Millisecond)
	failed := make(chan error, 1)
	jetq.Handle(w, func(ctx context.Context, job report) error { return nil })
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- err })
	start(t, w)
	began := time.Now()
	_, _ = c.Enqueue(context.Background(), sendEmail{})
	err := wait(t, failed, 5*time.Second)
	if !strings.Contains(err.Error(), "no handler") {
		t.Fatalf("failure = %v", err)
	}
	if waited := time.Since(began); waited < 500*time.Millisecond {
		t.Fatalf("dead-lettered after %s", waited)
	}
}

func TestSnoozeCount(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default"})
	snoozes := make(chan int, 3)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		info, _ := jetq.JobInfo(ctx)
		snoozes <- info.Snoozes
		if info.Snoozes < 2 {
			return jetq.Snooze(0)
		}
		return nil
	})
	start(t, w)
	_, _ = c.Enqueue(context.Background(), sendEmail{})
	for want := range 3 {
		if got := wait(t, snoozes, 5*time.Second); got != want {
			t.Fatalf("snoozes = %d, want %d", got, want)
		}
	}
}

func TestWorkerRecreatesDeletedConsumer(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jetq.Queue{Name: "default"})
	ran := make(chan struct{}, 2)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		ran <- struct{}{}
		return nil
	})
	start(t, w)
	_, _ = c.Enqueue(ctx, sendEmail{})
	wait(t, ran, 5*time.Second)
	if err := c.JetStream().DeleteConsumer(ctx, c.StreamName(), "jetq-default"); err != nil {
		t.Fatal(err)
	}
	_, _ = c.Enqueue(ctx, sendEmail{})
	wait(t, ran, 10*time.Second)
}

func TestCancelMarkerSurvivesImmediateRequeue(t *testing.T) {
	for _, mode := range []string{"snooze", "retry", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			c := newClient(t)
			ctx := context.Background()
			id, err := c.Enqueue(ctx, sendEmail{}, jetq.Delay(300*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			stream, err := c.JetStream().Stream(ctx, c.StreamName())
			if err != nil {
				t.Fatal(err)
			}
			scheduled, err := stream.GetLastMsgForSubject(ctx, "jetq.at."+id)
			if err != nil {
				t.Fatal(err)
			}
			// Ensure the state bucket exists, as after any Cancel.
			if _, err := c.Enqueue(ctx, sendEmail{}, jetq.UniqueUntilDone("bucket"), jetq.Delay(time.Hour)); err != nil {
				t.Fatal(err)
			}
			state, err := c.JetStream().KeyValue(ctx, c.StreamName()+"_STATE")
			if err != nil {
				t.Fatal(err)
			}
			runs := make(chan struct{}, 4)
			newWorker := func() *jetq.Worker {
				w := c.NewWorker(jetq.Queue{Name: "default"})
				w.SetShutdownTimeout(100 * time.Millisecond)
				jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
					runs <- struct{}{}
					// Cancel lost the race with the schedule: it records the
					// marker while the job runs.
					if _, err := state.Put(context.Background(), "cancel."+id, []byte(scheduled.Header.Get(jetq.HeaderEnqueuedAt))); err != nil {
						t.Error(err)
					}
					switch mode {
					case "snooze":
						return jetq.Snooze(0)
					case "retry":
						return jetq.RetryAfter(0, errors.New("boom"))
					}
					<-ctx.Done()
					return ctx.Err()
				})
				return w
			}
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- newWorker().Run(runCtx) }()
			wait(t, runs, 5*time.Second)
			if mode == "shutdown" {
				cancel()
				<-done
				start(t, newWorker())
			} else {
				t.Cleanup(func() { cancel(); <-done })
			}
			expectNothing(t, runs, time.Second)
		})
	}
}

func TestTimeoutFailsWhateverHandlerReturns(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1, Timeout: 100 * time.Millisecond})
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		<-ctx.Done()
		if job.To == "snooze" {
			return jetq.Snooze(0)
		}
		return nil
	})
	failed := make(chan error, 2)
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- err })
	start(t, w)
	for _, to := range []string{"nil", "snooze"} {
		_, _ = c.Enqueue(context.Background(), sendEmail{To: to})
		if err := wait(t, failed, 5*time.Second); !errors.Is(err, jetq.ErrTimeout) {
			t.Fatalf("%s: failure = %v", to, err)
		}
	}
}

func TestPermanentErrorDuringShutdownIsDeadLettered(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 3})
	w.SetShutdownTimeout(100 * time.Millisecond)
	started := make(chan struct{})
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		close(started)
		<-ctx.Done()
		return jetq.Permanent(errors.New("invalid"))
	})
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	id, _ := c.Enqueue(context.Background(), sendEmail{})
	wait(t, started, 5*time.Second)
	cancel()
	<-done
	assertDeadLetter(t, c, "default", id, "invalid", "1")
}

func TestRedeliveryDropsRequeuedCopy(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	id, err := c.Enqueue(ctx, sendEmail{})
	if err != nil {
		t.Fatal(err)
	}
	// A worker put the job back for a retry but its ack was lost: the
	// original is redelivered while the delayed copy is pending.
	consumer, err := c.JetStream().CreateOrUpdateConsumer(ctx, c.StreamName(), jetstream.ConsumerConfig{
		Durable: "jetq-default", FilterSubject: "jetq.q.default", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: time.Second, MaxDeliver: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(1)
	if err != nil {
		t.Fatal(err)
	}
	for msg := range batch.Messages() {
		copyMsg := nats.NewMsg("jetq.at." + id)
		copyMsg.Data = msg.Data()
		for key, values := range msg.Headers() {
			if !strings.HasPrefix(key, "Nats-") {
				copyMsg.Header[key] = values
			}
		}
		copyMsg.Header.Set("Jetq-Attempt-Base", "1")
		copyMsg.Header.Set("Nats-Schedule", "@at "+time.Now().Add(2*time.Second).UTC().Format(time.RFC3339Nano))
		copyMsg.Header.Set("Nats-Schedule-Target", "jetq.q.default")
		if _, err := c.JetStream().PublishMsg(ctx, copyMsg); err != nil {
			t.Fatal(err)
		}
	}
	w := c.NewWorker(jetq.Queue{Name: "default", AckWait: time.Second})
	runs := make(chan struct{}, 2)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		runs <- struct{}{}
		return nil
	})
	start(t, w)
	wait(t, runs, 5*time.Second)
	expectNothing(t, runs, 3*time.Second)
}
