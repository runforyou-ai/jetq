package jetq_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
	"github.com/runforyou-ai/jetq/jetqtest"
)

// flakyJS fails publishes to job subjects without an answer, following a plan.
type flakyJS struct {
	jetstream.JetStream
	mu sync.Mutex
	// plan has one entry per publish to fail: true sends the message before
	// reporting the failure, as when the answer is lost.
	plan []bool
}

func (f *flakyJS) fail(plan ...bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plan = plan
}

func (f *flakyJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if strings.HasPrefix(msg.Subject, "jetq.q.") || strings.HasPrefix(msg.Subject, "jetq.at.") {
		f.mu.Lock()
		failing := len(f.plan) > 0
		var deliver bool
		if failing {
			deliver, f.plan = f.plan[0], f.plan[1:]
		}
		f.mu.Unlock()
		if failing {
			if deliver {
				if _, err := f.JetStream.PublishMsg(ctx, msg, opts...); err != nil {
					return nil, err
				}
			}
			return nil, context.DeadlineExceeded
		}
	}
	return f.JetStream.PublishMsg(ctx, msg, opts...)
}

func newFlakyClient(t *testing.T) (*jetq.Client, *flakyJS) {
	t.Helper()
	srv := jetqtest.Start(t)
	f := &flakyJS{JetStream: srv.JetStream}
	c, err := jetq.New(context.Background(), f, jetq.WithMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	return c, f
}

// runCount starts a worker counting runs per recipient.
func runCount(t *testing.T, c *jetq.Client) (chan string, *atomic.Int32) {
	t.Helper()
	w := c.NewWorker(jetq.Queue{Name: "default"})
	ran := make(chan string, 10)
	counts := &atomic.Int32{}
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		counts.Add(1)
		ran <- job.To
		return nil
	})
	start(t, w)
	return ran, counts
}

func TestUncertainPublishIsResolved(t *testing.T) {
	for _, deliver := range []bool{false, true} {
		t.Run("stored="+strconv.FormatBool(deliver), func(t *testing.T) {
			c, f := newFlakyClient(t)
			ctx := context.Background()
			ran, counts := runCount(t, c)
			// The first publish fails without an answer; the second, with the
			// same message id, tells the outcome and the job is stored once.
			f.fail(deliver)
			if _, err := c.Enqueue(ctx, sendEmail{To: "a"}, jetq.UniqueUntilDone("resolved")); err != nil {
				t.Fatalf("Enqueue = %v", err)
			}
			wait(t, ran, 5*time.Second)
			expectNothing(t, ran, 500*time.Millisecond)
			if counts.Load() != 1 {
				t.Fatalf("runs = %d", counts.Load())
			}
		})
	}
}

func TestUncertainPublishKeepsLockUntilRetried(t *testing.T) {
	c, f := newFlakyClient(t)
	ctx := context.Background()
	f.fail(false, false)
	id, err := c.Enqueue(ctx, sendEmail{To: "a"}, jetq.UniqueUntilDone("unknown"))
	if !errors.Is(err, jetq.ErrUncertain) || errors.Is(err, jetq.ErrDuplicate) || id == "" {
		t.Fatalf("Enqueue = %q, %v", id, err)
	}
	// The job may be stored, so the key stays taken for other jobs.
	if _, err := c.Enqueue(ctx, sendEmail{To: "a"}, jetq.UniqueUntilDone("unknown")); !errors.Is(err, jetq.ErrDuplicate) {
		t.Fatalf("enqueue of another job = %v", err)
	}
	// Enqueueing the same job id again resolves it.
	if _, err := c.Enqueue(ctx, sendEmail{To: "a"}, jetq.UniqueUntilDone("unknown"), jetq.JobID(id)); err != nil {
		t.Fatalf("retry = %v", err)
	}
	ran, counts := runCount(t, c)
	wait(t, ran, 5*time.Second)
	eventually(t, func() bool {
		_, err := c.Enqueue(ctx, sendEmail{To: "b"}, jetq.UniqueUntilDone("unknown"), jetq.Delay(time.Hour))
		return err == nil
	})
	if counts.Load() != 1 {
		t.Fatalf("runs = %d", counts.Load())
	}

	// A job published and then retried with the same id runs once.
	stored, f2 := newFlakyClient(t)
	f2.fail(true, false)
	id, err = stored.Enqueue(ctx, sendEmail{To: "a"}, jetq.UniqueUntilDone("stored"), jetq.Delay(300*time.Millisecond))
	if !errors.Is(err, jetq.ErrUncertain) {
		t.Fatalf("Enqueue = %v", err)
	}
	if _, err := stored.Enqueue(ctx, sendEmail{To: "a"}, jetq.UniqueUntilDone("stored"), jetq.JobID(id), jetq.Delay(300*time.Millisecond)); err != nil {
		t.Fatalf("retry = %v", err)
	}
	ran, counts = runCount(t, stored)
	wait(t, ran, 5*time.Second)
	expectNothing(t, ran, time.Second)
	if counts.Load() != 1 {
		t.Fatalf("runs = %d", counts.Load())
	}
}

func TestUncertainPlainPublish(t *testing.T) {
	c, f := newFlakyClient(t)
	f.fail(false)
	id, err := c.Enqueue(context.Background(), sendEmail{})
	if !errors.Is(err, jetq.ErrUncertain) || id == "" {
		t.Fatalf("Enqueue = %q, %v", id, err)
	}
	// A context cancelled before publishing is not uncertain.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Enqueue(ctx, sendEmail{}, jetq.UniqueUntilDone("cancelled")); !errors.Is(err, context.Canceled) || errors.Is(err, jetq.ErrUncertain) {
		t.Fatalf("Enqueue = %v", err)
	}
	if _, err := c.Enqueue(context.Background(), sendEmail{}, jetq.UniqueUntilDone("cancelled")); err != nil {
		t.Fatalf("Enqueue after cancelled call = %v", err)
	}
}

func TestJobIDInUse(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, sendEmail{To: "first"}, jetq.JobID("x"), jetq.Delay(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, sendEmail{To: "second"}, jetq.JobID("x"), jetq.Delay(time.Hour)); !errors.Is(err, jetq.ErrJobIDInUse) {
		t.Fatalf("Enqueue = %v", err)
	}
	// The rejected job released its unique key.
	if _, err := c.Enqueue(ctx, sendEmail{To: "second"}, jetq.JobID("x"), jetq.Delay(time.Hour), jetq.UniqueUntilDone("in-use")); !errors.Is(err, jetq.ErrJobIDInUse) {
		t.Fatalf("Enqueue = %v", err)
	}
	if _, err := c.Enqueue(ctx, sendEmail{To: "third"}, jetq.UniqueUntilDone("in-use"), jetq.Delay(time.Hour)); err != nil {
		t.Fatalf("Enqueue = %v", err)
	}
	ran, _ := runCount(t, c)
	if got := wait(t, ran, 5*time.Second); got != "first" {
		t.Fatalf("ran %q", got)
	}
}

func TestRetryDoesNotReplaceDelayedJobWithSameID(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jetq.Queue{Name: "default", Backoff: jetq.Constant(300 * time.Millisecond)})
	ran := make(chan string, 4)
	started := make(chan struct{})
	release := make(chan struct{})
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		ran <- job.To
		if job.To == "retrying" {
			if info, _ := jetq.JobInfo(ctx); info.Attempt == 1 {
				close(started)
				<-release
				return errors.New("boom")
			}
		}
		return nil
	})
	start(t, w)
	if _, err := c.Enqueue(ctx, sendEmail{To: "retrying"}, jetq.JobID("shared")); err != nil {
		t.Fatal(err)
	}
	wait(t, started, 5*time.Second)
	wait(t, ran, 5*time.Second)
	// Another delayed job takes the id while the first one runs; its retry
	// does not replace it.
	if _, err := c.Enqueue(ctx, sendEmail{To: "delayed"}, jetq.JobID("shared"), jetq.Delay(time.Second)); err != nil {
		t.Fatal(err)
	}
	close(release)
	got := map[string]bool{wait(t, ran, 5*time.Second): true, wait(t, ran, 5*time.Second): true}
	if !got["retrying"] || !got["delayed"] {
		t.Fatalf("ran %v", got)
	}
}

func TestDeadLettersSkipDeletedEntries(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	failed := make(chan struct{}, 10)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error { return jetq.Permanent(errors.New(job.To)) })
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- struct{}{} })
	start(t, w)
	for i := 1; i <= 10; i++ {
		if _, err := c.Enqueue(ctx, sendEmail{To: strconv.Itoa(i)}); err != nil {
			t.Fatal(err)
		}
		wait(t, failed, 5*time.Second)
	}
	dead, err := c.JetStream().Stream(ctx, c.DeadLetterStreamName())
	if err != nil {
		t.Fatal(err)
	}
	// Sequences follow the order above; delete some to leave gaps.
	for _, seq := range []uint64{9, 8, 5, 1} {
		if err := dead.DeleteMsg(ctx, seq); err != nil {
			t.Fatal(err)
		}
	}
	var pages [][]string
	var before uint64
	for {
		letters, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Limit: 3, Before: before})
		if err != nil {
			t.Fatal(err)
		}
		if len(letters) == 0 {
			break
		}
		var page []string
		for _, l := range letters {
			page = append(page, l.Error)
		}
		pages = append(pages, page)
		before = letters[len(letters)-1].Sequence
	}
	if got := joinPages(pages); got != "10,7,6|4,3,2" {
		t.Fatalf("pages = %s", got)
	}
}

func joinPages(pages [][]string) string {
	parts := make([]string, len(pages))
	for i, p := range pages {
		parts[i] = strings.Join(p, ",")
	}
	return strings.Join(parts, "|")
}
