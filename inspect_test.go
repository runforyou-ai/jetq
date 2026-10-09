package jetq_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
)

func TestRawJobs(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	got := make(chan string, 1)
	failed := make(chan string, 1)
	w.HandleRaw("legacy.action", func(ctx context.Context, payload json.RawMessage) error {
		var body struct{ N int }
		if err := json.Unmarshal(payload, &body); err != nil {
			return err
		}
		if body.N == 0 {
			return errors.New("zero")
		}
		got <- string(payload)
		return nil
	}, jetq.OnRawFailure(func(ctx context.Context, payload json.RawMessage, err error) { failed <- string(payload) }))
	start(t, w)

	ctx := context.Background()
	// The payload is delivered byte for byte, including whitespace and HTML characters.
	original := "{\n  \"N\": 7, \"html\": \"<b>&</b>\"\n}"
	if _, err := c.Enqueue(ctx, jetq.RawJob{Name: "legacy.action", Payload: json.RawMessage(original)}); err != nil {
		t.Fatal(err)
	}
	if p := wait(t, got, 5*time.Second); p != original {
		t.Fatalf("payload = %q", p)
	}
	if _, err := c.Enqueue(ctx, jetq.RawJob{Name: "legacy.action", Payload: json.RawMessage(`{bad`)}); err == nil {
		t.Fatal("expected invalid JSON error")
	}
	if _, err := c.Enqueue(ctx, jetq.RawJob{Name: "legacy.action", Payload: json.RawMessage(`{"N":0}`)}); err != nil {
		t.Fatal(err)
	}
	if p := wait(t, failed, 5*time.Second); p != `{"N":0}` {
		t.Fatalf("failed payload = %s", p)
	}
}

func TestStatsAndDeadLetters(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1}, jetq.Queue{Name: "mail", MaxAttempts: 1})
	failed := make(chan struct{}, 3)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error { return errors.New("down " + job.To) })
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- struct{}{} })
	start(t, w)

	ids := map[string]string{}
	for _, to := range []string{"a", "b"} {
		id, err := c.Enqueue(ctx, sendEmail{To: to}, jetq.OnQueue("mail"))
		if err != nil {
			t.Fatal(err)
		}
		ids[to] = id
		wait(t, failed, 5*time.Second)
	}
	if _, err := c.Enqueue(ctx, sendEmail{To: "c"}); err != nil {
		t.Fatal(err)
	}
	wait(t, failed, 5*time.Second)
	if _, err := c.Enqueue(ctx, sendEmail{To: "later"}, jetq.Delay(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := c.SyncSchedules(ctx, jetq.Cron("nightly", "@daily", report{})); err != nil {
		t.Fatal(err)
	}

	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Queues) != 2 || stats.Queues[0].Queue != "default" || stats.Queues[1].Queue != "mail" {
		t.Fatalf("queues = %+v", stats.Queues)
	}
	if stats.Queues[0].Dead != 1 || stats.Queues[1].Dead != 2 || stats.Delayed != 1 || stats.Schedules != 1 ||
		stats.Queues[0].Ready != 0 || stats.Queues[0].InFlight != 0 {
		t.Fatalf("stats = %+v", stats)
	}

	all, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].Queue != "default" || all[0].Error != "down c" {
		t.Fatalf("dead letters = %+v", all)
	}
	mail, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "mail", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(mail) != 1 || mail[0].ID != ids["b"] || mail[0].Attempts != 1 || mail[0].Name != "send-email" || mail[0].FailedAt.IsZero() {
		t.Fatalf("mail page 1 = %+v", mail)
	}
	next, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "mail", Before: mail[0].Sequence})
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 || next[0].ID != ids["a"] || string(next[0].Payload) != `{"to":"a"}` {
		t.Fatalf("mail page 2 = %+v", next)
	}
}

func TestStatsCountsReadyAndInFlight(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jetq.Queue{Name: "default", Backoff: jetq.Constant(time.Hour)})
	failed := make(chan struct{}, 1)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error {
		failed <- struct{}{}
		return errors.New("down")
	})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	if _, err := c.Enqueue(ctx, sendEmail{}); err != nil {
		t.Fatal(err)
	}
	wait(t, failed, 5*time.Second)
	cancel()
	<-done
	for range 2 {
		if _, err := c.Enqueue(ctx, sendEmail{}); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// One job waits for its retry delay as a delayed job, two were never delivered.
	if q := stats.Queues[0]; q.Ready != 2 || q.InFlight != 0 || stats.Delayed != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestInspectionIsConcurrencySafe(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	if err := c.SyncSchedules(ctx, jetq.Cron("nightly", "@daily", report{})); err != nil {
		t.Fatal(err)
	}
	id, err := c.Enqueue(ctx, sendEmail{}, jetq.Delay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 5 {
				if _, err := c.Stats(ctx); err != nil {
					t.Error(err)
				}
				if _, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{}); err != nil {
					t.Error(err)
				}
				if _, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "default"}); err != nil {
					t.Error(err)
				}
				if err := c.SyncSchedules(ctx, jetq.Cron("nightly", "@daily", report{})); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if err := c.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLettersQueueFilter(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1, Concurrency: 8}, jetq.Queue{Name: "mail", MaxAttempts: 1})
	failed := make(chan struct{}, 400)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error { return errors.New(job.To) })
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- struct{}{} })
	start(t, w)
	if _, err := c.Enqueue(ctx, sendEmail{To: "mail-1"}, jetq.OnQueue("mail")); err != nil {
		t.Fatal(err)
	}
	wait(t, failed, 5*time.Second)
	for range 300 {
		if _, err := c.Enqueue(ctx, sendEmail{To: "other"}); err != nil {
			t.Fatal(err)
		}
	}
	for range 300 {
		wait(t, failed, 10*time.Second)
	}
	if _, err := c.Enqueue(ctx, sendEmail{To: "mail-2"}, jetq.OnQueue("mail")); err != nil {
		t.Fatal(err)
	}
	wait(t, failed, 5*time.Second)

	mail, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "mail", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(mail) != 2 || mail[0].Error != "mail-2" || mail[1].Error != "mail-1" {
		t.Fatalf("mail = %+v", mail)
	}
	older, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "mail", Before: mail[0].Sequence})
	if err != nil {
		t.Fatal(err)
	}
	if len(older) != 1 || older[0].Error != "mail-1" {
		t.Fatalf("older = %+v", older)
	}
	if _, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "bad queue"}); err == nil {
		t.Fatal("expected invalid queue error")
	}
}

func TestJobIDAndHandleRawPanics(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	id, err := c.Enqueue(ctx, sendEmail{}, jetq.JobID("order-42"), jetq.Delay(time.Hour))
	if err != nil || id != "order-42" {
		t.Fatalf("id = %q, err = %v", id, err)
	}
	if err := c.Cancel(ctx, "order-42"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, sendEmail{}, jetq.JobID("bad id")); err == nil {
		t.Fatal("expected invalid id error")
	}
	w := c.NewWorker(jetq.Queue{Name: "default"})
	w.HandleRaw("x", func(ctx context.Context, payload json.RawMessage) error { return nil })
	for _, name := range []string{"x", ""} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("HandleRaw(%q) did not panic", name)
				}
			}()
			w.HandleRaw(name, func(ctx context.Context, payload json.RawMessage) error { return nil })
		}()
	}
}

func TestDeadLettersQueueFilterAcrossBatches(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	w := c.NewWorker(jetq.Queue{Name: "mail", MaxAttempts: 1, Concurrency: 16})
	failed := make(chan struct{}, 600)
	jetq.Handle(w, func(ctx context.Context, job sendEmail) error { return errors.New(job.To) })
	w.OnFailed(func(ctx context.Context, info jetq.Info, payload []byte, err error) { failed <- struct{}{} })
	start(t, w)
	for i := range 600 {
		if _, err := c.Enqueue(ctx, sendEmail{To: strconv.Itoa(i)}, jetq.OnQueue("mail")); err != nil {
			t.Fatal(err)
		}
	}
	for range 600 {
		wait(t, failed, 10*time.Second)
	}
	all, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Limit: 600})
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "mail", Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 50 || page[0].Sequence != all[0].Sequence || page[49].Sequence != all[49].Sequence {
		t.Fatalf("first page: got %d..%d, want %d..%d", page[0].Sequence, page[len(page)-1].Sequence, all[0].Sequence, all[49].Sequence)
	}
	next, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "mail", Limit: 50, Before: page[49].Sequence})
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 50 || next[0].Sequence != all[50].Sequence {
		t.Fatalf("second page starts at %d, want %d", next[0].Sequence, all[50].Sequence)
	}
}

func TestRawJobPointer(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default"})
	got := make(chan string, 1)
	w.HandleRaw("raw", func(ctx context.Context, payload json.RawMessage) error {
		got <- string(payload)
		return nil
	})
	start(t, w)
	original := `{ "a": "<b>" }`
	if _, err := c.Enqueue(context.Background(), &jetq.RawJob{Name: "raw", Payload: json.RawMessage(original)}); err != nil {
		t.Fatal(err)
	}
	if p := wait(t, got, 5*time.Second); p != original {
		t.Fatalf("payload = %q", p)
	}
}
