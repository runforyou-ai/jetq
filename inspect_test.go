package jetq_test

import (
	"context"
	"encoding/json"
	"errors"
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
	}, func(ctx context.Context, payload json.RawMessage, err error) { failed <- string(payload) })
	start(t, w)

	ctx := context.Background()
	if _, err := c.Enqueue(ctx, jetq.RawJob{Name: "legacy.action", Payload: json.RawMessage(`{"N":7}`)}); err != nil {
		t.Fatal(err)
	}
	if p := wait(t, got, 5*time.Second); p != `{"N":7}` {
		t.Fatalf("payload = %s", p)
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
	if stats.Queues[0].Dead != 1 || stats.Queues[1].Dead != 2 || stats.Delayed != 1 || stats.Schedules != 1 {
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
