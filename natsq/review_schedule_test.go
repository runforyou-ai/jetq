package natsq

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetq"
)

func reviewSeedSchedule(t *testing.T, d *Driver, owner string, catchUp bool) (*ScheduleRunner, time.Time) {
	t.Helper()
	s, e := d.compileSchedule(Schedule{Key: "same-key", Cron: "@every 1s", Task: "task." + owner, Payload: json.RawMessage("1"), Queue: "q", CatchUp: catchUp, MaxCatchUp: 10})
	if e != nil {
		t.Fatal(e)
	}
	m := scheduleManifest{Owner: owner, Version: 1, Schedules: []scheduleRecord{s}}
	if _, e = d.ledger.put(context.Background(), manifestKey(d.cfg.Namespace, owner), 0, &record{Data: mustJSON(m)}); e != nil {
		t.Fatal(e)
	}
	return d.Scheduler(owner), s.Next
}
func TestReviewScheduleOwnerOccurrenceIsolation(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "owner-occurrence"))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d.hks.now = func() time.Time { return base }
	a, when := reviewSeedSchedule(t, d, "ownerA", false)
	b, _ := reviewSeedSchedule(t, d, "ownerB", false)
	a.Now = func() time.Time { return when }
	b.Now = a.Now
	ctx := context.Background()
	if e := a.Fire(ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.Fire(ctx); e != nil {
		t.Errorf("second owner occurrence collided: %v", e)
	}
	page, e := d.List(ctx, jetq.JobQuery{Limit: 10})
	if e != nil || len(page.Jobs) != 2 {
		t.Fatalf("independent owners must create two jobs: count=%d error=%v", len(page.Jobs), e)
	}
}

type reviewLoseScheduleCursor struct {
	jetstream.KeyValue
	once sync.Once
}

func (k *reviewLoseScheduleCursor) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	n, e := k.KeyValue.Update(ctx, key, value, rev)
	if e == nil && strings.HasPrefix(key, "sc.") {
		lost := false
		k.once.Do(func() { lost = true })
		if lost {
			return 0, context.DeadlineExceeded
		}
	}
	return n, e
}
func TestReviewScheduleCursorResponseLossDoesNotLoseOccurrence(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "schedule-cursor"))
	r, when := reviewSeedSchedule(t, d, "owner", false)
	r.Now = func() time.Time { return when }
	ctx := context.Background()
	d.ledger.kv = &reviewLoseScheduleCursor{KeyValue: d.ledger.kv}
	_ = r.Fire(ctx)
	if e := r.Fire(ctx); e != nil {
		t.Fatal(e)
	}
	page, e := d.List(ctx, jetq.JobQuery{Limit: 10})
	if e != nil || len(page.Jobs) != 1 {
		t.Fatalf("committed cursor plus lost response lost occurrence: count=%d error=%v", len(page.Jobs), e)
	}
}
func TestReviewScheduleCatchUpFiresEveryMissedOccurrence(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "schedule-catchup"))
	r, when := reviewSeedSchedule(t, d, "owner", true)
	r.Now = func() time.Time { return when.Add(4 * time.Second) }
	ctx := context.Background()
	if e := r.Fire(ctx); e != nil {
		t.Fatal(e)
	}
	page, e := d.List(ctx, jetq.JobQuery{Limit: 10})
	if e != nil || len(page.Jobs) != 5 {
		t.Fatalf("five due occurrences expected with CatchUp=true: count=%d error=%v", len(page.Jobs), e)
	}
}

type reviewFailScheduleEnqueue struct {
	jetstream.KeyValue
	once sync.Once
}

func (k *reviewFailScheduleEnqueue) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	if strings.HasPrefix(key, "ek.") {
		fail := false
		k.once.Do(func() { fail = true })
		if fail {
			return 0, context.DeadlineExceeded
		}
	}
	return k.KeyValue.Create(ctx, key, value, opts...)
}
func TestReviewScheduleFailedEnqueueIsRecoveredAfterCursorAdvance(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "schedule-retry"))
	r, when := reviewSeedSchedule(t, d, "owner", false)
	r.Now = func() time.Time { return when }
	ctx := context.Background()
	d.ledger.kv = &reviewFailScheduleEnqueue{KeyValue: d.ledger.kv}
	if e := r.Fire(ctx); e == nil {
		t.Fatal("first enqueue should fail before an intent is stored")
	}
	if _, e := d.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	if e := r.Fire(ctx); e != nil {
		t.Fatal(e)
	}
	page, e := d.List(ctx, jetq.JobQuery{Limit: 10})
	if e != nil || len(page.Jobs) != 1 {
		t.Fatalf("durable occurrence was abandoned after enqueue failure: count=%d error=%v", len(page.Jobs), e)
	}
}

func TestReviewSchedulePreservesRawPayload(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "schedule-payload"))
	r, when := reviewSeedSchedule(t, d, "owner", false)
	r.Now = func() time.Time { return when }
	ctx := context.Background()
	if e := r.Fire(ctx); e != nil {
		t.Fatal(e)
	}
	page, e := d.List(ctx, jetq.JobQuery{Limit: 10})
	if e != nil || len(page.Jobs) != 1 {
		t.Fatal(page, e)
	}
	if got := string(page.Jobs[0].Envelope.Payload); got != "1" {
		t.Fatalf("schedule changed JSON payload encoding: got %s want 1", got)
	}
}
