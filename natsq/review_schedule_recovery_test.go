package natsq

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetq"
)

func TestReviewScheduleWithoutCatchUpSkipsMissedBacklog(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "schedule-skip"))
	r, when := reviewSeedSchedule(t, d, "owner", false)
	now := when.Add(10 * time.Second)
	r.Now = func() time.Time { return now }
	ctx := context.Background()
	if e := r.Fire(ctx); e != nil {
		t.Fatal(e)
	}
	_, m, ok, e := d.readManifest(ctx, "owner")
	if e != nil || !ok {
		t.Fatal(m, ok, e)
	}
	if !m.Schedules[0].Next.After(now) {
		t.Errorf("CatchUp=false left cursor in missed backlog: next=%s now=%s", m.Schedules[0].Next, now)
	}
	before, _ := d.List(ctx, jetq.JobQuery{Limit: 100})
	if e := r.Fire(ctx); e != nil {
		t.Fatal(e)
	}
	after, _ := d.List(ctx, jetq.JobQuery{Limit: 100})
	if len(after.Jobs) != len(before.Jobs) {
		t.Errorf("CatchUp=false kept replaying missed occurrences at the same current time: %d -> %d", len(before.Jobs), len(after.Jobs))
	}
}
func TestReviewClaimedOccurrenceKeepsItsOriginalDefinition(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "updated"
		if remove {
			name = "removed"
		}
		t.Run(name, func(t *testing.T) {
			s := startServer(t)
			d := openDriver(t, s, testConfig(t, "schedule-definition"))
			r, when := reviewSeedSchedule(t, d, "owner", false)
			r.Now = func() time.Time { return when }
			ctx := context.Background()
			d.ledger.kv = &reviewFailScheduleEnqueue{KeyValue: d.ledger.kv}
			if e := r.Fire(ctx); e == nil {
				t.Fatal("enqueue must fail before intent creation")
			}
			ent, m, _, e := d.readManifest(ctx, "owner")
			if e != nil {
				t.Fatal(e)
			}
			m.Version++
			if remove {
				m.Schedules = nil
			} else {
				m.Schedules[0].Task = "changed.task"
				m.Schedules[0].Payload = json.RawMessage("2")
				m.Schedules[0].Next = when.Add(time.Second)
			}
			if _, e = d.ledger.put(ctx, manifestKey(d.cfg.Namespace, "owner"), ent.rev, &record{Data: mustJSON(*m)}); e != nil {
				t.Fatal(e)
			}
			if _, e = d.Recover(ctx); e != nil {
				t.Fatal(e)
			}
			page, e := d.List(ctx, jetq.JobQuery{Limit: 10})
			if e != nil || len(page.Jobs) != 1 {
				t.Fatalf("claimed occurrence vanished after definition change: count=%d error=%v", len(page.Jobs), e)
			}
			job := page.Jobs[0].Envelope
			if job.Task != "task.owner" || string(job.Payload) != "1" {
				t.Fatalf("recovery rewrote committed occurrence with new definition: task=%s payload=%s", job.Task, job.Payload)
			}
		})
	}
}

type reviewKeepOccurrenceReceipt struct{ jetstream.KeyValue }

func (k reviewKeepOccurrenceReceipt) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	if strings.HasPrefix(key, "oc.") {
		return 0, context.DeadlineExceeded
	}
	return k.KeyValue.Update(ctx, key, value, rev)
}
func (k reviewKeepOccurrenceReceipt) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	if strings.HasPrefix(key, "oc.") {
		return context.DeadlineExceeded
	}
	return k.KeyValue.Delete(ctx, key, opts...)
}
func TestReviewOccurrenceReceiptCannotReplayACollectedJob(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "schedule-gc")
	cfg.JobRetention = time.Hour
	d := openDriver(t, s, cfg)
	r, when := reviewSeedSchedule(t, d, "owner", false)
	r.Now = func() time.Time { return when }
	ctx := context.Background()
	kv := d.ledger.kv
	d.ledger.kv = reviewKeepOccurrenceReceipt{KeyValue: kv}
	_ = r.Fire(ctx)
	page, e := d.List(ctx, jetq.JobQuery{Limit: 10})
	if e != nil || len(page.Jobs) != 1 {
		t.Fatal(page, e)
	}
	id := page.Jobs[0].Envelope.JobID
	d.hks.now = func() time.Time { return when }
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	a, e := dlv.Admit(ctx)
	if e != nil {
		t.Fatal(e)
	}
	_, e = dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideSucceed, ExecutionID: a.ExecutionID, TransitionID: jetq.TransitionID(a.ExecutionID, jetq.DecideSucceed)})
	if e != nil {
		t.Fatal(e)
	}
	later := when.Add(25 * time.Hour)
	d.hks.now = func() time.Time { return later }
	_, _ = d.recoverJob(ctx, jobKey(id), later)
	d.ledger.kv = kv
	if _, e = d.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	job, ok, e := d.GetJob(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if ok && !job.State.Terminal() {
		t.Fatalf("old occurrence receipt resurrected an executed and collected job: state=%s", job.State)
	}
}
