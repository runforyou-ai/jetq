package natsq

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetq"
)

// TestScheduleLatePollFiresOnce: without catch-up, a scheduler that polls a
// little after the occurrence fires it once and does not fire it again.
func TestScheduleLatePollFiresOnce(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "late-poll"))
	r, when := reviewSeedSchedule(t, d, "owner", false)
	r.Now = func() time.Time { return when.Add(300 * time.Millisecond) }
	ctx := context.Background()
	for range 2 {
		if err := r.Fire(ctx); err != nil {
			t.Fatal(err)
		}
	}
	page, err := d.List(ctx, jetq.JobQuery{})
	if err != nil || len(page.Jobs) != 1 || !page.Jobs[0].Envelope.RunAt.Equal(when) {
		t.Fatalf("late poll = %+v, %v", page.Jobs, err)
	}
}

// TestScheduleClaimSnapshotSurvivesChange: once an occurrence is claimed and
// stored, changing the schedule leaves that job alone and only later
// occurrences use the new definition.
func TestScheduleClaimSnapshotSurvivesChange(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "snapshot-change"))
	r, when := reviewSeedSchedule(t, d, "owner", false)
	ctx := context.Background()
	r.Now = func() time.Time { return when }
	if err := r.Fire(ctx); err != nil {
		t.Fatal(err)
	}
	ent, m, _, _ := d.readManifest(ctx, "owner")
	m.Version++
	m.Schedules[0].Task = "changed.task"
	if _, err := d.ledger.put(ctx, manifestKey(d.cfg.Namespace, "owner"), ent.rev, &record{Data: mustJSON(*m)}); err != nil {
		t.Fatal(err)
	}
	// Re-firing the claimed time (a racing scheduler) and recovery keep the
	// claimed job; the next occurrence uses the new task.
	if err := r.Fire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return when.Add(time.Second) }
	if err := r.Fire(ctx); err != nil {
		t.Fatal(err)
	}
	first, _, _ := d.GetJob(ctx, occurrenceJobID("owner", "same-key", when))
	second, ok, _ := d.GetJob(ctx, occurrenceJobID("owner", "same-key", when.Add(time.Second)))
	if first.Envelope.Task != "task.owner" || !ok || second.Envelope.Task != "changed.task" {
		t.Fatalf("first=%s second=%s (found %v)", first.Envelope.Task, second.Envelope.Task, ok)
	}
}

// loseClaimResponse commits the first occurrence claim but reports a timeout.
type loseClaimResponse struct {
	jetstream.KeyValue
	once sync.Once
}

func (k *loseClaimResponse) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	rev, err := k.KeyValue.Create(ctx, key, value, opts...)
	if err == nil && strings.HasPrefix(key, "oc.") {
		lost := false
		k.once.Do(func() { lost = true })
		if lost {
			return 0, context.DeadlineExceeded
		}
	}
	return rev, err
}

// TestScheduleLostClaimResponseFiresOnce: a claim that was committed but whose
// response was lost is found again by the next firing, which submits the
// snapshot once.
func TestScheduleLostClaimResponseFiresOnce(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "lost-claim"))
	r, when := reviewSeedSchedule(t, d, "owner", false)
	r.Now = func() time.Time { return when }
	ctx := context.Background()
	d.ledger.kv = &loseClaimResponse{KeyValue: d.ledger.kv}
	if err := r.Fire(ctx); err == nil {
		t.Fatal("lost claim response was not reported")
	}
	if err := r.Fire(ctx); err != nil {
		t.Fatal(err)
	}
	page, err := d.List(ctx, jetq.JobQuery{})
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("jobs = %d, %v", len(page.Jobs), err)
	}
}

// TestScheduleRetentionCollectsClaimWithJob: after retention the job and its
// claim are collected together; a later recovery recreates nothing.
func TestScheduleRetentionCollectsClaimWithJob(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "claim-gc")
	cfg.JobRetention = time.Hour
	d := openDriver(t, s, cfg)
	r, when := reviewSeedSchedule(t, d, "owner", false)
	r.Now = func() time.Time { return when }
	ctx := context.Background()
	if err := r.Fire(ctx); err != nil {
		t.Fatal(err)
	}
	id := occurrenceJobID("owner", "same-key", when)
	d.hks.now = func() time.Time { return when }
	c, err := d.Consumer(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	dlv := receive(t, c)
	a, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideSucceed, ExecutionID: a.ExecutionID, TransitionID: jetq.TransitionID(a.ExecutionID, jetq.DecideSucceed)}); err != nil {
		t.Fatal(err)
	}
	// Before retention the claim stays, as the record that the job ran.
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.ledger.get(ctx, occurrenceKey(d.cfg.Namespace, id)); !ok {
		t.Fatal("claim removed before its job was collected")
	}
	d.hks.now = func() time.Time { return when.Add(25 * time.Hour) }
	for range 2 {
		if _, err := d.Recover(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, _ := d.ledger.get(ctx, occurrenceKey(d.cfg.Namespace, id)); ok {
		t.Fatal("claim leaked after its job was collected")
	}
	if _, ok, _ := d.ledger.get(ctx, jobKey(id)); ok {
		t.Fatal("job not collected, or recreated by its claim")
	}
}
