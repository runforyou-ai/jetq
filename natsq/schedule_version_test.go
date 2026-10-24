package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
)

func everySchedule(key string) Schedule {
	return Schedule{Key: key, Cron: "@every 1s", Task: "t." + key, Payload: json.RawMessage("1"), Queue: "q"}
}

// TestScheduleStaleSyncCannotDeleteNewerSet reproduces a rolling deploy: the
// old set A is written (v1), the new deploy writes A+B (v2), then an old
// process syncs A again on top of v1. That must conflict and leave A+B.
func TestScheduleStaleSyncCannotDeleteNewerSet(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "stale-sync"))
	ctx := context.Background()
	a, b := everySchedule("a"), everySchedule("b")
	v1, err := d.SyncSchedules(ctx, "svc", 0, a)
	if err != nil || v1 != 1 {
		t.Fatal(v1, err)
	}
	v2, err := d.SyncSchedules(ctx, "svc", v1, a, b)
	if err != nil || v2 != 2 {
		t.Fatal(v2, err)
	}
	if _, err := d.SyncSchedules(ctx, "svc", v1, a); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale sync = %v, want ErrVersionConflict", err)
	}
	if _, err := d.SyncSchedules(ctx, "svc", 0, a); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("create-only sync over a different set = %v, want ErrVersionConflict", err)
	}
	_, m, _, _ := d.readManifest(ctx, "svc")
	if m.Version != 2 || len(m.Schedules) != 2 {
		t.Fatalf("stale sync changed the manifest: %+v", m)
	}
	// The same definitions are idempotent from any expected version.
	for _, exp := range []ScheduleVersion{0, v1, v2} {
		if v, err := d.SyncSchedules(ctx, "svc", exp, b, a); err != nil || v != v2 {
			t.Fatalf("idempotent sync from %d = %d, %v", exp, v, err)
		}
	}
	if v, err := d.CurrentScheduleVersion(ctx, "svc"); err != nil || v != v2 {
		t.Fatalf("current version = %d, %v", v, err)
	}
	if v, err := d.CurrentScheduleVersion(ctx, "nobody"); err != nil || v != 0 {
		t.Fatalf("missing owner version = %d, %v", v, err)
	}
	if _, err := d.SyncSchedules(ctx, "nobody", 3, a); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("update of a missing set = %v", err)
	}
}

// TestScheduleFiringDoesNotChangeVersion: cursor moves are runtime state.
func TestScheduleFiringDoesNotChangeVersion(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "fire-version"))
	ctx := context.Background()
	v1, err := d.SyncSchedules(ctx, "svc", 0, everySchedule("a"))
	if err != nil {
		t.Fatal(err)
	}
	r := d.Scheduler("svc")
	r.Now = func() time.Time { return time.Now().Add(3 * time.Second) }
	if err := r.Fire(ctx); err != nil {
		t.Fatal(err)
	}
	if v, _ := d.CurrentScheduleVersion(ctx, "svc"); v != v1 {
		t.Fatalf("firing changed the version: %d -> %d", v1, v)
	}
	if v, err := d.SyncSchedules(ctx, "svc", v1, everySchedule("a"), everySchedule("b")); err != nil || v != v1+1 {
		t.Fatalf("sync after firing = %d, %v", v, err)
	}
}

// TestScheduleAddingKeepsUnchangedCursor: adding B must keep A's cursor, so
// A's due occurrence that was not fired yet is still fired.
func TestScheduleAddingKeepsUnchangedCursor(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "keep-cursor"))
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d.hks.now = func() time.Time { return base }
	v1, err := d.SyncSchedules(ctx, "svc", 0, everySchedule("a"))
	if err != nil {
		t.Fatal(err)
	}
	_, m, _, _ := d.readManifest(ctx, "svc")
	due := m.Schedules[0].Next
	// Later, before A fired, a deploy adds B and changes nothing about A.
	d.hks.now = func() time.Time { return base.Add(time.Hour) }
	if _, err := d.SyncSchedules(ctx, "svc", v1, everySchedule("a"), everySchedule("b")); err != nil {
		t.Fatal(err)
	}
	_, m, _, _ = d.readManifest(ctx, "svc")
	if !m.Schedules[0].Next.Equal(due) {
		t.Fatalf("unchanged A cursor reset: %s -> %s", due, m.Schedules[0].Next)
	}
	r := d.Scheduler("svc")
	r.Now = func() time.Time { return due }
	if err := r.Fire(ctx); err != nil {
		t.Fatal(err)
	}
	snap, ok, err := d.GetJob(ctx, occurrenceJobID("svc", "a", due))
	if err != nil || !ok || snap.Envelope.Task != "t.a" {
		t.Fatalf("A's due occurrence was skipped: %v, %v", ok, err)
	}
}

// TestScheduleOccurrenceConflictIsReported: an occurrence identity already
// holding different content is never overwritten, and Fire reports it.
func TestScheduleOccurrenceConflictIsReported(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "occ-conflict"))
	ctx := context.Background()
	if _, err := d.SyncSchedules(ctx, "svc", 0, everySchedule("a")); err != nil {
		t.Fatal(err)
	}
	_, m, _, _ := d.readManifest(ctx, "svc")
	due := m.Schedules[0].Next
	id := occurrenceJobID("svc", "a", due)
	client, err := jetq.NewClient(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Submit(ctx, "t.a", json.RawMessage("2"), jetq.Submission{
		JobID: id, Queue: "q", RunAt: due, EnqueueKey: "schedule:svc:" + id,
	}); err != nil {
		t.Fatal(err)
	}
	r := d.Scheduler("svc")
	r.Now = func() time.Time { return due }
	if err := r.Fire(ctx); !errors.Is(err, jetq.ErrEnqueueKeyConflict) {
		t.Fatalf("conflicting occurrence = %v, want ErrEnqueueKeyConflict", err)
	}
	snap, ok, err := d.GetJob(ctx, id)
	if err != nil || !ok || string(snap.Envelope.Payload) != "2" {
		t.Fatalf("stored occurrence was overwritten: %+v %v %v", snap.Envelope, ok, err)
	}
}
