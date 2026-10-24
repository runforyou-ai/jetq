package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
)

// TestInspectSnapshotAndPages: snapshots carry the logical state and a copy of
// the envelope, paging is stable by job id, and filters select by queue and
// state.
func TestInspectSnapshotAndPages(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "inspect"))
	ctx := context.Background()
	for _, id := range []string{"a1", "a2", "b1"} {
		queue := "qa"
		if strings.HasPrefix(id, "b") {
			queue = "qb"
		}
		res := mustSubmit(t, d, prepare(t, id, queue, nil))
		if res.Status != jetq.Accepted {
			t.Fatalf("submit %s = %+v", id, res)
		}
	}
	// Fail one job so failed queries have something to find.
	c, _ := d.Consumer(ctx, "qa")
	dlv := receive(t, c)
	if got := dlv.Envelope().JobID; got != "a1" && got != "a2" {
		t.Fatalf("delivered %s", got)
	}
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	failedID := dlv.Envelope().JobID
	if _, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideFail, ExecutionID: adm.ExecutionID,
		TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideFail), Error: "boom"}); err != nil {
		t.Fatal(err)
	}

	snap, ok, err := d.GetJob(ctx, failedID)
	if err != nil || !ok {
		t.Fatalf("GetJob = %v, %v", ok, err)
	}
	if snap.State != jetq.StateFailed || snap.LastError != "boom" || snap.Budget != 1 || snap.Deliveries != 1 {
		t.Fatalf("snapshot = %+v", snap)
	}
	if snap.Envelope.JobID != failedID || snap.Envelope.Task != "task.a" {
		t.Fatalf("envelope = %+v", snap.Envelope)
	}
	snap.Envelope.Payload[0] = 'X'
	again, _, err := d.GetJob(ctx, failedID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Envelope.Payload[0] == 'X' {
		t.Fatal("snapshot shares the stored envelope")
	}

	failed, err := d.List(ctx, jetq.JobQuery{FailedOnly: true})
	if err != nil || len(failed.Jobs) != 1 || failed.Jobs[0].Envelope.JobID != failedID {
		t.Fatalf("failed = %+v, %v", failed, err)
	}
	page, err := d.List(ctx, jetq.JobQuery{Queue: "qa", Limit: 1})
	if err != nil || len(page.Jobs) != 1 || page.LastJobID == "" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	rest, err := d.List(ctx, jetq.JobQuery{Queue: "qa", After: page.LastJobID, Limit: 10})
	if err != nil || len(rest.Jobs) != 1 || rest.Jobs[0].Envelope.JobID <= page.Jobs[0].Envelope.JobID {
		t.Fatalf("second page = %+v, %v", rest, err)
	}
	stats, err := d.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]jetq.QueueStats{}
	for _, q := range stats.Queues {
		got[q.Queue] = q
	}
	if got["qb"].Ready != 1 || got["qa"].Failed != 1 || stats.Succeeded+stats.Canceled != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if _, _, err := d.GetJob(ctx, "bad id"); err == nil {
		t.Fatal("GetJob accepted an invalid id")
	}
}

// TestRedriveEndToEnd: a failed job is redriven into a new job with a parent,
// the original stays, and a retried redrive returns the same replacement.
func TestRedriveEndToEnd(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "redrive"))
	ctx := context.Background()
	job := prepare(t, "orig", "q", nil)
	job.Envelope.EnqueueKey = "outbox-orig"
	job.Fingerprint = job.Envelope.Fingerprint()
	mustSubmit(t, d, job)
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	adm, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideFail, ExecutionID: adm.ExecutionID,
		TransitionID: jetq.TransitionID(adm.ExecutionID, jetq.DecideFail), Error: "boom"}); err != nil {
		t.Fatal(err)
	}
	client, err := jetq.NewClient(d)
	if err != nil {
		t.Fatal(err)
	}
	// Redriving a failed job is a normal submission with its own identity.
	if _, err := client.Redrive(ctx, "orig", jetq.RedriveEnqueueKey("outbox-orig")); err == nil {
		t.Fatal("redrive inherited the parent's enqueue key")
	}
	runAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	res, err := client.Redrive(ctx, "orig", jetq.RedriveEnqueueKey("redrive-1"), jetq.RedriveRunAt(runAt))
	if err != nil || res.Status != jetq.Accepted || !res.RunAt.Equal(runAt) {
		t.Fatalf("redrive = %+v, %v", res, err)
	}
	// A retried redrive repeats the same fixed options and gets the original
	// replacement instead of a second job.
	again, err := client.Redrive(ctx, "orig", jetq.RedriveEnqueueKey("redrive-1"), jetq.RedriveRunAt(runAt))
	if err != nil || again.Status != jetq.AlreadyAccepted || again.JobID != res.JobID {
		t.Fatalf("retried redrive = %+v, %v", again, err)
	}
	snap, ok, err := d.GetJob(ctx, res.JobID)
	if err != nil || !ok || snap.ParentJobID != "orig" || snap.Envelope.EnqueueKey != "redrive-1" {
		t.Fatalf("replacement = %+v, %v, %v", snap, ok, err)
	}
	if orig, ok, _ := d.GetJob(ctx, "orig"); !ok || orig.State != jetq.StateFailed {
		t.Fatalf("original = %+v", orig)
	}
	// The replacement keeps the original's content and policy.
	if string(snap.Envelope.Payload) != string(job.Envelope.Payload) || snap.Envelope.Policy != job.Envelope.Policy {
		t.Fatalf("replacement envelope = %+v", snap.Envelope)
	}
	if _, err := client.Redrive(ctx, res.JobID); !errors.Is(err, jetq.ErrNotRedrivable) {
		t.Fatalf("redriving a live job = %v", err)
	}
	if _, err := client.Redrive(ctx, "missing"); !errors.Is(err, jetq.ErrNotFound) {
		t.Fatalf("redriving a missing job = %v", err)
	}
}

// TestScheduleSyncOwnership: sync is per owner and versioned; one owner never
// deletes another owner's schedules.
func TestScheduleSyncOwnership(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "sync"))
	ctx := context.Background()
	a1 := Schedule{Key: "a", Cron: "@every 1h", Task: "t.a", Payload: json.RawMessage("1")}
	b1 := Schedule{Key: "b", Cron: "@every 1h", Task: "t.b", Payload: json.RawMessage("1")}
	va, err := d.SyncSchedules(ctx, "ownerA", 0, a1)
	if err != nil || va != 1 {
		t.Fatal(va, err)
	}
	if _, err := d.SyncSchedules(ctx, "ownerB", 0, b1); err != nil {
		t.Fatal(err)
	}
	_, ma, ok, _ := d.readManifest(ctx, "ownerA")
	if !ok || len(ma.Schedules) != 1 || ma.Schedules[0].Key != "a" || ma.Version != 1 {
		t.Fatalf("ownerA = %+v", ma)
	}
	// Re-syncing the same set keeps the version and the timing.
	next := ma.Schedules[0].Next
	if v, err := d.SyncSchedules(ctx, "ownerA", va, a1); err != nil || v != va {
		t.Fatal(v, err)
	}
	_, ma2, _, _ := d.readManifest(ctx, "ownerA")
	if ma2.Version != 1 || !ma2.Schedules[0].Next.Equal(next) {
		t.Fatalf("unchanged sync reset the manifest: %+v", ma2)
	}
	// A change bumps the version and leaves owner B alone.
	if _, err := d.SyncSchedules(ctx, "ownerA", va); err != nil {
		t.Fatal(err)
	}
	_, ma3, _, _ := d.readManifest(ctx, "ownerA")
	_, mb, _, _ := d.readManifest(ctx, "ownerB")
	if ma3.Version != 2 || len(ma3.Schedules) != 0 {
		t.Fatalf("ownerA after change = %+v", ma3)
	}
	if len(mb.Schedules) != 1 || mb.Schedules[0].Key != "b" || mb.Version != 1 {
		t.Fatalf("ownerB was touched: %+v", mb)
	}
	if _, err := d.SyncSchedules(ctx, "ownerA", 2, Schedule{Key: "x", Cron: "not a cron", Task: "t", Payload: json.RawMessage("1")}); err == nil {
		t.Fatal("invalid cron accepted")
	}
	if _, err := d.SyncSchedules(ctx, "ownerA", 2, Schedule{Key: "x", Cron: "@every 1h", Task: "t", Payload: json.RawMessage("1"), Timezone: "Nowhere/Nothing"}); err == nil {
		t.Fatal("invalid timezone accepted")
	}
}

// TestScheduleConcurrentSyncAndFiring: concurrent syncs of one owner converge
// on one set, and concurrent schedulers fire each occurrence exactly once.
func TestScheduleConcurrentSyncAndFiring(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "concurrent"))
	ctx := context.Background()
	sched := Schedule{Key: "every", Cron: "@every 1s", Task: "tick", Payload: json.RawMessage("1"), Queue: "q", CatchUp: true, MaxCatchUp: 100}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := d.SyncSchedules(ctx, "owner", 0, sched); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	_, m, _, _ := d.readManifest(ctx, "owner")
	if m.Version != 1 || len(m.Schedules) != 1 {
		t.Fatalf("concurrent syncs produced %+v", m)
	}
	base := time.Now().UTC()
	r1, r2 := d.Scheduler("owner"), d.Scheduler("owner")
	r1.Now = func() time.Time { return base.Add(10 * time.Second) }
	r2.Now = r1.Now
	var fw sync.WaitGroup
	for _, r := range []*ScheduleRunner{r1, r2} {
		fw.Add(1)
		go func() {
			defer fw.Done()
			if err := r.Fire(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	fw.Wait()
	page, err := d.List(ctx, jetq.JobQuery{})
	if err != nil || len(page.Jobs) != 10 {
		t.Fatalf("concurrent firing produced %d jobs (want 10 occurrences, none twice), %v", len(page.Jobs), err)
	}
	for _, snap := range page.Jobs {
		if !snap.Envelope.RunAt.Before(base.Add(11 * time.Second)) {
			t.Fatalf("occurrence run time %s is not the scheduled absolute time", snap.Envelope.RunAt)
		}
	}
	// A crash between claiming an occurrence and submitting it is repaired:
	// the occurrence record exists, its job does not.
	before, _ := d.List(ctx, jetq.JobQuery{Limit: 1000})
	// Occurrences sit on second boundaries, so the seeded one does too.
	at := base.Add(20 * time.Second).Truncate(time.Second)
	_, held, _, _ := d.readManifest(ctx, "owner")
	job, err := d.Scheduler("owner").prepare(held.Schedules[0], at)
	if err != nil {
		t.Fatal(err)
	}
	rec := occurrenceRecord{Owner: "owner", Schedule: "every", RunAt: at, Job: *job}
	if _, err := d.ledger.put(ctx, occurrenceKey(d.cfg.Namespace, job.Envelope.JobID), 0, &record{Data: mustJSON(rec)}); err != nil {
		t.Fatal(err)
	}
	r3 := d.Scheduler("owner")
	r3.Now = func() time.Time { return base.Add(21 * time.Second) }
	if err := r3.Fire(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := d.List(ctx, jetq.JobQuery{Limit: 1000})
	// The occurrence itself plus the further ones catch-up fires are added;
	// none of them is a duplicate of an existing job.
	seen := map[string]bool{}
	for _, job := range before.Jobs {
		seen[job.Envelope.JobID] = true
	}
	for _, job := range after.Jobs {
		if seen[job.Envelope.JobID] {
			delete(seen, job.Envelope.JobID)
			continue
		}
	}
	if len(after.Jobs) <= len(before.Jobs) {
		t.Fatalf("crash window was not repaired: %d -> %d jobs", len(before.Jobs), len(after.Jobs))
	}
	wantID := occurrenceJobID("owner", "every", at)
	found := false
	for _, job := range after.Jobs {
		if job.Envelope.JobID == wantID {
			found = true
		}
	}
	if !found {
		t.Fatalf("occurrence %s was not submitted after its claim", at)
	}
}

// TestScheduleFiresThroughWorker: a worker runs the scheduler it is given and
// the fired job is handled by the same worker.
func TestScheduleFiresThroughWorker(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "worker-sched"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := d.SyncSchedules(ctx, "svc", 0, Schedule{Key: "fast", Cron: "@every 1s", Task: "tick", Payload: json.RawMessage("1"), Queue: "q", CatchUp: true}); err != nil {
		t.Fatal(err)
	}
	client, err := jetq.NewClient(d)
	if err != nil {
		t.Fatal(err)
	}
	w := client.NewWorker(jetq.Queue{Name: "q"})
	runner := d.Scheduler("svc")
	runner.Poll = 20 * time.Millisecond
	w.SetScheduler(runner)
	ran := make(chan struct{}, 4)
	jetq.HandleTask(w, "tick", func(ctx context.Context, v int) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	// Stop the worker and wait for it before t.Cleanup closes the server, so
	// no settlement or renewal runs against a closed connection.
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("worker did not stop")
		}
	}()
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("scheduled job did not run")
	}
}

// TestLedgerReadsAreLeaderConsistent: Open refuses a ledger whose backing
// stream still allows direct (follower) reads, and provision turns it off.
func TestLedgerReadsAreLeaderConsistent(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "leader")
	if _, err := Provision(context.Background(), s.js, cfg); err != nil {
		t.Fatal(err)
	}
	stream, err := s.js.Stream(context.Background(), kvStreamName(cfg.Namespace+"_LEDGER"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.AllowDirect {
		t.Fatal("provision left direct reads enabled on the ledger")
	}
	// A ledger re-enabled for direct reads is rejected, never silently used.
	config := info.Config
	config.AllowDirect = true
	if _, err := s.js.UpdateStream(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), s.js, cfg); err == nil {
		t.Fatal("Open accepted a ledger with possibly stale follower reads")
	}
}

// TestOpenRejectsDeploymentConflicts: storage and replica settings are shared
// deployment parameters; a mismatch is reported, and the connection and the
// resources stay usable.
func TestOpenRejectsDeploymentConflicts(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "deploy")
	if _, err := Provision(context.Background(), s.js, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), s.js, cfg); err != nil {
		t.Fatal(err)
	}
	file := cfg
	file.Memory = false
	if _, err := Open(context.Background(), s.js, file); err == nil || !strings.Contains(err.Error(), "storage") {
		t.Fatalf("storage conflict = %v", err)
	}
	replicas := cfg
	replicas.Replicas = 3
	if _, err := Open(context.Background(), s.js, replicas); err == nil || !strings.Contains(err.Error(), "replicas") {
		t.Fatalf("replica conflict = %v", err)
	}
	// The application's connection and the driver keep working.
	d, err := Open(context.Background(), s.js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !s.nc.IsConnected() {
		t.Fatal("connection was closed")
	}
	if res, err := d.Submit(context.Background(), prepare(t, "after", "q", nil)); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("submit after conflicts = %+v, %v", res, err)
	}
}
