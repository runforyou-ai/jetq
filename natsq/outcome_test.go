package natsq

import (
	"context"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
)

// TestHelperSuppressedThenOwnerEndsDoesNotActivate: a helper finishes a
// pending submission against a live reservation, commits Suppressed, and only
// then does the owner finish. The old producer, resuming after both, must read
// the committed outcome and never activate its job.
func TestHelperSuppressedThenOwnerEndsDoesNotActivate(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "helper-suppressor"))
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "one-job"}
	mustSubmit(t, d, prepare(t, "owner", "q", unique))

	pause := &reviewPauseIntent{KeyValue: d.ledger.kv, entered: make(chan struct{}), release: make(chan struct{})}
	d.ledger.kv = pause
	candidate := prepare(t, "candidate", "q", unique)
	candidate.Envelope.EnqueueKey = "candidate-intent"
	candidate.Fingerprint = candidate.Envelope.Fingerprint()
	done := make(chan jetq.EnqueueResult, 1)
	errs := make(chan error, 1)
	go func() {
		res, err := d.Submit(ctx, candidate)
		done <- res
		errs <- err
	}()
	select {
	case <-pause.entered:
	case <-time.After(2 * time.Second):
		close(pause.release)
		t.Fatal("intent was not persisted")
	}
	// A helper (here the recovery scan) finishes the pending submission while
	// the owner's lock is still live: it must commit Suppressed.
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if jobState, ok, _ := d.ledger.get(ctx, jobKey("candidate")); !ok || jobState.rec.Outcome != outcomeSuppressed {
		t.Fatalf("candidate outcome = %+v", jobState)
	}
	// The owner then finishes, and only now does the original producer resume.
	c, _ := d.Consumer(ctx, "q")
	owner := receive(t, c)
	settle(t, owner, jetq.Decision{})
	d.ledger.kv = pause.KeyValue
	close(pause.release)
	res := <-done
	if err := <-errs; err != nil || res.Status != jetq.Suppressed {
		t.Fatalf("resumed producer = %+v, %v; want Suppressed", res, err)
	}
	if _, ok, _ := d.JobState(ctx, "candidate"); ok {
		t.Fatal("suppressed candidate became a live job")
	}
	// A retried submission of the same intent stays Suppressed for the whole
	// retention: it never turns into a runnable job.
	d.hks.now = func() time.Time { return time.Now().Add(time.Minute) }
	if res, err := d.Submit(ctx, candidate); err != nil || res.Status != jetq.Suppressed {
		t.Fatalf("retry = %+v, %v", res, err)
	}
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.JobState(ctx, "candidate"); ok {
		t.Fatal("recovery activated a suppressed candidate")
	}
}

// TestRecoveryDoesNotCreateSuppressedCandidate: a pending record whose
// reservation is live is committed Suppressed by the scan, without a producer
// ever resuming.
func TestRecoveryDoesNotCreateSuppressedCandidate(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "scan-suppressor"))
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mustSubmit(t, d, prepare(t, "owner", "q", unique))
	// A pending record as a producer would create it before deciding.
	env := prepare(t, "pending", "q", unique).Envelope.Clone()
	rec := &record{Env: env, Fingerprint: env.Fingerprint(), State: jetq.StateAwaiting, Outcome: outcomePending}
	if _, err := d.ledger.put(ctx, jobKey("pending"), 0, rec); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	ent, ok, _ := d.ledger.get(ctx, jobKey("pending"))
	if !ok || ent.rec.Outcome != outcomeSuppressed || ent.rec.ExistingJobID != "owner" {
		t.Fatalf("record = %+v", ent.rec)
	}
	c, _ := d.Consumer(ctx, "q")
	// The owner runs; the suppressed candidate never does.
	if got := receive(t, c).Envelope().JobID; got != "owner" {
		t.Fatalf("delivered %s", got)
	}
	expectNoDelivery(t, c, 150*time.Millisecond)
}

// TestSuppressedOutcomeIsFinalAndExpires: a suppressed record answers every
// retry and is cleaned up after the retention like a finished job.
func TestSuppressedOutcomeIsFinalAndExpires(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "suppressed-final")
	cfg.JobRetention = time.Minute
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeWindow, Key: "w", Window: time.Hour}
	mustSubmit(t, d, prepare(t, "first", "q", unique))
	second := prepare(t, "second", "q", unique)
	if res := mustSubmit(t, d, second); res.Status != jetq.Suppressed || res.ExistingJobID != "first" {
		t.Fatalf("suppressed = %+v", res)
	}
	for range 3 { // idempotent
		if res := mustSubmit(t, d, second); res.Status != jetq.Suppressed || res.ExistingJobID != "first" {
			t.Fatalf("repeat = %+v", res)
		}
	}
	d.hks.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.ledger.get(ctx, jobKey("second")); ok {
		t.Fatal("suppressed record outlived its retention")
	}
}

// TestSuppressedIntentRetention: a final Suppressed intent answers retries for
// its retention, then frees the key; it never becomes executable.
func TestSuppressedIntentRetention(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "suppressed-retention")
	cfg.JobRetention = time.Minute
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "k"}
	mustSubmit(t, d, prepare(t, "holder", "q", unique))
	mk := func(id string) *jetq.PreparedJob {
		p := prepare(t, id, "q", unique)
		p.Envelope.EnqueueKey = "stable"
		p.Fingerprint = p.Envelope.Fingerprint()
		return p
	}
	if res := mustSubmit(t, d, mk("first")); res.Status != jetq.Suppressed {
		t.Fatalf("first = %+v", res)
	}
	// The holder finishes; the retry still reports Suppressed.
	c, _ := d.Consumer(ctx, "q")
	settle(t, receive(t, c), jetq.Decision{})
	for range 2 {
		if res := mustSubmit(t, d, mk("retry")); res.Status != jetq.Suppressed || res.ExistingJobID != "holder" {
			t.Fatalf("retry = %+v", res)
		}
	}
	if _, ok, _ := d.JobState(ctx, "retry"); ok {
		t.Fatal("a suppressed submission became a live job")
	}
	// After the retention the intent and the suppressed record are gone.
	d.hks.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := d.readIntent(ctx, "stable"); ok {
		t.Fatal("suppressed intent outlived its retention")
	}
}
