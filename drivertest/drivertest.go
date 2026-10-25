// Package drivertest checks that a [jobq.Driver] keeps the driver contract.
// Driver packages call [Run] from a test with a factory returning a fresh,
// isolated driver for every subtest.
package drivertest

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
)

// Options tunes the suite to a driver.
type Options struct {
	// Resolution is the precision of run times; delays are checked with
	// this tolerance (default 1s).
	Resolution time.Duration
	// CancelReady reports whether the driver can cancel jobs that are due
	// but not claimed.
	CancelReady bool
}

// Run runs the conformance suite. newDriver returns a fresh driver whose
// jobs are not visible to others; the suite closes it.
func Run(t *testing.T, newDriver func(t *testing.T) jobq.Driver, opts Options) {
	if opts.Resolution <= 0 {
		opts.Resolution = time.Second
	}
	s := &suite{newDriver: newDriver, opts: opts}
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"EnqueueClaimComplete", s.enqueueClaimComplete},
		{"PayloadAndMetadataRoundTrip", s.roundTrip},
		{"ClaimRespectsLimit", s.claimLimit},
		{"ClaimReturnsWhenIdle", s.claimIdle},
		{"ClaimStopsOnContext", s.claimContext},
		{"ClaimWakesOnEnqueue", s.claimWakes},
		{"QueuesAreIsolated", s.queuesIsolated},
		{"DelayedJob", s.delayed},
		{"RetryCountsAttempt", s.retry},
		{"ReleaseKeepsAttempt", s.release},
		{"ExpiredLeaseIsReclaimed", s.expiredLease},
		{"ExtendKeepsLease", s.extend},
		{"StaleLeaseIsFenced", s.fenced},
		{"SettleEndsLease", s.settleEnds},
		{"JobIDInUse", s.jobIDInUse},
		{"UniqueUntilDone", s.uniqueUntilDone},
		{"UniqueFor", s.uniqueFor},
		{"CancelDelayed", s.cancelDelayed},
		{"CancelRunningStopsRetries", s.cancelRunning},
		{"CancelUnknown", s.cancelUnknown},
		{"FailDeadLetters", s.fail},
		{"CompetingClaims", s.competing},
		{"Inspector", s.inspector},
		{"Scheduler", s.scheduler},
		{"Worker", s.worker},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}
}

type suite struct {
	newDriver func(t *testing.T) jobq.Driver
	opts      Options
}

func (s *suite) driver(t *testing.T) jobq.Driver {
	t.Helper()
	d := s.newDriver(t)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return c
}

var seq atomic.Int64

func message(t *testing.T, queue string, opts ...jobq.EnqueueOption) *jobq.Message {
	t.Helper()
	n := seq.Add(1)
	msg, err := jobq.NewMessage("test-job", map[string]int64{"n": n}, append([]jobq.EnqueueOption{jobq.OnQueue(queue)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func enqueue(t *testing.T, d jobq.Driver, msg *jobq.Message) {
	t.Helper()
	if err := d.Enqueue(ctx(t), msg); err != nil {
		t.Fatalf("enqueue %s: %v", msg.ID, err)
	}
}

// claimOne claims until a job arrives or within expires.
func claimOne(t *testing.T, d jobq.Driver, queue string, lease, within time.Duration) jobq.Lease {
	t.Helper()
	leases := claimN(t, d, queue, 1, lease, within)
	if len(leases) == 0 {
		t.Fatalf("no job claimed from %s within %s", queue, within)
	}
	return leases[0]
}

func claimN(t *testing.T, d jobq.Driver, queue string, n int, lease, within time.Duration) []jobq.Lease {
	t.Helper()
	deadline := time.Now().Add(within)
	var got []jobq.Lease
	for len(got) < n && time.Now().Before(deadline) {
		c, cancel := context.WithDeadline(ctx(t), deadline)
		leases, err := d.Claim(c, queue, n-len(got), lease)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("claim: %v", err)
		}
		got = append(got, leases...)
	}
	return got
}

// expectNone checks that nothing can be claimed for d.
func expectNone(t *testing.T, d jobq.Driver, queue string, during time.Duration) {
	t.Helper()
	c, cancel := context.WithTimeout(ctx(t), during)
	defer cancel()
	for c.Err() == nil {
		leases, err := d.Claim(c, queue, 10, time.Minute)
		if len(leases) > 0 {
			t.Fatalf("claimed %s, want nothing", leases[0].Message().ID)
		}
		if err != nil && c.Err() == nil {
			t.Fatalf("claim: %v", err)
		}
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (s *suite) enqueueClaimComplete(t *testing.T) {
	d := s.driver(t)
	msg := message(t, "q")
	enqueue(t, d, msg)
	l := claimOne(t, d, "q", time.Minute, 5*time.Second)
	if l.Message().ID != msg.ID || l.Attempt() != 1 {
		t.Fatalf("claimed %s attempt %d", l.Message().ID, l.Attempt())
	}
	must(t, l.Complete(ctx(t)))
	expectNone(t, d, "q", time.Second)
	// The id is free again once the job finished.
	enqueue(t, d, message(t, "q", jobq.JobID(msg.ID)))
}

func (s *suite) roundTrip(t *testing.T) {
	d := s.driver(t)
	msg := message(t, "q", jobq.MaxAttempts(7), jobq.Timeout(3*time.Second), jobq.WithHeader("traceparent", "00-abc"))
	msg.Payload = []byte(`{"b": [1, 2.50, "x"],  "a":null}`)
	enqueue(t, d, msg)
	got := claimOne(t, d, "q", time.Minute, 5*time.Second).Message()
	if got.ID != msg.ID || got.Name != msg.Name || got.Queue != "q" || string(got.Payload) != string(msg.Payload) {
		t.Fatalf("message = %+v, want %+v", got, msg)
	}
	if got.MaxAttempts != 7 || got.Timeout != 3*time.Second || got.Header["traceparent"] != "00-abc" {
		t.Fatalf("metadata = %d %s %v", got.MaxAttempts, got.Timeout, got.Header)
	}
	if d := got.EnqueuedAt.Sub(msg.EnqueuedAt).Abs(); d > time.Millisecond {
		t.Fatalf("enqueued at %s, want %s", got.EnqueuedAt, msg.EnqueuedAt)
	}
	noLimit := message(t, "q2", jobq.Timeout(0))
	enqueue(t, d, noLimit)
	if got := claimOne(t, d, "q2", time.Minute, 5*time.Second).Message(); got.Timeout != jobq.NoTimeout {
		t.Fatalf("timeout = %s, want NoTimeout", got.Timeout)
	}
}

func (s *suite) claimLimit(t *testing.T) {
	d := s.driver(t)
	for range 5 {
		enqueue(t, d, message(t, "q"))
	}
	leases, err := d.Claim(ctx(t), "q", 3, time.Minute)
	must(t, err)
	if len(leases) == 0 || len(leases) > 3 {
		t.Fatalf("claimed %d, want 1 to 3", len(leases))
	}
	rest := claimN(t, d, "q", 5-len(leases), time.Minute, 5*time.Second)
	if len(leases)+len(rest) != 5 {
		t.Fatalf("claimed %d in total, want 5", len(leases)+len(rest))
	}
}

func (s *suite) claimIdle(t *testing.T) {
	d := s.driver(t)
	start := time.Now()
	leases, err := d.Claim(ctx(t), "idle", 1, time.Minute)
	must(t, err)
	if len(leases) != 0 {
		t.Fatalf("claimed %d jobs from an empty queue", len(leases))
	}
	if time.Since(start) > 30*time.Second {
		t.Fatalf("Claim took %s on an empty queue", time.Since(start))
	}
}

func (s *suite) claimContext(t *testing.T) {
	d := s.driver(t)
	c, cancel := context.WithCancel(ctx(t))
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err := d.Claim(c, "idle", 1, time.Minute)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Claim returned %s after cancellation", elapsed)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Claim error = %v, want nil or context.Canceled", err)
	}
}

func (s *suite) claimWakes(t *testing.T) {
	d := s.driver(t)
	got := make(chan time.Time, 1)
	go func() {
		leases := claimN(t, d, "q", 1, time.Minute, 10*time.Second)
		if len(leases) == 1 {
			got <- time.Now()
		}
		close(got)
	}()
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	enqueue(t, d, message(t, "q"))
	at, ok := <-got
	if !ok {
		t.Fatal("waiting claim got nothing")
	}
	if latency := at.Sub(start); latency > s.opts.Resolution+time.Second {
		t.Fatalf("waiting claim took %s to see a new job", latency)
	}
}

func (s *suite) queuesIsolated(t *testing.T) {
	d := s.driver(t)
	enqueue(t, d, message(t, "a"))
	expectNone(t, d, "b", time.Second)
	claimOne(t, d, "a", time.Minute, 5*time.Second)
}

func (s *suite) delayed(t *testing.T) {
	d := s.driver(t)
	delay := 2 * time.Second
	msg := message(t, "q", jobq.Delay(delay))
	enqueue(t, d, msg)
	l := claimOne(t, d, "q", time.Minute, delay+s.opts.Resolution+5*time.Second)
	if early := time.Until(msg.RunAt); early > s.opts.Resolution/2 && early > 100*time.Millisecond {
		t.Fatalf("delayed job claimed %s early", early)
	}
	if l.Attempt() != 1 {
		t.Fatalf("attempt = %d", l.Attempt())
	}
}

func (s *suite) retry(t *testing.T) {
	d := s.driver(t)
	enqueue(t, d, message(t, "q"))
	l := claimOne(t, d, "q", time.Minute, 5*time.Second)
	must(t, l.Retry(ctx(t), time.Now(), "boom"))
	l = claimOne(t, d, "q", time.Minute, 5*time.Second)
	if l.Attempt() != 2 {
		t.Fatalf("attempt after retry = %d, want 2", l.Attempt())
	}
	at := time.Now().Add(2 * time.Second)
	must(t, l.Retry(ctx(t), at, "boom"))
	expectNone(t, d, "q", time.Second)
	l = claimOne(t, d, "q", time.Minute, 2*time.Second+s.opts.Resolution+5*time.Second)
	if l.Attempt() != 3 {
		t.Fatalf("attempt after delayed retry = %d, want 3", l.Attempt())
	}
}

func (s *suite) release(t *testing.T) {
	d := s.driver(t)
	enqueue(t, d, message(t, "q"))
	l := claimOne(t, d, "q", time.Minute, 5*time.Second)
	since := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	must(t, l.Release(ctx(t), jobq.Release{Snooze: true, UnknownSince: since}))
	l = claimOne(t, d, "q", time.Minute, 5*time.Second)
	if l.Attempt() != 1 {
		t.Fatalf("attempt after release = %d, want 1", l.Attempt())
	}
	if m := l.Message(); m.Snoozes != 1 || !m.UnknownSince.Equal(since) {
		t.Fatalf("snoozes = %d, unknown since = %s, want 1, %s", m.Snoozes, m.UnknownSince, since)
	}
	must(t, l.Release(ctx(t), jobq.Release{At: time.Now().Add(2 * time.Second)}))
	expectNone(t, d, "q", time.Second)
	l = claimOne(t, d, "q", time.Minute, 2*time.Second+s.opts.Resolution+5*time.Second)
	if m := l.Message(); l.Attempt() != 1 || m.Snoozes != 1 || !m.UnknownSince.Equal(since) {
		t.Fatalf("after delayed release: attempt %d, snoozes %d, unknown since %s", l.Attempt(), m.Snoozes, m.UnknownSince)
	}
}

func (s *suite) expiredLease(t *testing.T) {
	d := s.driver(t)
	enqueue(t, d, message(t, "q"))
	claimOne(t, d, "q", time.Second, 5*time.Second)
	l := claimOne(t, d, "q", time.Minute, s.opts.Resolution+10*time.Second)
	if l.Attempt() != 2 {
		t.Fatalf("attempt after expired lease = %d, want 2", l.Attempt())
	}
}

func (s *suite) extend(t *testing.T) {
	d := s.driver(t)
	enqueue(t, d, message(t, "q"))
	l := claimOne(t, d, "q", 2*time.Second, 5*time.Second)
	stop := time.Now().Add(4 * time.Second)
	for time.Now().Before(stop) {
		must(t, l.Extend(ctx(t)))
		time.Sleep(500 * time.Millisecond)
	}
	expectNone(t, d, "q", 500*time.Millisecond)
	must(t, l.Complete(ctx(t)))
}

func (s *suite) fenced(t *testing.T) {
	d := s.driver(t)
	enqueue(t, d, message(t, "q"))
	stale := claimOne(t, d, "q", time.Second, 5*time.Second)
	fresh := claimOne(t, d, "q", time.Minute, s.opts.Resolution+10*time.Second)
	for name, op := range map[string]func() error{
		"Extend":   func() error { return stale.Extend(ctx(t)) },
		"Complete": func() error { return stale.Complete(ctx(t)) },
		"Retry":    func() error { return stale.Retry(ctx(t), time.Now(), "stale") },
		"Release":  func() error { return stale.Release(ctx(t), jobq.Release{}) },
		"Fail":     func() error { return stale.Fail(ctx(t), "stale") },
	} {
		if err := op(); !errors.Is(err, jobq.ErrLeaseLost) {
			t.Errorf("stale %s = %v, want ErrLeaseLost", name, err)
		}
	}
	// The fresh claim is untouched.
	must(t, fresh.Extend(ctx(t)))
	expectNone(t, d, "q", 500*time.Millisecond)
	must(t, fresh.Complete(ctx(t)))
}

func (s *suite) settleEnds(t *testing.T) {
	d := s.driver(t)
	enqueue(t, d, message(t, "q"))
	l := claimOne(t, d, "q", time.Minute, 5*time.Second)
	must(t, l.Retry(ctx(t), time.Now().Add(time.Hour), "later"))
	if err := l.Complete(ctx(t)); !errors.Is(err, jobq.ErrLeaseLost) {
		t.Fatalf("Complete after Retry = %v, want ErrLeaseLost", err)
	}
}

func (s *suite) jobIDInUse(t *testing.T) {
	d := s.driver(t)
	msg := message(t, "q", jobq.Delay(time.Hour))
	enqueue(t, d, msg)
	if err := d.Enqueue(ctx(t), message(t, "q", jobq.JobID(msg.ID))); !errors.Is(err, jobq.ErrJobIDInUse) {
		t.Fatalf("delayed id reused: %v, want ErrJobIDInUse", err)
	}
	ready := message(t, "q")
	enqueue(t, d, ready)
	if err := d.Enqueue(ctx(t), message(t, "other", jobq.JobID(ready.ID))); !errors.Is(err, jobq.ErrJobIDInUse) {
		t.Fatalf("ready id reused on another queue: %v, want ErrJobIDInUse", err)
	}
}

func (s *suite) uniqueUntilDone(t *testing.T) {
	d := s.driver(t)
	enqueue(t, d, message(t, "q", jobq.Unique("k")))
	if err := d.Enqueue(ctx(t), message(t, "q", jobq.Unique("k"))); !errors.Is(err, jobq.ErrDuplicate) {
		t.Fatalf("second unique enqueue = %v, want ErrDuplicate", err)
	}
	// Another key is independent.
	enqueue(t, d, message(t, "other", jobq.Unique("other")))

	// Retrying keeps the key.
	l := claimOne(t, d, "q", time.Minute, 5*time.Second)
	must(t, l.Retry(ctx(t), time.Now(), "again"))
	if err := d.Enqueue(ctx(t), message(t, "q", jobq.Unique("k"))); !errors.Is(err, jobq.ErrDuplicate) {
		t.Fatalf("enqueue while retrying = %v, want ErrDuplicate", err)
	}
	// Completing releases it.
	l = claimOne(t, d, "q", time.Minute, 5*time.Second)
	must(t, l.Complete(ctx(t)))
	enqueue(t, d, message(t, "q", jobq.Unique("k")))

	// Dead-lettering releases the key too.
	l = claimOne(t, d, "q", time.Minute, 5*time.Second)
	must(t, l.Fail(ctx(t), "dead"))
	enqueue(t, d, message(t, "q", jobq.Unique("k")))
}

func (s *suite) uniqueFor(t *testing.T) {
	d := s.driver(t)
	window := 2 * time.Second
	enqueue(t, d, message(t, "q", jobq.UniqueFor("w", window)))
	l := claimOne(t, d, "q", time.Minute, 5*time.Second)
	must(t, l.Complete(ctx(t)))
	if err := d.Enqueue(ctx(t), message(t, "q", jobq.UniqueFor("w", window))); !errors.Is(err, jobq.ErrDuplicate) {
		t.Fatalf("enqueue within window after completion = %v, want ErrDuplicate", err)
	}
	time.Sleep(window + s.opts.Resolution)
	enqueue(t, d, message(t, "q", jobq.UniqueFor("w", window)))
}

func (s *suite) cancelDelayed(t *testing.T) {
	d := s.driver(t)
	msg := message(t, "q", jobq.Delay(2*time.Second), jobq.Unique("c"))
	enqueue(t, d, msg)
	must(t, d.Cancel(ctx(t), msg.ID))
	if err := d.Cancel(ctx(t), msg.ID); !errors.Is(err, jobq.ErrNotFound) {
		t.Fatalf("second cancel = %v, want ErrNotFound", err)
	}
	expectNone(t, d, "q", 2*time.Second+s.opts.Resolution+time.Second)
	// The id and the unique key are free again.
	enqueue(t, d, message(t, "q", jobq.JobID(msg.ID), jobq.Unique("c")))

	if s.opts.CancelReady {
		ready := message(t, "r")
		enqueue(t, d, ready)
		must(t, d.Cancel(ctx(t), ready.ID))
		expectNone(t, d, "r", time.Second)
	}
}

func (s *suite) cancelRunning(t *testing.T) {
	d := s.driver(t)
	msg := message(t, "q")
	enqueue(t, d, msg)
	l := claimOne(t, d, "q", time.Minute, 5*time.Second)
	// Retrying first makes the job cancelable on every driver.
	must(t, l.Retry(ctx(t), time.Now().Add(time.Hour), "wait"))
	must(t, d.Cancel(ctx(t), msg.ID))
	expectNone(t, d, "q", time.Second)

	msg = message(t, "q")
	enqueue(t, d, msg)
	l = claimOne(t, d, "q", time.Minute, 5*time.Second)
	err := d.Cancel(ctx(t), msg.ID)
	if err != nil && !errors.Is(err, jobq.ErrNotFound) {
		t.Fatalf("cancel running = %v", err)
	}
	if err == nil {
		// Accepted while running: the retry must not run.
		_ = l.Retry(ctx(t), time.Now(), "again")
		expectNone(t, d, "q", time.Second+s.opts.Resolution)
	} else {
		must(t, l.Complete(ctx(t)))
	}
}

func (s *suite) cancelUnknown(t *testing.T) {
	d := s.driver(t)
	if err := d.Cancel(ctx(t), "missing"); !errors.Is(err, jobq.ErrNotFound) {
		t.Fatalf("cancel missing = %v, want ErrNotFound", err)
	}
}

func (s *suite) fail(t *testing.T) {
	d := s.driver(t)
	msg := message(t, "q")
	enqueue(t, d, msg)
	l := claimOne(t, d, "q", time.Minute, 5*time.Second)
	must(t, l.Fail(ctx(t), "permanent"))
	expectNone(t, d, "q", time.Second)
	insp, ok := d.(jobq.Inspector)
	if !ok {
		return
	}
	page, err := insp.DeadLetters(ctx(t), jobq.DeadLetterQuery{Limit: 10})
	must(t, err)
	if len(page.DeadLetters) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(page.DeadLetters))
	}
	dl := page.DeadLetters[0]
	if dl.Message.ID != msg.ID || dl.Error != "permanent" || dl.Attempts != 1 || dl.FailedAt.IsZero() ||
		string(dl.Message.Payload) != string(msg.Payload) {
		t.Fatalf("dead letter = %+v", dl)
	}
}

func (s *suite) competing(t *testing.T) {
	d := s.driver(t)
	const jobs = 50
	for range jobs {
		enqueue(t, d, message(t, "q"))
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				mu.Lock()
				done := len(seen) == jobs
				mu.Unlock()
				if done {
					return
				}
				c, cancel := context.WithTimeout(ctx(t), time.Second)
				leases, _ := d.Claim(c, "q", 3, time.Minute)
				cancel()
				for _, l := range leases {
					mu.Lock()
					seen[l.Message().ID]++
					mu.Unlock()
					_ = l.Complete(ctx(t))
				}
			}
		})
	}
	wg.Wait()
	if len(seen) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
}

func (s *suite) inspector(t *testing.T) {
	d := s.driver(t)
	insp, ok := d.(jobq.Inspector)
	if !ok {
		t.Skip("driver is not an Inspector")
	}
	enqueue(t, d, message(t, "s"))
	enqueue(t, d, message(t, "s"))
	enqueue(t, d, message(t, "s", jobq.Delay(time.Hour)))
	running := claimOne(t, d, "s", time.Minute, 5*time.Second)
	deadMsg := message(t, "s2")
	enqueue(t, d, deadMsg)
	must(t, claimOne(t, d, "s2", time.Minute, 5*time.Second).Fail(ctx(t), "x"))

	var stats jobq.Stats
	want := []jobq.QueueStats{{Queue: "s", Ready: 1, Scheduled: 1, Running: 1}, {Queue: "s2", Dead: 1}}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var err error
		stats, err = insp.Stats(ctx(t))
		must(t, err)
		if slices.Equal(stats.Queues, want) || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !slices.Equal(stats.Queues, want) {
		t.Fatalf("stats = %+v, want %+v", stats.Queues, want)
	}
	must(t, running.Complete(ctx(t)))

	// Paging and retry of dead letters.
	var failedOrder []string
	for range 3 {
		m := message(t, "s3")
		enqueue(t, d, m)
		must(t, claimOne(t, d, "s3", time.Minute, 5*time.Second).Fail(ctx(t), m.ID))
		failedOrder = append([]string{m.ID}, failedOrder...)
		time.Sleep(10 * time.Millisecond)
	}
	var ids []string
	cursor := ""
	for {
		page, err := insp.DeadLetters(ctx(t), jobq.DeadLetterQuery{Queue: "s3", Cursor: cursor, Limit: 2})
		must(t, err)
		for _, dl := range page.DeadLetters {
			ids = append(ids, dl.Message.ID)
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if !slices.Equal(ids, failedOrder) {
		t.Fatalf("paged dead letters = %v, want 3 newest first", ids)
	}
	must(t, insp.RetryDead(ctx(t), ids[0]))
	if err := insp.RetryDead(ctx(t), ids[0]); !errors.Is(err, jobq.ErrNotFound) {
		t.Fatalf("second RetryDead = %v, want ErrNotFound", err)
	}
	l := claimOne(t, d, "s3", time.Minute, 5*time.Second)
	if l.Message().ID != ids[0] || l.Attempt() != 1 {
		t.Fatalf("retried dead letter claimed as %s attempt %d", l.Message().ID, l.Attempt())
	}
	must(t, insp.DeleteDead(ctx(t), ids[1]))
	if err := insp.DeleteDead(ctx(t), ids[1]); !errors.Is(err, jobq.ErrNotFound) {
		t.Fatalf("second DeleteDead = %v, want ErrNotFound", err)
	}
}

func (s *suite) scheduler(t *testing.T) {
	d := s.driver(t)
	sch, ok := d.(jobq.Scheduler)
	if !ok {
		t.Skip("driver is not a Scheduler")
	}
	msg := message(t, "cron")
	msg.ID, msg.EnqueuedAt = "", time.Time{}
	spec := jobq.ScheduleSpec{Key: "tick", Cron: "@every 1s", Message: msg}
	must(t, sch.SyncSchedules(ctx(t), []jobq.ScheduleSpec{spec}))
	must(t, sch.SyncSchedules(ctx(t), []jobq.ScheduleSpec{spec})) // idempotent
	var ids []string
	for len(ids) < 2 {
		l := claimOne(t, d, "cron", time.Minute, 5*time.Second+s.opts.Resolution)
		m := l.Message()
		if !strings.HasPrefix(m.ID, "tick-") || string(m.Payload) != string(msg.Payload) {
			t.Fatalf("fired job = %+v", m)
		}
		if slices.Contains(ids, m.ID) {
			t.Fatalf("occurrence %s fired twice", m.ID)
		}
		ids = append(ids, m.ID)
		must(t, l.Complete(ctx(t)))
	}
	must(t, sch.SyncSchedules(ctx(t), nil))
	time.Sleep(s.opts.Resolution + time.Second)
	// Drain an occurrence that fired while removing.
	c, cancel := context.WithTimeout(ctx(t), 500*time.Millisecond)
	leases, _ := d.Claim(c, "cron", 10, time.Minute)
	cancel()
	for _, l := range leases {
		_ = l.Complete(ctx(t))
	}
	expectNone(t, d, "cron", 2*time.Second+s.opts.Resolution)
	if insp, ok := d.(jobq.Inspector); ok {
		stats, err := insp.Stats(ctx(t))
		must(t, err)
		if stats.Schedules != 0 {
			t.Fatalf("schedules after removal = %d", stats.Schedules)
		}
	}
}

type ping struct{ N int }

func (ping) JobName() string { return "ping" }

// worker runs the jobq worker on the driver end to end.
func (s *suite) worker(t *testing.T) {
	d := s.driver(t)
	c := jobq.New(d)
	w := c.NewWorker(jobq.Queue{Name: "w", Concurrency: 4, MaxAttempts: 2, Backoff: jobq.Constant(0), Lease: 5 * time.Second})
	var mu sync.Mutex
	attempts := map[int]int{}
	done := make(chan int, 10)
	failed := make(chan int, 10)
	jobq.Handle(w, func(ctx context.Context, p ping) error {
		mu.Lock()
		attempts[p.N]++
		n := attempts[p.N]
		mu.Unlock()
		switch {
		case p.N == 2 && n == 1:
			return errors.New("flaky")
		case p.N == 3:
			return errors.New("always")
		}
		done <- p.N
		return nil
	}, jobq.OnFailure(func(_ context.Context, p ping, _ error) { failed <- p.N }))
	runCtx, stop := context.WithCancel(ctx(t))
	exited := make(chan error, 1)
	go func() { exited <- w.Run(runCtx) }()
	for n := 1; n <= 3; n++ {
		if _, err := c.Enqueue(ctx(t), ping{N: n}, jobq.OnQueue("w")); err != nil {
			t.Fatal(err)
		}
	}
	got := map[int]bool{}
	timeout := time.After(20*time.Second + 2*s.opts.Resolution)
	for len(got) < 2 || len(failed) == 0 {
		select {
		case n := <-done:
			got[n] = true
		case <-timeout:
			t.Fatalf("done = %v, failed = %d", got, len(failed))
		case <-time.After(10 * time.Millisecond):
		}
	}
	if n := <-failed; n != 3 {
		t.Fatalf("failed job = %d", n)
	}
	stop()
	if err := <-exited; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts[1] != 1 || attempts[2] != 2 || attempts[3] != 2 {
		t.Fatalf("attempts = %v", attempts)
	}
}
