// Package memq is an in-memory jobq driver for tests, examples and
// single-process programs that can afford to lose jobs on exit.
package memq

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/runforyou-ai/jobq"
)

// Driver keeps jobs in memory. It implements [jobq.Driver],
// [jobq.Scheduler] and [jobq.Inspector].
type Driver struct {
	mu        sync.Mutex
	seq       int64
	jobs      map[string]*entry
	dead      []*dead // oldest first
	unique    map[string]uniqueHold
	queues    map[string]bool
	schedules map[string]*schedule
	wake      chan struct{} // closed and replaced when a job becomes due
	now       func() time.Time
	closed    bool
	stop      chan struct{}
	done      chan struct{}
}

type entry struct {
	seq         int64 // insertion order, for a fair claim order
	msg         *jobq.Message
	state       int
	attempt     int
	token       int // fences leases: incremented on every claim
	lockedUntil time.Time
	lastError   string
	canceled    bool // cancelled while running: dropped when the attempt ends
}

const (
	pending = iota
	running
)

type dead struct {
	msg      *jobq.Message
	attempts int
	err      string
	failedAt time.Time
	seq      int
}

type uniqueHold struct {
	id    string
	until time.Time // zero: until the job finishes
}

type schedule struct {
	spec jobq.ScheduleSpec
	cron jobq.Cron
	loc  *time.Location
	next time.Time
}

// New returns an empty driver. Close it to stop its scheduler.
func New() *Driver {
	d := &Driver{
		jobs:      map[string]*entry{},
		unique:    map[string]uniqueHold{},
		queues:    map[string]bool{},
		schedules: map[string]*schedule{},
		wake:      make(chan struct{}),
		now:       time.Now,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go d.scheduler()
	return d
}

// Enqueue implements [jobq.Driver].
func (d *Driver) Enqueue(_ context.Context, msg *jobq.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errClosed
	}
	return d.insert(msg.Clone())
}

func (d *Driver) insert(msg *jobq.Message) error {
	if _, ok := d.jobs[msg.ID]; ok {
		return jobq.ErrJobIDInUse
	}
	now := d.now()
	if msg.UniqueKey != "" {
		if h, ok := d.unique[msg.UniqueKey]; ok && !h.until.IsZero() && !now.Before(h.until) {
			delete(d.unique, msg.UniqueKey) // the window has passed
		}
		if h, ok := d.unique[msg.UniqueKey]; ok && (h.until.IsZero() || now.Before(h.until)) {
			return jobq.ErrDuplicate
		}
		hold := uniqueHold{id: msg.ID}
		if msg.UniqueFor > 0 {
			hold.until = msg.EnqueuedAt.Add(msg.UniqueFor)
		}
		d.unique[msg.UniqueKey] = hold
	}
	d.seq++
	d.jobs[msg.ID] = &entry{seq: d.seq, msg: msg}
	d.queues[msg.Queue] = true
	d.signal()
	return nil
}

// signal wakes claimers; the caller holds d.mu.
func (d *Driver) signal() {
	close(d.wake)
	d.wake = make(chan struct{})
}

// releaseUnique frees the job's key if it holds one until done; the caller holds d.mu.
func (d *Driver) releaseUnique(msg *jobq.Message) {
	if msg.UniqueKey == "" {
		return
	}
	if h, ok := d.unique[msg.UniqueKey]; ok && h.id == msg.ID && h.until.IsZero() {
		delete(d.unique, msg.UniqueKey)
	}
}

// pollInterval bounds how long Claim waits before returning empty.
const pollInterval = 2 * time.Second

// Claim implements [jobq.Driver].
func (d *Driver) Claim(ctx context.Context, queue string, limit int, lease time.Duration) ([]jobq.Lease, error) {
	deadline := time.NewTimer(pollInterval)
	defer deadline.Stop()
	if limit <= 0 {
		return nil, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return nil, errClosed
		}
		d.queues[queue] = true
		leases, next := d.claim(queue, limit, lease)
		wake := d.wake
		d.mu.Unlock()
		if len(leases) > 0 {
			return leases, nil
		}
		var due <-chan time.Time
		if !next.IsZero() {
			t := time.NewTimer(max(next.Sub(d.now()), time.Millisecond))
			defer t.Stop()
			due = t.C
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, nil
		case <-wake:
		case <-due:
		}
	}
}

// claim leases due jobs and returns when the next one becomes due; the
// caller holds d.mu.
func (d *Driver) claim(queue string, limit int, lease time.Duration) ([]jobq.Lease, time.Time) {
	now := d.now()
	var due []*entry
	var next time.Time
	for id, e := range d.jobs {
		if e.msg.Queue != queue {
			continue
		}
		if e.canceled && !now.Before(e.lockedUntil) {
			delete(d.jobs, id) // cancelled during an attempt that never settled
			continue
		}
		at := e.msg.RunAt
		if e.state == running {
			at = e.lockedUntil
		}
		if at.After(now) {
			if next.IsZero() || at.Before(next) {
				next = at
			}
			continue
		}
		due = append(due, e)
	}
	slices.SortFunc(due, func(a, b *entry) int {
		if c := dueAt(a, now).Compare(dueAt(b, now)); c != 0 {
			return c
		}
		return cmp.Compare(a.seq, b.seq)
	})
	var leases []jobq.Lease
	for _, e := range due[:min(len(due), limit)] {
		e.state = running
		e.attempt++
		e.token++
		e.lockedUntil = now.Add(lease)
		leases = append(leases, &memLease{d: d, e: e, token: e.token, attempt: e.attempt, msg: e.msg.Clone(), lease: lease})
	}
	return leases, next
}

// dueAt is when the entry became claimable: the run time, the end of a
// running lease, or the enqueue time for a job that was due at once.
func dueAt(e *entry, now time.Time) time.Time {
	switch {
	case e.state == running:
		return e.lockedUntil
	case !e.msg.RunAt.IsZero():
		return e.msg.RunAt
	case !e.msg.EnqueuedAt.IsZero():
		return e.msg.EnqueuedAt
	}
	return now
}

// Cancel implements [jobq.Driver]. Pending jobs, due or not, can be cancelled;
// a running job is removed once its current attempt ends.
func (d *Driver) Cancel(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.jobs[id]
	if !ok || e.canceled {
		return jobq.ErrNotFound
	}
	d.releaseUnique(e.msg)
	if e.state == running && d.now().Before(e.lockedUntil) {
		e.canceled = true
		return nil
	}
	delete(d.jobs, id)
	return nil
}

// Close implements [jobq.Driver].
func (d *Driver) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.signal()
	d.mu.Unlock()
	close(d.stop)
	<-d.done
	return nil
}

var errClosed = fmt.Errorf("memq: driver closed")

type memLease struct {
	d       *Driver
	e       *entry
	token   int
	attempt int
	msg     *jobq.Message
	lease   time.Duration
}

func (l *memLease) Message() *jobq.Message { return l.msg }
func (l *memLease) Attempt() int           { return l.attempt }

// held checks the fence; the caller holds d.mu.
func (l *memLease) held() error {
	e, ok := l.d.jobs[l.msg.ID]
	if !ok || e != l.e || e.token != l.token || e.state != running {
		return jobq.ErrLeaseLost
	}
	if !l.d.now().Before(e.lockedUntil) {
		return jobq.ErrLeaseLost
	}
	return nil
}

func (l *memLease) Extend(context.Context) error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	if err := l.held(); err != nil {
		return err
	}
	l.e.lockedUntil = l.d.now().Add(l.lease)
	return nil
}

func (l *memLease) Complete(context.Context) error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	if err := l.held(); err != nil {
		return err
	}
	delete(l.d.jobs, l.msg.ID)
	l.d.releaseUnique(l.e.msg)
	return nil
}

func (l *memLease) Retry(_ context.Context, at time.Time, reason string) error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	if err := l.held(); err != nil {
		return err
	}
	if l.e.canceled {
		delete(l.d.jobs, l.msg.ID)
		return nil
	}
	l.e.state = pending
	l.e.msg.RunAt = at
	l.e.lastError = reason
	l.d.signal()
	return nil
}

func (l *memLease) Release(_ context.Context, r jobq.Release) error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	if err := l.held(); err != nil {
		return err
	}
	if l.e.canceled {
		delete(l.d.jobs, l.msg.ID)
		return nil
	}
	l.e.state = pending
	l.e.attempt--
	l.e.msg.RunAt = r.At
	if r.Snooze {
		l.e.msg.Snoozes++
	}
	l.e.msg.UnknownSince = r.UnknownSince
	l.d.signal()
	return nil
}

func (l *memLease) Fail(_ context.Context, reason string) error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	if err := l.held(); err != nil {
		return err
	}
	delete(l.d.jobs, l.msg.ID)
	l.d.releaseUnique(l.e.msg)
	if l.e.canceled {
		return nil
	}
	seq := 1
	if n := len(l.d.dead); n > 0 {
		seq = l.d.dead[n-1].seq + 1
	}
	l.d.dead = append(l.d.dead, &dead{msg: l.e.msg, attempts: l.e.attempt, err: reason, failedAt: l.d.now(), seq: seq})
	return nil
}

// SyncSchedules implements [jobq.Scheduler].
func (d *Driver) SyncSchedules(_ context.Context, specs []jobq.ScheduleSpec) error {
	parsed := make(map[string]*schedule, len(specs))
	for _, s := range specs {
		cron, err := jobq.ParseCron(s.Cron)
		if err != nil {
			return err
		}
		loc := time.UTC
		if s.Timezone != "" {
			if loc, err = time.LoadLocation(s.Timezone); err != nil {
				return err
			}
		}
		parsed[s.Key] = &schedule{spec: s, cron: cron, loc: loc}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	for key, s := range parsed {
		if old, ok := d.schedules[key]; ok && sameSpec(old.spec, s.spec) {
			parsed[key] = old
			continue
		}
		s.next = s.cron.Next(now, s.loc)
	}
	d.schedules = parsed
	return nil
}

func sameSpec(a, b jobq.ScheduleSpec) bool {
	if a.Cron != b.Cron || a.Timezone != b.Timezone {
		return false
	}
	x, y := a.Message, b.Message
	return x.Name == y.Name && x.Queue == y.Queue && string(x.Payload) == string(y.Payload) &&
		maps.Equal(x.Header, y.Header) && x.MaxAttempts == y.MaxAttempts && x.Timeout == y.Timeout
}

func (d *Driver) scheduler() {
	defer close(d.done)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
		}
		d.mu.Lock()
		now := d.now()
		for key, s := range d.schedules {
			if s.next.IsZero() || s.next.After(now) {
				continue
			}
			msg := s.spec.Message.Clone()
			msg.ID = key + "-" + strconv.FormatInt(s.next.Unix(), 10)
			_ = d.insert(msg) // an id still in use skips the occurrence
			s.next = s.cron.Next(now, s.loc)
		}
		d.mu.Unlock()
	}
}

// Stats implements [jobq.Inspector].
func (d *Driver) Stats(context.Context) (jobq.Stats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	byQueue := map[string]*jobq.QueueStats{}
	get := func(q string) *jobq.QueueStats {
		s, ok := byQueue[q]
		if !ok {
			s = &jobq.QueueStats{Queue: q}
			byQueue[q] = s
		}
		return s
	}
	for q := range d.queues {
		get(q)
	}
	for _, e := range d.jobs {
		s := get(e.msg.Queue)
		switch {
		case e.state == running && now.Before(e.lockedUntil):
			s.Running++
		case e.state == running || !e.msg.RunAt.After(now):
			s.Ready++
		default:
			s.Scheduled++
		}
	}
	for _, x := range d.dead {
		get(x.msg.Queue).Dead++
	}
	stats := jobq.Stats{Schedules: len(d.schedules)}
	for _, q := range slices.Sorted(maps.Keys(byQueue)) {
		stats.Queues = append(stats.Queues, *byQueue[q])
	}
	return stats, nil
}

// DeadLetters implements [jobq.Inspector].
func (d *Driver) DeadLetters(_ context.Context, q jobq.DeadLetterQuery) (jobq.DeadLetterPage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	before := -1
	if q.Cursor != "" {
		n, err := strconv.Atoi(q.Cursor)
		if err != nil {
			return jobq.DeadLetterPage{}, fmt.Errorf("memq: invalid cursor %q", q.Cursor)
		}
		before = n
	}
	var page jobq.DeadLetterPage
	lastSeq := 0
	for i := len(d.dead) - 1; i >= 0; i-- {
		x := d.dead[i]
		if (before >= 0 && x.seq >= before) || (q.Queue != "" && x.msg.Queue != q.Queue) {
			continue
		}
		if len(page.DeadLetters) == q.Limit {
			page.Next = strconv.Itoa(lastSeq)
			break
		}
		page.DeadLetters = append(page.DeadLetters, jobq.DeadLetter{Message: x.msg.Clone(), Attempts: x.attempts, Error: x.err, FailedAt: x.failedAt})
		lastSeq = x.seq
	}
	return page, nil
}

// RetryDead implements [jobq.Inspector].
func (d *Driver) RetryDead(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := len(d.dead) - 1; i >= 0; i-- {
		x := d.dead[i]
		if x.msg.ID != id {
			continue
		}
		if _, ok := d.jobs[id]; ok {
			return jobq.ErrJobIDInUse
		}
		msg := x.msg.Clone()
		msg.RunAt, msg.Snoozes, msg.UnknownSince = time.Time{}, 0, time.Time{}
		msg.UniqueKey = "" // the key may have been taken since
		d.jobs[id] = &entry{msg: msg}
		d.dead = slices.Delete(d.dead, i, i+1)
		d.signal()
		return nil
	}
	return jobq.ErrNotFound
}

// DeleteDead implements [jobq.Inspector].
func (d *Driver) DeleteDead(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := len(d.dead) - 1; i >= 0; i-- {
		if d.dead[i].msg.ID == id {
			d.dead = slices.Delete(d.dead, i, i+1)
			return nil
		}
	}
	return jobq.ErrNotFound
}
