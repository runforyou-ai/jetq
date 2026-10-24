package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/runforyou-ai/jetq"
)

// Schedule is a recurring job. Schedules are a separate extension: a schedule is
// not a job, and every firing submits an ordinary job through the normal
// submission path.
//
// A schedule set is owned: SyncSchedules writes the set under one owner name
// on top of the configuration version the caller expects, so a stale process
// can never delete what a newer deploy added. Owners never touch each other's
// schedules. Firing only moves cursors and never changes the version.
//
// Missed occurrences: with CatchUp false a scheduler that was down skips them
// and fires the next due one; with CatchUp true it fires up to MaxCatchUp of
// them, oldest first.
//
// Firing is at-least-once with a stable identity per occurrence: an
// occurrence's job carries an EnqueueKey derived from the schedule key and the
// occurrence's absolute time. A scheduler that crashes or races another one
// therefore submits the same job again instead of activating a second copy.
type Schedule struct {
	// Key identifies the schedule within its owner.
	Key string
	// Cron is a five-field cron expression, or a descriptor such as @hourly
	// or @every 5m. Times are evaluated in Timezone.
	Cron string
	// Timezone is an IANA time zone; empty means UTC. It does not apply to
	// @every.
	Timezone string
	// Task is the task name used to select the handler.
	Task string
	// Payload is the JSON payload stored byte for byte.
	Payload json.RawMessage
	// Queue the fired jobs go to; empty means [jetq.DefaultQueue].
	Queue string
	// Policy is the resolved policy of every fired job. A zero value means
	// the producer defaults at firing time.
	Policy jetq.Policy
	// Metadata is attached to every fired job.
	Metadata jetq.Metadata
	// CatchUp fires occurrences that were missed while no scheduler ran. With
	// CatchUp false (the default) a missed occurrence is skipped: the next
	// firing is the next occurrence after now.
	CatchUp bool
	// MaxCatchUp bounds how many missed occurrences are fired after a pause
	// (default 100). Older ones are skipped and logged.
	MaxCatchUp int
}

// scheduleManifest is one owner's durable schedule set.
type scheduleManifest struct {
	Owner     string           `json:"owner"`
	Version   int64            `json:"version"`
	Schedules []scheduleRecord `json:"schedules"`
}

// scheduleRecord is one compiled schedule.
type scheduleRecord struct {
	Key      string `json:"key"`
	Cron     string `json:"cron"`
	Timezone string `json:"timezone,omitempty"`
	Task     string `json:"task"`
	// Payload is the JSON payload kept byte for byte: stored as opaque bytes
	// (base64 in the manifest) so encoding never compacts or escapes it.
	Payload    []byte        `json:"payload_bytes"`
	Queue      string        `json:"queue"`
	Policy     jetq.Policy   `json:"policy"`
	Metadata   jetq.Metadata `json:"metadata,omitempty"`
	CatchUp    bool          `json:"catch_up,omitempty"`
	MaxCatchUp int           `json:"max_catch_up,omitempty"`
	// Next is the next occurrence this schedule fires, in UTC. It is advanced
	// with a compare-and-set, so one occurrence is fired by one scheduler.
	Next time.Time `json:"next"`
}

func manifestKey(namespace, owner string) string {
	return "sc." + hashKey(namespace, "schedule", owner)
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func (d *Driver) readManifest(ctx context.Context, owner string) (*ledgerEntry, *scheduleManifest, bool, error) {
	ent, ok, err := d.ledger.get(ctx, manifestKey(d.cfg.Namespace, owner))
	if err != nil || !ok {
		return nil, nil, false, err
	}
	var m scheduleManifest
	if err := json.Unmarshal(ent.rec.Data, &m); err != nil {
		return nil, nil, false, fmt.Errorf("natsq: decode schedule manifest of %s: %w", owner, err)
	}
	return ent, &m, true, nil
}

// compileSchedule validates a schedule and computes its next occurrence.
func (d *Driver) compileSchedule(s Schedule) (scheduleRecord, error) {
	if err := jetq.ValidateID("schedule key", s.Key); err != nil {
		return scheduleRecord{}, err
	}
	if err := jetq.ValidateTaskName(s.Task); err != nil {
		return scheduleRecord{}, err
	}
	if !json.Valid(s.Payload) {
		return scheduleRecord{}, fmt.Errorf("natsq: schedule %s: payload is not valid JSON", s.Key)
	}
	if s.Queue != "" {
		if err := jetq.ValidateID("queue", s.Queue); err != nil {
			return scheduleRecord{}, err
		}
	}
	if err := s.Policy.Validate(); err != nil {
		return scheduleRecord{}, fmt.Errorf("natsq: schedule %s: %w", s.Key, err)
	}
	sched, loc, err := parseCron(s.Cron, s.Timezone)
	if err != nil {
		return scheduleRecord{}, fmt.Errorf("natsq: schedule %s: %w", s.Key, err)
	}
	next := sched.Next(d.now().In(loc)).UTC()
	if next.IsZero() {
		return scheduleRecord{}, fmt.Errorf("natsq: schedule %s: cron %q never fires again", s.Key, s.Cron)
	}
	return scheduleRecord{
		Key: s.Key, Cron: s.Cron, Timezone: s.Timezone, Task: s.Task,
		Payload: append([]byte(nil), s.Payload...), Queue: s.Queue, Policy: s.Policy, Metadata: s.Metadata.Clone(),
		CatchUp: s.CatchUp, MaxCatchUp: s.MaxCatchUp, Next: next,
	}, nil
}

// parseCron parses a schedule expression in loc.
func parseCron(spec, timezone string) (cron.Schedule, *time.Location, error) {
	loc := time.UTC
	if timezone != "" {
		var err error
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid timezone %q: %w", timezone, err)
		}
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(spec)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid cron %q: %w", spec, err)
	}
	return sched, loc, nil
}

// ScheduleRunner fires the schedules of one owner. A process starts it with
// [jetq.Worker.SetScheduler] so the worker runs it, or runs it directly with
// Run.
type ScheduleRunner struct {
	driver *Driver
	owner  string
	// Poll bounds how long a due schedule may wait (default 1s).
	Poll time.Duration
	// Now overrides the clock; for tests.
	Now func() time.Time
}

// Scheduler returns a runner for the owner's schedules.
func (d *Driver) Scheduler(owner string) *ScheduleRunner {
	return &ScheduleRunner{driver: d, owner: owner, Poll: time.Second}
}

// RunUntil implements [jetq.BackgroundTasks], so a worker can run the runner.
func (r *ScheduleRunner) RunUntil(ctx context.Context) error { return r.Run(ctx) }

// Run fires due schedules until ctx is cancelled.
func (r *ScheduleRunner) Run(ctx context.Context) error {
	poll := r.Poll
	if poll <= 0 {
		poll = time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if err := r.Fire(ctx); err != nil && ctx.Err() == nil {
			r.driver.cfg.Logger.WarnContext(ctx, "natsq schedule firing failed", "owner", r.owner, "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Fire fires every due occurrence of the owner's schedules once and reports
// the errors it hit.
func (r *ScheduleRunner) Fire(ctx context.Context) error {
	_, m, ok, err := r.driver.readManifest(ctx, r.owner)
	if err != nil || !ok {
		return err
	}
	var errs []error
	for _, s := range m.Schedules {
		if err := r.fireOne(ctx, s.Key); err != nil && ctx.Err() == nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// fireOne fires the occurrences of one schedule that are due.
//
// Every occurrence is claimed by creating its own record (oc.<schedule>.<time>)
// before anything else happens. The create is the commit point: of any number
// of schedulers racing for an occurrence, exactly one creates it. The
// occurrence's job then carries an EnqueueKey derived from that record, so a
// crash between the create and the submit is repaired by the next firing,
// which finds the record and submits the same job again instead of a second
// one. Schedulers that lose the race read the record and finish its follow-up
// (submit, if the creator died; cursor refresh) without creating a second job.
func (r *ScheduleRunner) fireOne(ctx context.Context, key string) error {
	_, m, ok, err := r.driver.readManifest(ctx, r.owner)
	if err != nil || !ok {
		return err
	}
	var sched *scheduleRecord
	for i := range m.Schedules {
		if m.Schedules[i].Key == key {
			sched = &m.Schedules[i]
			break
		}
	}
	if sched == nil {
		return nil // removed by a newer sync
	}
	now := r.now()
	if sched.Next.After(now) {
		return nil
	}
	s, loc, err := parseCron(sched.Cron, sched.Timezone)
	if err != nil {
		return err
	}
	var errs []error
	next := sched.Next
	limit := 1
	if sched.CatchUp {
		limit = sched.MaxCatchUp
		if limit <= 0 {
			limit = 100
		}
	} else {
		// CatchUp=false: missed occurrences are skipped. Only the latest
		// occurrence at or before now fires, so a scheduler that polls a
		// little late still fires it once, and the cursor then moves past
		// now so the backlog is never replayed.
		for {
			candidate := s.Next(next.In(loc)).UTC()
			if candidate.IsZero() || candidate.After(now) {
				break
			}
			next = candidate
		}
	}
	fired := 0
	for fired < limit {
		if next.After(now) {
			break
		}
		job, err := r.claim(ctx, *sched, next)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		// Whoever created the claim, the job stored in it is submitted: a
		// changed definition never rewrites a claimed occurrence.
		if err := r.driver.submitOccurrence(ctx, job); err != nil {
			// The claim stays: recovery retries the same snapshot. A content
			// conflict (the identity already holds other content) is
			// reported, never overwritten.
			errs = append(errs, fmt.Errorf("natsq: schedule %s occurrence %s: %w", key, next, err))
		}
		fired++
		next = s.Next(next.In(loc)).UTC()
	}
	if next.IsZero() {
		next = s.Next(now.In(loc)).UTC()
	}
	if !next.Equal(sched.Next) {
		if err := r.refreshCursor(ctx, key, next); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// occurrenceKey is the ledger key of an occurrence's claim. It is derived
// from the occurrence's job id, so cleanup of a job finds the claim that
// depends on it.
func occurrenceKey(namespace, jobID string) string {
	return "oc." + hashKey(namespace, "occurrence", jobID)
}

// occurrenceRecord is the durable claim of one firing. Job is the fully
// prepared submission, fixed at claim time: its id, enqueue key, run time,
// task, payload and resolved policy never depend on the manifest again, so
// a later change or removal of the schedule affects only unclaimed
// occurrences.
//
// The claim lives as long as its job: it is removed by the job's cleanup,
// just before the job, so a claim can never re-create a collected job.
type occurrenceRecord struct {
	Owner    string           `json:"owner"`
	Schedule string           `json:"schedule"`
	RunAt    time.Time        `json:"run_at"`
	Job      jetq.PreparedJob `json:"job"`
}

// claim creates the occurrence's claim with its prepared job, or returns the
// job stored by the scheduler that claimed it first.
func (r *ScheduleRunner) claim(ctx context.Context, s scheduleRecord, at time.Time) (*jetq.PreparedJob, error) {
	job, err := r.prepare(s, at)
	if err != nil {
		return nil, err
	}
	key := occurrenceKey(r.driver.cfg.Namespace, job.Envelope.JobID)
	data := mustJSON(occurrenceRecord{Owner: r.owner, Schedule: s.Key, RunAt: at, Job: *job})
	for range 5 {
		_, err := r.driver.ledger.put(ctx, key, 0, &record{Data: data})
		if err == nil {
			return job, nil
		}
		if !errors.Is(err, errRevisionMismatch) {
			return nil, fmt.Errorf("natsq: schedule %s occurrence %s: %w", s.Key, at, err)
		}
		held, ok, err := r.driver.readOccurrence(ctx, key)
		if err != nil {
			return nil, err
		}
		if ok {
			return &held.Job, nil
		}
	}
	return nil, errRevisionMismatch
}

// readOccurrence reads a claim.
func (d *Driver) readOccurrence(ctx context.Context, key string) (*occurrenceRecord, bool, error) {
	ent, ok, err := d.ledger.get(ctx, key)
	if err != nil || !ok {
		return nil, false, err
	}
	var rec occurrenceRecord
	if err := json.Unmarshal(ent.rec.Data, &rec); err != nil {
		return nil, false, fmt.Errorf("natsq: decode occurrence %s: %w", key, err)
	}
	if rec.Job.Envelope.JobID == "" {
		return nil, false, fmt.Errorf("natsq: occurrence %s has no prepared job", key)
	}
	return &rec, true, nil
}

// refreshCursor advances the schedule's next occurrence with a compare-and-set
// on the manifest. Losing the race is harmless: the other scheduler advanced
// it too, and occurrences are claimed individually.
func (r *ScheduleRunner) refreshCursor(ctx context.Context, key string, next time.Time) error {
	for range 5 {
		ent, m, ok, err := r.driver.readManifest(ctx, r.owner)
		if err != nil || !ok {
			return err
		}
		changed := false
		for i := range m.Schedules {
			if m.Schedules[i].Key == key && m.Schedules[i].Next.Before(next) {
				m.Schedules[i].Next = next
				changed = true
			}
		}
		if !changed {
			return nil
		}
		if _, err := r.driver.ledger.put(ctx, manifestKey(r.driver.cfg.Namespace, r.owner), ent.rev, &record{Data: mustJSON(*m)}); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return err
		}
		return nil
	}
	return errRevisionMismatch
}

// prepare resolves the job of one occurrence through the normal producer
// path. Its identity and enqueue key come from the owner, the schedule and
// the occurrence's absolute time.
func (r *ScheduleRunner) prepare(s scheduleRecord, at time.Time) (*jetq.PreparedJob, error) {
	client, err := jetq.NewClient(r.driver, jetq.WithLogger(r.driver.cfg.Logger))
	if err != nil {
		return nil, err
	}
	id := occurrenceJobID(r.owner, s.Key, at)
	sub := jetq.Submission{
		JobID:      id,
		RunAt:      at,
		EnqueueKey: "schedule:" + r.owner + ":" + id,
		Policy:     s.Policy,
		Metadata:   s.Metadata,
		Queue:      s.Queue,
	}
	return client.Prepare(s.Task, json.RawMessage(s.Payload), sub)
}

// submitOccurrence stores a claimed occurrence's prepared job. It is
// idempotent: the job's identity is fixed in the claim.
func (d *Driver) submitOccurrence(ctx context.Context, job *jetq.PreparedJob) error {
	_, err := d.Submit(ctx, job)
	return err
}

// occurrenceJobID is the deterministic job id of one occurrence, so every
// scheduler and every retry addresses the same job.
func occurrenceJobID(owner, schedule string, at time.Time) string {
	return "sched-" + hashKey(owner, schedule, at.UTC().Format(time.RFC3339Nano))[:32]
}

func (r *ScheduleRunner) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return r.driver.now()
}
