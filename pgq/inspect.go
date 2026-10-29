package pgq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/runforyou-ai/jobq"
)

// === schedules ===

// scheduleMsg is the job a schedule enqueues, stored as JSON.
type scheduleMsg struct {
	Name        string            `json:"name"`
	Queue       string            `json:"queue"`
	Payload     []byte            `json:"payload"`
	Header      map[string]string `json:"header,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	Timeout     time.Duration     `json:"timeout,omitempty"`
}

// parseSchedule parses a schedule's cron and timezone (empty: UTC).
func parseSchedule(cronSpec, timezone string) (jobq.Cron, *time.Location, error) {
	cron, err := jobq.ParseCron(cronSpec)
	if err != nil {
		return jobq.Cron{}, nil, err
	}
	if timezone == "" {
		return cron, time.UTC, nil
	}
	loc, err := time.LoadLocation(timezone)
	return cron, loc, err
}

// SyncSchedules implements [jobq.Scheduler]. Unchanged schedules keep their
// next run time.
func (d *Driver) SyncSchedules(ctx context.Context, specs []jobq.ScheduleSpec) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgq: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, d.cfg.schema+":schedules"); err != nil {
		return fmt.Errorf("pgq: schedule lock: %w", err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return fmt.Errorf("pgq: read clock: %w", err)
	}
	keys := make([]string, 0, len(specs))
	for _, s := range specs {
		cron, loc, err := parseSchedule(s.Cron, s.Timezone)
		if err != nil {
			return fmt.Errorf("pgq: schedule %q: %w", s.Key, err)
		}
		doc, err := json.Marshal(scheduleMsg{
			Name: s.Message.Name, Queue: s.Message.Queue, Payload: s.Message.Payload,
			Header: s.Message.Header, MaxAttempts: s.Message.MaxAttempts, Timeout: s.Message.Timeout,
		})
		if err != nil {
			return fmt.Errorf("pgq: encode schedule %q: %w", s.Key, err)
		}
		next := cron.Next(now, loc)
		if next.IsZero() {
			return fmt.Errorf("pgq: schedule %q never fires", s.Key)
		}
		keys = append(keys, s.Key)
		// Only a changed schedule gets a new next run time.
		_, err = tx.Exec(ctx, `INSERT INTO `+d.s+`.schedules (key, cron, timezone, spec, next_run_at)
VALUES ($1, $2, $3, $4::jsonb, $5)
ON CONFLICT (key) DO UPDATE SET cron = EXCLUDED.cron, timezone = EXCLUDED.timezone,
  spec = EXCLUDED.spec, next_run_at = EXCLUDED.next_run_at
WHERE `+d.s+`.schedules.cron IS DISTINCT FROM EXCLUDED.cron
   OR `+d.s+`.schedules.timezone IS DISTINCT FROM EXCLUDED.timezone
   OR `+d.s+`.schedules.spec IS DISTINCT FROM EXCLUDED.spec`,
			s.Key, cron.String(), s.Timezone, string(doc), next)
		if err != nil {
			return fmt.Errorf("pgq: store schedule %q: %w", s.Key, err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM `+d.s+`.schedules WHERE NOT (key = ANY($1))`, keys); err != nil {
		return fmt.Errorf("pgq: remove schedules: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgq: commit schedules: %w", err)
	}
	return nil
}

// scheduler fires due schedules. Every driver instance runs it unless
// created with [WithoutScheduler]; the row
// lock and the job id (key plus occurrence time) make each occurrence fire
// once, so no leader is needed.
func (d *Driver) scheduler() {
	defer d.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := d.fire(ctx); err != nil && ctx.Err() == nil {
			d.cfg.logger.Warn("pgq: fire schedules", "error", err)
		}
		cancel()
	}
}

type dueSchedule struct {
	key, cron, timezone string
	spec                []byte
	at                  time.Time
}

func (d *Driver) fire(ctx context.Context) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT key, cron, timezone, spec, next_run_at FROM `+d.s+`.schedules
WHERE next_run_at <= now() ORDER BY next_run_at LIMIT 20 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return err
	}
	var due []dueSchedule
	for rows.Next() {
		var s dueSchedule
		if err := rows.Scan(&s.key, &s.cron, &s.timezone, &s.spec, &s.at); err != nil {
			rows.Close()
			return err
		}
		due = append(due, s)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(due) == 0 {
		return nil
	}
	for _, s := range due {
		sp, err := tx.Begin(ctx)
		if err != nil {
			return err
		}
		if err := d.fireOne(ctx, sp, s, now); err != nil {
			// A schedule that cannot fire must not hold up the others: undo
			// its work, log it and try again at its next occurrence.
			_ = sp.Rollback(context.WithoutCancel(ctx))
			d.cfg.logger.Warn("pgq: schedule failed", "key", s.key, "error", err)
			if _, err := tx.Exec(ctx, `UPDATE `+d.s+`.schedules SET next_run_at = $2 WHERE key = $1`, s.key, retryAt(s, now)); err != nil {
				return err
			}
			continue
		}
		if err := sp.Commit(ctx); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// fireOne enqueues one occurrence of s and moves its next run time past now.
func (d *Driver) fireOne(ctx context.Context, tx pgx.Tx, s dueSchedule, now time.Time) error {
	cron, loc, err := parseSchedule(s.cron, s.timezone)
	if err != nil {
		return err
	}
	var sm scheduleMsg
	if err := json.Unmarshal(s.spec, &sm); err != nil {
		return err
	}
	msg := &jobq.Message{
		ID:   fmt.Sprintf("%s-%d", s.key, s.at.Unix()),
		Name: sm.Name, Queue: sm.Queue, Payload: sm.Payload, Header: sm.Header,
		MaxAttempts: sm.MaxAttempts, Timeout: sm.Timeout,
	}
	// An id still in use means the occurrence already exists: skip it.
	if err := d.EnqueueTx(ctx, tx, msg); err != nil && !errors.Is(err, jobq.ErrJobIDInUse) {
		return err
	}
	// Missed occurrences are not replayed: the next one is after now.
	next := cron.Next(now, loc)
	if next.IsZero() {
		return errors.New("schedule never fires again")
	}
	_, err = tx.Exec(ctx, `UPDATE `+d.s+`.schedules SET next_run_at = $2 WHERE key = $1`, s.key, next)
	return err
}

// retryAt is when a schedule that failed to fire is tried again: its next
// occurrence, so it never fires off its cron, or in an hour when the schedule
// itself cannot be read.
func retryAt(s dueSchedule, now time.Time) time.Time {
	if cron, loc, err := parseSchedule(s.cron, s.timezone); err == nil {
		if next := cron.Next(now, loc); !next.IsZero() {
			return next
		}
	}
	return now.Add(time.Hour)
}

// === inspection ===

// Stats implements [jobq.Inspector].
func (d *Driver) Stats(ctx context.Context) (jobq.Stats, error) {
	var stats jobq.Stats
	rows, err := d.pool.Query(ctx, `SELECT queue,
  count(*) FILTER (WHERE (state = 'pending' AND run_at <= now()) OR (state = 'running' AND locked_until <= now())),
  count(*) FILTER (WHERE state = 'pending' AND run_at > now()),
  count(*) FILTER (WHERE state = 'running' AND locked_until > now()),
  count(*) FILTER (WHERE state = 'failed')
FROM `+d.s+`.jobs WHERE state IN ('pending', 'running', 'failed') GROUP BY queue
ORDER BY queue`)
	if err != nil {
		return stats, fmt.Errorf("pgq: stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var q jobq.QueueStats
		if err := rows.Scan(&q.Queue, &q.Ready, &q.Scheduled, &q.Running, &q.Dead); err != nil {
			return stats, fmt.Errorf("pgq: scan stats: %w", err)
		}
		stats.Queues = append(stats.Queues, q)
	}
	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("pgq: stats: %w", err)
	}
	rows.Close()
	if err := d.pool.QueryRow(ctx, `SELECT count(*) FROM `+d.s+`.schedules`).Scan(&stats.Schedules); err != nil {
		return stats, fmt.Errorf("pgq: count schedules: %w", err)
	}
	return stats, nil
}

// DeadLetters implements [jobq.Inspector]. The cursor is the finish time and
// id of the last entry returned.
func (d *Driver) DeadLetters(ctx context.Context, q jobq.DeadLetterQuery) (jobq.DeadLetterPage, error) {
	if q.Limit <= 0 {
		q.Limit = 50
	}
	var page jobq.DeadLetterPage
	curAt, curID, err := parseDeadCursor(q.Cursor)
	if err != nil {
		return page, err
	}
	// One extra row tells whether another page follows.
	rows, err := d.pool.Query(ctx, `SELECT id, name, queue, payload, header, attempt, max_attempts, timeout_ns,
  snoozes, unknown_since, unique_key, unique_for_ns, enqueued_at, coalesce(last_error, ''), finished_at
FROM `+d.s+`.jobs
WHERE state = 'failed' AND ($1 = '' OR queue = $1)
  AND ($2::timestamptz IS NULL OR (finished_at, id) < ($2::timestamptz, $3))
ORDER BY finished_at DESC, id DESC LIMIT $4`, q.Queue, curAt, curID, q.Limit+1)
	if err != nil {
		return page, fmt.Errorf("pgq: dead letters: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			m              jobq.Message
			header         []byte
			dl             jobq.DeadLetter
			timeoutNs, unq int64
			unknown, enq   *time.Time
			key            *string
		)
		if err := rows.Scan(&m.ID, &m.Name, &m.Queue, &m.Payload, &header, &dl.Attempts, &m.MaxAttempts, &timeoutNs,
			&m.Snoozes, &unknown, &key, &unq, &enq, &dl.Error, &dl.FailedAt); err != nil {
			return page, fmt.Errorf("pgq: scan dead letter: %w", err)
		}
		if len(header) > 0 {
			if err := json.Unmarshal(header, &m.Header); err != nil {
				return page, fmt.Errorf("pgq: decode header of %s: %w", m.ID, err)
			}
		}
		m.Timeout, m.UniqueFor = time.Duration(timeoutNs), time.Duration(unq)
		if key != nil {
			m.UniqueKey = *key
		}
		if unknown != nil {
			m.UnknownSince = *unknown
		}
		if enq != nil {
			m.EnqueuedAt = *enq
		}
		dl.Message = &m
		page.DeadLetters = append(page.DeadLetters, dl)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("pgq: dead letters: %w", err)
	}
	if len(page.DeadLetters) > q.Limit {
		page.DeadLetters = page.DeadLetters[:q.Limit]
		page.Next = deadCursor(page.DeadLetters[q.Limit-1])
	}
	return page, nil
}

// deadCursor is the cursor of the page after dl: its finish time and id.
func deadCursor(dl jobq.DeadLetter) string {
	return dl.FailedAt.UTC().Format(time.RFC3339Nano) + "|" + dl.Message.ID
}

// parseDeadCursor reverses deadCursor; an empty cursor is the first page,
// returned as a nil time.
func parseDeadCursor(cursor string) (*time.Time, string, error) {
	if cursor == "" {
		return nil, "", nil
	}
	ts, id, ok := strings.Cut(cursor, "|")
	t, err := time.Parse(time.RFC3339Nano, ts)
	if !ok || err != nil {
		return nil, "", fmt.Errorf("pgq: invalid cursor %q", cursor)
	}
	return &t, id, nil
}

// RetryDead implements [jobq.Inspector]. The unique key of the dead job is
// not taken again, since another job may hold it by now.
func (d *Driver) RetryDead(ctx context.Context, id string) error {
	var got string
	err := d.pool.QueryRow(ctx, `UPDATE `+d.s+`.jobs SET state = 'pending', attempt = 0, snoozes = 0,
  unknown_since = NULL, run_at = now(), locked_until = NULL, finished_at = NULL, last_error = NULL,
  unique_key = NULL, unique_for_ns = 0, claim_token = nextval('`+d.s+`.claim_token_seq')
WHERE id = $1 AND state = 'failed' RETURNING id`, id).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobq.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("pgq: retry dead letter: %w", err)
	}
	return nil
}

// DeleteDead implements [jobq.Inspector].
func (d *Driver) DeleteDead(ctx context.Context, id string) error {
	tag, err := d.pool.Exec(ctx, `DELETE FROM `+d.s+`.jobs WHERE id = $1 AND state = 'failed'`, id)
	if err != nil {
		return fmt.Errorf("pgq: delete dead letter: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return jobq.ErrNotFound
	}
	return nil
}
