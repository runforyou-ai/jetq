package pgq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/runforyou-ai/jobq"
)

// claimWindow bounds how long one Claim waits for work.
const claimWindow = 5 * time.Second

func isPgError(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe)
}

// untilMicros is how long from now t lies in the future, in microseconds, zero
// for a zero or past time. Run times are applied as an offset to the database
// clock, so a client clock that differs from it cannot make a job due too late
// or too early.
func untilMicros(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return max(time.Until(t), 0).Microseconds()
}

func nilIfZero(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// === enqueue ===

// Enqueue implements [jobq.Driver].
func (d *Driver) Enqueue(ctx context.Context, msg *jobq.Message) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgq: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := d.EnqueueTx(ctx, tx, msg); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		if isPgError(err) {
			return fmt.Errorf("pgq: commit enqueue: %w", err)
		}
		return fmt.Errorf("%w: pgq: commit enqueue: %w", jobq.ErrUncertain, err)
	}
	return nil
}

// EnqueueTx stores msg inside the caller's transaction, so the job commits
// or rolls back together with the application's data. Build msg with
// [jobq.NewMessage]. The job becomes visible to workers when tx commits.
// Errors such as [jobq.ErrDuplicate] leave tx usable.
func (d *Driver) EnqueueTx(ctx context.Context, tx pgx.Tx, msg *jobq.Message) error {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgq: savepoint: %w", err)
	}
	if err := d.insert(ctx, sp, msg); err != nil {
		_ = sp.Rollback(context.WithoutCancel(ctx))
		return err
	}
	return sp.Commit(ctx)
}

func (d *Driver) insert(ctx context.Context, tx pgx.Tx, msg *jobq.Message) error {
	// A finished job releases its id.
	if _, err := tx.Exec(ctx, "DELETE FROM "+d.s+".jobs WHERE id = $1 AND state IN ('succeeded', 'canceled')", msg.ID); err != nil {
		return fmt.Errorf("pgq: free job id: %w", err)
	}
	if msg.UniqueKey != "" {
		tag, err := tx.Exec(ctx, `INSERT INTO `+d.s+`.unique_keys (key, job_id, until)
VALUES ($1, $2, CASE WHEN $3::bigint > 0 THEN clock_timestamp() + $3::bigint * interval '1 microsecond' END)
ON CONFLICT (key) DO UPDATE SET job_id = EXCLUDED.job_id, until = EXCLUDED.until
WHERE `+d.s+`.unique_keys.until IS NOT NULL AND `+d.s+`.unique_keys.until <= clock_timestamp()`,
			msg.UniqueKey, msg.ID, msg.UniqueFor.Microseconds())
		if err != nil {
			return fmt.Errorf("pgq: unique key: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return jobq.ErrDuplicate
		}
	}
	var header []byte
	if len(msg.Header) > 0 {
		var err error
		if header, err = json.Marshal(msg.Header); err != nil {
			return fmt.Errorf("pgq: encode header: %w", err)
		}
	}
	var key *string
	if msg.UniqueKey != "" {
		key = &msg.UniqueKey
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+d.s+`.jobs
(id, queue, name, payload, header, state, max_attempts, timeout_ns, run_at, unique_key, unique_for_ns, enqueued_at)
VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7, clock_timestamp() + $8::bigint * interval '1 microsecond', $9, $10, $11)`,
		msg.ID, msg.Queue, msg.Name, msg.Payload, header, msg.MaxAttempts, int64(msg.Timeout),
		untilMicros(msg.RunAt), key, int64(msg.UniqueFor), nilIfZero(msg.EnqueuedAt))
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" {
			return jobq.ErrJobIDInUse
		}
		return fmt.Errorf("pgq: insert job: %w", err)
	}
	return nil
}

// === claim ===

// Claim implements [jobq.Driver].
func (d *Driver) Claim(ctx context.Context, queue string, limit int, leaseFor time.Duration) ([]jobq.Lease, error) {
	if limit <= 0 {
		return nil, nil
	}
	// Subscribe before looking, so a notification in between is not lost.
	ch, unsubscribe := d.subscribe(queue)
	defer unsubscribe()
	window := time.NewTimer(claimWindow)
	defer window.Stop()
	for {
		d.mu.Lock()
		closed := d.closed
		d.mu.Unlock()
		if closed {
			return nil, errClosed
		}
		leases, err := d.claim(ctx, queue, limit, leaseFor)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		if len(leases) > 0 {
			return leases, nil
		}
		wait, err := d.untilNext(ctx, queue)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-d.stop:
			timer.Stop()
			return nil, errClosed
		case <-window.C:
			timer.Stop()
			return nil, nil
		case <-ch:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// untilNext returns how long to wait before a job of queue may be due,
// bounded by the poll interval.
func (d *Driver) untilNext(ctx context.Context, queue string) (time.Duration, error) {
	var ms *float64
	err := d.pool.QueryRow(ctx, `SELECT (extract(epoch FROM (min(t) - now())) * 1000)::float8 FROM (
  SELECT min(run_at) AS t FROM `+d.s+`.jobs WHERE queue = $1 AND state = 'pending'
  UNION ALL
  SELECT min(locked_until) FROM `+d.s+`.jobs WHERE queue = $1 AND state = 'running'
) x`, queue).Scan(&ms)
	if err != nil {
		return 0, fmt.Errorf("pgq: next due: %w", err)
	}
	wait := d.cfg.poll
	if ms != nil {
		wait = min(wait, time.Duration(*ms*float64(time.Millisecond)))
	}
	// The database clock can run ahead of the timer; never spin.
	return max(wait, 10*time.Millisecond), nil
}

func (d *Driver) claim(ctx context.Context, queue string, limit int, leaseFor time.Duration) ([]jobq.Lease, error) {
	rows, err := d.pool.Query(ctx, `UPDATE `+d.s+`.jobs SET state = 'running', attempt = attempt + 1,
  claim_token = nextval('`+d.s+`.claim_token_seq'), locked_until = now() + $3::bigint * interval '1 microsecond'
WHERE id IN (
  SELECT id FROM `+d.s+`.jobs
  WHERE queue = $1 AND ((state = 'pending' AND run_at <= now()) OR (state = 'running' AND locked_until <= now()))
  ORDER BY run_at, id LIMIT $2 FOR UPDATE SKIP LOCKED)
RETURNING id, name, queue, payload, header, attempt, max_attempts, timeout_ns, snoozes, unknown_since,
  claim_token, unique_key, unique_for_ns, enqueued_at, run_at`, queue, limit, leaseFor.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("pgq: claim: %w", err)
	}
	defer rows.Close()
	var leases []jobq.Lease
	for rows.Next() {
		var (
			m                  jobq.Message
			header             []byte
			attempt            int
			timeoutNs, uniqueN int64
			token              int64
			unknown, enq       *time.Time
			key                *string
		)
		if err := rows.Scan(&m.ID, &m.Name, &m.Queue, &m.Payload, &header, &attempt, &m.MaxAttempts, &timeoutNs,
			&m.Snoozes, &unknown, &token, &key, &uniqueN, &enq, &m.RunAt); err != nil {
			return nil, fmt.Errorf("pgq: scan claimed job: %w", err)
		}
		if len(header) > 0 {
			if err := json.Unmarshal(header, &m.Header); err != nil {
				return nil, fmt.Errorf("pgq: decode header of %s: %w", m.ID, err)
			}
		}
		m.Timeout = time.Duration(timeoutNs)
		m.UniqueFor = time.Duration(uniqueN)
		if key != nil {
			m.UniqueKey = *key
		}
		if unknown != nil {
			m.UnknownSince = *unknown
		}
		if enq != nil {
			m.EnqueuedAt = *enq
		}
		leases = append(leases, &pgLease{d: d, msg: &m, attempt: attempt, token: token, leaseFor: leaseFor})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgq: claim: %w", err)
	}
	return leases, nil
}

// === cancel ===

// Cancel implements [jobq.Driver]. A running job is marked cancelled, so its
// lease operations fail with [jobq.ErrLeaseLost].
func (d *Driver) Cancel(ctx context.Context, id string) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgq: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var key *string
	var uniqueNs int64
	err = tx.QueryRow(ctx, `UPDATE `+d.s+`.jobs SET state = 'canceled', finished_at = now(), locked_until = NULL
WHERE id = $1 AND state IN ('pending', 'running') RETURNING unique_key, unique_for_ns`, id).Scan(&key, &uniqueNs)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobq.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("pgq: cancel: %w", err)
	}
	if err := d.releaseUnique(ctx, tx, id, key, uniqueNs); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgq: commit cancel: %w", err)
	}
	return nil
}

// releaseUnique frees an until-done key held by job id.
func (d *Driver) releaseUnique(ctx context.Context, tx pgx.Tx, id string, key *string, uniqueNs int64) error {
	if key == nil || uniqueNs > 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+d.s+".unique_keys WHERE key = $1 AND job_id = $2", *key, id); err != nil {
		return fmt.Errorf("pgq: release unique key: %w", err)
	}
	return nil
}

// === lease ===

type pgLease struct {
	d        *Driver
	msg      *jobq.Message
	attempt  int
	token    int64
	leaseFor time.Duration
}

func (l *pgLease) Message() *jobq.Message { return l.msg }
func (l *pgLease) Attempt() int           { return l.attempt }

// fence is the condition every lease operation carries.
const fence = " WHERE id = $1 AND claim_token = $2 AND state = 'running' AND locked_until > now()"

// Extend implements [jobq.Lease].
func (l *pgLease) Extend(ctx context.Context) error {
	tag, err := l.d.pool.Exec(ctx, "UPDATE "+l.d.s+".jobs SET locked_until = now() + $3::bigint * interval '1 microsecond'"+fence,
		l.msg.ID, l.token, l.leaseFor.Microseconds())
	if err != nil {
		return fmt.Errorf("pgq: extend: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return jobq.ErrLeaseLost
	}
	return nil
}

// settle runs one fenced UPDATE ... RETURNING unique_key, unique_for_ns and
// optionally frees the job's unique key in the same transaction.
func (l *pgLease) settle(ctx context.Context, set string, release bool, args ...any) error {
	tx, err := l.d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgq: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var key *string
	var uniqueNs int64
	err = tx.QueryRow(ctx, "UPDATE "+l.d.s+".jobs SET "+set+fence+" RETURNING unique_key, unique_for_ns",
		append([]any{l.msg.ID, l.token}, args...)...).Scan(&key, &uniqueNs)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobq.ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("pgq: settle: %w", err)
	}
	if release {
		if err := l.d.releaseUnique(ctx, tx, l.msg.ID, key, uniqueNs); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgq: commit settle: %w", err)
	}
	return nil
}

// Complete implements [jobq.Lease].
func (l *pgLease) Complete(ctx context.Context) error {
	return l.settle(ctx, "state = 'succeeded', finished_at = now(), locked_until = NULL", true)
}

// Retry implements [jobq.Lease].
func (l *pgLease) Retry(ctx context.Context, at time.Time, reason string) error {
	return l.settle(ctx, "state = 'pending', run_at = now() + $3::bigint * interval '1 microsecond', last_error = $4, locked_until = NULL", false, untilMicros(at), reason)
}

// Release implements [jobq.Lease].
func (l *pgLease) Release(ctx context.Context, r jobq.Release) error {
	snoozes := 0
	if r.Snooze {
		snoozes = 1
	}
	return l.settle(ctx, `state = 'pending', attempt = attempt - 1, run_at = now() + $3::bigint * interval '1 microsecond',
snoozes = snoozes + $4, unknown_since = coalesce($5, unknown_since), locked_until = NULL`, false,
		untilMicros(r.At), snoozes, nilIfZero(r.UnknownSince))
}

// Fail implements [jobq.Lease].
func (l *pgLease) Fail(ctx context.Context, reason string) error {
	return l.settle(ctx, "state = 'failed', finished_at = now(), last_error = $3, locked_until = NULL", true, reason)
}
