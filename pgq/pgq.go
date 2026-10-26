// Package pgq implements a jobq driver on PostgreSQL using pgx/v5.
//
// Jobs live in one table. Claiming uses a single UPDATE ... FOR UPDATE SKIP
// LOCKED statement that also reclaims jobs whose lease expired. Every lease
// operation is fenced on a claim token, so a stale lease never alters a newer
// claim. All times come from the database clock. LISTEN/NOTIFY wakes waiting
// claims; a timer for the next due job and a poll cover lost notifications.
package pgq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var schemaRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Option configures a [Driver].
type Option func(*config)

type config struct {
	schema        string
	retention     time.Duration
	deadRetention time.Duration
	poll          time.Duration
	noListen      bool
	logger        *slog.Logger
}

// WithSchema sets the schema holding jobq's tables (default "jobq").
func WithSchema(name string) Option { return func(c *config) { c.schema = name } }

// WithRetention sets how long succeeded and cancelled jobs are kept (default 24h).
func WithRetention(d time.Duration) Option { return func(c *config) { c.retention = d } }

// WithDeadRetention sets how long dead letters are kept (default 14 days).
func WithDeadRetention(d time.Duration) Option { return func(c *config) { c.deadRetention = d } }

// WithPollInterval sets the fallback poll interval of waiting claims (default 5s).
func WithPollInterval(d time.Duration) Option { return func(c *config) { c.poll = d } }

// WithoutListen disables LISTEN/NOTIFY, for PgBouncer in transaction mode.
func WithoutListen() Option { return func(c *config) { c.noListen = true } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// Driver implements [jobq.Driver], [jobq.Scheduler] and [jobq.Inspector].
type Driver struct {
	pool *pgxpool.Pool
	cfg  config
	s    string // quoted schema

	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
	closed  bool
	stop    chan struct{}
	wg      sync.WaitGroup
}

// New migrates the schema and returns a driver using pool. The pool is not
// closed by [Driver.Close].
func New(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*Driver, error) {
	cfg := config{
		schema:        "jobq",
		retention:     24 * time.Hour,
		deadRetention: 14 * 24 * time.Hour,
		poll:          5 * time.Second,
		logger:        slog.Default(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if !schemaRE.MatchString(cfg.schema) {
		return nil, fmt.Errorf("pgq: invalid schema name %q", cfg.schema)
	}
	d := &Driver{
		pool:    pool,
		cfg:     cfg,
		s:       pgx.Identifier{cfg.schema}.Sanitize(),
		waiters: map[string]map[chan struct{}]struct{}{},
		stop:    make(chan struct{}),
	}
	if err := d.migrate(ctx); err != nil {
		return nil, err
	}
	if !cfg.noListen {
		d.wg.Add(1)
		go d.listen()
	}
	d.wg.Add(1)
	go d.janitor()
	d.wg.Add(1)
	go d.scheduler()
	return d, nil
}

// Close stops background work. It does not close the pool.
func (d *Driver) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	close(d.stop)
	d.wg.Wait()
	return nil
}

func (d *Driver) migrate(ctx context.Context) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgq: migrate: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, d.cfg.schema+":migrate"); err != nil {
		return fmt.Errorf("pgq: migrate lock: %w", err)
	}
	if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+d.s); err != nil {
		return fmt.Errorf("pgq: create schema: %w", err)
	}
	if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+d.s+".migrations (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		return fmt.Errorf("pgq: migrations table: %w", err)
	}
	var current int
	if err := tx.QueryRow(ctx, "SELECT coalesce(max(version), 0) FROM "+d.s+".migrations").Scan(&current); err != nil {
		return fmt.Errorf("pgq: read version: %w", err)
	}
	for i := current; i < len(migrations); i++ {
		if _, err := tx.Exec(ctx, fmt.Sprintf(migrations[i], d.s)); err != nil {
			return fmt.Errorf("pgq: migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO "+d.s+".migrations (version) VALUES ($1)", i+1); err != nil {
			return fmt.Errorf("pgq: record migration %d: %w", i+1, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgq: migrate commit: %w", err)
	}
	return nil
}

// === wake-ups ===

// subscribe returns a channel signalled when a job of queue may be due.
func (d *Driver) subscribe(queue string) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	d.mu.Lock()
	m := d.waiters[queue]
	if m == nil {
		m = map[chan struct{}]struct{}{}
		d.waiters[queue] = m
	}
	m[ch] = struct{}{}
	d.mu.Unlock()
	return ch, func() {
		d.mu.Lock()
		delete(d.waiters[queue], ch)
		d.mu.Unlock()
	}
}

func (d *Driver) wake(queue string) {
	d.mu.Lock()
	for ch := range d.waiters[queue] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	d.mu.Unlock()
}

func (d *Driver) wakeAll() {
	d.mu.Lock()
	for _, m := range d.waiters {
		for ch := range m {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	d.mu.Unlock()
}

// listen holds one connection on LISTEN jobq and fans notifications out.
func (d *Driver) listen() {
	defer d.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-d.stop
		cancel()
	}()
	defer cancel()
	for ctx.Err() == nil {
		if err := d.listenOnce(ctx); err != nil && ctx.Err() == nil {
			d.cfg.logger.Warn("pgq: listener failed, retrying", "error", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

func (d *Driver) listenOnce(ctx context.Context) error {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN jobq"); err != nil {
		return err
	}
	// Notifications may have been missed while disconnected.
	d.wakeAll()
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			_, _ = conn.Exec(context.WithoutCancel(ctx), "UNLISTEN *")
			return err
		}
		d.wake(n.Payload)
	}
}

// === janitor ===

func (d *Driver) janitor() {
	defer d.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		d.clean(ctx)
		cancel()
	}
}

func (d *Driver) clean(ctx context.Context) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return
	}
	// One instance cleans at a time; the lock is held until this returns.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var got bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, d.cfg.schema+":clean").Scan(&got); err != nil || !got {
		return
	}
	const batch = 1000
	stmts := []struct {
		sql string
		arg int64
	}{
		{"DELETE FROM " + d.s + ".jobs WHERE id IN (SELECT id FROM " + d.s + ".jobs WHERE state IN ('succeeded','canceled') AND finished_at < clock_timestamp() - $1 * interval '1 microsecond' LIMIT 1000)", d.cfg.retention.Microseconds()},
		{"DELETE FROM " + d.s + ".jobs WHERE id IN (SELECT id FROM " + d.s + ".jobs WHERE state = 'failed' AND finished_at < clock_timestamp() - $1 * interval '1 microsecond' LIMIT 1000)", d.cfg.deadRetention.Microseconds()},
		{"DELETE FROM " + d.s + ".unique_keys WHERE key IN (SELECT key FROM " + d.s + ".unique_keys WHERE until IS NOT NULL AND until < clock_timestamp() AND $1::bigint >= 0 LIMIT 1000)", 0},
	}
	// Repeat each statement in its own short transaction until it is caught
	// up, so a burst of finished jobs is cleared within the time budget.
	for _, st := range stmts {
		for ctx.Err() == nil {
			tag, err := d.pool.Exec(ctx, st.sql, st.arg)
			if err != nil {
				d.cfg.logger.Warn("pgq: clean", "error", err)
				return
			}
			if tag.RowsAffected() < batch {
				break
			}
		}
	}
}

var errClosed = errors.New("pgq: driver closed")
