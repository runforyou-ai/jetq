package pgq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/drivertest"
	"github.com/runforyou-ai/jobq/pgq"
)

// testPool connects to DATABASE_URL and skips the test when it is not set.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// newDriver returns a driver in a schema of its own, dropped when the test ends.
func newDriver(t *testing.T, pool *pgxpool.Pool, opts ...pgq.Option) *pgq.Driver {
	t.Helper()
	schema := "t_" + strings.ToLower(jobq.NewID()[:12])
	d, err := pgq.New(t.Context(), pool, append([]pgq.Option{pgq.WithSchema(schema)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = d.Close()
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", pgx.Identifier{schema}.Sanitize()))
	})
	return d
}

func TestConformance(t *testing.T) {
	pool := testPool(t)
	drivertest.Run(t, func(t *testing.T) jobq.Driver {
		return newDriver(t, pool)
	}, drivertest.Options{Resolution: 2 * time.Second, CancelReady: true})
}

// Without LISTEN/NOTIFY a waiting claim still sees new jobs by polling.
func TestConformanceWithoutListen(t *testing.T) {
	pool := testPool(t)
	d := newDriver(t, pool, pgq.WithoutListen(), pgq.WithPollInterval(200*time.Millisecond))
	got := make(chan int, 1)
	go func() {
		leases, _ := d.Claim(t.Context(), "q", 1, time.Minute)
		got <- len(leases)
	}()
	time.Sleep(300 * time.Millisecond)
	msg, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(t.Context(), msg); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-got:
		if n != 1 {
			t.Fatalf("claimed %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting claim did not see the job")
	}
}

// A job enqueued in the caller's transaction exists only if it commits.
func TestEnqueueTx(t *testing.T) {
	pool := testPool(t)
	d := newDriver(t, pool)
	ctx := t.Context()

	rolledBack, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"), jobq.Unique("k"))
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.EnqueueTx(ctx, tx, rolledBack); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithTimeout(ctx, time.Second)
	leases, _ := d.Claim(c, "q", 1, time.Minute)
	cancel()
	if len(leases) != 0 {
		t.Fatal("rolled back job was claimed")
	}

	committed, _ := jobq.NewMessage("x", 2, jobq.OnQueue("q"), jobq.Unique("k"))
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.EnqueueTx(ctx, tx, committed); err != nil {
		t.Fatal(err)
	}
	// A duplicate leaves the transaction usable.
	dup, _ := jobq.NewMessage("x", 3, jobq.OnQueue("q"), jobq.Unique("k"))
	if err := d.EnqueueTx(ctx, tx, dup); !errors.Is(err, jobq.ErrDuplicate) {
		t.Fatalf("duplicate in transaction = %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	leases, err = d.Claim(ctx, "q", 1, time.Minute)
	if err != nil || len(leases) != 1 || leases[0].Message().ID != committed.ID {
		t.Fatalf("claim = %v, %v", leases, err)
	}
}

// Two drivers on one schema (two processes) fire a schedule occurrence once.
func TestSchedulesFireOnceAcrossDrivers(t *testing.T) {
	pool := testPool(t)
	a := newDriver(t, pool)
	schema := "t_" + strings.ToLower(jobq.NewID()[:12])
	b, err := pgq.New(t.Context(), pool, pgq.WithSchema(schema))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = b.Close()
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	_ = a
	ca := jobq.New(b)
	if err := ca.SyncSchedules(t.Context(), jobq.Schedule{Key: "tick", Cron: "@every 1s", Job: jobq.RawJob{Name: "tick"}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)
	stats, err := ca.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Schedules != 1 {
		t.Fatalf("schedules = %d", stats.Schedules)
	}
}
