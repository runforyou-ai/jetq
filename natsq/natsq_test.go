package natsq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/drivertest"
	"github.com/runforyou-ai/jobq/natsq"
)

// startServer runs an embedded NATS server with JetStream.
func startServer(t *testing.T) *server.Server {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func TestConformance(t *testing.T) {
	srv := startServer(t)
	drivertest.Run(t, func(t *testing.T) jobq.Driver {
		d := newDriver(t, srv)
		return d
	}, drivertest.Options{Resolution: 2 * time.Second, CancelReady: true})
}

func newDriver(t *testing.T, srv *server.Server) *natsq.Driver {
	t.Helper()
	conn, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "t" + jobq.NewID()[:10]
	d, err := natsq.New(t.Context(), js,
		natsq.WithPrefix(prefix),
		natsq.WithStream(prefix+"_JOBS"),
		natsq.WithStateBucket(prefix+"_STATE"),
		natsq.WithDeadBucket(prefix+"_DEAD"),
		natsq.WithMemoryStorage(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// A worker whose lease ran out must not settle a job that another worker
// claimed meanwhile, although JetStream would accept its ack.
func TestStaleAckDoesNotSettleNewClaim(t *testing.T) {
	srv := startServer(t)
	d := newDriver(t, srv)
	ctx := t.Context()
	msg, err := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	stale := claim(t, d, time.Second)
	time.Sleep(1500 * time.Millisecond)
	fresh := claim(t, d, time.Minute)
	if fresh.Attempt() != 2 {
		t.Fatalf("attempt = %d", fresh.Attempt())
	}
	if err := stale.Complete(ctx); !errors.Is(err, jobq.ErrLeaseLost) {
		t.Fatalf("stale complete = %v", err)
	}
	// The fresh claim still settles, and the job is gone afterwards.
	if err := fresh.Retry(ctx, time.Now(), "x"); err != nil {
		t.Fatal(err)
	}
	again := claim(t, d, time.Minute)
	if again.Attempt() != 3 {
		t.Fatalf("attempt after retry = %d", again.Attempt())
	}
	if err := again.Complete(ctx); err != nil {
		t.Fatal(err)
	}
}

// A job redelivered by NATS before its lease in the record ran out is put
// back, not handed to a second worker.
func TestEarlyRedeliveryIsHeldBack(t *testing.T) {
	srv := startServer(t)
	d := newDriver(t, srv)
	ctx := t.Context()
	msg, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	// A short lease sets the consumer's AckWait; the long one outlives it.
	short := claim(t, d, time.Second)
	if err := short.Release(ctx, jobq.Release{}); err != nil {
		t.Fatal(err)
	}
	long := claim(t, d, 10*time.Second)
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for c.Err() == nil {
		leases, _ := d.Claim(c, "q", 1, 10*time.Second)
		if len(leases) > 0 {
			t.Fatalf("job claimed twice while its lease runs")
		}
	}
	if err := long.Complete(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCancelRunningJob(t *testing.T) {
	srv := startServer(t)
	d := newDriver(t, srv)
	ctx := t.Context()
	msg, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"), jobq.Unique("u"))
	if err := d.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Minute)
	if err := d.Cancel(ctx, msg.ID); err != nil {
		t.Fatal(err)
	}
	// The running attempt keeps its lease.
	if err := l.Extend(ctx); err != nil {
		t.Fatalf("extend after cancel = %v", err)
	}
	// The unique key is free again.
	other, _ := jobq.NewMessage("x", 2, jobq.OnQueue("q2"), jobq.Unique("u"))
	if err := d.Enqueue(ctx, other); err != nil {
		t.Fatal(err)
	}
	// Its retry ends the job instead of running it again.
	if err := l.Retry(ctx, time.Now(), "x"); err != nil {
		t.Fatalf("retry after cancel = %v", err)
	}
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for c.Err() == nil {
		if ls, _ := d.Claim(c, "q", 1, time.Minute); len(ls) > 0 {
			t.Fatalf("cancelled job ran again: attempt %d", ls[0].Attempt())
		}
	}
}

func TestScheduleTimezone(t *testing.T) {
	srv := startServer(t)
	d := newDriver(t, srv)
	c := jobq.New(d)
	err := c.SyncSchedules(t.Context(), jobq.Schedule{
		Key: "daily", Cron: "0 2 * * *", Timezone: "Asia/Shanghai",
		Job: jobq.RawJob{Name: "report"},
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := c.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Schedules != 1 {
		t.Fatalf("schedules = %d", stats.Schedules)
	}
}

func claim(t *testing.T, d jobq.Driver, lease time.Duration) jobq.Lease {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leases, err := d.Claim(t.Context(), "q", 1, lease)
		if err != nil {
			t.Fatal(err)
		}
		if len(leases) > 0 {
			return leases[0]
		}
	}
	t.Fatal("nothing claimed")
	return nil
}

// The delivery settled by Retry comes back when its ack is lost. It is a
// stale copy and must not run beside the requeued one.
func TestLostAckAfterRetryDoesNotRunTwice(t *testing.T) {
	srv := startServer(t)
	d := newDriver(t, srv)
	ctx := t.Context()
	msg, _ := jobq.NewMessage("x", 1, jobq.OnQueue("q"))
	if err := d.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	l := claim(t, d, time.Second)
	// Retry far in the future, then add a stray copy of the job, as a lost
	// ack or an uncertain publish leaves behind.
	if err := l.Retry(ctx, time.Now().Add(time.Hour), "x"); err != nil {
		t.Fatal(err)
	}
	if err := natsq.PublishCopy(ctx, d, msg); err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for c.Err() == nil {
		if leases, _ := d.Claim(c, "q", 1, time.Minute); len(leases) > 0 {
			t.Fatalf("stale copy claimed as attempt %d", leases[0].Attempt())
		}
	}
	// The job is still pending and can be cancelled.
	if err := d.Cancel(ctx, msg.ID); err != nil {
		t.Fatal(err)
	}
}
