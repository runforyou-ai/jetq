# jobq

[![CI](https://github.com/runforyou-ai/jobq/actions/workflows/ci.yml/badge.svg)](https://github.com/runforyou-ai/jobq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/runforyou-ai/jobq.svg)](https://pkg.go.dev/github.com/runforyou-ai/jobq)

[中文](README.zh-CN.md)

jobq is a background job queue for Go with interchangeable storage drivers: [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream), PostgreSQL and memory.
Jobs carry explicitly named JSON payloads. Handlers are ordinary Go functions; the queue manages retries, backoff, delays, uniqueness, failure callbacks and recurring schedules.

- **One API, several drivers.** Switch storage by changing the driver you pass to `jobq.New`. Retries, backoff, snoozes, timeouts and shutdown are implemented once in the core, so they behave the same everywhere.
- **Typed handlers.** Payloads are ordinary structs; `EnqueueNamed` and `HandleNamed` share an explicit job name, or a type can own its name with `JobName()`.
- **Retries that make sense.** Per-queue attempt limits and backoff, `Permanent` errors, `RetryAfter`, and `Snooze` for "not now" without burning an attempt.
- **Delayed and recurring jobs.** `Delay`, `At` and cron schedules, without a scheduler process in your app.
- **Uniqueness.** `Unique(key)` holds a key until the job finishes; `UniqueFor(key, d)` holds it for a time window.
- **Long jobs.** Running handlers keep their lease alive; a crashed worker's job is claimed again after the lease expires. A lost lease cancels the handler's context.
- **Dead letters.** Jobs that run out of attempts are kept with their last error; list, retry or delete them through the client.
- **Conformance suite.** `jobq/drivertest` checks every driver against the same contract.

Requires Go 1.27+.

## Modules

| Module | Purpose | Dependencies |
|---|---|---|
| `github.com/runforyou-ai/jobq` | Client, worker, driver contract, `memq`, `drivertest` | standard library |
| `github.com/runforyou-ai/jobq/natsq` | NATS JetStream driver (nats-server 2.14+) | nats.go |
| `github.com/runforyou-ai/jobq/pgq` | PostgreSQL driver (PostgreSQL 14+) | pgx/v5 |

```sh
go get github.com/runforyou-ai/jobq
go get github.com/runforyou-ai/jobq/pgq   # and/or natsq
```

## Usage

```go
type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

func (WelcomeEmail) JobName() string { return "welcome-email" }

// Pick a driver.
driver, err := pgq.New(ctx, pool)       // pool is a *pgxpool.Pool
// driver, err := natsq.New(ctx, js)    // js is a jetstream.JetStream
// driver := memq.New()                 // tests and examples
defer driver.Close()

q := jobq.New(driver)

id, err := q.Enqueue(ctx, WelcomeEmail{UserID: 42},
	jobq.OnQueue("mail"),
	jobq.Delay(10*time.Minute),      // or jobq.At(t)
	jobq.Unique("welcome-42"),       // ErrDuplicate until that job finishes
	jobq.MaxAttempts(5),
	jobq.Timeout(time.Minute),
)
// errors.Is(err, jobq.ErrUncertain): the job may or may not be stored; id is returned.
// Enqueue again with jobq.JobID(id): you get ErrJobIDInUse if the first copy was stored.
_ = q.Cancel(ctx, id)

// Recurring jobs: every instance passes the same full set at startup.
err = q.SyncSchedules(ctx,
	jobq.Schedule{Key: "daily-report", Cron: "0 2 * * *", Timezone: "Asia/Shanghai", Job: DailyReport{}},
	jobq.Schedule{Key: "sweep", Cron: "@every 30s", Job: Sweep{}, Options: []jobq.EnqueueOption{jobq.OnQueue("maintenance")}},
)

// Workers.
w := q.NewWorker(
	jobq.Queue{Name: "default"},
	jobq.Queue{Name: "mail", Concurrency: 8, MaxAttempts: 5, Backoff: jobq.Exponential(10*time.Second, time.Hour), Timeout: 5 * time.Minute},
)
jobq.Handle(w, func(ctx context.Context, job WelcomeEmail) error {
	info, _ := jobq.JobInfo(ctx) // id, attempt, queue, headers...
	return send(ctx, job.UserID)
}, jobq.OnFailure(func(ctx context.Context, job WelcomeEmail, err error) {
	// Record the terminal failure.
}))
w.Use(loggingMiddleware)
err = w.Run(ctx) // blocks; on cancel, waits for running jobs
```

For job names known only at runtime, use `EnqueueNamed(ctx, name, payload)` with `HandleNamed`, or `jobq.RawJob` with `w.HandleRaw` to receive the JSON payload byte for byte.

### Inspecting queues

`q.Stats(ctx)` reports per queue the jobs that are ready, scheduled (delayed, retrying or snoozed), running and dead-lettered. `q.DeadLetters(ctx, jobq.DeadLetterQuery{...})` pages through dead letters, newest first. `q.RetryDead(ctx, id)` and `q.DeleteDead(ctx, id)` act on one. Drivers that cannot do this return `jobq.ErrUnsupported`.

### Handler results

| Return | Effect |
|---|---|
| `nil` | Job done. |
| any error | Retried after the queue's backoff; dead-lettered when attempts run out. |
| `jobq.RetryAfter(d, err)` | Retried after `d` instead of the backoff. |
| `jobq.Permanent(err)` | Dead-lettered immediately. |
| `jobq.Snooze(d)` | Runs again after `d`; the attempt is not counted (`Info.Snoozes` counts snoozes). |
| panic | Treated as an error. |
| timeout | The context is cancelled with cause `jobq.ErrTimeout`; the attempt fails. |

A job this worker has no handler for is put back for other workers without using up an attempt, and dead-lettered after `w.SetUnknownJobTimeout` (default 1h). Jobs interrupted by the shutdown timeout are put back without using up an attempt; handlers that ignore cancellation are abandoned 10 seconds later.

Delivery is **at least once**: make handlers idempotent.

## Drivers

| | memq | natsq | pgq |
|---|---|---|---|
| Storage | process memory | JetStream stream and KV bucket | one PostgreSQL schema |
| Delayed jobs | yes | server-side message schedules | `run_at` on the database clock |
| Recurring schedules | yes | server-side message schedules | rows claimed with `SKIP LOCKED` |
| Inspection and dead letters | yes | yes | yes |
| Transactional enqueue | no | no | `pgq.Driver.EnqueueTx` |

### pgq

`pgq.New` creates or migrates the schema (default `jobq`). `EnqueueTx` stores a job inside your own transaction, so it commits or rolls back with your data:

```go
tx, _ := pool.Begin(ctx)
// ... write your rows ...
msg, _ := jobq.NewMessage("welcome-email", WelcomeEmail{UserID: 42}, jobq.OnQueue("mail"))
err := driver.EnqueueTx(ctx, tx, msg)
err = tx.Commit(ctx)
```

Notes:

- Waiting workers are woken by `LISTEN/NOTIFY` on a channel of the driver's own, separate from the pool, so a small pool is fine. With PgBouncer in transaction mode use `pgq.WithoutListen()`; workers then poll (`pgq.WithPollInterval`).
- Every driver fires due schedules by default; each occurrence still fires once. To fire them only in dedicated processes, create the other drivers with `pgq.WithoutScheduler()`.
- All times are applied against the database clock, so clock skew between application hosts does not shift delays.
- Succeeded and cancelled jobs are kept for 24 hours (`WithRetention`), dead letters for 14 days (`WithDeadRetention`).

### natsq

`natsq.New` creates the work stream, the state bucket and the dead-letter stream. Applications sharing a NATS account need distinct prefixes (`natsq.WithPrefix`). Lease expiry is judged by process clocks, so keep clocks synchronised to within a few seconds. See [docs/design.md](docs/design.md#nats-driver-natsq) for how the state record fences stale workers.

### Writing a driver

Implement `jobq.Driver` (and optionally `jobq.Scheduler` and `jobq.Inspector`), then run the conformance suite:

```go
func TestConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) jobq.Driver { return newMyDriver(t) }, drivertest.Options{})
}
```

## Development

```sh
make test   # all modules; pgq tests need DATABASE_URL and are skipped without it
make lint
```

## Design

See [docs/design.md](docs/design.md) for the driver contract, worker semantics and each driver's storage layout.

## License

[MIT](LICENSE)
