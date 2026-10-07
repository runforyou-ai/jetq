# jetq

[![CI](https://github.com/runforyou-ai/jetq/actions/workflows/ci.yml/badge.svg)](https://github.com/runforyou-ai/jetq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/runforyou-ai/jetq.svg)](https://pkg.go.dev/github.com/runforyou-ai/jetq)

[中文](README.zh-CN.md)

jetq is a background job queue for Go built on [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream).
If you know Laravel queues, you already know jetq: jobs are structs, handlers are functions,
and retries, backoff, delays, unique jobs, failed-job handling and cron schedules work the way you expect.

- **Typed jobs.** A job is a struct with a `JobName()` method; handlers receive the decoded struct.
- **Retries that make sense.** Per-queue attempt limits and backoff, `Permanent` errors, `RetryAfter`, and `Snooze` for "not now" without burning an attempt.
- **Delayed and recurring jobs on the server.** Delays and cron schedules use JetStream message scheduling, so there is no scheduler process, polling or leader election in your app.
- **Long jobs.** Running handlers keep their message alive; a crashed worker's job is redelivered after `AckWait`.
- **Dead letters.** Jobs that run out of attempts land in a dead-letter stream with the last error, and your `OnFailure` callbacks run.
- **Small surface.** One stream, one consumer per queue, plain `nats.go`. Embedded or standalone NATS is your application's choice.

Requires nats-server **2.14+** (2.15 recommended) and Go 1.27+.

## Install

```sh
go get github.com/runforyou-ai/jetq
```

## Usage

```go
type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

func (WelcomeEmail) JobName() string { return "welcome-email" }

q, err := jetq.New(ctx, js) // js is a jetstream.JetStream; creates the streams if needed

// Enqueue after your database transaction has committed.
id, err := q.Enqueue(ctx, WelcomeEmail{UserID: 42},
	jetq.OnQueue("mail"),
	jetq.Delay(10*time.Minute),   // or jetq.At(t)
	jetq.Unique("welcome-42"),    // ErrDuplicate within the duplicate window
	// jetq.JobID(id) sets the id yourself, e.g. to store it before enqueueing
)
_ = q.Cancel(ctx, id)            // cancel a pending delayed job

// Recurring jobs: every instance passes the same full set at startup.
err = q.SyncSchedules(ctx,
	jetq.Cron("daily-report", "0 2 * * *", DailyReport{}).In("Asia/Shanghai"),
	jetq.Cron("sweep", "@every 30s", Sweep{}, jetq.OnQueue("maintenance")),
)

// Workers.
w := q.NewWorker(
	jetq.Queue{Name: "default"},
	jetq.Queue{Name: "mail", Concurrency: 8, MaxAttempts: 5, Backoff: jetq.Exponential(10*time.Second, time.Hour)},
)
jetq.Handle(w, func(ctx context.Context, job WelcomeEmail) error {
	info, _ := jetq.JobInfo(ctx) // id, attempt, queue, headers...
	return send(ctx, job.UserID)
}, jetq.OnFailure(func(ctx context.Context, job WelcomeEmail, err error) {
	// like Laravel's failed()
}))
w.Use(loggingMiddleware)
err = w.Run(ctx) // blocks; on cancel, waits for running jobs
```

### Runtime job names

When job names are only known at runtime (for example when bridging an existing task system),
enqueue `jetq.RawJob{Name: name, Payload: json}` and register `w.HandleRaw(name, fn, jetq.OnRawFailure(...))`.
The payload must be valid JSON and is delivered byte for byte.

### Inspecting queues

`q.Stats(ctx)` reports per queue the jobs not yet delivered (`Ready`), the delivered but unsettled
jobs, running or waiting for a retry delay (`InFlight`), and the dead-lettered jobs, plus the number
of pending delayed jobs and installed schedules. `q.DeadLetters(ctx, jetq.DeadLetterQuery{...})` pages
through dead-lettered jobs, newest first, with their last error.

### Log context

`w.SetLogContext(func(ctx, info) context.Context)` derives the context of jetq's own job log records
and of failure callbacks, so your logging handler can attach a trace id or tenant.

### Handler results

| Return | Effect |
|---|---|
| `nil` | Job done, removed from the queue. |
| any error | Retried after the queue's backoff; dead-lettered when attempts run out. |
| `jetq.RetryAfter(d, err)` | Retried after `d` instead of the backoff. |
| `jetq.Permanent(err)` | Dead-lettered immediately. |
| `jetq.Snooze(d)` | Runs again after `d`; the attempt is not counted. |
| panic | Treated as an error. |

Delivery is **at least once**: make handlers idempotent.

## Laravel cheat sheet

| Laravel | jetq |
|---|---|
| `implements ShouldQueue` | `JobName() string` |
| `handle()` | `jetq.Handle(w, fn)` |
| `Job::dispatch()->onQueue('mail')->delay(...)` | `q.Enqueue(ctx, job, jetq.OnQueue("mail"), jetq.Delay(d))` |
| `$tries`, `backoff()` | `Queue.MaxAttempts`, `Queue.Backoff`, `jetq.MaxAttempts(n)` |
| `$this->release($delay)` | `return jetq.Snooze(d)` |
| `$this->fail()` | `return jetq.Permanent(err)` |
| `failed()` / `failed_jobs` | `jetq.OnFailure`, `w.OnFailed`, dead-letter stream |
| `ShouldBeUnique` | `jetq.Unique(key)` (within the duplicate window) |
| `$schedule->job(...)->cron(...)->timezone(...)` | `jetq.Cron(key, spec, job).In(tz)` + `q.SyncSchedules` |
| `queue:work` | `w.Run(ctx)` |
| `after_commit` | call `Enqueue` after commit |

## Deployment

jetq only needs a `jetstream.JetStream`. Run NATS however suits you:

- **Single server:** embed `nats-server` in your process (see [docs/design.md](docs/design.md#embedding-nats)).
- **Several servers:** use a standalone NATS server or cluster. Use `jetq.WithReplicas(3)` on a three-node cluster.

## Design

See [docs/design.md](docs/design.md) for streams, subjects, delivery semantics and the roadmap
(transactional outbox for PostgreSQL and MySQL, job chains and batches, a dashboard).

## License

[MIT](LICENSE)
