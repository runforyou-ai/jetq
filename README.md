# jetq

[![CI](https://github.com/runforyou-ai/jetq/actions/workflows/ci.yml/badge.svg)](https://github.com/runforyou-ai/jetq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/runforyou-ai/jetq.svg)](https://pkg.go.dev/github.com/runforyou-ai/jetq)

[中文](README.zh-CN.md)

jetq is a background job queue for Go built on [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream).
Jobs carry explicitly named JSON payloads. Handlers are ordinary Go functions; the queue manages retries, backoff, delays, uniqueness, failure callbacks and recurring schedules.

- **Typed handlers.** Payloads are ordinary structs; `EnqueueNamed` and `HandleNamed` share an explicit job name.
- **Retries that make sense.** Per-queue attempt limits and backoff, `Permanent` errors, `RetryAfter`, and `Snooze` for "not now" without burning an attempt.
- **Delayed and recurring jobs on the server.** Delays and cron schedules use JetStream message scheduling, so there is no scheduler process, polling or leader election in your app.
- **Long jobs.** Running handlers keep their message alive; a crashed worker's job is redelivered after `AckWait`. `Queue.Timeout` and `jetq.Timeout(d)` bound a single attempt.
- **Dead letters.** Jobs that run out of attempts land in a dead-letter stream with the last error, and your `OnFailure` callbacks run.
- **Small surface.** One stream, one consumer per queue, plain `nats.go`. Embedded or standalone NATS is your application's choice.

Requires nats-server **2.14+** (2.15 recommended) and Go 1.27+.

## Install

```sh
go get github.com/runforyou-ai/jetq
```

Version 0.4 removes `Cron` and `Schedule.In`; see [migration instructions](CHANGELOG.md#v040) before upgrading.

## Usage

```go
type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

q, err := jetq.New(ctx, js) // js is a jetstream.JetStream; creates the streams if needed

// Enqueue after your database transaction has committed.
id, err := q.EnqueueNamed(ctx, "welcome-email", WelcomeEmail{UserID: 42},
	jetq.OnQueue("mail"),
	jetq.Delay(10*time.Minute),   // or jetq.At(t)
	jetq.UniqueUntilDone("welcome-42"), // ErrDuplicate until that job settles; jetq.Unique(key) uses a time window
	// jetq.JobID(id) sets the id yourself, e.g. to store it before enqueueing
)
// errors.Is(err, jetq.ErrUncertain): the job may or may not be enqueued (timeout, lost
// connection); id is returned. A UniqueUntilDone job is published once more to settle
// this, even after ctx is cancelled, and otherwise keeps its key until it settles or expires.
_ = q.Cancel(ctx, id)            // cancel a pending delayed job

// Recurring jobs: every instance passes the same full set at startup.
err = q.SyncSchedules(ctx,
	jetq.Schedule{Key: "daily-report", Cron: "0 2 * * *", Name: "daily-report", Payload: DailyReport{}, Timezone: "Asia/Shanghai"},
	jetq.Schedule{Key: "sweep", Cron: "@every 30s", Name: "sweep", Payload: Sweep{}, Queue: "maintenance"},
)

// Workers.
w := q.NewWorker(
	jetq.Queue{Name: "default"},
	jetq.Queue{Name: "mail", Concurrency: 8, MaxAttempts: 5, Backoff: jetq.Exponential(10*time.Second, time.Hour), Timeout: 5 * time.Minute},
)
jetq.HandleNamed(w, "welcome-email", func(ctx context.Context, job WelcomeEmail) error {
	info, _ := jetq.JobInfo(ctx) // id, attempt, queue, headers...
	return send(ctx, job.UserID)
}, jetq.OnFailure(func(ctx context.Context, job WelcomeEmail, err error) {
	// Record the terminal failure.
}))
w.Use(loggingMiddleware)
err = w.Run(ctx) // blocks; on cancel, waits for running jobs
```

### Provisioned resources and durable outboxes

`jetq.ResourceConfigs(options...)` returns the work stream, dead-letter stream and state bucket definitions without writing to NATS. Provision these separately, then use `jetq.Attach(ctx, js, options...)` in application processes. Attach validates subjects, queue semantics and minimum retention while preserving replicas, storage, placement and capacity. `New` remains the convenience constructor that creates or updates resources.

For an outbox retry, keep the same payload, absolute scheduled time, `jetq.JobID(id)` and `jetq.MsgID(publicationID)` on every attempt. `MsgID` uses JetStream deduplication and can resume a `UniqueUntilDone` lock owned by the same job. It requires an explicit JobID and cannot be combined with `Unique` or relative `Delay`; use `At` for a fixed schedule. For UniqueUntilDone, the state TTL must cover the duplicate window; completion is recorded before releasing the lock. A retry acknowledges the same publication even if it has since completed or been cancelled. A recorded cancellation is respected for the state bucket TTL, including retries after the duplicate window. Keep the outbox row after `ErrUncertain`; remove it only after publication is confirmed. Deduplication lasts for the configured duplicate window, so handlers must remain idempotent for later retries.


### Runtime job names

When job names are only known at runtime (for example when bridging an existing task system),
use `EnqueueNamed(ctx, name, payload)` and `HandleNamed(w, name, fn, OnFailure(...))`. Ordinary structs need no methods. For byte-for-byte JSON handlers, pass `json.RawMessage` and register `w.HandleRaw(name, fn, jetq.OnRawFailure(...))`.
The payload must be valid JSON and is delivered byte for byte.

### Type-owned names

`Enqueue` and `Handle` also accept types implementing `JobName() string`. Use this convenience when a type owns its stable protocol name. Explicit names need no methods.

`json.RawMessage` is validated and preserved byte for byte; an empty raw message or nil payload is encoded as `null`. Other values follow `encoding/json`, including base64 JSON strings for `[]byte`. Decoding `null` into a struct yields its zero value; handlers validate required business fields.

### Inspecting queues

`q.Stats(ctx)` reports per queue the jobs not yet delivered (`Ready`), the delivered but unsettled
jobs (`InFlight`, mostly running ones), and the dead-lettered jobs, plus the number of pending
delayed jobs (including jobs waiting for a retry or snoozed) and installed schedules. `q.DeadLetters(ctx, jetq.DeadLetterQuery{...})` pages
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
| `jetq.Snooze(d)` | Runs again after `d`; the attempt is not counted (`Info.Snoozes` counts snoozes). |
| panic | Treated as an error. |
| timeout | The context is cancelled with cause `jetq.ErrTimeout`; the attempt fails. |

Jobs waiting for a retry or a snooze are stored as delayed jobs, so they never hold up a queue and
can be cancelled with `Cancel`. A job this worker has no handler for (for example during a rolling
deploy) is put back for other workers without using up an attempt and dead-lettered after
`w.SetUnknownJobTimeout` (default 1h). Jobs interrupted by the shutdown timeout are put back without
using up an attempt; handlers that ignore cancellation are abandoned 10 seconds later. Retry delays
under a second are served by redelivery, and delayed jobs fire no sooner than about 250ms.

Delivery is **at least once**: make handlers idempotent.

## Deployment

jetq only needs a `jetstream.JetStream`. Run NATS however suits you:

- **Single server:** embed `nats-server` in your process (see [docs/design.md](docs/design.md#embedding-nats)).
- **Several servers:** use a standalone NATS server or cluster. Use `jetq.WithReplicas(3)` on a three-node cluster.

## Design

See [docs/design.md](docs/design.md) for streams, subjects, delivery semantics and the roadmap
(transactional outbox for PostgreSQL and MySQL, job chains and batches, a dashboard).

## License

[MIT](LICENSE)
