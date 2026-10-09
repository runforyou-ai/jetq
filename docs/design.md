# jetq design

## Goals

- A job queue with Laravel-like ergonomics on top of NATS JetStream.
- Let the NATS server do the hard parts: persistence, redelivery, delays and
  cron.
- Keep the library database-agnostic. A transactional outbox is an optional
  add-on (see [Roadmap](#roadmap)).

## Streams and subjects

`jetq.New` creates or updates two streams. The subject prefix (default `jetq`)
and stream name (default `JETQ`) are configurable.

| Stream | Subjects | Retention | Purpose |
|---|---|---|---|
| `JETQ` | `jetq.q.<queue>` | WorkQueue | Runnable jobs. |
|  | `jetq.at.<job id>` |  | Pending delayed jobs (server schedule messages). |
|  | `jetq.cron.<key>` |  | Recurring schedules. |
| `JETQ_DEAD` | `jetq.dead.<queue>` | Limits, MaxAge 14d | Dead-lettered jobs. |

All queues share the work stream. Message schedules require the schedule
message and its target to live in the same stream, so a shared stream lets a
delayed job or schedule target any queue. The work stream enables
`AllowMsgSchedules` and `AllowRollup`; it never uses `DiscardNew`, which
message scheduling does not support.

Each queue is a durable pull consumer `jetq-<queue>` filtered on
`jetq.q.<queue>` with explicit acks. Schedule subjects match no consumer, so
schedule messages stay in the stream until they fire or are cancelled.

## Message format

The body is the job encoded as JSON. Headers:

| Header | Meaning |
|---|---|
| `Jetq-Job` | Job name (`JobName()`), used to find the handler. |
| `Jetq-Id` | Job id: a NUID assigned at enqueue time, or the id given with `JobID`. |
| `Jetq-Enqueued-At` | RFC 3339 enqueue time. |
| `Jetq-Max-Attempts` | Optional per-job attempt limit. |
| `Jetq-Timeout` | Optional per-job attempt timeout (Go duration). |
| `Jetq-Attempt-Base` | Attempts used before the job was put back (internal). |
| `Jetq-Snoozes` | Number of snoozes (internal, exposed as `Info.Snoozes`). |
| `Jetq-Unknown-Since` | When a worker first found no handler for the job (internal). |
| `Nats-Msg-Id` | `Unique` key, deduplicated by the stream within its duplicate window. |
| `Jetq-Unique-Lock` | Lock key of a `UniqueUntilDone` job. |

Application headers set with `WithHeader` (for example `traceparent`) are kept.
Headers starting with `Jetq-` or `Nats-` are reserved.

## Delayed jobs

`Delay`/`At` publish a schedule message on `jetq.at.<id>` with
`Nats-Schedule: @at <time>` and `Nats-Schedule-Target: jetq.q.<queue>`. When it fires, the
server publishes a copy (minus scheduling headers and `Nats-Msg-Id`) to the
queue subject and purges the schedule message. `Cancel(id)` removes the
pending schedule message; jobs waiting for a retry or a snooze are stored the
same way and can be cancelled too. A schedule can fire while `Cancel` runs, so `Cancel`
first records a marker `cancel.<id>` (holding the job's enqueue time) in the
`<STREAM>_STATE` bucket, then deletes exactly the schedule message it read. If
the delete succeeds, `Cancel` returns nil and workers skip a copy the
scheduler published meanwhile, including redeliveries of it, releasing its
lock. If it fails because the schedule already fired, `Cancel` returns
`ErrNotFound` and the job either runs or is skipped; the same holds when
`Cancel` fails with another error after recording the marker. Retries and
snoozes of a job whose marker was recorded are skipped too, so a running job
finishes its current attempt but is not retried. Markers are never
withdrawn, so concurrent cancels cannot undo each other; they expire with the
bucket TTL, and a later job reusing the id has another enqueue time and is not
skipped. Workers and lock releases read markers and locks through the bucket
stream's leader, because key-value direct gets may be answered by a lagging
follower. When the marker cannot be read, the copy is put back like a snooze
and checked again after 5 seconds instead of running, without using up an
attempt; if putting it back fails too, the copy is redelivered and that
delivery counts as an attempt. Cancelling does not clear the `Unique` key, which
stays reserved for the rest of the duplicate window; a `UniqueUntilDone` lock
is released.

## Recurring jobs

`SyncSchedules` installs schedule messages on `jetq.cron.<key>` with a cron or
`@every` pattern and an optional `Nats-Schedule-Time-Zone`. Publishing on the
same subject replaces the previous schedule (implicit subject rollup).

The call is declarative: schedules missing from the given set are purged, and
unchanged schedules are left alone so that restarts do not reset their timing.
Every instance passes the same set; during a rolling deploy the last writer
wins, which is harmless when versions agree on the set.

Five-field cron expressions are converted to the six-field (seconds-first)
format NATS expects. Time zones do not apply to `@every`. Jobs fired by a
schedule have the id `<key>-<stream sequence>`.

The server fires each occurrence once regardless of how many workers run, so
there is no leader election. Missed occurrences while the server is down are
skipped, not replayed.

## Processing

A worker runs one fetch loop per queue. It only fetches as many messages as it
has free slots (`Concurrency`), so no job waits in a local buffer while its ack
timer runs.

For each message:

1. Build `Info`: attempt = JetStream delivery count + attempt base.
2. Start a keep-alive that sends `InProgress` every `AckWait/3`, at most every
   5 seconds.
3. Without a handler for the job name, put it back (see below).
4. Run middleware and the handler under the attempt timeout; panics become
   errors; undecodable payloads are permanent errors.
5. Settle:
   - success: `Ack`;
   - `Snooze(d)`: put back after `d` with attempt base = attempt - 1, so this
     attempt is not counted, and `Jetq-Snoozes` + 1;
   - shutdown timeout cancelled the handler: put back right away with attempt
     base = attempt - 1;
   - permanent error or attempts exhausted: copy to the dead-letter stream with
     `Jetq-Error`, `Jetq-Attempts`, `Jetq-Failed-At`, `Jetq-Queue`; run
     `OnFailure` and `OnFailed`; `Ack`;
   - otherwise: put back after `RetryAfter` or `Backoff(attempt)` with attempt
     base = attempt.

Putting back republishes the job with the same id, as a delayed job on
`jetq.at.<id>` when there is a delay, and then acks the delivery. Jobs waiting
for a retry or a snooze are therefore not in flight: the consumer's
`MaxAckPending` (default 1000) only counts running jobs, and a backlog of
failing jobs never blocks healthy ones. They can be cancelled like other
delayed jobs. If republishing fails, the delivery is nak'ed with the delay
instead and its redelivery counts as the next attempt.

jetq decides retry timing and the attempt limit itself; the consumer has
`MaxDeliver: -1` and no server backoff, so there is a single source of truth.
`AckWait` only bounds recovery after a worker crash. Crash redeliveries count as
attempts.

Delay times (`Delay`, snoozes, retries) are turned into an absolute `@at` time
with the clock of the process that publishes them; keep clocks synchronised.

### Timeouts

`Queue.Timeout`, overridden per job by `Timeout(d)` (header `Jetq-Timeout`),
bounds one attempt: when it passes, the handler's context is cancelled with
cause `ErrTimeout` and the attempt fails with an error wrapping `ErrTimeout`,
retried like any other failure. A handler that has not returned 10 seconds
after its context was cancelled, by a timeout or by shutdown, is abandoned:
its goroutine keeps running, but the worker settles the job, frees the slot
and stops keeping the message alive. Abandoned handlers therefore no longer
count towards `Concurrency`.

### Unknown jobs

When a worker has no handler for a job, for example because a newer version or
another service produces it, the worker puts it back after 10 seconds without
using up an attempt and records in `Jetq-Unknown-Since` when this first
happened. Middleware and handlers do not run. Once
`SetUnknownJobTimeout` (default 1h) has passed since then, the worker
dead-letters the job; only `OnFailed` callbacks run, since the job's own
`OnFailure` is registered elsewhere. A non-positive timeout puts it back
forever.

### Logging

jetq logs retries, dead-lettering and settlement problems through the
client's `slog.Logger`. `Worker.SetLogContext` derives the context of those
records from the job's `Info`, so a logging handler can attach application
fields such as a trace id or tenant; failure callbacks receive the same
context.

### Shutdown

When the `Run` context is cancelled the worker stops fetching, waits for
running handlers up to the shutdown timeout (default 30s), then cancels their
contexts and puts their jobs back for immediate delivery elsewhere without
using up the attempt. `Run` returns once every handler has returned or been
abandoned (10 seconds after the cancellation), and the jobs are settled.

Settlement (dead-lettering, failure callbacks, putting back) stays under the
keep-alive and uses its own bounded context, so it completes during shutdown and
the job is not redelivered while it runs. Panics in failure callbacks are logged
and do not stop the worker. A delivery whose attempt already exceeds the limit
(the previous worker crashed on the last attempt) is dead-lettered without
running the handler.

The dead-letter copy carries `Nats-Msg-Id: jetq-dead-<stream sequence>`, so a
redelivery after a lost ack within the dead-letter stream's duplicate window
(2m) does not add a second entry; failure callbacks run again, so they must be
idempotent.

If fetching fails, for example because the consumer was deleted, the worker
waits a second and creates the consumer again.

## Inspection

`Stats` reads consumer info for every `jetq-<queue>` consumer (ready =
`NumPending`, in flight = `NumAckPending`) and subject counts of the
dead-letter, delayed and cron subjects. Jobs waiting for a retry or a snooze
count as delayed; `Redelivered` counts jobs redelivered after a crash or a
failed settlement.

`DeadLetters` without a queue filter walks the dead-letter stream backwards
from the newest sequence, one message per result. With a queue filter it reads
that queue's subject through an ordered consumer in batches and keeps the
newest entries below the cursor. `Before` is an exclusive sequence cursor.

Inspection calls read stream info through fresh stream handles, because
`Stream.Info` caches its result on the handle and the client is used
concurrently.

## Guarantees

- At-least-once delivery. A handler may run more than once (crash, ack lost,
  keep-alive lost); handlers must be idempotent.
- `Unique` only deduplicates within the stream's duplicate window (default
  2m).
- `UniqueUntilDone` takes a lock in the `<STREAM>_STATE` key-value bucket,
  created on first use (an existing bucket keeps its configuration; the client
  needs permission to create it and to publish to `$KV.<STREAM>_STATE.>`) (key
  = SHA-256 of the unique key, value = job id) before publishing, and returns
  `ErrDuplicate` while it is held. The worker releases it before acking a
  success and after dead-lettering, before failure callbacks run; `Cancel`
  releases it for delayed jobs; a publish the server rejected releases it;
  after an uncertain failure such as a timeout the job may have been stored,
  so the lock is kept. `Cancel` deletes exactly the schedule message it read
  and releases the lock only if that delete succeeds. A lock is only released
  by the job that holds it. Snoozed and retrying jobs keep it. Every lock
  expires at the bucket TTL (`WithUniqueLockTTL`, default 24h) counted from
  enqueue, which bounds locks left behind by crashes and also ends
  deduplication for jobs that stay unsettled longer.
- `Enqueue` is not transactional with your database. Enqueue after commit; a
  crash between commit and enqueue loses the job. The outbox add-on closes
  this gap.
- Job state lives only in JetStream. Store business results in your own
  tables.
- Workers of the same queue share one durable consumer whose `AckWait` is set
  by the last worker to start; give every worker of a queue the same `Queue`
  settings. Keep-alives are sent at least every 5 seconds, so mixed `AckWait`
  values of 15 seconds or more are safe during a rolling deploy.
- Applications sharing a NATS account must use distinct stream names and
  subject prefixes: consumers are named `jetq-<queue>` and `SyncSchedules`
  removes schedules it was not given.

## Embedding NATS

jetq does not start NATS. For a single server you can embed it:

```go
srv, _ := server.NewServer(&server.Options{JetStream: true, StoreDir: dataDir, DontListen: true})
srv.Start()
srv.ReadyForConnections(10 * time.Second)
nc, _ := nats.Connect("", nats.InProcessServer(srv))
js, _ := jetstream.New(nc)
q, _ := jetq.New(ctx, js)
```

For several servers use a standalone NATS server or cluster. Do not run an
embedded JetStream cluster of two nodes: Raft needs a majority, so losing either
node stops the stream.

## Roadmap

- Transactional outbox: `EnqueueTx(ctx, tx, job)` writes to an outbox table in
  the caller's transaction and a relay publishes it, with PostgreSQL and MySQL
  dialect modules.
- Job chains and batches.
- Dead-letter requeue (API and CLI), dashboard.
- OpenTelemetry instrumentation.
