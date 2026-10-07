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
| `Jetq-Attempt-Base` | Attempts consumed before a snooze (internal). |
| `Nats-Msg-Id` | `Unique` key, deduplicated by the stream within its duplicate window. |
| `Jetq-Unique-Lock` | Lock key of a `UniqueUntilDone` job. |

Application headers set with `WithHeader` (for example `traceparent`) are kept.
Headers starting with `Jetq-` or `Nats-` are reserved.

## Delayed jobs

`Delay`/`At` publish a schedule message on `jetq.at.<id>` with
`Nats-Schedule: @at <time>` and `Nats-Schedule-Target: jetq.q.<queue>`. When it fires, the
server publishes a copy (minus scheduling headers and `Nats-Msg-Id`) to the
queue subject and purges the schedule message. `Cancel(id)` removes the
pending schedule message. A schedule can fire while `Cancel` runs, so `Cancel`
first stores a marker `cancel.<id>` (holding the job's enqueue time) in the
`<STREAM>_STATE` bucket, then deletes exactly the schedule message it read. If
the delete fails, the Cancel that wrote the marker withdraws it (a concurrent
Cancel that found it leaves it alone) and the job may still run
(`ErrNotFound` when the schedule already fired). If it succeeds, workers skip
a copy the scheduler published meanwhile, including redeliveries of it, and
release its lock; when the marker cannot be read, the copy is redelivered
after 5 seconds instead of running; the marker expires with the bucket TTL. A job reusing the id
later has another enqueue time and is not skipped. Cancel stays best effort
for a copy a worker checked before the marker was written, and for a few
seconds after the state bucket is first created (workers recheck a missing
bucket every 5 seconds). Cancelling does not clear the `Unique` key, which
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

1. Build `Info`: attempt = JetStream delivery count (+ snooze base).
2. Start a keep-alive that sends `InProgress` every `AckWait/3`.
3. Run middleware and the handler; panics become errors; undecodable payloads
   are permanent errors; unknown job names are ordinary errors (a newer
   producer may be ahead of this worker).
4. Settle:
   - success: `Ack`;
   - `Snooze(d)`: republish as a delayed job with the same id and an attempt
     base so this attempt is not counted, then `Ack`;
   - shutdown timeout cancelled the handler: `Nak` for immediate redelivery;
   - permanent error or attempts exhausted: copy to the dead-letter stream with
     `Jetq-Error`, `Jetq-Attempts`, `Jetq-Failed-At`, `Jetq-Queue`; run
     `OnFailure` and `OnFailed`; `Ack`;
   - otherwise: `NakWithDelay(RetryAfter or Backoff(attempt))`.

jetq decides retry timing and the attempt limit itself; the consumer has
`MaxDeliver: -1` and no server backoff, so there is a single source of truth.
`AckWait` only bounds recovery after a worker crash. Crash redeliveries count as
attempts.

### Logging

jetq logs retries, dead-lettering and settlement problems through the
client's `slog.Logger`. `Worker.SetLogContext` derives the context of those
records from the job's `Info`, so a logging handler can attach application
fields such as a trace id or tenant; failure callbacks receive the same
context.

### Shutdown

When the `Run` context is cancelled the worker stops fetching, waits for
running handlers up to the shutdown timeout (default 30s), then cancels their
contexts and naks them for immediate redelivery elsewhere. `Run` returns only
after every handler has returned, so handlers must honour context cancellation.

Settlement (dead-lettering, failure callbacks, snoozing) stays under the
keep-alive and uses its own bounded context, so it completes during shutdown and
the job is not redelivered while it runs. Panics in failure callbacks are logged
and do not stop the worker. A delivery whose attempt already exceeds the limit
(the previous worker crashed on the last attempt) is dead-lettered without
running the handler.

## Inspection

`Stats` reads consumer info for every `jetq-<queue>` consumer (ready =
`NumPending`, in flight = `NumAckPending`) and subject counts of the
dead-letter, delayed and cron subjects. A job waiting for its retry delay was
nak'ed with a delay and stays in flight; consumer info cannot tell it apart
from a running job.

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
  settings.
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
