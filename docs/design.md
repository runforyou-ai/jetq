# jobq design

## Goals

- One job queue API for Go with interchangeable storage drivers: NATS
  JetStream (`jobq/natsq`), PostgreSQL (`jobq/pgq`) and memory (`jobq/memq`).
- The same semantics on every driver for retries, backoff, snoozes,
  timeouts, unknown jobs and shutdown. They live in the core package, and
  drivers only record state transitions.
- A thin driver contract, checked by a shared conformance suite
  (`jobq/drivertest`).
- Separate Go modules for each driver, so an application only pulls in the
  dependencies of the drivers it uses.

## Modules

| Module | Contents | Dependencies |
|---|---|---|
| `github.com/runforyou-ai/jobq` | Client, Worker, Driver contract, cron parser, `memq`, `drivertest` | standard library |
| `github.com/runforyou-ai/jobq/natsq` | JetStream driver | nats.go, nats-server (tests) |
| `github.com/runforyou-ai/jobq/pgq` | PostgreSQL driver | pgx/v5 |

## Driver contract

```go
type Driver interface {
    Enqueue(ctx, *Message) error                               // ErrJobIDInUse, ErrDuplicate, ErrUncertain
    Claim(ctx, queue string, max int, lease time.Duration) ([]Lease, error)
    Cancel(ctx, id string) error                               // ErrNotFound
    Close() error
}

type Lease interface {
    Message() *Message
    Attempt() int
    Extend(ctx) error
    Complete(ctx) error
    Retry(ctx, at time.Time, reason string) error              // counts the attempt
    Release(ctx, Release) error                                // does not: snooze, shutdown, unknown job
    Fail(ctx, reason string) error                             // dead letter
}
```

Optional capabilities are detected with type assertions. When a driver lacks
one, the client returns `ErrUnsupported`:

- `Scheduler`: `SyncSchedules`
- `Inspector`: `Stats`, `DeadLetters`, `RetryDead`, `DeleteDead`

The rules every driver keeps, and `drivertest` checks:

- **Attempts.** The driver owns the attempt counter. A claim increments it.
  `Release` takes it back. A lease that expires without settling keeps its
  increment, so a worker crash counts as an attempt.
- **Fencing.** Every lease method is fenced on its claim. Once the lease
  expired and the job may be claimed again, the methods return
  `ErrLeaseLost` and never touch the newer claim. Settling ends the lease.
- **Claim.** It returns at most `max` jobs. It may wait for work, but returns
  empty within a few seconds, and promptly once ctx is done. Expired leases
  are claimable again.
- **Unique keys.** `UniqueFor == 0` holds the key until the job succeeds, is
  dead-lettered or is cancelled. Retries and snoozes keep it.
  `UniqueFor > 0` holds the key for that long after enqueue, whatever
  happens to the job.
- **Ids.** A job id is unique among unfinished jobs on every queue. Once the
  job has finished, the id can be used again. Retrying an enqueue that
  failed with `ErrUncertain` under the same `JobID` returns `ErrJobIDInUse`
  while the first copy is unfinished. If that copy has already finished,
  the job is stored again, as at-least-once delivery allows.
- **Cancel.** It removes jobs waiting for their run time, a retry or a
  snooze, and releases the job's unique key. A running job keeps its lease,
  finishes its attempt, and ends as a cancellation: it is neither retried
  nor dead-lettered. A cancel request whose worker never settles ends the
  job once its lease runs out.
- **Schedules.** Each occurrence fires once however many processes run. The
  fired job's id is `<key>-<suffix>`, where the suffix is unique per
  occurrence. Missed occurrences are not replayed.

Compared with the reverted driver-neutral attempt (PR #13), this contract has
no admission ledger, execution budget or decision log. The worker decides,
and the driver applies the transition atomically. The cost is that a crash
between a handler's return and its settlement runs the job again, as at
least once delivery allows.

## Worker

A worker runs one claim loop per queue and claims only as many jobs as it
has free slots. For each job it:

1. Builds `Info` from the message and `Lease.Attempt()`.
2. Starts a keep-alive that calls `Extend` every `Lease/3`. When the driver
   reports `ErrLeaseLost`, the handler's context is cancelled with that
   cause, and the job is left to whoever holds it now.
3. Without a handler, puts the job back with `Release` after 10s, without
   using up an attempt. After `SetUnknownJobTimeout` (default 1h) it
   dead-letters the job.
4. Runs middleware and the handler under the attempt timeout. Panics become
   errors, and payloads that do not decode are permanent errors.
5. Settles the job:

   | Outcome | Transition |
   |---|---|
   | success | `Complete` |
   | `Snooze(d)` | `Release{At: now+d, Snooze: true}` |
   | `Permanent` error | `Fail`, then failure callbacks |
   | interrupted by shutdown | `Release{}` (immediately, attempt not counted) |
   | attempts exhausted | `Fail`, then failure callbacks |
   | other error | `Retry(now + RetryAfter or Backoff(attempt))` |

   A failed settlement is logged. The lease then expires and the job is
   claimed again as a new attempt.

A job that is claimed again with an attempt above its limit, because the
previous worker crashed on the last attempt, is dead-lettered without running
the handler.

Timeouts, abandoning handlers that ignore cancellation (10s), shutdown
(default 30s grace) and log contexts work as in jetq 0.x.

## PostgreSQL driver (pgq)

### Schema

The tables live in a configurable schema (default `jobq`) and are created by
versioned migrations recorded in `<schema>.migrations`. Migrations run under
`pg_advisory_xact_lock`, keyed by the schema name.

```sql
CREATE SEQUENCE jobq.claim_token_seq;
CREATE TABLE jobq.jobs (
  id            text PRIMARY KEY,
  queue         text NOT NULL,
  name          text NOT NULL,
  payload       bytea NOT NULL,
  header        jsonb,
  state         text NOT NULL,         -- pending | running | succeeded | failed | canceled
  attempt       int  NOT NULL DEFAULT 0,
  max_attempts  int  NOT NULL DEFAULT 0,
  timeout_ns    bigint NOT NULL DEFAULT 0,
  snoozes       int  NOT NULL DEFAULT 0,
  unknown_since timestamptz,
  run_at        timestamptz NOT NULL,
  locked_until  timestamptz,
  claim_token   bigint NOT NULL DEFAULT 0,
  last_error    text,
  unique_key    text,
  unique_for_ns bigint NOT NULL DEFAULT 0,
  enqueued_at   timestamptz,
  finished_at   timestamptz
);
-- partial indexes: pending (queue, run_at, id), running (queue, locked_until),
-- failed (finished_at DESC, id), finished (finished_at)

CREATE TABLE jobq.unique_keys (key text PRIMARY KEY, job_id text NOT NULL, until timestamptz);
CREATE TABLE jobq.schedules (key text PRIMARY KEY, cron text, timezone text, spec jsonb, next_run_at timestamptz);
```

Dead letters stay in `jobs` with `state = 'failed'`. `RetryDead` resets one to
`pending` with `attempt = 0` and takes a new claim token.

### Operations

- **Enqueue.** One transaction deletes a finished row with the same id, takes
  the unique key with `INSERT ... ON CONFLICT DO UPDATE ... WHERE expired`, and
  inserts the job. A primary key conflict means `ErrJobIDInUse`.
  `Driver.EnqueueTx(ctx, tx, msg)` runs the same statements in the caller's
  `pgx.Tx` inside a savepoint, so a job commits or rolls back with the business
  data and a duplicate leaves the transaction usable.
- **Claim.** One statement claims due jobs and expired leases in a batch with
  `FOR UPDATE SKIP LOCKED`, increments `attempt` and takes `claim_token` from
  the schema-wide sequence. Tokens never repeat, so a reused job id cannot
  accept the lease of an earlier job with that id.
- **Fencing.** Every lease method carries
  `WHERE id = $1 AND claim_token = $2 AND state = 'running' AND locked_until > now()`.
  Zero affected rows means `ErrLeaseLost`.
- **Wake-ups.** A trigger runs `pg_notify('jobq.<schema>', queue)` when a job
  becomes pending, so drivers of other schemas in the same database are not
  woken. One listener per driver fans notifications out to waiting claims,
  and a timer set to the next due job plus a poll (default 5s) cover delayed
  jobs and lost notifications. The listener uses a connection of its own, not
  a pool slot. `WithoutListen()` turns it off, for PgBouncer in transaction
  mode.
- **Jobs that must end.** A cancel request whose worker is gone is applied
  when its lease runs out, so such a job is never claimed again. A janitor
  run holds one pool connection under a session-level advisory lock and
  deletes in batches of 1000 until it is caught up.
- **Time.** Run times and unique windows are offsets applied to the database
  clock (`clock_timestamp()`), so client clock skew and the age of a caller's
  transaction do not shift them.
- **Schedules.** Every driver instance ticks once a second and claims due
  schedule rows with `FOR UPDATE SKIP LOCKED`. Each schedule fires in its own
  savepoint: it inserts the job `<key>-<unix seconds of the occurrence>` and
  moves `next_run_at` past now. The primary key makes an occurrence fire at
  most once, no leader is needed, and a schedule that fails is logged and
  retried a minute later without blocking the others. `WithoutScheduler()`
  turns the ticker off for a driver, so only chosen processes fire schedules.
- **Retention.** A janitor, serialised by a schema-scoped advisory lock, deletes
  succeeded and cancelled jobs after 24h (`WithRetention`), dead letters after
  14 days (`WithDeadRetention`) and expired unique keys, in batches of 1000
  until caught up.

## NATS driver (natsq)

### Storage

| Resource | Contents |
|---|---|
| Stream `JOBQ` (work queue, message schedules) | `<prefix>.q.<queue>` ready jobs, `<prefix>.at.<queue>.<id>` delayed jobs, `<prefix>.cron.<key>` schedules |
| KV `JOBQ_STATE` | `job.<id>` state record per unfinished job, `uniq.<hash>` unique-key locks (per-key TTL) |
| Stream `JOBQ_DEAD` (limits, MaxAge 14d) | `<prefix>.dead.<queue>` dead letters |

Each queue has one durable pull consumer. Its `AckWait` is the shortest
lease asked for on that queue.

A message body is an 8-byte header length, the JSON envelope (id, name,
queue, header, attempts and timeout overrides, snoozes, unique key, schedule
key, generation), and then the payload unchanged.

### The state record is the authority

JetStream acks are not tied to a delivery. After a redelivery, a slow
worker's ack would settle the newer claim. natsq therefore fences
everything with a KV record per job:

```
{attempt, running, lease_until, lease_start, seq, gen, queue, unique_key, unique_for, canceled, finished_at, last_error}
```

- **Generation.** Every publish of a job (enqueue, retry, release) gets a
  new `gen`. It is written to the record *before* the publish. A delivery
  whose envelope `gen` differs from the record's is a stale copy, left
  behind by a lost ack, a failed or uncertain publish, or a cancel, and it
  is terminated on sight.
- **Claim.** Read the record. Drop the delivery if it is stale, cancelled
  or finished. If NATS redelivered early, while the record's lease still
  runs, nak it until the lease ends. Otherwise CAS `attempt+1, running,
  lease_until`.
- **Extend.** `InProgress`, then CAS `lease_until`.
- **Complete / Fail.** CAS-check the claim, store the dead letter (Fail),
  delete the record, release the unique key, then ack. A lost ack is
  harmless: the redelivery finds no record.
- **Retry / Release.** CAS the record to `pending, attempt (or attempt-1),
  new gen`, publish the new copy (delayed with `@at` when needed), record
  its sequence, then ack. If the publish fails outright, the old
  generation is restored and the delivery nak'ed, so the job is not lost.
- **Cancel.** CAS the record to a cancellation tombstone, delete the pending
  message, and release the key. The same record keeps a leftover copy from
  being read as a first delivery. A cancellation marks the record rather than
  ending the lease, so a running job finishes its attempt and its settlement
  ends as a cancellation.
- **Enqueue.** Check the id (an unfinished record means `ErrJobIDInUse`),
  take the unique key, create the record, then publish. After an uncertain
  publish, the record and the lock stay in place.
- **Schedules.** Server-side message schedules fire into the queue subject.
  The first claim of an occurrence derives its id, `<key>-<stream
  sequence>`, and creates the record. An id that is already taken skips the
  occurrence.

Lease expiry is judged by process clocks, with the server's message
timestamp as a cross-check. Keep clocks synchronised to within a few
seconds.

## Deferred

- **Lanes and priorities.** `Message` can gain `Lane` and `Priority` fields
  without changing the driver interface. pgq would implement lanes with a
  partial unique index over running jobs; natsq has no efficient equivalent.
- Job chains and batches, dashboard, OpenTelemetry middleware.
