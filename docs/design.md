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
  snooze, and releases the job's unique key. A running job finishes its
  attempt but is not retried.
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

The schema lives in a configurable schema (default `jobq`) and is created by
versioned migrations recorded in `jobq.migrations`. Migrations run under
`pg_advisory_xact_lock`.

```sql
CREATE TABLE jobq.jobs (
  id            text PRIMARY KEY,
  queue         text NOT NULL,
  name          text NOT NULL,
  payload       bytea NOT NULL,
  header        jsonb,
  state         text NOT NULL,         -- pending | running | succeeded | failed | canceled
  attempt       int  NOT NULL DEFAULT 0,
  max_attempts  int  NOT NULL DEFAULT 0,
  timeout_ms    bigint NOT NULL DEFAULT 0,
  snoozes       int  NOT NULL DEFAULT 0,
  unknown_since timestamptz,
  run_at        timestamptz NOT NULL,
  locked_until  timestamptz,
  claim_token   bigint NOT NULL DEFAULT 0,
  last_error    text,
  unique_key    text,
  unique_until  timestamptz,          -- null: until done
  enqueued_at   timestamptz NOT NULL,
  finished_at   timestamptz
);
CREATE INDEX ON jobq.jobs (queue, run_at, id) WHERE state = 'pending';
CREATE INDEX ON jobq.jobs (locked_until) WHERE state = 'running';
CREATE INDEX ON jobq.jobs (queue, finished_at DESC) WHERE state = 'failed';
CREATE INDEX ON jobq.jobs (finished_at) WHERE state IN ('succeeded', 'canceled');

-- Unique keys live in their own table, so the until-done and windowed
-- rules are one primary key check.
CREATE TABLE jobq.unique_keys (
  key    text PRIMARY KEY,
  job_id text NOT NULL,
  until  timestamptz            -- null: until the job finishes
);

CREATE TABLE jobq.schedules (
  key         text PRIMARY KEY,
  spec        jsonb NOT NULL,     -- cron, timezone, message
  next_run_at timestamptz NOT NULL
);
```

Dead letters stay in `jobs` with `state = 'failed'`. `RetryDead` resets a
dead letter to `pending` with `attempt = 0`.

### Operations

- **Enqueue.** One transaction claims the unique key with
  `INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE expired` and inserts the
  job. If the id belongs to a finished job, the insert reuses that row;
  if it belongs to an unfinished job, it fails with `ErrJobIDInUse`.
  `pgq.EnqueueTx(ctx, tx, ...)` runs the same statements inside the caller's
  `pgx.Tx`, so a job commits or rolls back with the business data.
- **Claim.** One statement claims due jobs and expired leases in batches:

  ```sql
  UPDATE jobq.jobs SET state = 'running', attempt = attempt + 1,
         claim_token = claim_token + 1, locked_until = now() + $lease
  WHERE id IN (SELECT id FROM jobq.jobs
               WHERE queue = $1 AND ((state = 'pending' AND run_at <= now())
                                  OR (state = 'running' AND locked_until < now()))
               ORDER BY run_at, id LIMIT $max FOR UPDATE SKIP LOCKED)
  RETURNING ...
  ```

- **Fencing.** Every lease method carries
  `WHERE id = $1 AND claim_token = $2 AND state = 'running' AND locked_until > now()`.
  Zero affected rows means `ErrLeaseLost`.
- **Wake-ups.** A trigger runs `pg_notify('jobq', queue)` when a job becomes
  pending. One listener connection per driver fans notifications out to
  waiting claims. A timer set to the next `run_at` handles delayed jobs, and
  a poll (default 5s) covers lost notifications. `WithoutListen()` disables
  the listener for PgBouncer transaction pooling.
- **Time.** All times come from the database clock (`now()`).
- **Schedules.** Every driver instance ticks once a second and claims due
  schedule rows with `FOR UPDATE SKIP LOCKED`. In the same transaction it
  inserts the job `<key>-<unix seconds of the occurrence>` and advances
  `next_run_at`. The primary key makes an occurrence fire at most once, so no
  leader election is needed.
- **Retention.** A janitor deletes succeeded and cancelled jobs after 24h
  (`WithRetention`), dead letters after 14 days (`WithDeadRetention`) and
  expired unique keys. It deletes in batches, and only one instance runs it
  at a time, under an advisory lock.

## NATS driver (natsq)

natsq keeps jetq 0.x's storage layout. It maps the contract as follows:

| Contract | JetStream |
|---|---|
| `Enqueue` | Publish to `<prefix>.q.<queue>`, or as a schedule message on `<prefix>.at.<id>` when delayed. Job ids and unique keys are claimed in the state bucket. |
| `Claim` | Fetch from the queue's durable pull consumer, up to `max`. `AckWait` is the lease. |
| `Attempt` | Delivery count plus the attempt base carried in a header. |
| `Extend` | `InProgress` |
| `Complete` | Release the unique key and job id, then `Ack`. |
| `Retry` / `Release` | Republish with the new attempt base (delayed when `at` is in the future), then `Ack`. |
| `Fail` | Copy to `<STREAM>_DEAD`, release the key, then `Ack`. |
| `Cancel` | Cancel marker plus deleting the schedule message (as in 0.x). |
| `Scheduler` | Server-side message schedules on `<prefix>.cron.<key>`. |

The outbox-oriented `MsgID` option of 0.x is dropped. Use pgq with
`EnqueueTx` when enqueueing must be transactional with database writes.

## Deferred

- **Lanes and priorities.** `Message` can gain `Lane` and `Priority` fields
  without changing the driver interface. pgq would implement lanes with a
  partial unique index over running jobs; natsq has no efficient equivalent.
- Job chains and batches, dashboard, OpenTelemetry middleware.
