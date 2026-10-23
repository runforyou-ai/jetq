# jetq

[![CI](https://github.com/runforyou-ai/jetq/actions/workflows/ci.yml/badge.svg)](https://github.com/runforyou-ai/jetq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/runforyou-ai/jetq.svg)](https://pkg.go.dev/github.com/runforyou-ai/jetq)

[中文](README.zh-CN.md)

jetq is a background job queue for Go. Jobs carry an explicit name and a JSON payload; producers and consumers are independent processes that share a storage backend. The root package defines submission, typed handlers, retries, timeouts, snooze, cancellation, uniqueness, inspection and redrive against a driver-neutral contract. The `natsq` package implements the contract on NATS JetStream: a KeyValue ledger is the authority for every job and decision, and a wake stream only shortens the delay before a new job is claimed.

Requires Go 1.27+ and, for the NATS driver, nats-server 2.14+ with JetStream.

## Install

```sh
go get github.com/runforyou-ai/jetq
```

## Usage

The complete, compiling example is [example_test.go](example_test.go).

```go
type Email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// A task is a stable name plus default policy. It is not bound to a queue,
// a connection or a driver.
var SendEmail = jetq.MustTask[Email]("email.send", jetq.Policy{
	MaxAttempts: 3,
	Timeout:     jetq.TimeoutOf(30 * time.Second),
	Backoff:     jetq.ExponentialBackoff(time.Second, time.Minute),
})

// Provisioning (once per deployment) and opening validate shared parameters.
cfg := natsq.Config{Namespace: "prod", Replicas: 3}
if _, err := natsq.Provision(ctx, js, cfg); err != nil { ... }
driver, err := natsq.Open(ctx, js, cfg)

// Producer.
client, err := jetq.NewClient(driver, jetq.WithMaxPayloadBytes(256<<10))
res, err := jetq.SubmitTask(ctx, client, SendEmail,
	Email{To: "a@example.com", Subject: "Welcome"},
	jetq.Submission{Queue: "mail", EnqueueKey: "welcome:a@example.com"})
// errors.Is(err, jetq.ErrEnqueueKeyConflict): the key is already held by
// different content. res.Status == jetq.Accepted on the first submission and
// jetq.AlreadyAccepted for an idempotent retry.

// Consumer, in its own process if you like.
worker := client.NewWorker(jetq.Queue{Name: "mail", Concurrency: 4})
jetq.Handle(worker, SendEmail, func(ctx context.Context, e Email) error {
	return send(ctx, e)
})
err = worker.Run(ctx) // blocks until ctx is cancelled and running handlers return
```

### The same task on several queues

A task name is the handler lookup key; the queue is chosen per submission. `SendEmail` above can be
submitted to `mail`, `bulk` and `priority` with the same handler registered on a worker that serves
several queues (`client.NewWorker(jetq.Queue{Name: "mail"}, jetq.Queue{Name: "bulk"})`). Different
producers can also set different default queues and policies with `WithDefaultQueue` and
`WithDefaultPolicy`.

### Raw and dynamic task names

Typed payloads are the normal case. Two other forms share the same envelope, execution policy,
retry and admission path:

```go
// Known at runtime, payload already JSON.
client.Submit(ctx, "legacy.import", json.RawMessage(`{"id":7}`), jetq.Submission{})
worker := client.NewWorker(jetq.Queue{Name: "imports"})
jetq.HandleTask(worker, "legacy.import", func(ctx context.Context, raw json.RawMessage) error { ... })

// Or with a raw task value, which applies a task default policy.
raw := jetq.Raw("legacy.import")
client.SubmitRaw(ctx, raw, json.RawMessage(`{"id":7}`), jetq.Submission{})
```

Raw JSON is preserved byte for byte, including whitespace and characters such as `<`. An empty raw
payload is stored as `null`. `WithMaxPayloadBytes` bounds the encoded payload of every path (typed,
raw, dynamic, prepared); a larger payload returns `jetq.ErrPayloadTooLarge` and never reaches the
driver.

### Preparing once, retrying idempotently

`Prepare`/`PrepareTask` resolve the job id, the absolute run time and the full policy without
storing anything. Repeating `SubmitPrepared` with the same value is how an outbox retries: the run
time does not move and an `EnqueueKey` makes the retry idempotent.

```go
job, err := jetq.PrepareTask(client, SendEmail, email,
	jetq.Submission{Queue: "mail", Delay: 10 * time.Minute, EnqueueKey: "digest:42"})
res1, _ := client.SubmitPrepared(ctx, job) // records the job for now + 10m
res2, _ := client.SubmitPrepared(ctx, job) // AlreadyAccepted, same run time
```

`Prepare` with no job id and no generator returns an error instead of panicking; a submission that
sets both `RunAt` and `Delay`, or a negative delay, is rejected.

### Policy priority

The resolved policy is the merge of three layers, highest first: the submission's `Policy`, the
task's default policy, then the producer client's default policy (`WithDefaultPolicy`). A `Policy`
field left zero inherits the next layer down, except for the timeout: `jetq.NoTimeout()` explicitly
declares "no timeout" and stops inheritance. The resolved policy is part of the job envelope, so the
consumer executes exactly what the producer resolved, whatever its own defaults are.

### Handler results

| Return | Effect |
|---|---|
| `nil` | Succeeded; the job is terminal. |
| error | Retried after the resolved backoff until `MaxAttempts`; then failed. |
| `jetq.Permanent(err)` | Failed immediately. |
| `jetq.RetryAfter(d, err)` | Retried after `d`. |
| `jetq.Snooze(d)` | Snoozed until `d`; the admission budget is refunded, so a snooze does not use an attempt. |
| panic | Recovered and treated as an error. |
| timeout | The handler context is cancelled with cause `jetq.ErrTimeout`; the attempt fails regardless of what the handler returns, snooze and permanent included. |

A handler that ignores cancellation keeps running in the background after a shutdown, up to the
worker's abandon window; the job is not settled by it.

### Timeouts, shutdown and cancellation

Each attempt runs under the resolved timeout (`jetq.NoTimeout()` disables it). Long attempts renew
their lease automatically, so a slow handler is not redelivered just because it takes long. On
`Run` returning, the worker waits for running handlers up to `SetShutdownTimeout`; handlers that
finish are settled normally, handlers still running are released without settling (the attempt stays
spent), and deliveries claimed but not admitted are released.

`Cancel` applies before the first admission. A job that is awaiting, scheduled, or snoozed becomes
canceled and is never delivered; a running job is not interrupted.

### Inspection and redrive

```go
snap, found, err := client.Inspect(ctx, jobID)          // state, wait reason, budget, attempts,
                                                         // deliveries, snoozes, last error, pending
page, err := client.ListJobs(ctx, jetq.JobQuery{Queue: "mail", FailedOnly: true, Limit: 50})
stats, err := client.Stat(ctx)                           // per-queue ready/running/delayed/failed counts
res, err := client.Redrive(ctx, failedID, jetq.RedriveEnqueueKey("redrive:"+failedID))
```

Snapshots carry no backend identifiers: no stream sequences and no KV revisions. The original job of
a redrive is left untouched; the replacement gets a new `JobID`, records `ParentJobID`, and inherits
the parent's task, payload, queue, policy, metadata and unique rule unless an option overrides them.
It never inherits the parent's enqueue key.

`Redrive` with a relative `RedriveDelay` fixes the run time at the call. To retry such a redrive
idempotently, prepare it once and repeat the submit:

```go
job, err := client.PrepareRedrive(ctx, failedID,
	jetq.RedriveDelay(time.Hour), jetq.RedriveEnqueueKey("redrive:"+failedID))
_, err = client.SubmitPrepared(ctx, job) // again on retry: same replacement, same run time
```

### Recurring schedules (NATS driver)

Schedules are recurring submissions to the normal path; they are not jobs themselves. A set is
owned by an owner name and carries a configuration version. A sync states the version it expects,
so a process still running an old set cannot delete what a newer deployment added.

```go
var v natsq.ScheduleVersion
v, err = driver.SyncSchedules(ctx, "billing", v,
	natsq.Schedule{Key: "daily", Cron: "0 2 * * *", Timezone: "Asia/Shanghai",
		Task: "invoice.close", Payload: json.RawMessage(`{"region":"cn"}`), Queue: "billing"})
if errors.Is(err, natsq.ErrVersionConflict) {
	v, err = driver.CurrentScheduleVersion(ctx, "billing") // then re-sync on top of it
}

worker.SetScheduler(driver.Scheduler("billing")) // the worker runs the scheduler and shuts it down
// or: driver.Scheduler("billing").Run(ctx)
```

- `expected` 0 creates the set; it conflicts when the owner already holds a different set.
- A nonzero `expected` updates only the held version; otherwise `natsq.ErrVersionConflict` and no write.
- Syncing the stored definitions is idempotent and returns the held version.
- Only changed or new schedules lose their cursor; adding one schedule does not skip a due
  occurrence of another.
- Firing never changes the version.
- Every occurrence is claimed durably with a fixed job id, enqueue key and run time. A crash
  between claim and submission is repaired by recovery with that same job; a later edit or removal
  of the schedule does not affect a claimed occurrence.
- `CatchUp: false` (default) skips occurrences missed while no scheduler ran and fires the latest one
  due; `CatchUp: true` fires up to `MaxCatchUp` of them, oldest first.

### Deployment, connection ownership and failure modes

`natsq.Provision` creates the namespace's ledger bucket, wake stream and schedule support, and
`natsq.Open` validates them: ledger storage and replica count must match the config, the ledger must
not allow direct (follower) reads, and the wake stream's subjects, retention and storage must match.
`Open` never rewrites a mismatched deployment. Both take a `jetstream.JetStream` that the caller
created and still owns; `Driver.Close` stops the driver's own goroutines but does not close the
connection.

The KV ledger is authoritative: a wake message missing because of a lost publish or a crash is found
by the periodic ledger scan. `Recover` (run by `Maintain` in the worker, or directly) finishes
recorded decisions, takes over jobs whose claim expired, completes lost submissions, and collects
expired terminal jobs with their intents, unique locks and schedule claims.

## Guarantees and limits

**At least once.** A job whose settlement outcome was lost runs again; handlers must be idempotent
for the side effects they cause. The admission budget bounds how often: an exhausted budget fails the
job without running the handler.

**Submission.** `Accepted` means the job is durable. `AlreadyAccepted` means the same `EnqueueKey`
already holds this content. `Suppressed` means a unique rule suppressed it. `Unknown` (with
`ErrUncertain`) means the outcome cannot be known; retry with the same `EnqueueKey` and prepared
value.

**EnqueueKey retention.** An accepted key is kept while its job is retained and is removed with the
job; a retry within that window returns `AlreadyAccepted`, after it the content is new work. Use
stable, content-derived keys and keep outbox rows until the submission is confirmed.

**Unique rules.** `UniqueModeWindow` suppresses submissions for the window's duration from the
holder's submission, whether or not the holder is still running; `UniqueModeUntilDone` suppresses
until the holder succeeds, fails, is canceled, or its submission is definitively rejected. Stored
suppression receipts expire on their own short retention and are never listed as jobs.

**Retention.** Terminal jobs are kept for `JobRetention` (default 24h) for inspection and redrive,
then collected. Auxiliary records (intents, unique locks, schedule claims) depend on their job and
are removed with it; a claim that cannot be removed holds the job for the next pass instead of
letting a collected job be recreated.

**Scan cost.** The driver has no deliverability index: `Receive` lists the ledger's job keys and
reads candidate records, so claim cost grows with the number of *retained* jobs, not the number of
runnable ones. With the default 24-hour terminal retention, a namespace that stores millions of
finished jobs pays for them in every receive. Claim visibility is governed by the scan interval and
the wake stream, and a lease bounds how long a crashed worker's job stays claimed. The design targets
workloads where the ledger stays in the hundreds of thousands of records; it has not been load-tested
for throughput. See [docs/benchmarks.md](docs/benchmarks.md) for the reproducible micro-benchmarks
and their scope.

**Not measured.** No throughput, latency or production-stability claims are made here.

## Design and validation

- [docs/design.md](docs/design.md): the driver contract, core and driver
  responsibilities, the NATS submission/admission/settlement/schedule protocols and safe garbage
  collection, with a transactional PostgreSQL sketch.
- [docs/validation.md](docs/validation.md): which guarantee each test group checks, including the
  process-kill tests and the conformance suite every driver runs.
- [CHANGELOG.md](CHANGELOG.md): what changed in this release.

Run the whole suite with `make test` (race detector, 180-second timeout) and the linters with
`make lint`.

## License

[MIT](LICENSE)
