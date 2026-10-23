# Changelog

## v0.5.0

This release rebuilds jetq around a driver-neutral contract. The root package
depends only on the Go standard library; `natsq` implements the contract on NATS
JetStream as a KeyValue ledger. There is no compatibility layer for the previous
API: callers migrate by compiling.

### Submission

- `Task[T]` declares a stable task name and default policy without binding a
  queue, connection or driver. `SubmitTask`, `Submit`, `SubmitRaw` and the dynamic
  name path resolve the same envelope and execution policy.
- `Prepare`/`PrepareTask` and `PrepareRedrive` fix the job id, the absolute run
  time (including a relative delay) and the resolved policy, so repeating
  `SubmitPrepared` is an idempotent retry that keeps the original run time.
- `EnqueueKey` idempotency: the same key with the same content returns
  `AlreadyAccepted`; different content returns `ErrEnqueueKeyConflict`.
- `Suppressed` is a final outcome for unique-rule suppression and is retained
  for replay within the key's retention.
- `WithMaxPayloadBytes` bounds the encoded payload of every submission path;
  oversized payloads return `ErrPayloadTooLarge` before reaching the driver.
- Payload bytes are preserved: envelopes persist the payload as opaque bytes, so
  whitespace, key order and characters such as `<` survive, and a prepared job
  restored from JSON keeps its fingerprint.

### Execution

- `Handle`, `HandleTask`, `HandleRaw` and `OnFailure` register typed, explicitly
  named and raw JSON handlers on a worker; middleware via `Worker.Use`.
- Policy priority: submission, task defaults, producer defaults. `NoTimeout()`
  disables the timeout explicitly; a timeout fails the attempt whatever the
  handler returns.
- `Permanent`, `RetryAfter` and `Snooze` classify handler results; a snooze and a
  shutdown release refund the admission budget exactly once. The budget bounds
  how often a job runs, including after lost settlement responses.
- Admission, extension and settlement are separate operations. Extending a lease
  is not blocked by a concurrent settlement; long jobs renew until settlement
  completes.
- Graceful shutdown finishes running handlers within the shutdown timeout and
  releases the rest; handlers that ignore cancellation are abandoned after a
  bounded window.
- Unknown handlers defer without spending budget and fail after the unknown
  task timeout.

### Inspection, redrive, schedules

- `Inspector` (`Client.Inspect`, `ListJobs`, `Stat`) returns neutral snapshots:
  state, wait reason, budget, attempts, deliveries, snoozes, last error, pending
  decision, and no backend identifiers.
- `Redrive`/`PrepareRedrive` create a replacement for a failed job with a new
  id and `ParentJobID`; the original is untouched, the parent's enqueue key is
  never inherited, and a prepared redrive is idempotent across retries.
- Schedules (NATS extension) are owned sets with a configuration version.
  `SyncSchedules(ctx, owner, expected, ...)` writes only on the expected version
  and returns the new one; a stale sync returns `ErrVersionConflict` and changes
  nothing. Firing does not change the version, and adding a schedule preserves
  the cursors of unchanged ones. Every occurrence is claimed with an immutable
  prepared job, so a crash is repaired without using a changed definition.
- `CatchUp` controls whether occurrences missed while no scheduler ran are
  skipped or replayed up to `MaxCatchUp`.

### NATS driver

- KeyValue is authoritative; the wake stream only shortens claim latency.
- `Provision` creates resources and disables direct reads on the ledger stream;
  `Open` validates storage, replicas, wake subjects and leader-consistent reads
  without rewriting them.
- Safe collection: a terminal job and its intent, unique lock and schedule claim
  are removed in dependency order; while a claim exists the job is retained, so
  a collected job is never recreated.
- `Recover`/`Maintain` finish recorded decisions, take over expired claims,
  complete lost submissions and run retention.

### Testing and documentation

- `drivertest` provides a reusable conformance suite; `natsq` runs it against a
  real server.
- Process-kill tests SIGKILL a worker after a durable admission, a durable
  decision and a durable snooze, and verify recovery in another process.
- README, design and validation docs describe the final interfaces, behaviour,
  limits and the tests behind each guarantee.

### Migration

- Replace `jetq.New`/`Attach` usage with `natsq.Provision` (deployment) and
  `natsq.Open` (application).
- Replace `EnqueueNamed` with `SubmitTask`/`Submit`; replace `HandleNamed` with
  `Handle`/`HandleTask`; replace `OnFailure` option style with the same names on
  the worker.
- Replace `Queue{MaxAttempts, Backoff, Timeout}` worker settings with task or
  submission policies.
- Replace `SyncSchedules(ctx, schedules...)` with
  `SyncSchedules(ctx, owner, expectedVersion, schedules...)` and track the
  returned version.
- Replace `Cron`/`Schedule.In` with `natsq.Schedule{Key, Cron, Timezone, Task,
  Payload, Queue, Policy}` registered through the driver.
- `Cancel` now applies only before the first admission.
