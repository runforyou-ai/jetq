# Changelog

## v0.4.0

- Add `EnqueueNamed` and `HandleNamed` for explicit, stable job names and ordinary JSON payload types. `OnFailure` accepts ordinary types too.
- Configure recurring jobs with exported `Schedule` fields: `Key`, `Cron`, `Timezone`, `Name`, `Payload`, `Queue`, `MaxAttempts`, `Timeout` and `Header`. One-off enqueue options cannot be passed to schedules.
- Document the package independently, with named payloads as the primary examples.

### Migration

`Cron` and `Schedule.In` have been removed; callers must migrate to compile. Replace `Cron(key, spec, job, ...).In(zone)` with `Schedule{Key: key, Cron: spec, Name: name, Payload: job, Timezone: zone}` and set queue, attempt, timeout and header fields directly. `Enqueue` and `Handle` remain available for types implementing `JobName`. Existing stored jobs and schedules use the same wire format.

For a schedule, replace `WithHeader("traceparent", value)` with `Header: nats.Header{"Traceparent": []string{value}}`, `OnQueue(queue)` with `Queue: queue`, and `MaxAttempts(n)` with `MaxAttempts: n`. Set `Timeout` to a duration pointer to override the worker timeout (zero disables it); nil inherits the worker setting. Negative schedule attempts and timeouts are rejected.

## v0.3.0

- Jobs waiting for a retry of a second or more are put back as delayed jobs
  instead of being nak'ed with a delay, so they no longer count against the consumer's
  `MaxAckPending`: a backlog of failing jobs no longer stalls a queue. They
  count as `Stats.Delayed` instead of `InFlight` and can be cancelled.
- Jobs interrupted by the shutdown timeout are put back without using up an
  attempt.
- Jobs without a registered handler are put back for other workers without
  using up an attempt and dead-lettered after `Worker.SetUnknownJobTimeout`
  (default 1h).
- `Queue.Timeout` and the `Timeout` enqueue option bound an attempt
  (`ErrTimeout`); handlers that ignore cancellation are abandoned after 10
  seconds, so `Worker.Run` returns even then.
- `Info.Timeout` and `Info.Snoozes`.
- `Enqueue` errors wrap `ErrUncertain` when the job may have been stored, and
  the job id is returned with them; other errors mean it was not stored. A
  `UniqueUntilDone` job is published a second time with the same message id to
  resolve such an outcome and releases its lock when the publish was certainly
  not stored. A context that is already done fails `Enqueue` before anything
  is published. `Unique` keys starting with `jetq-` are reserved.
- `Enqueue` returns `ErrJobIDInUse` instead of replacing a pending delayed job
  with the same `JobID`; retries and snoozes never replace one either and are
  redelivered after their delay instead, which counts as an attempt.
- `DeadLetters` without a queue filter reads a page through one ordered
  consumer instead of one request per entry; reads fail when no entry arrives
  within 5 seconds instead of waiting.
- Dead-letter copies are deduplicated per delivered message; `OnFailure`
  logs payloads that do not decode; keep-alives are sent at least every 5
  seconds; workers recreate their consumer after fetch failures.

## v0.2.0

- `UniqueUntilDone` deduplicates a job by key until it settles, with locks in a
  key-value bucket (`WithUniqueLockTTL`).
- `Cancel` records the cancellation in the `<STREAM>_STATE` key-value bucket,
  so workers skip a copy the schedule published while it ran; when it returns
  nil the job does not run. The bucket is
  created on first use; clients need permission to create it and to use
  `$KV.<STREAM>_STATE.>`.

## v0.1.1

- `Worker.SetLogContext` derives the context of jetq's job log records and failure callbacks.

## v0.1.0

- `RawJob` and `Worker.HandleRaw` for job names known only at runtime; raw
  payloads are delivered byte for byte.
- `JobID` enqueue option to choose the job id.
- `Client.Stats` and `Client.DeadLetters` for monitoring.

- Initial release: typed jobs, queues with concurrency, retries with backoff,
  `Permanent`, `RetryAfter`, `Snooze`, dead-letter stream with failure
  callbacks, `Unique`, delayed jobs with `Cancel`, server-side cron schedules
  with time zones, middleware, graceful shutdown and the `jetqtest` helper.
