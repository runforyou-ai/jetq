# Changelog

## Unreleased

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
  resolve such an outcome, releases its lock when the publish was certainly
  not stored, and enqueueing it again with `JobID(id)` takes over its own lock
  instead of returning `ErrDuplicate`. A context that is already done fails
  `Enqueue` before anything is written.
- `Enqueue` returns `ErrJobIDInUse` instead of replacing a pending delayed job
  with the same `JobID`; retries and snoozes never replace one either.
- `DeadLetters` without a queue filter reads a page in one batch instead of
  one request per entry.
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
