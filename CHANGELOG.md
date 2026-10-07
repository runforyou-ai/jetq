# Changelog

## Unreleased

- `UniqueUntilDone` deduplicates a job by key until it settles, with locks in a
  key-value bucket (`WithUniqueLockTTL`).
- `Cancel` records the cancellation in the `<STREAM>_STATE` key-value bucket,
  so workers skip a copy the schedule published while it ran. The bucket is
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
