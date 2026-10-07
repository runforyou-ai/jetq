# Changelog

## Unreleased

- `RawJob` and `Worker.HandleRaw` for job names known only at runtime.
- `Client.Stats` and `Client.DeadLetters` for monitoring.

- Initial release: typed jobs, queues with concurrency, retries with backoff,
  `Permanent`, `RetryAfter`, `Snooze`, dead-letter stream with failure
  callbacks, `Unique`, delayed jobs with `Cancel`, server-side cron schedules
  with time zones, middleware, graceful shutdown and the `jetqtest` helper.
