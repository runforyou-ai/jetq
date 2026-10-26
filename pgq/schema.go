package pgq

// migrations are applied in order, each once, under an advisory lock. The
// placeholder %s is the quoted schema name.
var migrations = []string{
	// 1: tables, indexes and the notification trigger.
	`
CREATE TABLE IF NOT EXISTS %[1]s.jobs (
  id            text PRIMARY KEY,
  queue         text NOT NULL,
  name          text NOT NULL,
  payload       bytea NOT NULL,
  header        jsonb,
  state         text NOT NULL,
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
CREATE INDEX IF NOT EXISTS jobs_pending ON %[1]s.jobs (queue, run_at, id) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS jobs_running ON %[1]s.jobs (queue, locked_until) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS jobs_failed ON %[1]s.jobs (finished_at DESC, id) WHERE state = 'failed';
CREATE INDEX IF NOT EXISTS jobs_finished ON %[1]s.jobs (finished_at) WHERE state IN ('succeeded', 'canceled');

CREATE TABLE IF NOT EXISTS %[1]s.unique_keys (
  key    text PRIMARY KEY,
  job_id text NOT NULL,
  until  timestamptz
);

CREATE TABLE IF NOT EXISTS %[1]s.schedules (
  key         text PRIMARY KEY,
  cron        text NOT NULL,
  timezone    text NOT NULL DEFAULT '',
  spec        jsonb NOT NULL,
  next_run_at timestamptz NOT NULL
);

CREATE OR REPLACE FUNCTION %[1]s.notify_job() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('jobq', NEW.queue);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_notify ON %[1]s.jobs;
CREATE TRIGGER jobs_notify AFTER INSERT OR UPDATE OF state, run_at ON %[1]s.jobs
  FOR EACH ROW WHEN (NEW.state = 'pending') EXECUTE FUNCTION %[1]s.notify_job();
`,
}
