package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

// record is one job's ledger record. Every change goes through a
// compare-and-set on the key's revision, so the record is the authority for
// claims, admissions and decisions.
type record struct {
	// Env is set on job records. Auxiliary records (enqueue keys, unique
	// locks) share the ledger and its revision checks but are not envelopes;
	// they keep their payload in Data.
	Env         *jetq.Envelope  `json:"env,omitempty"`
	Data        json.RawMessage `json:"data,omitempty"`
	Fingerprint string          `json:"fingerprint"`
	// Outcome is the submission outcome of a job record: pending until one
	// compare-and-set decides it as accepted or suppressed. Auxiliary records
	// leave it empty. Only an accepted record can be delivered or settled.
	Outcome string `json:"outcome,omitempty"`
	// ExistingJobID is the job that suppressed this submission.
	ExistingJobID string          `json:"existing_job_id,omitempty"`
	RunAt         time.Time       `json:"run_at"` // zero: eligible now
	State         jetq.JobState   `json:"state"`
	Wait          jetq.WaitReason `json:"wait,omitempty"`
	Budget        int             `json:"budget"` // admitted executions minus refunds
	Snoozes       int             `json:"snoozes"`
	Deliveries    int             `json:"deliveries"` // physical deliveries, for reporting
	Admitted      int             `json:"admitted"`   // admissions without a recorded decision
	ExecutionID   string          `json:"execution_id,omitempty"`
	ClaimID       string          `json:"claim,omitempty"`
	ClaimUntil    time.Time       `json:"claim_until,omitzero"`
	LeaseApplied  int64           `json:"lease_applied,omitempty"` // claim generations applied to budget
	Pending       *jetq.Decision  `json:"pending,omitempty"`
	// Decided holds the decisions that are still being applied; Pending
	// points into it, so applying does not overwrite the record's own copy.
	Decided      []jetq.Decision  `json:"decided,omitempty"`
	Applied      []string         `json:"applied,omitempty"` // transition ids whose budget was applied
	Canceled     bool             `json:"canceled,omitempty"`
	UnknownSince time.Time        `json:"unknown_since,omitzero"`
	Unique       *jetq.UniqueRule `json:"unique,omitempty"`
	UniqKey      string           `json:"uniq_key,omitempty"`
	UniqRev      uint64           `json:"uniq_rev,omitempty"` // revision of the lock taken at submission
	EnqKey       string           `json:"enq_key,omitempty"`
	TerminalAt   time.Time        `json:"terminal_at,omitzero"`
	// LastError is the failure text of the latest failed execution.
	LastError string    `json:"last_error,omitempty"`
	UpdatedAt time.Time `json:"updated_at"` // read with the record; the TTL basis
}

func cloneRecord(r *record) *record {
	c := *r
	if r.Env != nil {
		c.Env = r.Env.Clone()
	}
	c.Data = append(json.RawMessage(nil), r.Data...)
	if r.Pending != nil {
		p := *r.Pending
		c.Pending = &p
	}
	c.Applied = append([]string(nil), r.Applied...)
	c.Decided = append([]jetq.Decision(nil), r.Decided...)
	return &c
}

// errRevisionMismatch: the key changed since it was read; re-read and retry.
var errRevisionMismatch = errors.New("natsq: ledger changed concurrently")

// ledgerEntry is a record with its key revision.
type ledgerEntry struct {
	rec *record
	rev uint64
}

// ledger is the key-value store of records. Records carry no TTL: claim
// expiry is a field the recovery scan acts on, so no record recovery still
// needs is deleted by time alone.
type ledger struct {
	kv    jetstream.KeyValue
	hooks *hooks
}

// get reads a key. found is false for an absent or deleted key.
func (l *ledger) get(ctx context.Context, key string) (*ledgerEntry, bool, error) {
	kv, err := l.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("natsq: read %s: %w", key, err)
	}
	var rec record
	if err := json.Unmarshal(kv.Value(), &rec); err != nil {
		return nil, false, fmt.Errorf("natsq: decode %s: %w", key, err)
	}
	return &ledgerEntry{rec: &rec, rev: kv.Revision()}, true, nil
}

// put writes rec at rev; rev 0 creates the key and fails if it exists.
func (l *ledger) put(ctx context.Context, key string, rev uint64, rec *record) (uint64, error) {
	if l.hooks != nil && l.hooks.beforeWrite != nil {
		l.hooks.beforeWrite(key)
	}
	rec.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	var newRev uint64
	if rev == 0 {
		newRev, err = l.kv.Create(ctx, key, data)
		if errors.Is(err, jetstream.ErrKeyExists) {
			return 0, errRevisionMismatch
		}
	} else {
		newRev, err = l.kv.Update(ctx, key, data, rev)
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, errRevisionMismatch
		}
	}
	if err != nil {
		return 0, fmt.Errorf("natsq: write %s: %w", key, err)
	}
	return newRev, nil
}

// isJob reports whether rec is a canonical job record.

// live reports whether a job record was accepted and may run.
func (r *record) live() bool { return r.Outcome == outcomeAccepted || r.Outcome == "" && r.State != "" }

// delete removes key if it is still at rev.
func (l *ledger) delete(ctx context.Context, key string, rev uint64) error {
	err := l.kv.Delete(ctx, key, jetstream.LastRevision(rev))
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyNotFound) {
		return errRevisionMismatch
	}
	return err
}

// jobKey is the ledger key of a job; id is validated.
func jobKey(id string) string { return "job." + id }

// Config configures a driver.
type Config struct {
	// Namespace prefixes the ledger bucket and the wake subject. Use a
	// distinct namespace per application in one NATS account.
	Namespace string
	// ClaimLease keeps a job with a delivery between Admit and Settle
	// (default 30s). Extend renews it.
	ClaimLease time.Duration
	// ScanInterval bounds how long a job whose wake message was lost or
	// whose claim expired stays invisible (default 30s).
	ScanInterval time.Duration
	// WakeWindow is the window in which duplicate wake messages are dropped
	// (default 2m). It must exceed the longest wait between enqueueing a wake
	// and another wake for the same job with the same run time.
	WakeWindow time.Duration
	// Replicas of the ledger and wake stream (default 1).
	Replicas int
	// Memory keeps the ledger and wake messages in memory; for tests.
	Memory bool
	// JobRetention keeps terminal records after which they are cleaned up
	// (default 24h; negative keeps them forever). EnqueueKey and UniqueWindow
	// retention follow it.
	JobRetention time.Duration
	// UniqueUntilDoneMax caps how long an until-done unique key is held from
	// its first job (default 24h).
	UniqueUntilDoneMax time.Duration
	// Logger receives recovery and cleanup problems (default slog.Default()).
	Logger *slog.Logger
}

func (c *Config) withDefaults() (Config, error) {
	if c.Namespace == "" || !nameRE.MatchString(c.Namespace) {
		return Config{}, fmt.Errorf("natsq: invalid namespace %q: use letters, digits, '-' and '_'", c.Namespace)
	}
	if c.ClaimLease <= 0 {
		c.ClaimLease = 30 * time.Second
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = 30 * time.Second
	}
	if c.WakeWindow <= 0 {
		c.WakeWindow = 2 * time.Minute
	}
	if c.Replicas <= 0 {
		c.Replicas = 1
	}
	if c.JobRetention == 0 {
		c.JobRetention = 24 * time.Hour
	}
	if c.UniqueUntilDoneMax <= 0 {
		c.UniqueUntilDoneMax = 24 * time.Hour
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return *c, nil
}

// Driver implements [jetq.Driver] on NATS JetStream.
type Driver struct {
	cfg    Config
	js     jetstream.JetStream
	ledger *ledger
	wake   jetstream.Stream
	hks    hooks
}

// hooks are fault-injection points for tests; a nil field does nothing.
type hooks struct {
	// afterDecisionWrite runs after a decision was durably written and
	// before the writer learns of it: an error makes Settle report unknown.
	afterDecisionWrite func(jetq.Decision) error
	// afterCleanup runs after a decision's follow-up work is recorded and
	// before the delivery is acknowledged: an error makes Settle report
	// recorded_cleanup_pending with the decision durable.
	afterCleanup func(jetq.Decision) error
	// afterAdmit runs after an admission was durably written and before the
	// caller learns of it; the admission stays spent.
	afterAdmit func(executionID string)
	// beforeWrite runs before each ledger write of key.
	beforeWrite func(key string)
	// kv wraps the ledger's key-value store, to inject transport failures.
	kv func(jetstream.KeyValue) jetstream.KeyValue
	// now overrides the clock.
	now func() time.Time
}

func (d *Driver) now() time.Time {
	if d.hks.now != nil {
		return d.hks.now().UTC()
	}
	return time.Now().UTC()
}
