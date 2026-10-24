package natsq

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

var nameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Resources are the deployment resources of one namespace, without contacting
// NATS. Provision them explicitly at deployment time.
type Resources struct {
	Ledger jetstream.KeyValueConfig
	Wake   jetstream.StreamConfig
}

// ResourceConfigs returns the resources for the given namespace.
func ResourceConfigs(cfg Config) (Resources, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return Resources{}, err
	}
	return Resources{
		Ledger: jetstream.KeyValueConfig{
			Bucket:      cfg.Namespace + "_LEDGER",
			Description: "jetq job ledger",
			Storage:     storage(cfg),
			Replicas:    cfg.Replicas,
		},
		Wake: jetstream.StreamConfig{
			Name:              cfg.Namespace + "_WAKE",
			Description:       "jetq wake-ups",
			Subjects:          []string{cfg.Namespace + ".wake"},
			Retention:         jetstream.LimitsPolicy,
			Storage:           storage(cfg),
			Replicas:          cfg.Replicas,
			MaxAge:            24 * time.Hour,
			Duplicates:        cfg.WakeWindow,
			Discard:           jetstream.DiscardOld,
			MaxMsgsPerSubject: 1 << 20,
		},
	}, nil
}

func storage(cfg Config) jetstream.StorageType {
	if cfg.Memory {
		return jetstream.MemoryStorage
	}
	return jetstream.FileStorage
}

// Provision creates the namespace's resources or updates them to the
// configured retention and replication. It is a deployment step: application
// processes use [Open] instead.
func Provision(ctx context.Context, js jetstream.JetStream, cfg Config) (*Resources, error) {
	res, err := ResourceConfigs(cfg)
	if err != nil {
		return nil, err
	}
	if _, err := js.CreateOrUpdateKeyValue(ctx, res.Ledger); err != nil {
		return nil, fmt.Errorf("natsq: provision ledger: %w", err)
	}
	// The ledger must be read from the stream leader. NATS key-value buckets
	// are created with AllowDirect on their backing stream, which makes Get
	// use a follower's possibly stale direct read; a claim or deletion that
	// is only visible on the follower would be misread. Provision turns it
	// off and Open refuses a ledger that still has it on. The KV config
	// itself cannot express this, so it is applied to the backing stream.
	if err := disableDirect(ctx, js, res.Ledger.Bucket); err != nil {
		return nil, err
	}
	if _, err := js.CreateOrUpdateStream(ctx, res.Wake); err != nil {
		return nil, fmt.Errorf("natsq: provision wake stream: %w", err)
	}
	return &res, nil
}

// kvStreamName is the backing stream of a key-value bucket.
func kvStreamName(bucket string) string { return "KV_" + bucket }

// disableDirect turns off direct gets on the bucket's backing stream, so every
// read goes to the leader (or follows a direct read to it).
func disableDirect(ctx context.Context, js jetstream.JetStream, bucket string) error {
	stream, err := js.Stream(ctx, kvStreamName(bucket))
	if err != nil {
		return fmt.Errorf("natsq: ledger stream %s: %w", kvStreamName(bucket), err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("natsq: ledger stream info: %w", err)
	}
	if !info.Config.AllowDirect {
		return nil
	}
	config := info.Config
	config.AllowDirect = false
	if _, err := js.UpdateStream(ctx, config); err != nil {
		return fmt.Errorf("natsq: disable direct reads on %s: %w", kvStreamName(bucket), err)
	}
	return nil
}

// Open attaches to provisioned resources without changing their deployment
// configuration. It fails when the resources are missing or incompatible, so
// a namespace is never silently half-created. The JetStream context and its
// connection belong to the application; Close does not close them.
func Open(ctx context.Context, js jetstream.JetStream, cfg Config) (*Driver, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	res, err := ResourceConfigs(cfg)
	if err != nil {
		return nil, err
	}
	stream, err := js.Stream(ctx, kvStreamName(res.Ledger.Bucket))
	if err != nil {
		return nil, fmt.Errorf("natsq: open ledger %s (run Provision first): %w", res.Ledger.Bucket, err)
	}
	ledgerInfo, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("natsq: ledger stream info: %w", err)
	}
	if err := checkLedgerDeployment(ledgerInfo.Config, res.Ledger); err != nil {
		return nil, err
	}
	if ledgerInfo.Config.AllowDirect {
		return nil, fmt.Errorf("natsq: ledger %s allows direct (possibly stale follower) reads; provision it with AllowDirect off and leader-consistent reads", res.Ledger.Bucket)
	}
	kv, err := js.KeyValue(ctx, res.Ledger.Bucket)
	if err != nil {
		return nil, fmt.Errorf("natsq: open ledger %s (run Provision first): %w", res.Ledger.Bucket, err)
	}
	status, err := kv.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("natsq: ledger status: %w", err)
	}
	if status.History() != 1 {
		return nil, fmt.Errorf("natsq: ledger %s keeps %d revisions per key, want 1", res.Ledger.Bucket, status.History())
	}
	if status.TTL() != 0 {
		return nil, fmt.Errorf("natsq: ledger %s expires keys after %s; records must persist until cleanup", res.Ledger.Bucket, status.TTL())
	}
	wakeStream, err := js.Stream(ctx, res.Wake.Name)
	if err != nil {
		return nil, fmt.Errorf("natsq: open wake stream %s (run Provision first): %w", res.Wake.Name, err)
	}
	info, err := wakeStream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("natsq: wake stream info: %w", err)
	}
	got := info.Config
	want := res.Wake
	if !slices.Equal(got.Subjects, want.Subjects) || got.Retention != jetstream.LimitsPolicy ||
		got.Sealed || got.DenyPurge || got.NoAck || got.Mirror != nil || len(got.Sources) > 0 ||
		got.Duplicates < want.Duplicates || got.MaxAge != 0 && got.MaxAge < want.MaxAge {
		return nil, fmt.Errorf("natsq: incompatible wake stream %s", got.Name)
	}
	if got.Storage != want.Storage {
		return nil, fmt.Errorf("natsq: wake stream storage is %s, configured %s", got.Storage, want.Storage)
	}
	if got.Replicas != want.Replicas {
		return nil, fmt.Errorf("natsq: wake stream replicas are %d, configured %d", got.Replicas, want.Replicas)
	}
	return &Driver{cfg: cfg, js: js, ledger: &ledger{kv: kv}, wake: wakeStream}, nil
}

// checkLedgerDeployment rejects a ledger whose shared deployment parameters
// differ from the configuration. Open never changes them: storage, replicas,
// history and TTL are deployment choices, and a mismatch means two processes
// disagree about the same resources.
func checkLedgerDeployment(got jetstream.StreamConfig, want jetstream.KeyValueConfig) error {
	if got.Storage != want.Storage {
		return fmt.Errorf("natsq: ledger storage is %s, configured %s", got.Storage, want.Storage)
	}
	if got.Replicas != want.Replicas {
		return fmt.Errorf("natsq: ledger replicas are %d, configured %d", got.Replicas, want.Replicas)
	}
	return nil
}

// Close releases local handles. Shared resources and the application's
// connection stay open.
func (d *Driver) Close() error { return nil }

// Namespace returns the namespace the driver operates in.
func (d *Driver) Namespace() string { return d.cfg.Namespace }

// wake publishes a wake-up so a consumer of the queue learns the job is
// eligible. Failure is tolerated: the recovery scan wakes the job later, and
// Receive only trusts what the ledger says.
func (d *Driver) wakeJob(ctx context.Context, env *jetq.Envelope) error {
	msg := &nats.Msg{
		Subject: d.cfg.Namespace + ".wake",
		Header:  nats.Header{},
		Data:    []byte(env.Queue + "\x00" + env.JobID),
	}
	opts := []jetstream.PublishOpt{jetstream.WithExpectStream(d.wake.CachedInfo().Config.Name)}
	if env.EnqueueKey != "" {
		// A retried submission reuses the wake id, so it does not create a
		// second message for the same prepared job.
		opts = append(opts, jetstream.WithMsgID("enq."+env.JobID))
	}
	_, err := d.js.PublishMsg(ctx, msg, opts...)
	return err
}

// Submit stores a prepared job whose outcome is decided by one CAS on the
// canonical record; see [Driver.submit].
func (d *Driver) Submit(ctx context.Context, job *jetq.PreparedJob) (jetq.EnqueueResult, error) {
	if job == nil {
		return jetq.EnqueueResult{}, fmt.Errorf("natsq: nil job")
	}
	env := job.Envelope.Clone()
	if err := env.Validate(); err != nil {
		return jetq.EnqueueResult{}, err
	}
	if job.Fingerprint == "" || env.Fingerprint() != job.Fingerprint {
		return jetq.EnqueueResult{}, fmt.Errorf("natsq: fingerprint of job %s does not match its envelope", env.JobID)
	}
	return d.submit(ctx, env, job.Fingerprint)
}

func uncertain(jobID string, err error) (jetq.EnqueueResult, error) {
	return jetq.EnqueueResult{Status: jetq.Unknown, JobID: jobID}, fmt.Errorf("natsq: submit %s: %w: %w", jobID, jetq.ErrUncertain, err)
}

func alreadyAccepted(env *jetq.Envelope) jetq.EnqueueResult {
	return jetq.EnqueueResult{Status: jetq.AlreadyAccepted, JobID: env.JobID, RunAt: env.RunAt}
}

// submit runs the submission protocol: intent, then one create of the
// canonical record that commits the outcome; see keys.go.
func (d *Driver) submit(ctx context.Context, env *jetq.Envelope, fingerprint string) (jetq.EnqueueResult, error) {
	// 1. The intent decides the job id and the frozen envelope for every
	// retry of this submission.
	intentStored := false
	if env.EnqueueKey != "" {
		in, err := d.claimIntent(ctx, env, fingerprint)
		if err != nil {
			if errors.Is(err, jetq.ErrEnqueueKeyConflict) {
				return jetq.EnqueueResult{}, err
			}
			return uncertain(env.JobID, err)
		}
		env = in.Envelope.Clone()
		intentStored = in.Stored
		if in.Suppressed {
			// A Suppressed submission of this key is final for its retention:
			// the unique rule is not evaluated again.
			return jetq.EnqueueResult{Status: jetq.Suppressed, ExistingJobID: in.SuppressedBy}, nil
		}
	}

	for range 10 {
		ent, ok, err := d.ledger.get(ctx, jobKey(env.JobID))
		if err != nil {
			return uncertain(env.JobID, err)
		}
		if ok {
			if ent.rec.Fingerprint != fingerprint {
				// The existing job is a definite conflict, not an uncertain
				// outcome: nothing was stored and the original is untouched.
				return jetq.EnqueueResult{}, fmt.Errorf("natsq: submit %s: %w", env.JobID, jetq.ErrJobIDInUse)
			}
			if ent.rec.Outcome == outcomePending {
				// A competitor died before deciding: finish its submission
				// through the same commit path.
				res, done, err := d.decidePending(ctx, env, fingerprint)
				if err != nil || done {
					return res, err
				}
				continue
			}
			return d.reportOutcome(ent), nil
		}
		if intentStored {
			// The intent's job was accepted and has since been cleaned up. It
			// is never created again.
			return alreadyAccepted(env), nil
		}

		// 2. The unique rule decides the outcome before the record exists.
		desired, holder, err := d.decideOutcome(ctx, env, fingerprint)
		if err != nil {
			return uncertain(env.JobID, err)
		}
		rec := &record{
			Env: env.Clone(), Fingerprint: fingerprint, RunAt: env.RunAt,
			State: jetq.StateAwaiting, Wait: jetq.WaitScheduled,
			EnqKey: env.EnqueueKey, Outcome: desired, ExistingJobID: holder,
		}
		if desired == outcomeSuppressed {
			// Final result: no job runs; the record is kept for the retention
			// so a retried submission keeps reporting Suppressed.
			rec.State = ""
			rec.RunAt = time.Time{}
			rec.TerminalAt = d.now()
		}
		// The create is the single commit point of the submission: a producer
		// that gets no answer retries and reads the committed outcome.
		if _, err := d.ledger.put(ctx, jobKey(env.JobID), 0, rec); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue // a concurrent attempt created it; read its outcome
			}
			return uncertain(env.JobID, err)
		}
		if desired == outcomeSuppressed {
			// The intent keeps the final outcome, so a retry with another
			// candidate id still reports Suppressed instead of re-evaluating
			// the unique rule.
			if err := d.markIntentSuppressed(ctx, env, holder); err != nil {
				return uncertain(env.JobID, err)
			}
			return jetq.EnqueueResult{Status: jetq.Suppressed, ExistingJobID: holder}, nil
		}
		d.repairMarkers(ctx, env)
		if err := d.wakeJob(ctx, env); err != nil {
			d.cfg.Logger.WarnContext(ctx, "natsq wake publish failed", "job", env.JobID, "error", err)
		}
		return jetq.EnqueueResult{Status: jetq.Accepted, JobID: env.JobID, RunAt: env.RunAt}, nil
	}
	return uncertain(env.JobID, errRevisionMismatch)
}

// decidePending commits the outcome of a record a competitor left pending. It
// decides against the unique reservation exactly as the original producer
// would, so a pending submission of a suppressed candidate becomes
// suppressed, never accepted.
func (d *Driver) decidePending(ctx context.Context, env *jetq.Envelope, fingerprint string) (jetq.EnqueueResult, bool, error) {
	desired, holder, err := d.decideOutcome(ctx, env, fingerprint)
	if err != nil {
		return jetq.EnqueueResult{}, true, err
	}
	cur, ok, err := d.ledger.get(ctx, jobKey(env.JobID))
	if err != nil {
		return jetq.EnqueueResult{}, true, err
	}
	if !ok {
		return jetq.EnqueueResult{}, false, nil
	}
	if cur.rec.Outcome != outcomePending {
		return d.reportOutcome(cur), true, nil
	}
	if desired == outcomeSuppressed {
		cur.rec.Outcome = outcomeSuppressed
		cur.rec.ExistingJobID = holder
		cur.rec.State = ""
		cur.rec.RunAt = time.Time{}
		cur.rec.TerminalAt = d.now()
		if _, err := d.ledger.put(ctx, jobKey(env.JobID), cur.rev, cur.rec); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				return jetq.EnqueueResult{}, false, nil
			}
			return jetq.EnqueueResult{}, true, err
		}
		if err := d.markIntentSuppressed(ctx, env, holder); err != nil {
			return jetq.EnqueueResult{}, true, err
		}
		return jetq.EnqueueResult{Status: jetq.Suppressed, ExistingJobID: holder}, true, nil
	}
	cur.rec.Outcome = outcomeAccepted
	cur.rec.State = jetq.StateAwaiting
	cur.rec.Wait = jetq.WaitScheduled
	cur.rec.RunAt = env.RunAt
	if _, err := d.ledger.put(ctx, jobKey(env.JobID), cur.rev, cur.rec); err != nil {
		if errors.Is(err, errRevisionMismatch) {
			return jetq.EnqueueResult{}, false, nil
		}
		return jetq.EnqueueResult{}, true, err
	}
	d.repairMarkers(ctx, env)
	if err := d.wakeJob(ctx, env); err != nil {
		d.cfg.Logger.WarnContext(ctx, "natsq wake publish failed", "job", env.JobID, "error", err)
	}
	return jetq.EnqueueResult{Status: jetq.Accepted, JobID: env.JobID, RunAt: env.RunAt}, true, nil
}

// reportOutcome turns a decided job record into the submission result.
func (d *Driver) reportOutcome(ent *ledgerEntry) jetq.EnqueueResult {
	switch ent.rec.Outcome {
	case outcomeSuppressed:
		return jetq.EnqueueResult{Status: jetq.Suppressed, ExistingJobID: ent.rec.ExistingJobID}
	case outcomeRejected:
		return jetq.EnqueueResult{Status: jetq.Unknown, JobID: ent.rec.Env.JobID}
	}
	return alreadyAccepted(ent.rec.Env)
}

// decideOutcome evaluates the unique rule for a submission and returns the
// outcome its canonical record must commit. A free reservation is taken over
// for this submission; a live one suppresses it.
func (d *Driver) decideOutcome(ctx context.Context, env *jetq.Envelope, fingerprint string) (string, string, error) {
	if env.Unique == nil {
		return outcomeAccepted, "", nil
	}
	suppressed, holder, err := d.takeUnique(ctx, env, fingerprint)
	if err != nil {
		return "", "", err
	}
	if suppressed {
		return outcomeSuppressed, holder, nil
	}
	return outcomeAccepted, "", nil
}

// repairMarkers records on the intent and reservation that the job was
// accepted. Failures change no outcome.
func (d *Driver) repairMarkers(ctx context.Context, env *jetq.Envelope) {
	if err := d.markIntentStored(ctx, env); err != nil {
		d.cfg.Logger.WarnContext(ctx, "natsq intent not marked stored", "job", env.JobID, "error", err)
	}
	if err := d.markLockStored(ctx, env); err != nil {
		d.cfg.Logger.WarnContext(ctx, "natsq unique lock not marked stored", "job", env.JobID, "error", err)
	}
}
