// Package natsq implements a jobq driver on NATS JetStream.
//
// Jobs are messages in a work-queue stream, one durable pull consumer per
// queue. Delayed jobs and recurring schedules use JetStream message
// scheduling, so the server fires them; nothing polls in the application.
//
// Fencing needs a record per job: JetStream acks and naks are not tied to a
// delivery, so a worker whose lease expired could otherwise settle a job that
// another worker claimed. Every claim and settlement therefore goes through a
// key-value record that holds the attempt and the lease, updated with
// compare-and-set. Job state also carries cancellation, so cancelling a
// running job makes its settlement fail with [jobq.ErrLeaseLost].
//
// Lease expiry is judged by the clocks of the processes involved, so keep
// them synchronised to within a few seconds.
package natsq

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jobq"
)

// Option configures a [Driver].
type Option func(*config)

type config struct {
	prefix        string
	stream        string
	stateBucket   string
	deadBucket    string
	stateTTL      time.Duration
	deadMaxAge    time.Duration
	uniqueLockTTL time.Duration
	replicas      int
	memory        bool
	logger        *slog.Logger
}

// WithPrefix sets the subject prefix (default "jobq"). It must be a single
// subject token; applications sharing an account need distinct prefixes.
func WithPrefix(prefix string) Option { return func(c *config) { c.prefix = prefix } }

// WithStream sets the work stream name (default "JOBQ").
func WithStream(name string) Option { return func(c *config) { c.stream = name } }

// WithReplicas sets the replica count of the streams (default 1).
func WithReplicas(n int) Option { return func(c *config) { c.replicas = n } }

// WithStateBucket sets the key-value bucket for job state (default "JOBQ_STATE").
func WithStateBucket(name string) Option { return func(c *config) { c.stateBucket = name } }

// WithDeadBucket sets the stream that holds dead letters (default "JOBQ_DEAD").
func WithDeadBucket(name string) Option { return func(c *config) { c.deadBucket = name } }

// WithMemoryStorage keeps the streams in memory.
func WithMemoryStorage() Option { return func(c *config) { c.memory = true } }

// WithStateTTL sets how long the state record of a finished job is kept for
// cleaning up, counted from the end of the job (default 24h). Records of
// unfinished jobs are never removed.
func WithStateTTL(d time.Duration) Option { return func(c *config) { c.stateTTL = d } }

// WithDeadLetterMaxAge sets how long dead letters are kept (default 14 days).
func WithDeadLetterMaxAge(d time.Duration) Option { return func(c *config) { c.deadMaxAge = d } }

// WithUniqueLockTTL bounds a [jobq.Unique] lock whose job never settles, for
// example because it was lost (default 7 days).
func WithUniqueLockTTL(d time.Duration) Option { return func(c *config) { c.uniqueLockTTL = d } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// New creates the streams, buckets and janitor and returns the driver.
func New(ctx context.Context, js jetstream.JetStream, opts ...Option) (*Driver, error) {
	cfg := config{
		prefix:        "jobq",
		stream:        "JOBQ",
		stateBucket:   "JOBQ_STATE",
		deadBucket:    "JOBQ_DEAD",
		stateTTL:      24 * time.Hour,
		deadMaxAge:    14 * 24 * time.Hour,
		uniqueLockTTL: 7 * 24 * time.Hour,
		replicas:      1,
		logger:        slog.Default(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	storage := jetstream.FileStorage
	if cfg.memory {
		storage = jetstream.MemoryStorage
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              cfg.stream,
		Description:       "jobq jobs, delayed jobs and schedules",
		Subjects:          []string{cfg.prefix + ".q.>", cfg.prefix + ".at.>", cfg.prefix + ".cron.>"},
		Retention:         jetstream.WorkQueuePolicy,
		Storage:           storage,
		Replicas:          cfg.replicas,
		AllowMsgSchedules: true,
		AllowRollup:       true,
	})
	if err != nil {
		return nil, fmt.Errorf("natsq: create stream %s: %w", cfg.stream, err)
	}
	state, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         cfg.stateBucket,
		Description:    "jobq job state",
		History:        1,
		Storage:        storage,
		Replicas:       cfg.replicas,
		LimitMarkerTTL: time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("natsq: create bucket %s: %w", cfg.stateBucket, err)
	}
	dead, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        cfg.deadBucket,
		Description: "jobq dead letters",
		Subjects:    []string{cfg.prefix + ".dead.>"},
		Storage:     storage,
		Replicas:    cfg.replicas,
		MaxAge:      cfg.deadMaxAge,
	})
	if err != nil {
		return nil, fmt.Errorf("natsq: create bucket %s: %w", cfg.deadBucket, err)
	}
	d := &Driver{
		js:        js,
		stream:    stream,
		state:     state,
		dead:      dead,
		cfg:       cfg,
		consumers: map[string]*queueConsumer{},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go d.janitor()
	return d, nil
}

// Driver implements [jobq.Driver], [jobq.Scheduler] and [jobq.Inspector].
type Driver struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	state  jetstream.KeyValue
	dead   jetstream.Stream
	cfg    config

	mu        sync.Mutex
	consumers map[string]*queueConsumer
	closed    bool
	stop      chan struct{}
	done      chan struct{}
}

// queueConsumer caches the durable consumer of a queue.
type queueConsumer struct {
	mu       sync.Mutex
	consumer jetstream.Consumer
	ackWait  time.Duration
}

// Close stops the janitor and releases local resources.
func (d *Driver) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	close(d.stop)
	<-d.done
	return nil
}

// === job state ===

// jobState is the record fencing one job.
type jobState struct {
	// Attempt counts claims, including claims whose lease expired.
	Attempt int `json:"attempt"`
	// Running is set between claim and settlement.
	Running bool `json:"running"`
	// LeaseUntil is when a running claim expires, and LeaseStart when it began.
	LeaseUntil time.Time `json:"lease_until,omitzero"`
	LeaseStart time.Time `json:"lease_start,omitzero"`
	// Seq is the stream sequence of the message that holds the job.
	Seq uint64 `json:"seq,omitempty"`
	// Gen identifies the current copy of the job. Every publish of the job
	// gets a new generation, written to the record before the publish, so
	// a delivery of an older copy, left behind by a lost ack or a failed
	// publish, is recognised and dropped.
	Gen string `json:"gen"`
	// Queue is the job's queue.
	Queue string `json:"queue"`
	// UniqueKey and UniqueFor mirror the message, to release the lock.
	UniqueKey string `json:"unique_key,omitempty"`
	UniqueFor int64  `json:"unique_for,omitempty"`
	// FinishedAt is when the job ended, for the janitor.
	FinishedAt time.Time `json:"finished_at,omitzero"`
	// Canceled marks a job cancelled while it ran; its next claim drops it.
	Canceled bool `json:"canceled,omitempty"`
	// Next is the generation of a copy published by a requeue whose record
	// switch is not confirmed yet. A claim of that copy completes the handoff.
	Next string `json:"next,omitempty"`
	// LastError is the reason of the last retry, for inspection.
	LastError string `json:"last_error,omitempty"`
}

func stateKey(id string) string { return "job." + id }

func (d *Driver) getState(ctx context.Context, id string) (*jobState, uint64, error) {
	entry, err := d.state.Get(ctx, stateKey(id))
	if err != nil {
		return nil, 0, err
	}
	var s jobState
	if err := json.Unmarshal(entry.Value(), &s); err != nil {
		return nil, 0, fmt.Errorf("natsq: decode state %s: %w", id, err)
	}
	return &s, entry.Revision(), nil
}

// putState writes s, creating the key when rev is zero. It returns the new
// revision and jobq.ErrLeaseLost when the record changed meanwhile.
func (d *Driver) putState(ctx context.Context, id string, rev uint64, s *jobState) (uint64, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return 0, err
	}
	key := stateKey(id)
	if rev == 0 {
		newRev, err := d.state.Create(ctx, key, data)
		if err != nil {
			if isConflict(err) {
				return 0, jobq.ErrLeaseLost
			}
			return 0, fmt.Errorf("natsq: create state %s: %w", id, err)
		}
		return newRev, nil
	}
	newRev, err := d.state.Update(ctx, key, data, rev)
	if err != nil {
		if isConflict(err) || errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, jobq.ErrLeaseLost
		}
		return 0, fmt.Errorf("natsq: update state %s: %w", id, err)
	}
	return newRev, nil
}

func isConflict(err error) bool {
	return errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch)
}

// deleteState removes the record when it is still at rev.
func (d *Driver) deleteState(ctx context.Context, id string, rev uint64) error {
	err := d.state.Delete(ctx, stateKey(id), jetstream.LastRevision(rev))
	if err == nil || errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if isConflict(err) {
		return jobq.ErrLeaseLost
	}
	return fmt.Errorf("natsq: delete state %s: %w", id, err)
}

// fetchWindow bounds one pull request. A pull the caller abandons keeps
// collecting messages on the server until it expires; those wait for their
// AckWait before they are delivered again.
const fetchWindow = time.Second

// publishTimeout bounds one publish. A publish whose answer never arrives is
// reported as jobq.ErrUncertain.
const publishTimeout = 10 * time.Second

// === subjects ===

func (d *Driver) queueSubject(queue string) string { return d.cfg.prefix + ".q." + queue }
func (d *Driver) delaySubject(queue, id string) string {
	return d.cfg.prefix + ".at." + queue + "." + id
}
func (d *Driver) cronSubject(key string) string { return d.cfg.prefix + ".cron." + key }

func (d *Driver) consumerName(queue string) string {
	return d.cfg.prefix + "_" + queue
}

// === envelope ===

// envelope is the stored form of a job. Payload is valid JSON by contract.
type envelope struct {
	// Payload is carried in the message body, before the JSON header line,
	// so it is stored byte for byte. Encode and decode handle it.
	Payload      json.RawMessage   `json:"-"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Queue        string            `json:"queue"`
	Header       map[string]string `json:"header,omitempty"`
	EnqueuedAt   time.Time         `json:"enqueued_at,omitzero"`
	MaxAttempts  int               `json:"max_attempts,omitempty"`
	Timeout      time.Duration     `json:"timeout,omitempty"`
	Snoozes      int               `json:"snoozes,omitempty"`
	UnknownSince time.Time         `json:"unknown_since,omitzero"`
	UniqueKey    string            `json:"unique_key,omitempty"`
	UniqueFor    time.Duration     `json:"unique_for,omitempty"`
	// Schedule is the key of the recurring schedule that fired this job.
	Schedule string `json:"schedule,omitempty"`
	// Gen is the generation of this copy; see jobState.Gen.
	Gen string `json:"gen,omitempty"`
}

func newEnvelope(m *jobq.Message) *envelope {
	return &envelope{
		ID:           m.ID,
		Name:         m.Name,
		Queue:        m.Queue,
		Payload:      json.RawMessage(m.Payload),
		Header:       maps.Clone(m.Header),
		EnqueuedAt:   m.EnqueuedAt,
		MaxAttempts:  m.MaxAttempts,
		Timeout:      m.Timeout,
		Snoozes:      m.Snoozes,
		UnknownSince: m.UnknownSince,
		UniqueKey:    m.UniqueKey,
		UniqueFor:    m.UniqueFor,
	}
}

// encode serializes the job: an 8-byte big-endian header length, the JSON
// envelope, then the payload unchanged.
func (e *envelope) encode() ([]byte, error) {
	head, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 8, 8+len(head)+len(e.Payload))
	binary.BigEndian.PutUint64(out, uint64(len(head)))
	out = append(out, head...)
	return append(out, e.Payload...), nil
}

// decode parses a message written by encode.
func (e *envelope) decode(data []byte) error {
	if len(data) < 8 {
		return errors.New("message is too short")
	}
	n := binary.BigEndian.Uint64(data[:8])
	if n > uint64(len(data)-8) {
		return errors.New("message header length out of range")
	}
	if err := json.Unmarshal(data[8:8+n], e); err != nil {
		return err
	}
	e.Payload = append([]byte(nil), data[8+n:]...)
	return nil
}

func (e *envelope) message() *jobq.Message {
	return &jobq.Message{
		ID:           e.ID,
		Name:         e.Name,
		Queue:        e.Queue,
		Payload:      append([]byte(nil), e.Payload...),
		Header:       maps.Clone(e.Header),
		EnqueuedAt:   e.EnqueuedAt,
		MaxAttempts:  e.MaxAttempts,
		Timeout:      e.Timeout,
		Snoozes:      e.Snoozes,
		UnknownSince: e.UnknownSince,
		UniqueKey:    e.UniqueKey,
		UniqueFor:    e.UniqueFor,
	}
}

// === enqueue ===

// Enqueue implements [jobq.Driver].
func (d *Driver) Enqueue(ctx context.Context, msg *jobq.Message) error {
	// Claim the job id: an existing record means the job is unfinished.
	var rev uint64
	if s, r, err := d.getState(ctx, msg.ID); err == nil {
		if !s.FinishedAt.IsZero() {
			// The janitor has not removed the finished record yet.
			if err := d.deleteState(ctx, msg.ID, r); err != nil && !errors.Is(err, jobq.ErrLeaseLost) {
				return err
			}
		} else {
			return jobq.ErrJobIDInUse
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if msg.UniqueKey != "" {
		ok, err := d.lockUnique(ctx, msg)
		if err != nil {
			return err
		}
		if !ok {
			return jobq.ErrDuplicate
		}
	}
	env := newEnvelope(msg)
	env.Gen = jobq.NewID()
	state := &jobState{Queue: msg.Queue, Gen: env.Gen, UniqueKey: msg.UniqueKey, UniqueFor: int64(msg.UniqueFor)}
	rev, err := d.putState(ctx, msg.ID, 0, state)
	if err != nil {
		if errors.Is(err, jobq.ErrLeaseLost) {
			err = jobq.ErrJobIDInUse
		}
		d.unlockUnique(context.WithoutCancel(ctx), msg.UniqueKey, msg.UniqueFor, msg.ID)
		return err
	}
	seq, err := d.publish(ctx, msg, env, msg.RunAt)
	if errors.Is(err, jobq.ErrUncertain) {
		// The job may be stored: keep the record and the lock, so a delivery
		// runs and a retry of Enqueue sees ErrJobIDInUse. A job that was not
		// stored holds them until Cancel or, for the lock, its TTL.
		return err
	}
	if err != nil {
		cleanup := context.WithoutCancel(ctx)
		_ = d.deleteState(cleanup, msg.ID, rev)
		d.unlockUnique(cleanup, msg.UniqueKey, msg.UniqueFor, msg.ID)
		return err
	}
	state.Seq = seq
	if _, err := d.putState(ctx, msg.ID, rev, state); err != nil && !errors.Is(err, jobq.ErrLeaseLost) {
		d.cfg.logger.WarnContext(ctx, "natsq: record job sequence", "id", msg.ID, "error", err)
	}
	return nil
}

// publish stores env as the job's message, scheduled at runAt when it is in
// the future, and returns its stream sequence.
func (d *Driver) publish(ctx context.Context, msg *jobq.Message, env *envelope, runAt time.Time) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	data, err := env.encode()
	if err != nil {
		return 0, err
	}
	nmsg := &nats.Msg{Subject: d.queueSubject(msg.Queue), Data: data, Header: nats.Header{}}
	opts := []jetstream.PublishOpt{jetstream.WithExpectStream(d.cfg.stream)}
	if !runAt.IsZero() && runAt.After(time.Now()) {
		nmsg.Subject = d.delaySubject(msg.Queue, msg.ID)
		opts = append(opts, jetstream.WithScheduleAt(runAt), jetstream.WithScheduleTarget(d.queueSubject(msg.Queue)))
	}
	ack, err := d.js.PublishMsg(ctx, nmsg, opts...)
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) || errors.Is(err, nats.ErrBadSubject) || errors.Is(err, jetstream.ErrNoStreamResponse) {
			return 0, fmt.Errorf("natsq: publish job %s: %w", msg.ID, err)
		}
		return 0, fmt.Errorf("%w: publish job %s: %w", jobq.ErrUncertain, msg.ID, err)
	}
	return ack.Sequence, nil
}

// === unique keys ===

func uniqueKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "uniq." + hex.EncodeToString(sum[:16])
}

// lockUnique takes the job's unique key. Windowed keys live for their window;
// the others live until released, bounded by the lock TTL.
func (d *Driver) lockUnique(ctx context.Context, msg *jobq.Message) (bool, error) {
	ttl := d.cfg.uniqueLockTTL
	if msg.UniqueFor > 0 {
		ttl = msg.UniqueFor
	}
	_, err := d.state.Create(ctx, uniqueKey(msg.UniqueKey), []byte(msg.ID), jetstream.KeyTTL(ttl))
	if errors.Is(err, jetstream.ErrKeyExists) {
		// A lock that ran out but whose marker is still there can be taken.
		if entry, getErr := d.state.Get(ctx, uniqueKey(msg.UniqueKey)); getErr == nil {
			if time.Since(entry.Created()) > ttl {
				if _, err := d.state.Update(ctx, uniqueKey(msg.UniqueKey), []byte(msg.ID), entry.Revision()); err == nil {
					return true, nil
				}
			}
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("natsq: lock unique key: %w", err)
	}
	return true, nil
}

// unlockUnique releases an until-done key; windowed keys expire on their own.
func (d *Driver) unlockUnique(ctx context.Context, key string, window time.Duration, owner string) {
	if key == "" || window > 0 {
		return
	}
	entry, err := d.state.Get(ctx, uniqueKey(key))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return
	}
	if err != nil {
		d.cfg.logger.WarnContext(ctx, "natsq: read unique key", "error", err)
		return
	}
	// A lock that expired may be held by another job by now: leave it alone.
	if string(entry.Value()) != owner {
		return
	}
	if err := d.state.Delete(ctx, uniqueKey(key), jetstream.LastRevision(entry.Revision())); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) && !isConflict(err) {
		d.cfg.logger.WarnContext(ctx, "natsq: release unique key", "error", err)
	}
}

// === claim ===

// Claim implements [jobq.Driver].
func (d *Driver) Claim(ctx context.Context, queue string, max int, lease time.Duration) ([]jobq.Lease, error) {
	if max <= 0 {
		return nil, nil
	}
	consumer, err := d.consumer(ctx, queue, lease)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Fetch takes its expiry from the context deadline; a context that is
		// already past yields no fetch at all.
		if time.Now().After(deadline) {
			return nil, nil
		}
		window := min(time.Until(deadline), fetchWindow)
		if dl, ok := ctx.Deadline(); ok {
			window = min(window, time.Until(dl))
		}
		if window < 10*time.Millisecond {
			return nil, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), window)
		// Fetch does not return on cancellation until its own expiry, so wait
		// on the result ourselves.
		type result struct {
			batch jetstream.MessageBatch
			err   error
		}
		done := make(chan result, 1)
		go func() {
			batch, err := consumer.Fetch(max, jetstream.FetchContext(fetchCtx))
			done <- result{batch, err}
		}()
		var res result
		select {
		case res = <-done:
		case <-ctx.Done():
			cancel()
			res = <-done
			if res.err == nil {
				// Give back what the fetch already took, so it does not wait
				// for AckWait.
				for msg := range res.batch.Messages() {
					_ = msg.Nak()
				}
			}
			return nil, ctx.Err()
		}
		// Messages is drained below while fetchCtx is still live.
		stopFetch := cancel
		if res.err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(res.err, context.Canceled) || errors.Is(res.err, context.DeadlineExceeded) {
				// The fetch window closed; the batch may still hold deliveries.
				res.err = nil
			}
		}
		if res.err != nil {
			stopFetch()
			if errors.Is(res.err, jetstream.ErrConsumerDeleted) || errors.Is(res.err, jetstream.ErrConsumerNotFound) {
				d.forgetConsumer(queue)
				if consumer, err = d.consumer(ctx, queue, lease); err != nil {
					return nil, err
				}
				continue
			}
			return nil, fmt.Errorf("natsq: fetch %s: %w", queue, res.err)
		}
		var leases []jobq.Lease
		for msg := range res.batch.Messages() {
			l, err := d.claimMessage(ctx, msg, lease)
			if err != nil {
				stopFetch()
				for _, claimed := range leases {
					_ = claimed.Release(context.WithoutCancel(ctx), jobq.Release{})
				}
				return nil, err
			}
			if l != nil {
				leases = append(leases, l)
			}
		}
		stopFetch()
		if err := res.batch.Error(); err != nil && ctx.Err() == nil &&
			!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			return nil, fmt.Errorf("natsq: fetch %s: %w", queue, err)
		}
		if len(leases) > 0 {
			return leases, nil
		}
		if time.Now().After(deadline) {
			return nil, nil
		}
	}
}

// claimMessage turns one delivery into a lease, or settles it when it is
// stale.
func (d *Driver) claimMessage(ctx context.Context, msg jetstream.Msg, leaseDur time.Duration) (jobq.Lease, error) {
	var env envelope
	if err := env.decode(msg.Data()); err != nil {
		d.cfg.logger.WarnContext(ctx, "natsq: undecodable job message", "subject", msg.Subject(), "error", err)
		_ = msg.Term()
		return nil, nil
	}
	if env.Schedule != "" && env.ID == "" {
		meta, err := msg.Metadata()
		if err != nil {
			return nil, fmt.Errorf("natsq: scheduled job metadata: %w", err)
		}
		env.ID = env.Schedule + "-" + strconv.FormatUint(meta.Sequence.Stream, 10)
		env.Gen = env.ID
	}
	now := time.Now()
	s, rev, err := d.getState(ctx, env.ID)
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
		if env.Schedule == "" {
			// No record: a copy of a cancelled or finished job.
			_ = msg.Term()
			return nil, nil
		}
		// First delivery of a schedule occurrence: the record starts here.
		s, rev = &jobState{Queue: env.Queue, Gen: env.Gen}, 0
	case err != nil:
		return nil, err
	}
	switch {
	case s.Canceled || !s.FinishedAt.IsZero():
		_ = msg.Term()
		return nil, nil
	case s.Gen != env.Gen && (s.Next == "" || s.Next != env.Gen):
		// An older copy of the job.
		_ = msg.Term()
		return nil, nil
	case s.Running && s.LeaseUntil.After(now) && msgTime(msg).Add(s.LeaseUntil.Sub(s.LeaseStart)).After(now):
		// Another worker holds the job and its lease has not run out since the
		// message was published: NATS redelivered early. The message time
		// stands in for the worker's clock, which may be ahead of this one.
		_ = msg.NakWithDelay(min(s.LeaseUntil.Sub(now), time.Minute))
		return nil, nil
	}
	attempt := s.Attempt + 1
	next := *s
	if s.Next != "" && s.Next == env.Gen {
		// Complete an unconfirmed requeue handoff: this copy becomes current.
		next.Gen = env.Gen
	}
	// Whichever copy is claimed first settles the handoff, so a stale Next
	// never outlives the claim.
	next.Next = ""
	next.Attempt = attempt
	next.Running = true
	next.LeaseUntil = now.Add(leaseDur)
	next.LeaseStart = now
	next.Queue = env.Queue
	next.UniqueKey, next.UniqueFor = env.UniqueKey, int64(env.UniqueFor)
	newRev, err := d.putState(ctx, env.ID, rev, &next)
	if err != nil {
		if errors.Is(err, jobq.ErrLeaseLost) && rev == 0 {
			// A schedule occurrence whose id is taken by another job.
			d.cfg.logger.WarnContext(ctx, "natsq: schedule occurrence id in use, skipping it", "id", env.ID)
			_ = msg.Term()
			return nil, nil
		}
		if errors.Is(err, jobq.ErrLeaseLost) {
			// Someone else claimed it first.
			_ = msg.NakWithDelay(time.Second)
			return nil, nil
		}
		return nil, err
	}
	return &jobLease{d: d, msg: env.message(), attempt: attempt, nmsg: msg, stateRev: newRev, lease: leaseDur, scheduled: env.Schedule != ""}, nil
}

// consumer returns the durable consumer of queue, creating it with AckWait
// equal to lease.
func (d *Driver) consumer(ctx context.Context, queue string, lease time.Duration) (jetstream.Consumer, error) {
	d.mu.Lock()
	qc := d.consumers[queue]
	if qc == nil {
		qc = &queueConsumer{}
		d.consumers[queue] = qc
	}
	d.mu.Unlock()
	qc.mu.Lock()
	defer qc.mu.Unlock()
	// AckWait is the shortest lease asked for on this queue, so an expired
	// lease is redelivered in time. A longer lease is redelivered early,
	// and claimMessage puts it back until the lease in the record runs out.
	if qc.consumer != nil && qc.ackWait <= lease {
		return qc.consumer, nil
	}
	cfg := jetstream.ConsumerConfig{
		Durable:       d.consumerName(queue),
		Description:   "jobq queue " + queue,
		FilterSubject: d.queueSubject(queue),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       lease,
		MaxDeliver:    -1,
		MaxAckPending: -1,
	}
	consumer, err := d.js.CreateOrUpdateConsumer(ctx, d.cfg.stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("natsq: consumer %s: %w", queue, err)
	}
	qc.consumer, qc.ackWait = consumer, lease
	return consumer, nil
}

func (d *Driver) forgetConsumer(queue string) {
	d.mu.Lock()
	delete(d.consumers, queue)
	d.mu.Unlock()
}

// msgTime returns when the delivery was stored, for the server clock.
func msgTime(msg jetstream.Msg) time.Time {
	if meta, err := msg.Metadata(); err == nil {
		return meta.Timestamp
	}
	return time.Now()
}

// === cancel ===

// Cancel implements [jobq.Driver].
func (d *Driver) Cancel(ctx context.Context, id string) error {
	for {
		s, rev, err := d.getState(ctx, id)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return jobq.ErrNotFound
		}
		if err != nil {
			return err
		}
		if !s.FinishedAt.IsZero() || s.Canceled {
			return jobq.ErrNotFound
		}
		if s.Running {
			// The worker settles under its lease; the record makes that fail.
			s.Canceled = true
			s.FinishedAt = time.Now()
			_, err = d.putState(ctx, id, rev, s)
		} else {
			err = d.deleteState(ctx, id, rev)
		}
		if errors.Is(err, jobq.ErrLeaseLost) {
			continue // the record changed meanwhile: look again
		}
		if err != nil {
			return err
		}
		// The job can no longer run: its copies are stale. Drop the pending
		// one so it does not wait in the stream, then free the key.
		if s.Seq != 0 && !s.Running {
			if err := d.stream.DeleteMsg(ctx, s.Seq); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
				d.cfg.logger.WarnContext(ctx, "natsq: delete cancelled job message", "id", id, "error", err)
			}
		}
		d.unlockUnique(ctx, s.UniqueKey, time.Duration(s.UniqueFor), id)
		return nil
	}
}

// === lease ===

type jobLease struct {
	d        *Driver
	msg      *jobq.Message
	attempt  int
	nmsg     jetstream.Msg
	stateRev uint64
	lease    time.Duration
	// scheduled marks a job fired by a recurring schedule.
	scheduled bool

	mu   sync.Mutex
	done bool
}

func (l *jobLease) Message() *jobq.Message { return l.msg }
func (l *jobLease) Attempt() int           { return l.attempt }

// Extend implements [jobq.Lease].
func (l *jobLease) Extend(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return jobq.ErrLeaseLost
	}
	if err := l.nmsg.InProgress(); err != nil {
		l.done = true
		return jobq.ErrLeaseLost
	}
	s, rev, err := l.d.getState(ctx, l.msg.ID)
	if err != nil || rev != l.stateRev || s.Attempt != l.attempt || !s.Running {
		l.done = true
		return jobq.ErrLeaseLost
	}
	s.LeaseUntil, s.LeaseStart = time.Now().Add(l.lease), time.Now()
	newRev, err := l.d.putState(ctx, l.msg.ID, rev, s)
	if err != nil {
		l.done = true
		return jobq.ErrLeaseLost
	}
	l.stateRev = newRev
	return nil
}

// Complete implements [jobq.Lease].
func (l *jobLease) Complete(ctx context.Context) error {
	return l.settle(ctx, func(s *jobState) (*jobState, error) {
		if l.scheduled {
			// A schedule occurrence has no record before its first claim, so
			// deleting it would make a redelivery look like a first delivery.
			// Keep a finished record; the janitor removes it after the state TTL.
			s.Running = false
			s.LeaseUntil, s.LeaseStart = time.Time{}, time.Time{}
			s.FinishedAt = time.Now()
			return s, nil
		}
		return nil, nil // finished: remove the record
	})
}

// Fail implements [jobq.Lease]. The dead letter is stored once the lease is
// confirmed, before the record is removed; a crash in between leaves a dead
// letter for a job that runs again.
func (l *jobLease) Fail(ctx context.Context, reason string) error {
	return l.settle(ctx, func(*jobState) (*jobState, error) {
		if _, err := l.d.storeDead(ctx, l.msg, l.attempt, reason); err != nil {
			return nil, err
		}
		return nil, nil
	})
}

// Retry implements [jobq.Lease].
func (l *jobLease) Retry(ctx context.Context, at time.Time, reason string) error {
	env := newEnvelope(l.msg)
	return l.requeue(ctx, env, at, l.attempt, reason, jobq.Release{})
}

// Release implements [jobq.Lease].
func (l *jobLease) Release(ctx context.Context, r jobq.Release) error {
	env := newEnvelope(l.msg)
	if r.Snooze {
		env.Snoozes++
	}
	if !r.UnknownSince.IsZero() {
		env.UnknownSince = r.UnknownSince
	}
	return l.requeue(ctx, env, r.At, l.attempt-1, "", r)
}

// requeue stores a new message for the job, updates the record, then acks the
// old delivery.
func (l *jobLease) requeue(ctx context.Context, env *envelope, at time.Time, attempt int, reason string, _ jobq.Release) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return jobq.ErrLeaseLost
	}
	s, rev, err := l.d.getState(ctx, l.msg.ID)
	if err != nil || rev != l.stateRev || s.Attempt != l.attempt || !s.Running {
		l.done = true
		return jobq.ErrLeaseLost
	}
	env.Gen = jobq.NewID()
	// Step 1: record the intent. The record stays current for the old copy and
	// remembers the new generation, so either copy can be recovered.
	pending := *s
	pending.Next = env.Gen
	pending.Running = false
	pending.Attempt = attempt
	pending.LeaseUntil, pending.LeaseStart = time.Time{}, time.Time{}
	pending.LastError = reason
	pendRev, err := l.d.putState(ctx, l.msg.ID, rev, &pending)
	if err != nil {
		l.done = true
		return err
	}
	l.stateRev = pendRev
	// Step 2: publish the new copy. On any failure leave the old delivery
	// unacked and report the error: nothing is lost, the old delivery comes
	// back after its lease and the record's pending generation is superseded.
	seq, err := l.d.publish(ctx, l.msg, env, at)
	if err != nil {
		l.done = true
		return err
	}
	// Step 3: switch the record to the new copy.
	done := pending
	done.Gen, done.Next, done.Seq = env.Gen, "", seq
	if _, err := l.d.putState(ctx, l.msg.ID, pendRev, &done); err != nil {
		// The new copy is published and the record names it as Next, so its
		// first claim completes the handoff. Do not fail the requeue.
		l.d.cfg.logger.WarnContext(ctx, "natsq: confirm requeue", "id", l.msg.ID, "error", err)
	}
	l.done = true
	// A lost ack is harmless: the old copy is stale once the record switched,
	// and is dropped after the switch.
	_ = l.nmsg.Ack()
	return nil
}

// settle applies change to the record and acks the delivery. change returns
// nil to finish the job.
func (l *jobLease) settle(ctx context.Context, change func(*jobState) (*jobState, error)) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return jobq.ErrLeaseLost
	}
	s, rev, err := l.d.getState(ctx, l.msg.ID)
	if err != nil || rev != l.stateRev || s.Attempt != l.attempt || !s.Running {
		l.done = true
		return jobq.ErrLeaseLost
	}
	next, err := change(s)
	if err != nil {
		return err // the lease is still held; the caller may try again
	}
	if next == nil {
		if err := l.d.deleteState(ctx, l.msg.ID, rev); err != nil {
			l.done = true
			return err
		}
	} else {
		if _, err := l.d.putState(ctx, l.msg.ID, rev, next); err != nil {
			l.done = true
			return err
		}
	}
	l.d.unlockUnique(ctx, s.UniqueKey, time.Duration(s.UniqueFor), l.msg.ID)
	l.done = true
	// A lost ack is harmless: the redelivery finds no record and is dropped.
	_ = l.nmsg.Ack()
	return nil
}

// === dead letters ===

// deadLetter is a stored dead letter.
type deadLetter struct {
	Envelope envelope  `json:"envelope"`
	Payload  []byte    `json:"payload"`
	Attempts int       `json:"attempts"`
	Error    string    `json:"error"`
	FailedAt time.Time `json:"failed_at"`
}

// storeDead appends the job to the dead-letter stream and returns its
// sequence, which is the paging cursor.
func (d *Driver) storeDead(ctx context.Context, msg *jobq.Message, attempts int, reason string) (uint64, error) {
	dl := deadLetter{Envelope: *newEnvelope(msg), Payload: msg.Payload, Attempts: attempts, Error: reason, FailedAt: time.Now()}
	data, err := json.Marshal(dl)
	if err != nil {
		return 0, err
	}
	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	ack, err := d.js.Publish(pubCtx, d.cfg.prefix+".dead."+msg.Queue, data)
	if err != nil {
		return 0, fmt.Errorf("natsq: store dead letter %s: %w", msg.ID, err)
	}
	return ack.Sequence, nil
}

// deadEntry is one dead letter with its stream sequence.
type deadEntry struct {
	seq uint64
	dl  deadLetter
}

// scanDead walks the dead-letter stream backwards from before (zero: the
// newest) and returns entries matching queue, newest first.
func (d *Driver) scanDead(ctx context.Context, queue string, before uint64, limit int) ([]deadEntry, error) {
	info, err := d.dead.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("natsq: dead letter stream: %w", err)
	}
	seq := info.State.LastSeq
	if before > 0 {
		seq = before - 1
	}
	var out []deadEntry
	for seq > 0 && (limit <= 0 || len(out) < limit) {
		raw, err := d.dead.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			seq--
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("natsq: read dead letter %d: %w", seq, err)
		}
		var dl deadLetter
		if json.Unmarshal(raw.Data, &dl) == nil && (queue == "" || dl.Envelope.Queue == queue) {
			dl.Envelope.Payload = dl.Payload
			out = append(out, deadEntry{seq: seq, dl: dl})
		}
		seq--
	}
	return out, nil
}

// Stats implements [jobq.Inspector].
func (d *Driver) Stats(ctx context.Context) (jobq.Stats, error) {
	var stats jobq.Stats
	counts := map[string]*jobq.QueueStats{}
	get := func(queue string) *jobq.QueueStats {
		s, ok := counts[queue]
		if !ok {
			s = &jobq.QueueStats{Queue: queue}
			counts[queue] = s
		}
		return s
	}
	consumers := d.stream.ListConsumers(ctx)
	for info := range consumers.Info() {
		queue, ok := strings.CutPrefix(info.Name, d.cfg.prefix+"_")
		if !ok || info.Config.FilterSubject != d.queueSubject(queue) {
			continue
		}
		s := get(queue)
		s.Ready = int64(info.NumPending)
		s.Running = int64(info.NumAckPending)
	}
	if err := consumers.Err(); err != nil {
		return stats, fmt.Errorf("natsq: list consumers: %w", err)
	}
	delayed, err := d.stream.Info(ctx, jetstream.WithSubjectFilter(d.cfg.prefix+".at.>"))
	if err != nil {
		return stats, fmt.Errorf("natsq: delayed stats: %w", err)
	}
	for subject, n := range delayed.State.Subjects {
		rest := strings.TrimPrefix(subject, d.cfg.prefix+".at.")
		queue, _, _ := strings.Cut(rest, ".")
		get(queue).Scheduled += int64(n)
	}
	schedules, err := d.stream.Info(ctx, jetstream.WithSubjectFilter(d.cfg.prefix+".cron.>"))
	if err != nil {
		return stats, fmt.Errorf("natsq: schedule stats: %w", err)
	}
	stats.Schedules = len(schedules.State.Subjects)

	// Dead letters are counted by scanning the stream once, newest first.
	dead, err := d.scanDead(ctx, "", 0, 0)
	if err != nil {
		return stats, err
	}
	for _, e := range dead {
		get(e.dl.Envelope.Queue).Dead++
	}
	queues := slices.SortedFunc(maps.Values(counts), func(a, b *jobq.QueueStats) int {
		return strings.Compare(a.Queue, b.Queue)
	})
	for _, q := range queues {
		stats.Queues = append(stats.Queues, *q)
	}
	return stats, nil
}

// DeadLetters implements [jobq.Inspector].
func (d *Driver) DeadLetters(ctx context.Context, q jobq.DeadLetterQuery) (jobq.DeadLetterPage, error) {
	before := uint64(0)
	if q.Cursor != "" {
		n, err := strconv.ParseUint(q.Cursor, 10, 64)
		if err != nil {
			return jobq.DeadLetterPage{}, fmt.Errorf("natsq: invalid cursor %q", q.Cursor)
		}
		before = n
	}
	entries, err := d.scanDead(ctx, q.Queue, before, q.Limit+1)
	if err != nil {
		return jobq.DeadLetterPage{}, err
	}
	page := jobq.DeadLetterPage{}
	if len(entries) > q.Limit {
		entries = entries[:q.Limit]
		page.Next = strconv.FormatUint(entries[len(entries)-1].seq, 10)
	}
	for _, e := range entries {
		page.DeadLetters = append(page.DeadLetters, jobq.DeadLetter{
			Message:  e.dl.Envelope.message(),
			Attempts: e.dl.Attempts,
			Error:    e.dl.Error,
			FailedAt: e.dl.FailedAt,
		})
	}
	return page, nil
}

// findDead returns the newest dead letter of job id.
func (d *Driver) findDead(ctx context.Context, id string) (deadEntry, bool, error) {
	entries, err := d.scanDead(ctx, "", 0, 0)
	if err != nil {
		return deadEntry{}, false, err
	}
	for _, e := range entries {
		if e.dl.Envelope.ID == id {
			return e, true, nil
		}
	}
	return deadEntry{}, false, nil
}

// RetryDead implements [jobq.Inspector].
func (d *Driver) RetryDead(ctx context.Context, id string) error {
	e, ok, err := d.findDead(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return jobq.ErrNotFound
	}
	msg := e.dl.Envelope.message()
	msg.Snoozes, msg.UnknownSince = 0, time.Time{}
	if err := d.Enqueue(ctx, msg); err != nil {
		return err
	}
	if err := d.dead.DeleteMsg(ctx, e.seq); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return fmt.Errorf("natsq: delete dead letter %s: %w", id, err)
	}
	return nil
}

// DeleteDead implements [jobq.Inspector].
func (d *Driver) DeleteDead(ctx context.Context, id string) error {
	e, ok, err := d.findDead(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return jobq.ErrNotFound
	}
	if err := d.dead.DeleteMsg(ctx, e.seq); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return fmt.Errorf("natsq: delete dead letter %s: %w", id, err)
	}
	return nil
}

// === schedules ===

// SyncSchedules implements [jobq.Scheduler].
func (d *Driver) SyncSchedules(ctx context.Context, schedules []jobq.ScheduleSpec) error {
	desired := make(map[string]*nats.Msg, len(schedules))
	for _, s := range schedules {
		env := newEnvelope(s.Message)
		env.ID, env.EnqueuedAt, env.Schedule = "", time.Time{}, s.Key
		data, err := env.encode()
		if err != nil {
			return err
		}
		msg := &nats.Msg{Subject: d.cronSubject(s.Key), Data: data, Header: nats.Header{}}
		msg.Header.Set("Nats-Schedule", s.Cron)
		msg.Header.Set("Nats-Schedule-Target", d.queueSubject(s.Message.Queue))
		if s.Timezone != "" {
			msg.Header.Set("Nats-Schedule-Time-Zone", s.Timezone)
		}
		desired[msg.Subject] = msg
	}
	info, err := d.stream.Info(ctx, jetstream.WithSubjectFilter(d.cfg.prefix+".cron.>"))
	if err != nil {
		return fmt.Errorf("natsq: list schedules: %w", err)
	}
	for subject := range info.State.Subjects {
		if _, keep := desired[subject]; keep {
			continue
		}
		if err := d.stream.Purge(ctx, jetstream.WithPurgeSubject(subject)); err != nil {
			return fmt.Errorf("natsq: remove schedule %s: %w", subject, err)
		}
	}
	for subject, msg := range desired {
		current, err := d.stream.GetLastMsgForSubject(ctx, subject)
		if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return fmt.Errorf("natsq: read schedule %s: %w", subject, err)
		}
		if current != nil && string(current.Data) == string(msg.Data) && scheduleEqual(current.Header, msg.Header) {
			continue
		}
		if _, err := d.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(d.cfg.stream)); err != nil {
			return fmt.Errorf("natsq: install schedule %s: %w", subject, err)
		}
	}
	return nil
}

func scheduleEqual(a, b nats.Header) bool {
	for key := range b {
		if key == "Nats-Schedule-Target" || key == "Nats-Schedule" || key == "Nats-Schedule-Time-Zone" {
			if a.Get(key) != b.Get(key) {
				return false
			}
		}
	}
	return true
}

// === janitor ===

// janitor removes the state records of finished jobs after their TTL and
// expired dead letters.
func (d *Driver) janitor() {
	defer close(d.done)
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		d.clean(ctx)
		cancel()
	}
}

func (d *Driver) clean(ctx context.Context) {
	keys, err := d.state.Keys(ctx)
	if err != nil {
		d.cfg.logger.WarnContext(ctx, "natsq: list state keys", "error", err)
		return
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, "job.") {
			continue
		}
		entry, err := d.state.Get(ctx, key)
		if err != nil {
			continue
		}
		var s jobState
		if json.Unmarshal(entry.Value(), &s) != nil || s.FinishedAt.IsZero() {
			continue
		}
		if time.Since(s.FinishedAt) < d.cfg.stateTTL {
			continue
		}
		if err := d.deleteState(ctx, strings.TrimPrefix(key, "job."), entry.Revision()); err != nil {
			continue
		}
	}
	// Dead letters live in their own stream, which expires them by MaxAge.
}
