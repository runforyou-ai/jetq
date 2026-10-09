package jetq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// EnqueueOption configures a single [Client.Enqueue] call.
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct {
	queue       string
	at          time.Time
	delay       time.Duration
	unique      string
	lock        string
	maxAttempts int
	timeout     *time.Duration
	header      nats.Header
	id          string
}

// OnQueue puts the job on the named queue (default [DefaultQueue]).
func OnQueue(queue string) EnqueueOption { return func(o *enqueueOptions) { o.queue = queue } }

// Delay makes the job available after d, computed with the local clock as an
// absolute time. Delayed jobs can be cancelled with [Client.Cancel].
func Delay(d time.Duration) EnqueueOption { return func(o *enqueueOptions) { o.delay = d } }

// At makes the job available at t. Delayed jobs can be cancelled with [Client.Cancel].
func At(t time.Time) EnqueueOption { return func(o *enqueueOptions) { o.at = t } }

// JobID sets the job id instead of a generated one, so it can be stored
// before the job is enqueued. It must consist of letters, digits, '-' and '_'
// and be unique among jobs that have not settled: [Client.Enqueue] returns
// [ErrJobIDInUse] when a pending delayed job (including one waiting for a
// retry or a snooze) has the id, but does not check other pending jobs; a job
// reusing the id of one of those runs as a separate job, and [Client.Cancel]
// can only reach the delayed one.
func JobID(id string) EnqueueOption { return func(o *enqueueOptions) { o.id = id } }

// Unique deduplicates the job by key within the client's duplicate window:
// a second enqueue with the same key returns [ErrDuplicate]. Keys starting
// with "jetq-" are reserved.
func Unique(key string) EnqueueOption { return func(o *enqueueOptions) { o.unique = key } }

// UniqueUntilDone deduplicates the job by key until it settles: while a job
// with the same key is pending, delayed, running or waiting for a retry, a new
// enqueue returns [ErrDuplicate]; once it succeeds, is dead-lettered or is
// cancelled, the key is free again. The lock lasts at most the lock TTL from
// enqueue (see [WithUniqueLockTTL]), so jobs that stay unsettled longer lose
// their deduplication.
func UniqueUntilDone(key string) EnqueueOption { return func(o *enqueueOptions) { o.lock = key } }

// MaxAttempts overrides the queue's attempt limit for this job.
func MaxAttempts(n int) EnqueueOption { return func(o *enqueueOptions) { o.maxAttempts = n } }

// Timeout overrides the queue's per-attempt timeout ([Queue.Timeout]) for
// this job; a non-positive d means no limit.
func Timeout(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) {
		d = max(d, 0)
		o.timeout = &d
	}
}

// WithHeader adds a message header, for example a W3C traceparent. Headers
// starting with "Jetq-" or "Nats-" are reserved.
func WithHeader(key, value string) EnqueueOption {
	return func(o *enqueueOptions) {
		if o.header == nil {
			o.header = nats.Header{}
		}
		o.header.Add(key, value)
	}
}

// Enqueue encodes job as JSON and adds it to its queue, returning the job id.
//
// Call it after the surrounding database transaction has committed; jetq does
// not take part in database transactions.
//
// It returns [ErrDuplicate] when a [Unique] or [UniqueUntilDone] key is taken
// and an error wrapping [ErrJobIDInUse] when a pending delayed job has the
// [JobID]. An error wrapping [ErrUncertain] means the job may or may not have
// been enqueued, for example after a timeout or a lost connection; the job id
// is returned with it (see [ErrUncertain]).
func (c *Client) Enqueue(ctx context.Context, job Job, opts ...EnqueueOption) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	o := enqueueOptions{queue: DefaultQueue}
	for _, opt := range opts {
		opt(&o)
	}
	if err := validName("queue", o.queue); err != nil {
		return "", err
	}
	name := job.JobName()
	if name == "" {
		return "", errors.New("jetq: job name is empty")
	}
	data, err := encodeJob(job)
	if err != nil {
		return "", err
	}

	id := o.id
	if id == "" {
		id = nuid.Next()
	} else if err := validName("job id", id); err != nil {
		return "", err
	}
	msg := &nats.Msg{Subject: c.queueSubject(o.queue), Data: data, Header: nats.Header{}}
	for key, values := range o.header {
		if reservedHeader(key) {
			return "", fmt.Errorf("jetq: header %q is reserved", key)
		}
		msg.Header[key] = values
	}
	msg.Header.Set(HeaderJob, name)
	msg.Header.Set(HeaderID, id)
	msg.Header.Set(HeaderEnqueuedAt, time.Now().UTC().Format(time.RFC3339Nano))
	if o.maxAttempts > 0 {
		msg.Header.Set(HeaderMaxAttempts, strconv.Itoa(o.maxAttempts))
	}
	if o.timeout != nil {
		msg.Header.Set(HeaderTimeout, o.timeout.String())
	}

	pubOpts := []jetstream.PublishOpt{jetstream.WithExpectStream(c.cfg.streamName)}
	at := o.at
	if o.delay > 0 {
		at = time.Now().Add(o.delay)
	}
	if !at.IsZero() && at.After(time.Now()) {
		// The server holds the schedule message and publishes a copy to the
		// queue subject when it fires, then purges the schedule message.
		msg.Header.Set(headerSchedule, "@at "+at.UTC().Format(time.RFC3339Nano))
		msg.Header.Set(headerScheduleTarget, msg.Subject)
		msg.Subject = c.delaySubject(id)
		// Publishing on the subject of a pending delayed job would replace it.
		pubOpts = append(pubOpts, jetstream.WithExpectLastSequencePerSubject(0))
	}

	if strings.HasPrefix(o.unique, "jetq-") {
		return "", fmt.Errorf("jetq: unique key %q: the prefix jetq- is reserved", o.unique)
	}
	var revision uint64
	if o.lock != "" {
		lock := lockKey(o.lock)
		var taken bool
		taken, revision, err = c.lockJob(ctx, lock, id, o.id == "")
		if err != nil {
			return "", fmt.Errorf("jetq: lock unique job %s: %w", name, err)
		}
		if taken {
			return "", ErrDuplicate
		}
		msg.Header.Set(HeaderUniqueLock, lock)
	}
	lock := msg.Header.Get(HeaderUniqueLock)
	if err := ctx.Err(); err != nil {
		// Nothing was published.
		if lock != "" {
			c.unlock(ctx, lock, id)
		}
		return "", err
	}

	// A UniqueUntilDone job gets a message id of its own, so that a second
	// publish tells whether a first one with an unknown outcome was stored.
	msgID := o.unique
	resolvable := o.lock != "" && o.unique == ""
	if resolvable {
		msgID = "jetq-lock-" + strconv.FormatUint(revision, 10)
	}
	if msgID != "" {
		pubOpts = append(pubOpts, jetstream.WithMsgID(msgID))
	}
	started := time.Now()
	err = c.publishJob(ctx, msg, pubOpts, resolvable)
	// The second publish is deduplicated only within the stream's duplicate
	// window; keep a margin for the time the publishes take.
	if err != nil && resolvable && !notStored(err) && time.Since(started)+resolveTimeout < c.cfg.duplicates/2 {
		resolveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resolveTimeout)
		resolved := c.publishJob(resolveCtx, msg, pubOpts, true)
		cancel()
		// A rejection of the second publish says nothing about the first.
		if resolved == nil {
			err = nil
		}
	}
	switch {
	case err == nil:
		return id, nil
	case errors.Is(err, ErrDuplicate):
		if lock != "" {
			c.unlock(ctx, lock, id)
		}
		return "", err
	case notStored(err):
		// The job was certainly not stored: free its key.
		if lock != "" {
			c.unlock(ctx, lock, id)
		}
		return "", fmt.Errorf("jetq: enqueue %s: %w", name, err)
	case errors.Is(err, ErrUncertain):
		return id, fmt.Errorf("jetq: enqueue %s: %w", name, err)
	default:
		// The job may have been stored: a UniqueUntilDone lock stays until the
		// job settles or the lock expires.
		return id, fmt.Errorf("jetq: enqueue %s: %w: %w", name, ErrUncertain, err)
	}
}

// resolveTimeout bounds the second publish that resolves an uncertain one.
const resolveTimeout = 5 * time.Second

// lockJob takes the UniqueUntilDone lock for job id and reports whether
// another job holds it, and the revision of the new lock. When Create fails
// otherwise, a lock it may have written is removed if the id is unique to
// this call.
func (c *Client) lockJob(ctx context.Context, lock, id string, generatedID bool) (taken bool, revision uint64, err error) {
	locks, err := c.stateBucket(ctx, true)
	if err != nil {
		return false, 0, err
	}
	revision, err = locks.Create(ctx, lock, []byte(id))
	switch {
	case err == nil:
		return false, revision, nil
	case errors.Is(err, jetstream.ErrKeyExists):
		return true, 0, nil
	}
	if generatedID {
		c.unlock(ctx, lock, id)
	}
	return false, 0, err
}

// publishJob publishes a job once. It returns nil when the job is stored,
// [ErrDuplicate] for a duplicate message id, an error wrapping [ErrJobIDInUse]
// when another pending delayed job has the id, or the publish error. With
// ownMsgID the message id belongs to this job alone, so a duplicate means the
// job is stored already.
func (c *Client) publishJob(ctx context.Context, msg *nats.Msg, pubOpts []jetstream.PublishOpt, ownMsgID bool) error {
	ack, err := c.js.PublishMsg(ctx, msg, pubOpts...)
	switch {
	case err == nil && ack.Duplicate && !ownMsgID:
		return ErrDuplicate
	case err == nil:
		return nil
	}
	err = classify(err)
	if !errors.Is(err, ErrJobIDInUse) || !ownMsgID {
		return err
	}
	// A delayed job with this id is pending: it may be this job, published by
	// an earlier attempt, or already put back for a retry by a worker.
	pending, getErr := c.stream.GetLastMsgForSubject(ctx, msg.Subject)
	switch {
	case getErr != nil:
		return fmt.Errorf("%w: read pending delayed job: %w", ErrUncertain, getErr)
	case sameJob(pending.Header, msg.Header):
		return nil
	default:
		return err
	}
}

// sameJob reports whether two messages carry the same enqueued job: the same
// id and the same enqueue time.
func sameJob(a, b nats.Header) bool {
	return a.Get(HeaderID) == b.Get(HeaderID) && a.Get(HeaderEnqueuedAt) == b.Get(HeaderEnqueuedAt)
}

// encodeJob encodes job as JSON; a [RawJob] payload is used byte for byte.
func encodeJob(job Job) ([]byte, error) {
	if raw, ok := job.(*RawJob); ok && raw != nil {
		job = *raw
	}
	if raw, ok := job.(RawJob); ok {
		if len(raw.Payload) == 0 {
			return []byte("null"), nil
		}
		if !json.Valid(raw.Payload) {
			return nil, fmt.Errorf("jetq: job %s: payload is not valid JSON", raw.Name)
		}
		return raw.Payload, nil
	}
	data, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("jetq: encode job %s: %w", job.JobName(), err)
	}
	return data, nil
}

func reservedHeader(key string) bool {
	lower := strings.ToLower(key)
	return strings.HasPrefix(lower, "jetq-") || strings.HasPrefix(lower, "nats-")
}

// Cancel removes a pending delayed job: one enqueued with [Delay] or [At], or
// a job waiting for a retry or a snooze. When it returns nil the job does not
// run (again): a copy the schedule may have published
// while Cancel ran is skipped by workers. It returns [ErrNotFound] when the job
// does not exist or has already become available for processing; when that
// happens during Cancel, the job either runs or is skipped. Once Cancel has
// recorded the cancellation, the job may be skipped even if Cancel then
// returns an error or ErrNotFound: a copy fired from the schedule and its
// later retries and snoozes are skipped, so a job that was running when
// Cancel lost the race finishes its current attempt but is not retried.
func (c *Client) Cancel(ctx context.Context, id string) error {
	if err := validName("job id", id); err != nil {
		return err
	}
	subject := c.delaySubject(id)
	scheduled, err := c.stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return ErrNotFound
		}
		return fmt.Errorf("jetq: cancel %s: %w", id, err)
	}
	// Record the cancellation before deleting: the scheduler may be publishing
	// a copy right now, and workers skip a recorded copy. The marker is never
	// withdrawn, so a concurrent Cancel cannot undo it; it expires with the
	// bucket TTL and holds the enqueue time, so a later job reusing the id is
	// not affected.
	state, err := c.stateBucket(ctx, true)
	if err != nil {
		return fmt.Errorf("jetq: cancel %s: %w", id, err)
	}
	if _, err := state.Put(ctx, cancelKey(id), []byte(scheduled.Header.Get(HeaderEnqueuedAt))); err != nil {
		return fmt.Errorf("jetq: cancel %s: %w", id, err)
	}
	// Delete exactly the message read above; if the schedule fired meanwhile,
	// the delete fails and the copy runs or is skipped, releasing its own lock.
	if err := c.stream.DeleteMsg(ctx, scheduled.Sequence); err != nil {
		if errors.Is(err, jetstream.ErrMsgDeleteUnsuccessful) || errors.Is(err, jetstream.ErrMsgNotFound) {
			return ErrNotFound
		}
		return fmt.Errorf("jetq: cancel %s: %w", id, err)
	}
	if lock := scheduled.Header.Get(HeaderUniqueLock); lock != "" {
		c.unlock(ctx, lock, id)
	}
	return nil
}

// notStored reports whether a publish error means the server did not store the
// message: it answered with an error or reported a duplicate, no stream
// listened, or the client refused to send it. Errors wrapping [ErrUncertain]
// never qualify.
func notStored(err error) bool {
	if errors.Is(err, ErrUncertain) {
		return false
	}
	var apiErr *jetstream.APIError
	return errors.Is(err, ErrDuplicate) || errors.Is(err, ErrJobIDInUse) || errors.As(err, &apiErr) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, nats.ErrMaxPayload) ||
		errors.Is(err, nats.ErrBadSubject) || errors.Is(err, nats.ErrInvalidConnection) ||
		errors.Is(err, nats.ErrConnectionDraining) || errors.Is(err, nats.ErrReconnectBufExceeded) ||
		errors.Is(err, nats.ErrHeadersNotSupported) || errors.Is(err, nats.ErrBadHeaderMsg)
}

// classify wraps the stream's rejection of a delayed job's publish, because
// its subject already has a message, in [ErrJobIDInUse].
func classify(err error) error {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
		return fmt.Errorf("%w: %w", ErrJobIDInUse, err)
	}
	return err
}

// unlockTimeout bounds releasing a UniqueUntilDone lock.
const unlockTimeout = 5 * time.Second

// cancelKey is the state key marking delayed job id as cancelled.
func cancelKey(id string) string { return "cancel." + id }

// cancelled reports whether a job fired from a delayed schedule was cancelled.
// It returns an error when that cannot be determined; the job must then not
// run yet. The marker is left in place, so a redelivered copy is skipped too,
// and expires with the bucket TTL; it holds the enqueue time, so a later job
// that reuses the id is not affected.
func (c *Client) cancelled(ctx context.Context, header nats.Header, id string, redelivered bool) (bool, error) {
	// A redelivery may be the original of a job whose put-back copy was
	// cancelled after the ack of that delivery was lost.
	if !c.wasDelayed(header) && !redelivered {
		return false, nil
	}
	state, err := c.stateBucket(ctx, false)
	if err != nil || state == nil {
		return false, err
	}
	value, _, found, err := c.leaderGet(ctx, state, cancelKey(id))
	if err != nil || !found {
		return false, err
	}
	return string(value) == header.Get(HeaderEnqueuedAt), nil
}

// wasDelayed reports whether a job message was fired from a delayed job, or
// put back by a worker after it was.
func (c *Client) wasDelayed(header nats.Header) bool {
	return strings.HasPrefix(header.Get(headerScheduler), c.delaySubject("")) || header.Get(headerDelayed) != ""
}

// leaderGet reads a state key through the bucket stream's leader. Key-value
// gets are direct gets that a lagging follower may answer with an older value,
// which would hide a just written cancel marker or lock revision.
func (c *Client) leaderGet(ctx context.Context, state jetstream.KeyValue, key string) (value []byte, revision uint64, found bool, err error) {
	stream, err := c.stateLeaderStream(ctx, state.Bucket())
	if err != nil {
		return nil, 0, false, err
	}
	// Keys of a jetq-created bucket live on the default key-value subject $KV.<bucket>.<key>.
	msg, err := stream.GetLastMsgForSubject(ctx, "$KV."+state.Bucket()+"."+key)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	switch msg.Header.Get("KV-Operation") {
	case "DEL", "PURGE":
		return nil, 0, false, nil
	}
	return msg.Data, msg.Sequence, true, nil
}

// stateLeaderStream returns a handle on the state bucket's stream whose gets
// go to the stream leader: the handle's cached info has direct gets turned off.
// Info is never called on it again, so the change stays in place.
func (c *Client) stateLeaderStream(ctx context.Context, bucket string) (jetstream.Stream, error) {
	c.stateMu.Lock()
	leader := c.stateLeader
	c.stateMu.Unlock()
	if leader != nil {
		return leader, nil
	}
	// Look the stream up outside the lock, so a slow server does not block
	// other calls; concurrent lookups keep the first handle.
	stream, err := c.js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return nil, err
	}
	// nats.go decides between direct and leader gets by the handle's cached
	// config and offers no option for it; the handle is private to the client.
	stream.CachedInfo().Config.AllowDirect = false
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.stateLeader == nil {
		c.stateLeader = stream
	}
	return c.stateLeader, nil
}

// lockKey maps a unique key to a valid key-value key.
func lockKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// unlock releases a UniqueUntilDone lock if it is still held by job id;
// failures are logged and the lock then expires with its TTL.
func (c *Client) unlock(ctx context.Context, lock, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
	defer cancel()
	locks, err := c.stateBucket(ctx, false)
	if err != nil {
		c.cfg.logger.WarnContext(ctx, "jetq unique lock release failed", "id", id, "error", err)
		return
	}
	if locks == nil {
		return
	}
	value, revision, found, err := c.leaderGet(ctx, locks, lock)
	if err == nil && (!found || string(value) != id) {
		return
	}
	if err == nil {
		err = locks.Delete(ctx, lock, jetstream.LastRevision(revision))
	}
	if err != nil {
		c.cfg.logger.WarnContext(ctx, "jetq unique lock release failed", "id", id, "error", err)
	}
}
