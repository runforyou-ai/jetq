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
	header      nats.Header
	id          string
}

// OnQueue puts the job on the named queue (default [DefaultQueue]).
func OnQueue(queue string) EnqueueOption { return func(o *enqueueOptions) { o.queue = queue } }

// Delay makes the job available after d. Delayed jobs can be cancelled with [Client.Cancel].
func Delay(d time.Duration) EnqueueOption { return func(o *enqueueOptions) { o.delay = d } }

// At makes the job available at t. Delayed jobs can be cancelled with [Client.Cancel].
func At(t time.Time) EnqueueOption { return func(o *enqueueOptions) { o.at = t } }

// JobID sets the job id instead of a generated one, so it can be stored
// before the job is enqueued. It must be unique and consist of letters,
// digits, '-' and '_'.
func JobID(id string) EnqueueOption { return func(o *enqueueOptions) { o.id = id } }

// Unique deduplicates the job by key within the client's duplicate window:
// a second enqueue with the same key returns [ErrDuplicate].
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
func (c *Client) Enqueue(ctx context.Context, job Job, opts ...EnqueueOption) (string, error) {
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
	}

	if o.lock != "" {
		lock := lockKey(o.lock)
		locks, err := c.stateBucket(ctx, true)
		if err != nil {
			return "", err
		}
		if _, err := locks.Create(ctx, lock, []byte(id)); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				return "", ErrDuplicate
			}
			return "", fmt.Errorf("jetq: lock unique job %s: %w", name, err)
		}
		msg.Header.Set(HeaderUniqueLock, lock)
	}

	pubOpts := []jetstream.PublishOpt{jetstream.WithExpectStream(c.cfg.streamName)}
	if o.unique != "" {
		pubOpts = append(pubOpts, jetstream.WithMsgID(o.unique))
	}
	ack, err := c.js.PublishMsg(ctx, msg, pubOpts...)
	if err == nil && ack.Duplicate {
		err = ErrDuplicate
	}
	if err != nil {
		// Release the lock only when the job was certainly not stored; after
		// a timeout it may have been, and the lock then expires with its TTL.
		if lock := msg.Header.Get(HeaderUniqueLock); lock != "" && notStored(err) {
			c.unlock(ctx, lock, id)
		}
		if errors.Is(err, ErrDuplicate) {
			return "", err
		}
		return "", fmt.Errorf("jetq: enqueue %s: %w", name, err)
	}
	return id, nil
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

// Cancel removes a pending delayed job enqueued with [Delay] or [At]. It
// returns [ErrNotFound] when the job does not exist or has already become
// available for processing. When it returns nil, a copy the schedule may have
// published while Cancel ran is skipped by workers; Cancel is best effort only
// for a copy a worker picked up before the cancellation was recorded.
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
	// Mark the job cancelled first: the scheduler may have copied it to the
	// queue already, and workers skip a marked copy.
	state, err := c.stateBucket(ctx, true)
	if err != nil {
		return fmt.Errorf("jetq: cancel %s: %w", id, err)
	}
	marker := cancelKey(id)
	revision, err := state.Put(ctx, marker, []byte(scheduled.Header.Get(HeaderEnqueuedAt)))
	if err != nil {
		return fmt.Errorf("jetq: cancel %s: %w", id, err)
	}
	// Delete exactly the message read above. On any failure the job may still
	// run, so the marker is withdrawn: if the schedule fired meanwhile the
	// delete fails and the job is left to run.
	if err := c.stream.DeleteMsg(ctx, scheduled.Sequence); err != nil {
		_ = state.Delete(context.WithoutCancel(ctx), marker, jetstream.LastRevision(revision))
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
// message: it answered with an error, reported a duplicate, or had no stream.
func notStored(err error) bool {
	var apiErr *jetstream.APIError
	return errors.Is(err, ErrDuplicate) || errors.As(err, &apiErr) || errors.Is(err, jetstream.ErrNoStreamResponse)
}

// unlockTimeout bounds releasing a UniqueUntilDone lock.
const unlockTimeout = 5 * time.Second

// cancelKey is the state key marking delayed job id as cancelled.
func cancelKey(id string) string { return "cancel." + id }

// cancelled reports whether a job fired from a delayed schedule was cancelled.
// The marker is left in place, so a redelivered copy is skipped too, and
// expires with the bucket TTL; it holds the enqueue time, so a later job that
// reuses the id is not affected.
func (c *Client) cancelled(ctx context.Context, header nats.Header, id string) bool {
	if !strings.HasPrefix(header.Get(headerScheduler), c.delaySubject("")) {
		return false
	}
	state, err := c.stateBucket(ctx, false)
	if err != nil || state == nil {
		return false
	}
	entry, err := state.Get(ctx, cancelKey(id))
	return err == nil && string(entry.Value()) == header.Get(HeaderEnqueuedAt)
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
	entry, err := locks.Get(ctx, lock)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return
	}
	if err == nil && string(entry.Value()) != id {
		return
	}
	if err == nil {
		err = locks.Delete(ctx, lock, jetstream.LastRevision(entry.Revision()))
	}
	if err != nil {
		c.cfg.logger.WarnContext(ctx, "jetq unique lock release failed", "id", id, "error", err)
	}
}
