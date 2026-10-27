package jobq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// EnqueueOption configures [Client.Enqueue].
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct {
	queue       string
	at          time.Time
	delay       time.Duration
	id          string
	unique      string
	uniqueFor   time.Duration
	maxAttempts int
	timeout     time.Duration
	header      map[string]string
	err         error
}

// OnQueue enqueues the job on queue instead of [DefaultQueue].
func OnQueue(queue string) EnqueueOption { return func(o *enqueueOptions) { o.queue = queue } }

// Delay makes the job due after d.
func Delay(d time.Duration) EnqueueOption { return func(o *enqueueOptions) { o.delay = d } }

// At makes the job due at t.
func At(t time.Time) EnqueueOption { return func(o *enqueueOptions) { o.at = t } }

// JobID sets the job id instead of generating one, for example to store it
// before enqueueing. While a job with the id is unfinished, enqueueing
// another returns [ErrJobIDInUse].
func JobID(id string) EnqueueOption { return func(o *enqueueOptions) { o.id = id } }

// Unique holds key while the job is unfinished: until it succeeds, is
// dead-lettered or is cancelled, enqueueing another job with the same key
// returns [ErrDuplicate].
func Unique(key string) EnqueueOption {
	return func(o *enqueueOptions) { o.unique, o.uniqueFor = key, 0 }
}

// UniqueFor holds key for d after enqueueing, whatever happens to the job.
func UniqueFor(key string, d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) {
		if d <= 0 {
			o.err = errors.New("jobq: UniqueFor needs a positive duration")
		}
		if key == "" {
			o.err = errors.New("jobq: UniqueFor needs a key")
		}
		o.unique, o.uniqueFor = key, d
	}
}

// MaxAttempts overrides the queue's attempt limit for this job.
func MaxAttempts(n int) EnqueueOption { return func(o *enqueueOptions) { o.maxAttempts = n } }

// Timeout overrides the queue's attempt timeout for this job; zero means no limit.
func Timeout(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) {
		if d <= 0 {
			d = NoTimeout
		}
		o.timeout = d
	}
}

// WithHeader adds application metadata, for example trace context. Header
// names starting with [HeaderPrefix] are reserved.
func WithHeader(key, value string) EnqueueOption {
	return func(o *enqueueOptions) {
		if strings.ContainsAny(value, "\r\n\x00") {
			o.err = fmt.Errorf("jobq: header %q has a control character in its value", key)
		}
		if o.header == nil {
			o.header = map[string]string{}
		}
		o.header[key] = value
	}
}

// Enqueue stores job and returns its id. See [Driver.Enqueue] for errors.
func (c *Client) Enqueue(ctx context.Context, job Job, opts ...EnqueueOption) (string, error) {
	return c.EnqueueNamed(ctx, job.JobName(), job, opts...)
}

// EnqueueNamed stores payload, encoded as JSON, as a job named name and
// returns its id. A [json.RawMessage] or []byte payload is stored as is and
// must be valid JSON.
func (c *Client) EnqueueNamed(ctx context.Context, name string, payload any, opts ...EnqueueOption) (string, error) {
	msg, err := NewMessage(name, payload, opts...)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := c.driver.Enqueue(ctx, msg); err != nil {
		if errors.Is(err, ErrUncertain) {
			return msg.ID, err
		}
		return "", err
	}
	return msg.ID, nil
}

// NewMessage validates a job and builds the [Message] [Client.EnqueueNamed]
// would store, with a generated id unless [JobID] is given. Drivers use it
// for transactional enqueueing; applications rarely need it.
func NewMessage(name string, payload any, opts ...EnqueueOption) (*Message, error) {
	o := enqueueOptions{queue: DefaultQueue}
	for _, opt := range opts {
		opt(&o)
	}
	if o.err != nil {
		return nil, o.err
	}
	if err := validJobName(name); err != nil {
		return nil, err
	}
	if err := validName("queue", o.queue); err != nil {
		return nil, err
	}
	if o.maxAttempts < 0 {
		return nil, errors.New("jobq: max attempts must not be negative")
	}
	if o.unique != "" && (len(o.unique) > 512 || strings.ContainsAny(o.unique, "\x00")) {
		return nil, errors.New("jobq: unique key must be at most 512 bytes without NUL")
	}
	for key := range o.header {
		if err := validHeader(key); err != nil {
			return nil, err
		}
	}
	data, err := encodePayload(payload)
	if err != nil {
		return nil, fmt.Errorf("jobq: encode job %s: %w", name, err)
	}
	now := time.Now()
	id := o.id
	if id == "" {
		id = NewID()
	} else if err := validName("job id", id); err != nil {
		return nil, err
	}
	at := o.at
	if o.delay > 0 {
		at = now.Add(o.delay)
	}
	if !at.After(now) {
		at = time.Time{}
	}
	return &Message{
		ID:          id,
		Name:        name,
		Queue:       o.queue,
		Payload:     data,
		Header:      o.header,
		RunAt:       at,
		EnqueuedAt:  now,
		MaxAttempts: o.maxAttempts,
		Timeout:     o.timeout,
		UniqueKey:   o.unique,
		UniqueFor:   o.uniqueFor,
	}, nil
}

func encodePayload(payload any) ([]byte, error) {
	var data []byte
	switch p := payload.(type) {
	case RawJob:
		data = p.Payload
	case *RawJob:
		data = p.Payload
	case json.RawMessage:
		data = p
	case []byte:
		data = p
	default:
		return json.Marshal(payload)
	}
	if len(data) == 0 {
		return []byte("null"), nil
	}
	if !json.Valid(data) {
		return nil, errors.New("payload is not valid JSON")
	}
	return append([]byte(nil), data...), nil
}

func validHeader(key string) error {
	if key == "" || strings.ContainsAny(key, " :\r\n") {
		return fmt.Errorf("jobq: invalid header name %q", key)
	}
	if len(key) >= len(HeaderPrefix) && strings.EqualFold(key[:len(HeaderPrefix)], HeaderPrefix) {
		return fmt.Errorf("jobq: header %q is reserved", key)
	}
	return nil
}

// Cancel prevents the job with id from running again: a job waiting for its
// run time, a retry or a snooze. A running job finishes its current attempt
// but is not retried. It returns [ErrNotFound] when no cancelable job has id.
func (c *Client) Cancel(ctx context.Context, id string) error {
	if err := validName("job id", id); err != nil {
		return err
	}
	return c.driver.Cancel(ctx, id)
}
