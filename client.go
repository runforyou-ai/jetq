package jetq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Client is the producer side: it prepares submissions and hands them to a
// [Driver]. It is safe for concurrent use. A Client is not bound to a queue or
// a connection beyond the driver's own.
type Client struct {
	driver   Driver
	defaults Policy
	queue    string
	metadata Metadata
	logger   *slog.Logger
	now      func() time.Time
	newID    func() string
	// maxPayload bounds an encoded payload in bytes; 0 means no limit.
	maxPayload int
}

// ClientOption configures a [Client].
type ClientOption func(*Client)

// WithDefaultPolicy sets the producer's default policy. It is the lowest
// priority layer, below the task's defaults and the submission's overrides.
func WithDefaultPolicy(p Policy) ClientOption {
	return func(c *Client) { c.defaults = clonePolicy(p) }
}

// WithDefaultQueue sets the queue used when a submission does not name one
// (default [DefaultQueue]). A task itself is never bound to a queue, so the
// same task can be submitted to several queues.
func WithDefaultQueue(queue string) ClientOption {
	return func(c *Client) { c.queue = queue }
}

// WithMetadata sets application metadata added to every submission. Metadata
// of a submission overrides it per key.
func WithMetadata(m Metadata) ClientOption {
	return func(c *Client) { c.metadata = m.Clone() }
}

// WithMaxPayloadBytes bounds the encoded JSON payload of every submission:
// typed, raw, dynamic, prepared and redriven. A larger payload is rejected
// with [ErrPayloadTooLarge] before it reaches the driver. 0 (the default)
// means no producer limit; a negative value is an error from [NewClient].
// The limit is about business payloads only: the driver's own record limits
// are a separate deployment concern.
func WithMaxPayloadBytes(n int) ClientOption {
	return func(c *Client) { c.maxPayload = n }
}

// ErrPayloadTooLarge is returned when a payload exceeds
// [WithMaxPayloadBytes].
var ErrPayloadTooLarge = errors.New("jetq: payload too large")

// WithLogger sets the logger of the client and workers it creates.
func WithLogger(l *slog.Logger) ClientOption {
	return func(c *Client) { c.logger = l }
}

// NewClient returns a producer on driver. The driver owns storage; the client
// never touches the transport directly. Validate the options and the default
// queue before the first submission.
func NewClient(driver Driver, opts ...ClientOption) (*Client, error) {
	if driver == nil {
		return nil, errors.New("jetq: nil driver")
	}
	c := &Client{
		driver: driver,
		queue:  DefaultQueue,
		logger: slog.Default(),
		now:    time.Now,
		newID:  newJobID,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.queue == "" {
		c.queue = DefaultQueue
	}
	if err := ValidateID("queue", c.queue); err != nil {
		return nil, err
	}
	if err := c.defaults.validate(); err != nil {
		return nil, err
	}
	if c.maxPayload < 0 {
		return nil, fmt.Errorf("jetq: max payload bytes %d is negative", c.maxPayload)
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c, nil
}

// Driver returns the driver the client submits to.
func (c *Client) Driver() Driver { return c.driver }

// Prepare resolves a submission without storing it. The result can be
// submitted any number of times: the job id, run time and policy are fixed
// here, so a retried idempotent submission never moves its execution time.
func (c *Client) Prepare(name string, payload any, sub Submission) (*PreparedJob, error) {
	data, err := EncodePayload(name, payload)
	if err != nil {
		return nil, err
	}
	return c.prepare(name, Policy{}, data, sub)
}

// Submit stores payload for task. Typed, raw and dynamically named
// submissions all resolve the same [Envelope] and [ExecutionPolicy]:
// submission overrides, then the task's defaults, then the producer's
// defaults.
func (c *Client) Submit(ctx context.Context, name string, payload any, sub Submission) (EnqueueResult, error) {
	data, err := EncodePayload(name, payload)
	if err != nil {
		return EnqueueResult{}, err
	}
	return c.submitData(ctx, name, data, sub)
}

// SubmitTask is the typed form of [Client.Submit]; task's defaults apply.
func SubmitTask[T any](ctx context.Context, c *Client, task Task[T], payload T, sub Submission) (EnqueueResult, error) {
	data, err := task.Encode(payload)
	if err != nil {
		return EnqueueResult{}, err
	}
	job, err := c.prepare(task.name, task.policy, data, sub)
	if err != nil {
		return EnqueueResult{}, err
	}
	return c.driver.Submit(ctx, job)
}

// SubmitRaw stores an already encoded JSON payload for task. The payload is
// stored byte for byte and the task's default policy applies, exactly as for
// a typed submission.
func (c *Client) SubmitRaw(ctx context.Context, task Task[json.RawMessage], payload json.RawMessage, sub Submission) (EnqueueResult, error) {
	data, err := task.Encode(payload)
	if err != nil {
		return EnqueueResult{}, err
	}
	job, err := c.prepare(task.name, task.policy, data, sub)
	if err != nil {
		return EnqueueResult{}, err
	}
	return c.driver.Submit(ctx, job)
}

func (c *Client) submitData(ctx context.Context, name string, data json.RawMessage, sub Submission) (EnqueueResult, error) {
	job, err := c.prepare(name, Policy{}, data, sub)
	if err != nil {
		return EnqueueResult{}, err
	}
	return c.driver.Submit(ctx, job)
}

// checkPayload enforces the producer's payload limit.
func (c *Client) checkPayload(task string, data []byte) error {
	if c.maxPayload > 0 && len(data) > c.maxPayload {
		return fmt.Errorf("%w: task %s payload is %d bytes, limit %d", ErrPayloadTooLarge, task, len(data), c.maxPayload)
	}
	return nil
}

func (c *Client) prepare(name string, taskDefaults Policy, data []byte, sub Submission) (*PreparedJob, error) {
	if err := c.checkPayload(name, data); err != nil {
		return nil, err
	}
	if sub.Queue == "" {
		sub.Queue = c.queue
	}
	if sub.Metadata != nil || c.metadata != nil {
		merged := c.metadata.Clone()
		if merged == nil {
			merged = Metadata{}
		}
		for k, v := range sub.Metadata {
			merged[k] = v
		}
		sub.Metadata = merged
	}
	return Prepare(name, data, sub, taskDefaults, c.defaults, c.now(), c.newID)
}

// SubmitPrepared stores a prepared job. Calling it again with the same value
// is safe: the driver recognizes an idempotent submission by its EnqueueKey,
// and a job id that already holds the same content is reported as
// AlreadyAccepted.
func (c *Client) SubmitPrepared(ctx context.Context, job *PreparedJob) (EnqueueResult, error) {
	if job == nil {
		return EnqueueResult{}, errors.New("jetq: nil prepared job")
	}
	// A job prepared elsewhere (or by package-level Prepare) is checked
	// here too, so no path reaches the driver over the limit.
	if err := c.checkPayload(job.Envelope.Task, job.Envelope.Payload); err != nil {
		return EnqueueResult{}, err
	}
	return c.driver.Submit(ctx, job)
}

// Raw returns a task whose payload is passed through as JSON, so task names
// and payloads only known at runtime use the same path as typed tasks.
func Raw(name string) Task[json.RawMessage] {
	return MustTask[json.RawMessage](name, Policy{})
}

// newJobID returns a random job id: 128 random bits, hex encoded. Job ids only
// need to be unique, not guessable, so no external dependency is needed.
func newJobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("jetq: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
