// Package jetq is a background job queue built on NATS JetStream.
//
// Jobs are plain Go structs that implement [Job]. A [Client] enqueues them,
// optionally delayed or deduplicated, and a [Worker] runs them with retries,
// backoff, long-running job keep-alive and a dead-letter stream. Delayed and
// recurring (cron) jobs are scheduled by the NATS server itself, so no leader
// election or polling is needed in the application.
//
// All queues share one work-queue stream. Each queue is a filtered durable pull
// consumer on that stream, so adding a queue never requires a new stream.
//
// Delivery is at-least-once: handlers must be idempotent.
package jetq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Message headers written by jetq.
const (
	HeaderJob         = "Jetq-Job"
	HeaderID          = "Jetq-Id"
	HeaderEnqueuedAt  = "Jetq-Enqueued-At"
	HeaderMaxAttempts = "Jetq-Max-Attempts"
	HeaderQueue       = "Jetq-Queue"
	HeaderError       = "Jetq-Error"
	HeaderAttempts    = "Jetq-Attempts"
	HeaderFailedAt    = "Jetq-Failed-At"
)

// NATS message scheduling headers (nats-server 2.12+, time zones 2.14+).
const (
	headerSchedule         = "Nats-Schedule"
	headerScheduleTarget   = "Nats-Schedule-Target"
	headerScheduleTimeZone = "Nats-Schedule-Time-Zone"
	headerScheduler        = "Nats-Scheduler"
)

// DefaultQueue is the queue used when a job is enqueued without [OnQueue].
const DefaultQueue = "default"

var nameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Client enqueues jobs, cancels delayed jobs, synchronises recurring schedules
// and creates workers. It is safe for concurrent use.
type Client struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	dead   jetstream.Stream
	cfg    config
}

type config struct {
	streamName  string
	deadName    string
	prefix      string
	replicas    int
	storage     jetstream.StorageType
	duplicates  time.Duration
	deadMaxAge  time.Duration
	logger      *slog.Logger
	maxJobBytes int32
}

// Option configures a [Client].
type Option func(*config)

// WithStreamName sets the work stream name (default "JETQ"). The dead-letter
// stream is named after it with a "_DEAD" suffix.
func WithStreamName(name string) Option { return func(c *config) { c.streamName = name } }

// WithSubjectPrefix sets the subject prefix of every subject jetq uses
// (default "jetq"). It must be a single subject token.
func WithSubjectPrefix(prefix string) Option { return func(c *config) { c.prefix = prefix } }

// WithReplicas sets the replica count of both streams (default 1).
func WithReplicas(n int) Option { return func(c *config) { c.replicas = n } }

// WithMemoryStorage keeps both streams in memory instead of on disk.
func WithMemoryStorage() Option { return func(c *config) { c.storage = jetstream.MemoryStorage } }

// WithDuplicateWindow sets how long [Unique] keys are remembered (default 2 minutes).
func WithDuplicateWindow(d time.Duration) Option { return func(c *config) { c.duplicates = d } }

// WithDeadLetterMaxAge sets how long dead-lettered jobs are kept (default 14 days).
func WithDeadLetterMaxAge(d time.Duration) Option { return func(c *config) { c.deadMaxAge = d } }

// WithMaxJobBytes limits the encoded size of a single job (default: server limit).
func WithMaxJobBytes(n int32) Option { return func(c *config) { c.maxJobBytes = n } }

// WithLogger sets the logger used by the client and its workers (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// New creates or updates the jetq streams and returns a client.
func New(ctx context.Context, js jetstream.JetStream, opts ...Option) (*Client, error) {
	cfg := config{
		streamName: "JETQ",
		prefix:     "jetq",
		replicas:   1,
		storage:    jetstream.FileStorage,
		duplicates: 2 * time.Minute,
		deadMaxAge: 14 * 24 * time.Hour,
		logger:     slog.Default(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.deadName == "" {
		cfg.deadName = cfg.streamName + "_DEAD"
	}
	if !nameRE.MatchString(cfg.prefix) {
		return nil, fmt.Errorf("jetq: invalid subject prefix %q", cfg.prefix)
	}
	if strings.ContainsAny(cfg.streamName, ". *>") || cfg.streamName == "" {
		return nil, fmt.Errorf("jetq: invalid stream name %q", cfg.streamName)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              cfg.streamName,
		Description:       "jetq jobs",
		Subjects:          []string{cfg.prefix + ".q.>", cfg.prefix + ".at.>", cfg.prefix + ".cron.>"},
		Retention:         jetstream.WorkQueuePolicy,
		Storage:           cfg.storage,
		Replicas:          cfg.replicas,
		Duplicates:        cfg.duplicates,
		MaxMsgSize:        cfg.maxJobBytes,
		AllowMsgSchedules: true,
		AllowRollup:       true,
	})
	if err != nil {
		return nil, fmt.Errorf("jetq: create stream %s: %w", cfg.streamName, err)
	}
	dead, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        cfg.deadName,
		Description: "jetq dead-lettered jobs",
		Subjects:    []string{cfg.prefix + ".dead.>"},
		Retention:   jetstream.LimitsPolicy,
		Storage:     cfg.storage,
		Replicas:    cfg.replicas,
		MaxAge:      cfg.deadMaxAge,
	})
	if err != nil {
		return nil, fmt.Errorf("jetq: create stream %s: %w", cfg.deadName, err)
	}
	return &Client{js: js, stream: stream, dead: dead, cfg: cfg}, nil
}

// JetStream returns the JetStream context the client was created with.
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// StreamName returns the work stream name.
func (c *Client) StreamName() string { return c.cfg.streamName }

// DeadLetterStreamName returns the dead-letter stream name.
func (c *Client) DeadLetterStreamName() string { return c.cfg.deadName }

func (c *Client) queueSubject(queue string) string { return c.cfg.prefix + ".q." + queue }
func (c *Client) delaySubject(id string) string    { return c.cfg.prefix + ".at." + id }
func (c *Client) cronSubject(key string) string    { return c.cfg.prefix + ".cron." + key }
func (c *Client) deadSubject(queue string) string  { return c.cfg.prefix + ".dead." + queue }

func validName(kind, name string) error {
	if !nameRE.MatchString(name) {
		return fmt.Errorf("jetq: invalid %s name %q: use letters, digits, '-' and '_'", kind, name)
	}
	return nil
}

// ErrDuplicate is returned by [Client.Enqueue] when a job with the same
// [Unique] key was enqueued within the duplicate window. The job is not enqueued again.
var ErrDuplicate = errors.New("jetq: duplicate job")

// ErrNotFound is returned by [Client.Cancel] when no pending delayed job has the given id.
var ErrNotFound = errors.New("jetq: delayed job not found")
