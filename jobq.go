// Package jobq is a background job queue for Go with pluggable storage
// drivers.
//
// Jobs are plain Go structs with a stable name. A [Client] enqueues them,
// optionally delayed or deduplicated, and a [Worker] runs them with retries,
// backoff, timeouts, lease keep-alive and dead-lettering. Storage is provided
// by a [Driver]: jobq/natsq stores jobs in NATS JetStream, jobq/pgq in
// PostgreSQL and jobq/memq in memory. The semantics of retries, snoozes,
// timeouts and shutdown live in this package, so they are the same on every
// driver; jobq/drivertest checks that a driver keeps its side of the
// contract.
//
// Delivery is at-least-once: handlers must be idempotent.
package jobq

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"
)

// DefaultQueue is the queue used when a job is enqueued without [OnQueue].
const DefaultQueue = "default"

// HeaderPrefix starts header names reserved for jobq and its drivers.
const HeaderPrefix = "Jobq-"

// Client enqueues jobs, cancels them, synchronises recurring schedules,
// inspects queues and creates workers. It is safe for concurrent use.
type Client struct {
	driver Driver
	logger *slog.Logger
}

type config struct {
	logger *slog.Logger
}

// Option configures a [Client].
type Option func(*config)

// WithLogger sets the logger used by the client and its workers (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// New returns a client storing jobs with driver. The client does not own the
// driver: close the driver yourself once workers have stopped.
func New(driver Driver, opts ...Option) *Client {
	if driver == nil {
		panic("jobq: driver is nil")
	}
	cfg := config{logger: slog.Default()}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	return &Client{driver: driver, logger: cfg.logger}
}

// Driver returns the driver the client was created with.
func (c *Client) Driver() Driver { return c.driver }

var (
	// ErrDuplicate is returned by [Client.Enqueue] when a job with the same
	// unique key holds it ([Unique], [UniqueFor]). The job is not enqueued.
	ErrDuplicate = errors.New("jobq: duplicate job")
	// ErrJobIDInUse is returned by [Client.Enqueue] when an unfinished job
	// has the id given with [JobID]. The job is not enqueued.
	ErrJobIDInUse = errors.New("jobq: job id in use")
	// ErrUncertain is wrapped by errors of [Client.Enqueue] when the job may
	// or may not have been stored, for example after a timeout or a lost
	// connection. Enqueue returns the job id with the error. Enqueueing the
	// job again with the same [JobID] returns ErrJobIDInUse if the first one
	// was stored and has not finished yet.
	ErrUncertain = errors.New("jobq: enqueue outcome unknown")
	// ErrNotFound is returned when no job, or no job in the required state,
	// has the given id.
	ErrNotFound = errors.New("jobq: job not found")
	// ErrLeaseLost is returned by [Lease] methods when the driver knows the
	// lease expired and the job may be claimed by another worker. The worker
	// cancels the handler's context with it as cause.
	ErrLeaseLost = errors.New("jobq: lease lost")
	// ErrUnsupported is returned when the driver does not implement an
	// optional capability such as [Scheduler] or [Inspector].
	ErrUnsupported = errors.New("jobq: not supported by driver")
	// ErrTimeout is the cause of the context cancellation when a job exceeds
	// its timeout ([Queue.Timeout] or [Timeout]), and the error the attempt
	// fails with. Read it with context.Cause in the handler.
	ErrTimeout = errors.New("jobq: job timed out")
)

var nameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// ValidName reports whether name is a valid queue name, job id or schedule
// key: 1 to 128 letters, digits, '-' and '_'. Drivers may rely on it.
func ValidName(name string) bool { return nameRE.MatchString(name) }

func validName(kind, name string) error {
	if !ValidName(name) {
		return fmt.Errorf("jobq: invalid %s %q: use 1 to 128 letters, digits, '-' and '_'", kind, name)
	}
	return nil
}

// validJobName accepts any non-empty name without control characters.
func validJobName(name string) error {
	if name == "" || len(name) > 255 {
		return fmt.Errorf("jobq: invalid job name %q: use 1 to 255 bytes", name)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("jobq: invalid job name %q: control characters are not allowed", name)
		}
	}
	return nil
}

var idEncoding = base32.NewEncoding("0123456789abcdefghjkmnpqrstvwxyz").WithPadding(base32.NoPadding)

// NewID returns a new job id: 26 characters that sort by creation time to the
// millisecond, followed by 80 random bits.
func NewID() string {
	var b [16]byte
	var ms [8]byte
	binary.BigEndian.PutUint64(ms[:], uint64(time.Now().UnixMilli()))
	copy(b[:6], ms[2:])
	_, _ = rand.Read(b[6:])
	return idEncoding.EncodeToString(b[:])
}
