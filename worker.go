package jetq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Queue configures how a [Worker] consumes one queue.
type Queue struct {
	// Name of the queue.
	Name string
	// Concurrency is the number of jobs this worker runs at once (default 1).
	Concurrency int
	// MaxAttempts is the number of attempts before a job is dead-lettered
	// (default 3). [MaxAttempts] overrides it per job.
	MaxAttempts int
	// Backoff decides the delay before the next attempt (default
	// Exponential(5s, 10m)). [RetryAfter] overrides it per attempt.
	Backoff Backoff
	// AckWait is how long the server waits for a sign of life before
	// redelivering a job to another worker (default 1m). Running handlers are
	// kept alive automatically, so it only bounds recovery after a crash.
	AckWait time.Duration
	// Timeout is how long one attempt may run before its context is cancelled
	// and the attempt fails with [ErrTimeout] (default: no limit). [Timeout]
	// overrides it per job.
	Timeout time.Duration
}

// Middleware wraps every job attempt. Use [JobInfo] to inspect the job.
type Middleware func(ctx context.Context, next func(context.Context) error) error

// FailedFunc is called once a job is dead-lettered, with the error of its last attempt.
type FailedFunc func(ctx context.Context, info Info, payload []byte, err error)

// Worker runs registered handlers for jobs on its queues.
type Worker struct {
	client          *Client
	queues          []Queue
	handlers        map[string]*handler
	middleware      []Middleware
	onFailed        []FailedFunc
	logContext      func(context.Context, Info) context.Context
	shutdownTimeout time.Duration
	unknownTimeout  time.Duration
	started         atomic.Bool
}

type handler struct {
	run func(ctx context.Context, payload []byte) error
	// failed runs the handler's failure callback; it returns an error when the
	// callback could not run.
	failed func(ctx context.Context, payload []byte, err error) error
}

// NewWorker returns a worker for the given queues. Register handlers with
// [Handle] before calling [Worker.Run].
func (c *Client) NewWorker(queues ...Queue) *Worker {
	return &Worker{
		client:          c,
		queues:          queues,
		handlers:        map[string]*handler{},
		shutdownTimeout: 30 * time.Second,
		unknownTimeout:  time.Hour,
	}
}

// Use appends middleware; the first one added is the outermost.
func (w *Worker) Use(middleware ...Middleware) {
	w.mustNotBeStarted()
	w.middleware = append(w.middleware, middleware...)
}

// OnFailed registers a callback for jobs that are dead-lettered.
func (w *Worker) OnFailed(fn FailedFunc) {
	w.mustNotBeStarted()
	w.onFailed = append(w.onFailed, fn)
}

// SetLogContext sets a function that derives the context used for jetq's own
// log records about a job (retries, dead-lettering, failure callbacks), for
// example to attach the application's trace or tenant fields that a logging
// handler reads from the context. Failure callbacks receive the derived
// context too. fn runs while the job is kept alive; if it panics, the panic is
// logged and the original context is used.
func (w *Worker) SetLogContext(fn func(ctx context.Context, info Info) context.Context) {
	w.mustNotBeStarted()
	w.logContext = fn
}

// SetShutdownTimeout sets how long [Worker.Run] waits for running jobs after
// its context is cancelled before cancelling their contexts (default 30s).
// Jobs interrupted this way are put back without using up an attempt.
func (w *Worker) SetShutdownTimeout(d time.Duration) {
	w.mustNotBeStarted()
	w.shutdownTimeout = d
}

// SetUnknownJobTimeout sets how long this worker puts back jobs it has no
// handler for before dead-lettering them (default 1h), counted from the first
// time a worker found no handler for the job. Until then such a job is put
// back every 10 seconds without using up an attempt, so that a worker of a
// newer version or of another service can run it, for example during a rolling
// deploy. A non-positive d puts them back forever.
func (w *Worker) SetUnknownJobTimeout(d time.Duration) {
	w.mustNotBeStarted()
	w.unknownTimeout = d
}

func (w *Worker) mustNotBeStarted() {
	if w.started.Load() {
		panic("jetq: worker already started")
	}
}

// HandleOption configures a handler registered with [Handle].
type HandleOption[T Job] func(*handler)

// OnFailure registers a typed callback for when a job of this type is
// dead-lettered, like Laravel's failed() method. If the payload does not
// decode into T, the callback is skipped and a warning is logged.
func OnFailure[T Job](fn func(ctx context.Context, job T, err error)) HandleOption[T] {
	return func(h *handler) {
		h.failed = func(ctx context.Context, payload []byte, err error) error {
			var job T
			if decodeErr := json.Unmarshal(payload, &job); decodeErr != nil {
				return decodeErr
			}
			fn(ctx, job, err)
			return nil
		}
	}
}

// Handle registers fn for jobs of type T. It panics if a handler for the same
// job name is already registered or the worker has started.
func Handle[T Job](w *Worker, fn func(ctx context.Context, job T) error, opts ...HandleOption[T]) {
	var zero T
	name := zero.JobName()
	h := &handler{run: func(ctx context.Context, payload []byte) error {
		var job T
		if err := json.Unmarshal(payload, &job); err != nil {
			return Permanent(fmt.Errorf("jetq: decode job %s: %w", name, err))
		}
		return fn(ctx, job)
	}}
	for _, opt := range opts {
		opt(h)
	}
	w.register(name, h)
}

func (w *Worker) register(name string, h *handler) {
	w.mustNotBeStarted()
	if name == "" {
		panic("jetq: job name is empty")
	}
	if _, exists := w.handlers[name]; exists {
		panic("jetq: handler already registered for job " + name)
	}
	w.handlers[name] = h
}

// Run consumes the worker's queues until ctx is cancelled, then stops taking
// new jobs and waits for running ones (see [Worker.SetShutdownTimeout]).
// Handlers that ignore the cancellation of their context are abandoned 10
// seconds after it, so Run returns even if a handler never does.
func (w *Worker) Run(ctx context.Context) error {
	if !w.started.CompareAndSwap(false, true) {
		return errors.New("jetq: worker already started")
	}
	if len(w.queues) == 0 {
		return errors.New("jetq: worker has no queues")
	}
	queues := make([]Queue, len(w.queues))
	consumers := make([]jetstream.Consumer, len(w.queues))
	for i, q := range w.queues {
		q = withQueueDefaults(q)
		if err := validName("queue", q.Name); err != nil {
			return err
		}
		consumer, err := w.consumer(ctx, q)
		if err != nil {
			return err
		}
		queues[i], consumers[i] = q, consumer
	}

	// Handlers get their own context so running jobs survive ctx cancellation
	// until the shutdown timeout.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()
	var running sync.WaitGroup
	var loops sync.WaitGroup
	for i := range queues {
		loops.Add(1)
		go func() {
			defer loops.Done()
			w.consume(ctx, jobCtx, queues[i], consumers[i], &running)
		}()
	}
	loops.Wait()

	done := make(chan struct{})
	go func() {
		running.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(w.shutdownTimeout):
		w.client.cfg.logger.WarnContext(ctx, "jetq shutdown timeout, cancelling running jobs")
		cancelJobs()
		// Bounded: handlers are abandoned abandonAfter after their context is
		// cancelled, then settlement runs under settleTimeout.
		<-done
	}
	return nil
}

// consumer creates or updates the durable consumer of queue q.
func (w *Worker) consumer(ctx context.Context, q Queue) (jetstream.Consumer, error) {
	consumer, err := w.client.js.CreateOrUpdateConsumer(ctx, w.client.cfg.streamName, jetstream.ConsumerConfig{
		Durable:       "jetq-" + q.Name,
		Description:   "jetq queue " + q.Name,
		FilterSubject: w.client.queueSubject(q.Name),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       q.AckWait,
		MaxDeliver:    -1,
	})
	if err != nil {
		return nil, fmt.Errorf("jetq: create consumer for queue %s: %w", q.Name, err)
	}
	return consumer, nil
}

func withQueueDefaults(q Queue) Queue {
	if q.Concurrency <= 0 {
		q.Concurrency = 1
	}
	if q.MaxAttempts <= 0 {
		q.MaxAttempts = 3
	}
	if q.Backoff == nil {
		q.Backoff = Exponential(5*time.Second, 10*time.Minute)
	}
	if q.AckWait <= 0 {
		q.AckWait = time.Minute
	}
	q.Timeout = max(q.Timeout, 0)
	return q
}

// consume fetches at most as many messages as there are free slots, so jobs
// never wait in a local buffer while their ack timer runs.
func (w *Worker) consume(ctx, jobCtx context.Context, q Queue, consumer jetstream.Consumer, running *sync.WaitGroup) {
	slots := make(chan struct{}, q.Concurrency)
	for i := 0; i < q.Concurrency; i++ {
		slots <- struct{}{}
	}
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-slots:
		}
		free := 1
		for free < q.Concurrency {
			select {
			case <-slots:
				free++
				continue
			default:
			}
			break
		}
		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		batch, err := consumer.Fetch(free, jetstream.FetchContext(fetchCtx))
		received := 0
		if err == nil {
			for msg := range batch.Messages() {
				received++
				running.Add(1)
				go func() {
					defer running.Done()
					defer func() { slots <- struct{}{} }()
					w.process(ctx, jobCtx, q, msg)
				}()
			}
			err = batch.Error()
		}
		cancel()
		for ; received < free; received++ {
			slots <- struct{}{}
		}
		if err == nil || ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
			failures = 0
			continue
		}
		failures++
		w.client.cfg.logger.WarnContext(ctx, "jetq fetch failed", "queue", q.Name, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		// The consumer was deleted or stopped sending heartbeats, or fetching
		// keeps failing: create it again.
		if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) ||
			errors.Is(err, jetstream.ErrNoHeartbeat) || failures%recreateAfter == 0 {
			if recreated, err := w.consumer(ctx, q); err == nil {
				consumer = recreated
			} else if ctx.Err() == nil {
				w.client.cfg.logger.WarnContext(ctx, "jetq consumer recreation failed", "queue", q.Name, "error", err)
			}
		}
	}
}

func (w *Worker) process(runCtx, jobCtx context.Context, q Queue, msg jetstream.Msg) {
	logger := w.client.cfg.logger
	meta, err := msg.Metadata()
	if err != nil {
		logger.ErrorContext(runCtx, "jetq message without metadata", "queue", q.Name, "error", err)
		_ = msg.Term()
		return
	}
	header := msg.Headers()
	info := Info{
		ID:          header.Get(HeaderID),
		Name:        header.Get(HeaderJob),
		Queue:       q.Name,
		Attempt:     int(meta.NumDelivered),
		MaxAttempts: q.MaxAttempts,
		Timeout:     q.Timeout,
		Header:      header,
	}
	if base, err := strconv.Atoi(header.Get(headerAttemptBase)); err == nil && base > 0 {
		info.Attempt += base
	}
	if n, err := strconv.Atoi(header.Get(HeaderMaxAttempts)); err == nil && n > 0 {
		info.MaxAttempts = n
	}
	if d, err := time.ParseDuration(header.Get(HeaderTimeout)); err == nil {
		info.Timeout = max(d, 0)
	}
	if n, err := strconv.Atoi(header.Get(headerSnoozes)); err == nil && n > 0 {
		info.Snoozes = n
	}
	if t, err := time.Parse(time.RFC3339Nano, header.Get(HeaderEnqueuedAt)); err == nil {
		info.EnqueuedAt = t
	}
	if info.ID == "" {
		key := strings.TrimPrefix(header.Get(headerScheduler), w.client.cronSubject(""))
		info.ID = fmt.Sprintf("%s-%d", key, meta.Sequence.Stream)
	}
	d := delivery{msg: msg, seq: meta.Sequence.Stream, redelivered: meta.NumDelivered - 1, info: info, queue: q}

	// The keep-alive covers both the handler and settlement (dead-lettering,
	// failure callbacks, requeueing), so the job is not redelivered meanwhile.
	stopKeepAlive := keepAlive(msg, q.AckWait)
	defer stopKeepAlive()
	runCtx = w.deriveLogContext(runCtx, info)
	// Settlement must finish even when Run's context is cancelled during the
	// shutdown grace period; its timeout starts when settlement starts.
	newSettleCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.WithoutCancel(runCtx), settleTimeout)
	}

	cancelled, err := w.client.cancelled(runCtx, header, info.ID, meta.NumDelivered > 1)
	if err != nil {
		// Whether the job was cancelled is unknown: put it back without running
		// it, so the check does not use up attempts.
		logger.WarnContext(runCtx, "jetq cancellation check failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", err)
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		w.requeue(settleCtx, d, cancelCheckRetry, info.Attempt-1, nil)
		return
	}
	if cancelled {
		// Cancelled while the schedule was firing: drop the copy unrun.
		w.releaseLock(runCtx, msg, info)
		if ackErr := msg.Ack(); ackErr != nil {
			logger.WarnContext(runCtx, "jetq ack failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", ackErr)
		}
		return
	}

	h, ok := w.handlers[info.Name]
	if !ok {
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		w.unknownJob(settleCtx, d)
		return
	}

	if info.Attempt > info.MaxAttempts {
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		// Redelivered after a crash on the last attempt: do not run the handler again.
		w.deadLetter(settleCtx, d, fmt.Errorf("jetq: attempt %d exceeds limit %d after redelivery", info.Attempt, info.MaxAttempts))
		return
	}

	err = w.attempt(runCtx, jobCtx, info, h, msg.Data())
	settleCtx, cancelSettle := newSettleCtx()
	defer cancelSettle()

	var snooze *snoozeError
	var retry *retryAfterError
	switch {
	case err == nil:
		w.dropRequeued(settleCtx, d)
		// Release the unique lock before acking: a lost ack then risks a
		// duplicate run, never a key that stays locked.
		w.releaseLock(settleCtx, msg, info)
		if ackErr := msg.Ack(); ackErr != nil {
			logger.WarnContext(settleCtx, "jetq ack failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", ackErr)
		}
	case errors.As(err, &snooze):
		w.requeue(settleCtx, d, snooze.delay, info.Attempt-1, map[string]string{headerSnoozes: strconv.Itoa(info.Snoozes + 1)})
	case IsPermanent(err):
		w.deadLetter(settleCtx, d, err)
	case jobCtx.Err() != nil && !errors.Is(err, ErrTimeout):
		// The shutdown timeout interrupted the job: put it back right away
		// without using up the attempt.
		logger.WarnContext(settleCtx, "jetq job interrupted by shutdown, putting it back", "queue", q.Name, "job", info.Name, "id", info.ID,
			"attempt", info.Attempt, "error", err)
		w.requeue(settleCtx, d, 0, info.Attempt-1, nil)
	case info.Attempt >= info.MaxAttempts:
		w.deadLetter(settleCtx, d, err)
	default:
		delay := q.Backoff(info.Attempt)
		if errors.As(err, &retry) {
			delay = retry.delay
		}
		logger.WarnContext(settleCtx, "jetq job failed, will retry", "queue", q.Name, "job", info.Name, "id", info.ID,
			"attempt", info.Attempt, "max_attempts", info.MaxAttempts, "retry_in", delay, "error", err)
		if delay < minRequeueDelay {
			// Short delays: redeliver the same message, which counts the next attempt.
			if nakErr := msg.NakWithDelay(delay); nakErr != nil {
				logger.WarnContext(settleCtx, "jetq nak failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", nakErr)
			}
			return
		}
		w.requeue(settleCtx, d, delay, info.Attempt, nil)
	}
}

// recreateAfter is the number of consecutive fetch failures after which a
// worker creates its consumer again.
const recreateAfter = 5

// minRequeueDelay is the shortest retry delay for which a failed job is put
// back as a delayed job; shorter delays nak the delivery, because the server
// fires delayed jobs no sooner than 250ms.
const minRequeueDelay = time.Second

// delivery is one delivery of a job to this worker.
type delivery struct {
	msg         jetstream.Msg
	seq         uint64 // stream sequence of the message
	redelivered uint64 // deliveries of the message before this one
	info        Info
	queue       Queue
}

// cancelCheckRetry is the delay before redelivering a job whose cancellation
// could not be checked.
const cancelCheckRetry = 5 * time.Second

// unknownJobRetry is the delay before a job without a handler is offered again.
var unknownJobRetry = 10 * time.Second

// settleTimeout bounds publishing to the dead-letter stream, requeueing and failure callbacks.
var settleTimeout = 30 * time.Second

// abandonAfter is how long a handler may take to return after its context was
// cancelled, by its timeout or by shutdown, before the worker stops waiting.
var abandonAfter = 10 * time.Second

// ErrTimeout is the cause of the context cancellation when a job exceeds its
// timeout ([Queue.Timeout] or [Timeout]), and the error the attempt fails with.
// Read it with context.Cause in the handler.
var ErrTimeout = errors.New("jetq: job timed out")

// attempt runs one attempt of the job under its timeout. A handler that has
// not returned abandonAfter after its context was cancelled is abandoned: it
// keeps running in the background, and the attempt fails.
func (w *Worker) attempt(logCtx, jobCtx context.Context, info Info, h *handler, payload []byte) error {
	ctx, cancel := context.WithCancel(context.WithValue(jobCtx, infoKey{}, info))
	defer cancel()
	if info.Timeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeoutCause(ctx, info.Timeout, ErrTimeout)
		defer cancelTimeout()
	}
	result := make(chan error, 1)
	go func() { result <- w.run(ctx, info, h, payload) }()
	var err error
	select {
	case err = <-result:
	case <-ctx.Done():
		abandon := time.NewTimer(abandonAfter)
		defer abandon.Stop()
		select {
		case err = <-result:
		case <-abandon.C:
			w.client.cfg.logger.ErrorContext(logCtx, "jetq job ignored cancellation, abandoning it", "queue", info.Queue, "job", info.Name, "id", info.ID,
				"attempt", info.Attempt, "cause", context.Cause(ctx))
			err = fmt.Errorf("jetq: job %s abandoned, it did not return within %s of cancellation: %w", info.Name, abandonAfter, context.Cause(ctx))
		}
	}
	// Once the timeout has passed, also when shutdown followed, the attempt is
	// an ordinary failure whatever the handler returned: its error only
	// contributes its text, so snoozes, RetryAfter and Permanent do not apply.
	if errors.Is(context.Cause(ctx), ErrTimeout) {
		if err == nil {
			err = context.DeadlineExceeded
		}
		if !errors.Is(err, ErrTimeout) {
			err = fmt.Errorf("%w after %s: %s", ErrTimeout, info.Timeout, err.Error())
		} else {
			err = fmt.Errorf("%w after %s: %s", ErrTimeout, info.Timeout, strings.TrimPrefix(err.Error(), ErrTimeout.Error()+": "))
		}
	}
	return err
}

// run invokes the handler through the middleware chain, turning panics into errors.
func (w *Worker) run(ctx context.Context, info Info, h *handler, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jetq: job %s panicked: %v\n%s", info.Name, r, debug.Stack())
		}
	}()
	next := func(ctx context.Context) error { return h.run(ctx, payload) }
	for i := len(w.middleware) - 1; i >= 0; i-- {
		mw, inner := w.middleware[i], next
		next = func(ctx context.Context) error { return mw(ctx, inner) }
	}
	return next(ctx)
}

// unknownJob puts back a job this worker has no handler for, so that another
// worker can run it, and dead-letters it once the unknown job timeout has passed.
func (w *Worker) unknownJob(ctx context.Context, d delivery) {
	info := d.info
	cause := fmt.Errorf("jetq: no handler registered for job %q", info.Name)
	now := time.Now()
	since, err := time.Parse(time.RFC3339Nano, info.Header.Get(headerUnknownSince))
	if err != nil {
		since = now
	}
	delay := unknownJobRetry
	if w.unknownTimeout > 0 {
		left := w.unknownTimeout - now.Sub(since)
		if left <= 0 {
			w.deadLetter(ctx, d, cause)
			return
		}
		delay = min(delay, left)
	}
	w.client.cfg.logger.WarnContext(ctx, "jetq no handler for job, putting it back", "queue", info.Queue, "job", info.Name, "id", info.ID,
		"unknown_since", since, "retry_in", delay)
	w.requeue(ctx, d, delay, info.Attempt-1, map[string]string{headerUnknownSince: since.UTC().Format(time.RFC3339Nano)})
}

// deriveLogContext applies the worker's log context function, falling back to
// ctx when it is unset, panics or returns nil.
func (w *Worker) deriveLogContext(ctx context.Context, info Info) (derived context.Context) {
	if w.logContext == nil {
		return ctx
	}
	defer func() {
		if r := recover(); r != nil {
			w.client.cfg.logger.ErrorContext(ctx, "jetq log context panicked", "job", info.Name, "id", info.ID,
				"panic", r, "stack", string(debug.Stack()))
			derived = ctx
		}
	}()
	if derived = w.logContext(ctx, info); derived == nil {
		return ctx
	}
	return derived
}

// maxKeepAliveInterval caps the keep-alive interval, so that a job stays alive
// even when another worker of the queue has since set a shorter AckWait (of at
// least three times this interval) on the shared consumer.
const maxKeepAliveInterval = 5 * time.Second

// keepAlive extends the ack deadline until the returned stop function is called.
func keepAlive(msg jetstream.Msg, ackWait time.Duration) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(min(max(ackWait/3, 100*time.Millisecond), maxKeepAliveInterval))
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_ = msg.InProgress()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}

// Internal headers.
const (
	// headerAttemptBase is the number of attempts used before the job was put back.
	headerAttemptBase = "Jetq-Attempt-Base"
	// headerSnoozes is the number of times the job was snoozed.
	headerSnoozes = "Jetq-Snoozes"
	// headerUnknownSince is when a worker first found no handler for the job.
	headerUnknownSince = "Jetq-Unknown-Since"
	// headerDelayed marks a job that was once a delayed job, so that it stays
	// subject to cancellation markers.
	headerDelayed = "Jetq-Delayed"
)

// dropRequeued deletes a delayed copy that an earlier delivery of this job put
// back before its ack was lost, when this redelivery settles the job; the copy
// would otherwise run the settled job again.
func (w *Worker) dropRequeued(ctx context.Context, d delivery) {
	if d.redelivered == 0 {
		return
	}
	c := w.client
	pending, err := c.stream.GetLastMsgForSubject(ctx, c.delaySubject(d.info.ID))
	if err != nil {
		if !errors.Is(err, jetstream.ErrMsgNotFound) {
			c.cfg.logger.WarnContext(ctx, "jetq check for requeued copy failed", "queue", d.info.Queue, "job", d.info.Name, "id", d.info.ID, "error", err)
		}
		return
	}
	h := pending.Header
	if h.Get(headerAttemptBase) == "" || h.Get(HeaderEnqueuedAt) != d.msg.Headers().Get(HeaderEnqueuedAt) {
		return
	}
	if err := c.stream.DeleteMsg(ctx, pending.Sequence); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		c.cfg.logger.WarnContext(ctx, "jetq requeued copy removal failed", "queue", d.info.Queue, "job", d.info.Name, "id", d.info.ID, "error", err)
	}
}

// requeue publishes the job again with the same id, available after delay
// (as a delayed job when delay is positive), and acks this delivery. The next
// delivery of the copy counts as attempt base+1. Headers in set are added;
// headerUnknownSince is kept only when set gives it. Requeued jobs do not stay
// in flight, so jobs waiting for a retry never hold up a queue. If publishing
// fails, the delivery is nak'ed with the delay instead, and its redelivery
// counts as the next attempt.
func (w *Worker) requeue(ctx context.Context, d delivery, delay time.Duration, base int, set map[string]string) {
	c := w.client
	info := d.info
	next := &nats.Msg{Subject: c.queueSubject(d.queue.Name), Data: d.msg.Data(), Header: nats.Header{}}
	for key, values := range d.msg.Headers() {
		if strings.HasPrefix(key, "Nats-") || key == headerUnknownSince {
			continue
		}
		next.Header[key] = values
	}
	next.Header.Set(HeaderID, info.ID)
	next.Header.Set(headerAttemptBase, strconv.Itoa(base))
	if c.wasDelayed(d.msg.Headers()) || d.redelivered > 0 {
		// Keep the copy subject to cancellation markers when it skips the
		// scheduler; a redelivery may be the original of a cancelled copy.
		next.Header.Set(headerDelayed, "1")
	}
	for key, value := range set {
		next.Header.Set(key, value)
	}
	pubOpts := []jetstream.PublishOpt{jetstream.WithExpectStream(c.cfg.streamName)}
	if delay > 0 {
		next.Header.Set(headerSchedule, "@at "+time.Now().Add(delay).UTC().Format(time.RFC3339Nano))
		next.Header.Set(headerScheduleTarget, next.Subject)
		next.Subject = c.delaySubject(info.ID)
		// Never replace another pending delayed job that has the same id.
		pubOpts = append(pubOpts, jetstream.WithExpectLastSequencePerSubject(0))
	}
	if _, err := c.js.PublishMsg(ctx, next, pubOpts...); err != nil {
		err = classify(err)
		c.cfg.logger.WarnContext(ctx, "jetq requeue failed", "queue", info.Queue, "job", info.Name, "id", info.ID, "error", err)
		if nakErr := d.msg.NakWithDelay(delay); nakErr != nil {
			c.cfg.logger.WarnContext(ctx, "jetq nak failed", "queue", info.Queue, "job", info.Name, "id", info.ID, "error", nakErr)
		}
		return
	}
	if err := d.msg.Ack(); err != nil {
		// The copy is stored; this delivery comes back after AckWait and runs again.
		c.cfg.logger.WarnContext(ctx, "jetq ack failed", "queue", info.Queue, "job", info.Name, "id", info.ID, "error", err)
	}
}

// deadLetter copies the job to the dead-letter stream, runs failure callbacks
// and acks it. The copy is deduplicated by the delivered message, so a
// redelivery after a lost ack does not add a second dead letter; the failure
// callbacks run again.
func (w *Worker) deadLetter(ctx context.Context, d delivery, cause error) {
	c := w.client
	logger := c.cfg.logger
	info, msg := d.info, d.msg
	dead := &nats.Msg{Subject: c.deadSubject(d.queue.Name), Data: msg.Data(), Header: nats.Header{}}
	for key, values := range msg.Headers() {
		if strings.HasPrefix(key, "Nats-") {
			continue
		}
		dead.Header[key] = values
	}
	dead.Header.Set(HeaderID, info.ID)
	dead.Header.Set(HeaderQueue, d.queue.Name)
	dead.Header.Set(HeaderAttempts, strconv.Itoa(info.Attempt))
	dead.Header.Set(HeaderFailedAt, time.Now().UTC().Format(time.RFC3339Nano))
	dead.Header.Set(HeaderError, truncate(cause.Error(), 4096))
	ack, err := c.js.PublishMsg(ctx, dead, jetstream.WithExpectStream(c.cfg.deadName),
		jetstream.WithMsgID("jetq-dead-"+strconv.FormatUint(d.seq, 10)))
	if err != nil {
		logger.ErrorContext(ctx, "jetq dead-letter publish failed", "queue", d.queue.Name, "job", info.Name, "id", info.ID, "error", err)
		_ = msg.NakWithDelay(d.queue.Backoff(info.Attempt))
		return
	}
	if ack.Duplicate {
		logger.WarnContext(ctx, "jetq job already dead-lettered, running failure callbacks again", "queue", d.queue.Name, "job", info.Name, "id", info.ID)
	} else {
		logger.ErrorContext(ctx, "jetq job failed permanently", "queue", d.queue.Name, "job", info.Name, "id", info.ID,
			"attempt", info.Attempt, "error", cause)
	}

	w.dropRequeued(ctx, d)
	// The job is settled as dead: free its unique key before the callbacks, so
	// they can enqueue a replacement with the same key.
	w.releaseLock(ctx, msg, info)
	failCtx := context.WithValue(ctx, infoKey{}, info)
	if h, ok := w.handlers[info.Name]; ok && h.failed != nil {
		w.callback(failCtx, info, func() {
			if err := h.failed(failCtx, msg.Data(), cause); err != nil {
				logger.WarnContext(failCtx, "jetq failure callback skipped, payload does not decode", "job", info.Name, "id", info.ID, "error", err)
			}
		})
	}
	for _, fn := range w.onFailed {
		w.callback(failCtx, info, func() { fn(failCtx, info, msg.Data(), cause) })
	}
	if err := msg.Ack(); err != nil {
		logger.WarnContext(ctx, "jetq ack failed", "queue", d.queue.Name, "job", info.Name, "id", info.ID, "error", err)
	}
}

// callback runs a failure callback, logging instead of crashing on panic.
func (w *Worker) callback(ctx context.Context, info Info, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			w.client.cfg.logger.ErrorContext(ctx, "jetq failure callback panicked", "job", info.Name, "id", info.ID,
				"panic", r, "stack", string(debug.Stack()))
		}
	}()
	fn()
}

// releaseLock frees the UniqueUntilDone lock of a settled job.
func (w *Worker) releaseLock(ctx context.Context, msg jetstream.Msg, info Info) {
	if lock := msg.Headers().Get(HeaderUniqueLock); lock != "" {
		w.client.unlock(ctx, lock, info.ID)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}
