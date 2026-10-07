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
	started         atomic.Bool
}

type handler struct {
	run    func(ctx context.Context, payload []byte) error
	failed func(ctx context.Context, payload []byte, err error)
}

// NewWorker returns a worker for the given queues. Register handlers with
// [Handle] before calling [Worker.Run].
func (c *Client) NewWorker(queues ...Queue) *Worker {
	return &Worker{client: c, queues: queues, handlers: map[string]*handler{}, shutdownTimeout: 30 * time.Second}
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
func (w *Worker) SetShutdownTimeout(d time.Duration) {
	w.mustNotBeStarted()
	w.shutdownTimeout = d
}

func (w *Worker) mustNotBeStarted() {
	if w.started.Load() {
		panic("jetq: worker already started")
	}
}

// HandleOption configures a handler registered with [Handle].
type HandleOption[T Job] func(*handler)

// OnFailure registers a typed callback for when a job of this type is
// dead-lettered, like Laravel's failed() method.
func OnFailure[T Job](fn func(ctx context.Context, job T, err error)) HandleOption[T] {
	return func(h *handler) {
		h.failed = func(ctx context.Context, payload []byte, err error) {
			var job T
			if json.Unmarshal(payload, &job) == nil {
				fn(ctx, job, err)
			}
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
		consumer, err := w.client.js.CreateOrUpdateConsumer(ctx, w.client.cfg.streamName, jetstream.ConsumerConfig{
			Durable:       "jetq-" + q.Name,
			Description:   "jetq queue " + q.Name,
			FilterSubject: w.client.queueSubject(q.Name),
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       q.AckWait,
			MaxDeliver:    -1,
		})
		if err != nil {
			return fmt.Errorf("jetq: create consumer for queue %s: %w", q.Name, err)
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
		<-done
	}
	return nil
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
	return q
}

// consume fetches at most as many messages as there are free slots, so jobs
// never wait in a local buffer while their ack timer runs.
func (w *Worker) consume(ctx, jobCtx context.Context, q Queue, consumer jetstream.Consumer, running *sync.WaitGroup) {
	slots := make(chan struct{}, q.Concurrency)
	for i := 0; i < q.Concurrency; i++ {
		slots <- struct{}{}
	}
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
		if err != nil && ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) {
			w.client.cfg.logger.WarnContext(ctx, "jetq fetch failed", "queue", q.Name, "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
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
		Header:      header,
	}
	if base, err := strconv.Atoi(header.Get(headerAttemptBase)); err == nil && base > 0 {
		info.Attempt += base
	}
	if n, err := strconv.Atoi(header.Get(HeaderMaxAttempts)); err == nil && n > 0 {
		info.MaxAttempts = n
	}
	if t, err := time.Parse(time.RFC3339Nano, header.Get(HeaderEnqueuedAt)); err == nil {
		info.EnqueuedAt = t
	}
	if info.ID == "" {
		key := strings.TrimPrefix(header.Get(headerScheduler), w.client.cronSubject(""))
		info.ID = fmt.Sprintf("%s-%d", key, meta.Sequence.Stream)
	}

	// The keep-alive covers both the handler and settlement (dead-lettering,
	// failure callbacks, snoozing), so the job is not redelivered meanwhile.
	stopKeepAlive := keepAlive(msg, q.AckWait)
	defer stopKeepAlive()
	runCtx = w.deriveLogContext(runCtx, info)
	// Settlement must finish even when Run's context is cancelled during the
	// shutdown grace period; its timeout starts when settlement starts.
	newSettleCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.WithoutCancel(runCtx), settleTimeout)
	}

	if info.Attempt > info.MaxAttempts {
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		// Redelivered after a crash on the last attempt: do not run the handler again.
		w.deadLetter(settleCtx, q, msg, info, fmt.Errorf("jetq: attempt %d exceeds limit %d after redelivery", info.Attempt, info.MaxAttempts))
		return
	}

	ctx, cancel := context.WithCancel(context.WithValue(jobCtx, infoKey{}, info))
	defer cancel()
	err = w.run(ctx, info, msg.Data())
	settleCtx, cancelSettle := newSettleCtx()
	defer cancelSettle()

	var snooze *snoozeError
	var retry *retryAfterError
	switch {
	case err == nil:
		if ackErr := msg.Ack(); ackErr != nil {
			logger.WarnContext(settleCtx, "jetq ack failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", ackErr)
		}
	case errors.As(err, &snooze):
		w.snooze(settleCtx, q, msg, info, snooze.delay)
	case jobCtx.Err() != nil:
		// Shutdown timeout cancelled the job: hand it to another worker right away.
		_ = msg.Nak()
	case IsPermanent(err) || info.Attempt >= info.MaxAttempts:
		w.deadLetter(settleCtx, q, msg, info, err)
	default:
		delay := q.Backoff(info.Attempt)
		if errors.As(err, &retry) {
			delay = retry.delay
		}
		logger.WarnContext(settleCtx, "jetq job failed, will retry", "queue", q.Name, "job", info.Name, "id", info.ID,
			"attempt", info.Attempt, "max_attempts", info.MaxAttempts, "retry_in", delay, "error", err)
		if nakErr := msg.NakWithDelay(delay); nakErr != nil {
			logger.WarnContext(settleCtx, "jetq nak failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", nakErr)
		}
	}
}

// settleTimeout bounds publishing to the dead-letter stream, snoozing and failure callbacks.
var settleTimeout = 30 * time.Second

// run invokes the handler through the middleware chain, turning panics into errors.
func (w *Worker) run(ctx context.Context, info Info, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jetq: job %s panicked: %v\n%s", info.Name, r, debug.Stack())
		}
	}()
	h, ok := w.handlers[info.Name]
	if !ok {
		return fmt.Errorf("jetq: no handler registered for job %q", info.Name)
	}
	next := func(ctx context.Context) error { return h.run(ctx, payload) }
	for i := len(w.middleware) - 1; i >= 0; i-- {
		mw, inner := w.middleware[i], next
		next = func(ctx context.Context) error { return mw(ctx, inner) }
	}
	return next(ctx)
}

// deriveLogContext applies the worker's log context function, falling back to
// ctx when it is unset or panics.
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
	return w.logContext(ctx, info)
}

// keepAlive extends the ack deadline until the returned stop function is called.
func keepAlive(msg jetstream.Msg, ackWait time.Duration) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(max(ackWait/3, 100*time.Millisecond))
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

const headerAttemptBase = "Jetq-Attempt-Base"

// snooze re-enqueues the job as a delayed job with the same id that does not
// count the current attempt, then acks the original delivery.
func (w *Worker) snooze(ctx context.Context, q Queue, msg jetstream.Msg, info Info, delay time.Duration) {
	c := w.client
	next := &nats.Msg{Subject: c.queueSubject(q.Name), Data: msg.Data(), Header: nats.Header{}}
	for key, values := range msg.Headers() {
		if strings.HasPrefix(key, "Nats-") {
			continue
		}
		next.Header[key] = values
	}
	next.Header.Set(HeaderID, info.ID)
	next.Header.Set(headerAttemptBase, strconv.Itoa(info.Attempt-1))
	if delay > 0 {
		next.Header.Set(headerSchedule, "@at "+time.Now().Add(delay).UTC().Format(time.RFC3339Nano))
		next.Header.Set(headerScheduleTarget, next.Subject)
		next.Subject = c.delaySubject(info.ID)
	}
	if _, err := c.js.PublishMsg(ctx, next, jetstream.WithExpectStream(c.cfg.streamName)); err != nil {
		c.cfg.logger.WarnContext(ctx, "jetq snooze failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", err)
		_ = msg.NakWithDelay(delay)
		return
	}
	_ = msg.Ack()
}

// deadLetter copies the job to the dead-letter stream, runs failure callbacks and acks it.
func (w *Worker) deadLetter(ctx context.Context, q Queue, msg jetstream.Msg, info Info, cause error) {
	c := w.client
	logger := c.cfg.logger
	dead := &nats.Msg{Subject: c.deadSubject(q.Name), Data: msg.Data(), Header: nats.Header{}}
	for key, values := range msg.Headers() {
		if strings.HasPrefix(key, "Nats-") {
			continue
		}
		dead.Header[key] = values
	}
	dead.Header.Set(HeaderID, info.ID)
	dead.Header.Set(HeaderQueue, q.Name)
	dead.Header.Set(HeaderAttempts, strconv.Itoa(info.Attempt))
	dead.Header.Set(HeaderFailedAt, time.Now().UTC().Format(time.RFC3339Nano))
	dead.Header.Set(HeaderError, truncate(cause.Error(), 4096))
	if _, err := c.js.PublishMsg(ctx, dead, jetstream.WithExpectStream(c.cfg.deadName)); err != nil {
		logger.ErrorContext(ctx, "jetq dead-letter publish failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", err)
		_ = msg.NakWithDelay(q.Backoff(info.Attempt))
		return
	}
	logger.ErrorContext(ctx, "jetq job failed permanently", "queue", q.Name, "job", info.Name, "id", info.ID,
		"attempt", info.Attempt, "error", cause)

	failCtx := context.WithValue(ctx, infoKey{}, info)
	if h, ok := w.handlers[info.Name]; ok && h.failed != nil {
		w.callback(failCtx, info, func() { h.failed(failCtx, msg.Data(), cause) })
	}
	for _, fn := range w.onFailed {
		w.callback(failCtx, info, func() { fn(failCtx, info, msg.Data(), cause) })
	}
	if err := msg.Ack(); err != nil {
		logger.WarnContext(ctx, "jetq ack failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", err)
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}
