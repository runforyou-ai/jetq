package jobq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
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
	// Lease is how long a claimed job stays reserved for this worker without
	// a sign of life (default 1m). Running jobs are extended automatically,
	// so it only bounds recovery after a crash.
	Lease time.Duration
	// Timeout is how long one attempt may run before its context is
	// cancelled and the attempt fails with [ErrTimeout] (default: no limit).
	// [Timeout] overrides it per job.
	Timeout time.Duration
}

func (q Queue) withDefaults() Queue {
	if q.Concurrency <= 0 {
		q.Concurrency = 1
	}
	if q.MaxAttempts <= 0 {
		q.MaxAttempts = 3
	}
	if q.Backoff == nil {
		q.Backoff = Exponential(5*time.Second, 10*time.Minute)
	}
	if q.Lease <= 0 {
		q.Lease = time.Minute
	}
	q.Timeout = max(q.Timeout, 0)
	return q
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
	run    func(ctx context.Context, payload []byte) error
	failed func(ctx context.Context, payload []byte, err error) error
}

// NewWorker returns a worker consuming queues. Register handlers before
// calling [Worker.Run].
func (c *Client) NewWorker(queues ...Queue) *Worker {
	return &Worker{
		client:          c,
		queues:          queues,
		handlers:        map[string]*handler{},
		shutdownTimeout: 30 * time.Second,
		unknownTimeout:  time.Hour,
	}
}

// Use appends middleware run around every job attempt, outermost first.
func (w *Worker) Use(middleware ...Middleware) {
	w.mustNotBeStarted()
	w.middleware = append(w.middleware, middleware...)
}

// OnFailed registers a callback for every dead-lettered job, including jobs
// without a handler. Callbacks must be idempotent: a crash during
// settlement may run them again.
func (w *Worker) OnFailed(fn FailedFunc) {
	w.mustNotBeStarted()
	w.onFailed = append(w.onFailed, fn)
}

// SetLogContext derives the context of log records and failure callbacks
// from the job's [Info], for example to attach a trace id.
func (w *Worker) SetLogContext(fn func(ctx context.Context, info Info) context.Context) {
	w.mustNotBeStarted()
	w.logContext = fn
}

// SetShutdownTimeout sets how long [Worker.Run] waits for running jobs after
// its context is cancelled before cancelling them (default 30s). Interrupted
// jobs are put back without using up an attempt.
func (w *Worker) SetShutdownTimeout(d time.Duration) {
	w.mustNotBeStarted()
	w.shutdownTimeout = max(d, 0)
}

// SetUnknownJobTimeout sets how long a job without a handler is put back
// before it is dead-lettered (default 1h). A non-positive timeout puts it
// back forever, for example while another service owns the job.
func (w *Worker) SetUnknownJobTimeout(d time.Duration) {
	w.mustNotBeStarted()
	w.unknownTimeout = d
}

func (w *Worker) mustNotBeStarted() {
	if w.started.Load() {
		panic("jobq: worker already started")
	}
}

// HandleOption configures a handler.
type HandleOption[T any] func(*handler)

// OnFailure registers a typed callback for when a job of this type is
// dead-lettered. If the payload does not decode into T, the callback is
// skipped and a warning is logged.
func OnFailure[T any](fn func(ctx context.Context, job T, err error)) HandleOption[T] {
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

// Handle registers fn for jobs of type T, named by [Job.JobName]. It panics
// if a handler for the name is already registered or the worker has started.
func Handle[T Job](w *Worker, fn func(ctx context.Context, job T) error, opts ...HandleOption[T]) {
	var zero T
	HandleNamed(w, zero.JobName(), fn, opts...)
}

// HandleNamed registers a typed handler under an explicit job name. T may be
// any JSON-decodable type. Payloads that do not decode fail permanently.
func HandleNamed[T any](w *Worker, name string, fn func(context.Context, T) error, opts ...HandleOption[T]) {
	if fn == nil {
		panic("jobq: handler is nil")
	}
	h := &handler{run: func(ctx context.Context, payload []byte) error {
		var job T
		if err := json.Unmarshal(payload, &job); err != nil {
			return Permanent(fmt.Errorf("jobq: decode job %s: %w", name, err))
		}
		return fn(ctx, job)
	}}
	for _, opt := range opts {
		opt(h)
	}
	w.register(name, h)
}

// RawHandleOption configures a handler registered with [Worker.HandleRaw].
type RawHandleOption func(*handler)

// OnRawFailure registers a callback for when a raw job is dead-lettered.
func OnRawFailure(fn func(ctx context.Context, payload json.RawMessage, err error)) RawHandleOption {
	return func(h *handler) {
		h.failed = func(ctx context.Context, payload []byte, err error) error {
			fn(ctx, payload, err)
			return nil
		}
	}
}

// HandleRaw registers fn for jobs named name, passing the JSON payload undecoded.
func (w *Worker) HandleRaw(name string, fn func(ctx context.Context, payload json.RawMessage) error, opts ...RawHandleOption) {
	if fn == nil {
		panic("jobq: handler is nil")
	}
	h := &handler{run: func(ctx context.Context, payload []byte) error { return fn(ctx, payload) }}
	for _, opt := range opts {
		opt(h)
	}
	w.register(name, h)
}

func (w *Worker) register(name string, h *handler) {
	w.mustNotBeStarted()
	if err := validJobName(name); err != nil {
		panic(err.Error())
	}
	if _, exists := w.handlers[name]; exists {
		panic("jobq: handler already registered for job " + name)
	}
	w.handlers[name] = h
}

// Run consumes the worker's queues until ctx is cancelled, then stops claiming
// jobs and waits for running ones (see [Worker.SetShutdownTimeout]). Handlers
// that ignore the cancellation of their context are abandoned 10 seconds
// after it, so Run returns even if a handler never does.
func (w *Worker) Run(ctx context.Context) error {
	if !w.started.CompareAndSwap(false, true) {
		return errors.New("jobq: worker already started")
	}
	if len(w.queues) == 0 {
		return errors.New("jobq: worker has no queues")
	}
	queues := make([]Queue, len(w.queues))
	seen := map[string]bool{}
	for i, q := range w.queues {
		if err := validName("queue", q.Name); err != nil {
			return err
		}
		if seen[q.Name] {
			return fmt.Errorf("jobq: queue %q listed twice", q.Name)
		}
		seen[q.Name] = true
		queues[i] = q.withDefaults()
	}

	// Handlers get their own context so running jobs survive ctx
	// cancellation until the shutdown timeout.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()
	var running, loops sync.WaitGroup
	for _, q := range queues {
		loops.Go(func() { w.consume(ctx, jobCtx, q, &running) })
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
		w.client.logger.WarnContext(ctx, "jobq shutdown timeout, cancelling running jobs")
		cancelJobs()
		// Bounded: handlers are abandoned abandonAfter after their context
		// is cancelled, then settlement runs under settleTimeout.
		<-done
	}
	return nil
}

// consume claims at most as many jobs as there are free slots, so no job
// waits in a local buffer while its lease runs.
func (w *Worker) consume(ctx, jobCtx context.Context, q Queue, running *sync.WaitGroup) {
	slots := make(chan struct{}, q.Concurrency)
	for range q.Concurrency {
		slots <- struct{}{}
	}
	failures := 0
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-slots:
		}
		free := 1
	collect:
		for free < q.Concurrency {
			select {
			case <-slots:
				free++
			default:
				break collect
			}
		}
		leases, err := w.client.driver.Claim(ctx, q.Name, free, q.Lease)
		if len(leases) > free {
			// The driver broke the contract; give the extra jobs back.
			for _, lease := range leases[free:] {
				_ = lease.Release(context.WithoutCancel(ctx), Release{})
			}
			leases = leases[:free]
		}
		for _, lease := range leases {
			running.Go(func() {
				defer func() { slots <- struct{}{} }()
				w.process(ctx, jobCtx, q, lease)
			})
		}
		for range free - len(leases) {
			slots <- struct{}{}
		}
		if err == nil || ctx.Err() != nil {
			failures = 0
			continue
		}
		failures++
		w.client.logger.WarnContext(ctx, "jobq claim failed", "queue", q.Name, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(min(time.Duration(failures)*time.Second, 30*time.Second)):
		}
	}
}

// settleTimeout bounds settlement and failure callbacks.
var settleTimeout = 30 * time.Second

// abandonAfter is how long a handler may take to return after its context
// was cancelled, by its timeout, shutdown or a lost lease, before the worker
// stops waiting.
var abandonAfter = 10 * time.Second

// unknownJobRetry is the delay before a job without a handler is offered again.
var unknownJobRetry = 10 * time.Second

// job is one claimed job being processed.
type job struct {
	lease  Lease
	msg    *Message
	info   Info
	q      Queue
	logCtx context.Context
}

func (w *Worker) process(runCtx, jobCtx context.Context, q Queue, lease Lease) {
	msg := lease.Message()
	info := Info{
		ID:          msg.ID,
		Name:        msg.Name,
		Queue:       msg.Queue,
		Attempt:     lease.Attempt(),
		MaxAttempts: q.MaxAttempts,
		Timeout:     q.Timeout,
		Snoozes:     msg.Snoozes,
		EnqueuedAt:  msg.EnqueuedAt,
		Header:      msg.Header,
	}
	if msg.MaxAttempts > 0 {
		info.MaxAttempts = msg.MaxAttempts
	}
	switch {
	case msg.Timeout == NoTimeout:
		info.Timeout = 0
	case msg.Timeout > 0:
		info.Timeout = msg.Timeout
	}
	j := &job{lease: lease, msg: msg, info: info, q: q, logCtx: w.deriveLogContext(runCtx, info)}

	// The keep-alive covers the handler and settlement, so the job is not
	// claimed elsewhere meanwhile. A lost lease cancels the handler.
	leaseCtx, cancelLease := context.WithCancelCause(jobCtx)
	defer cancelLease(nil)
	stopKeepAlive := w.keepAlive(j, cancelLease)
	defer stopKeepAlive()
	// Settlement must finish even when Run's context is cancelled during the
	// shutdown grace period; its timeout starts when settlement starts.
	settleCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.WithoutCancel(j.logCtx), settleTimeout)
	}

	h, ok := w.handlers[info.Name]
	if !ok {
		ctx, cancel := settleCtx()
		defer cancel()
		w.unknownJob(ctx, j)
		return
	}
	if info.Attempt > info.MaxAttempts {
		// Claimed again after a crash during the last attempt.
		ctx, cancel := settleCtx()
		defer cancel()
		w.deadLetter(ctx, j, h, fmt.Errorf("jobq: attempt %d exceeds limit %d after a lost lease", info.Attempt, info.MaxAttempts))
		return
	}

	err := w.attempt(leaseCtx, j, h)
	ctx, cancel := settleCtx()
	defer cancel()
	if errors.Is(context.Cause(leaseCtx), ErrLeaseLost) {
		w.client.logger.WarnContext(ctx, "jobq lease lost, job may run elsewhere", w.attrs(j, "error", err)...)
		return
	}

	var snooze *snoozeError
	var retry *retryAfterError
	switch {
	case err == nil:
		w.settled(ctx, j, "complete", lease.Complete(ctx))
	case errors.As(err, &snooze):
		w.settled(ctx, j, "snooze", lease.Release(ctx, Release{At: time.Now().Add(snooze.delay), Snooze: true}))
	case IsPermanent(err):
		w.deadLetter(ctx, j, h, err)
	case jobCtx.Err() != nil && !errors.Is(err, ErrTimeout):
		// Shutdown interrupted the job: put it back at once without using
		// up the attempt.
		w.client.logger.WarnContext(ctx, "jobq job interrupted by shutdown, putting it back", w.attrs(j, "error", err)...)
		w.settled(ctx, j, "release", lease.Release(ctx, Release{}))
	case info.Attempt >= info.MaxAttempts:
		w.deadLetter(ctx, j, h, err)
	default:
		delay := q.Backoff(info.Attempt)
		if errors.As(err, &retry) {
			delay = retry.delay
		}
		w.client.logger.WarnContext(ctx, "jobq job failed, will retry",
			w.attrs(j, "max_attempts", info.MaxAttempts, "retry_in", delay, "error", err)...)
		w.settled(ctx, j, "retry", lease.Retry(ctx, time.Now().Add(delay), errorText(err)))
	}
}

// attempt runs one attempt of the job under its timeout. A handler that has
// not returned abandonAfter after its context was cancelled is abandoned: it
// keeps running in the background, and the attempt fails.
func (w *Worker) attempt(parent context.Context, j *job, h *handler) error {
	ctx, cancel := context.WithCancel(context.WithValue(parent, infoKey{}, j.info))
	defer cancel()
	if j.info.Timeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeoutCause(ctx, j.info.Timeout, ErrTimeout)
		defer cancelTimeout()
	}
	result := make(chan error, 1)
	go func() { result <- w.run(ctx, j.info, h, j.msg.Payload) }()
	var err error
	// Only a result taken after the timeout fired is a timeout failure. A
	// handler that returned first keeps its own result, even when the timer
	// goes off before the check below.
	timedOut := false
	select {
	case err = <-result:
	case <-ctx.Done():
		timedOut = errors.Is(context.Cause(ctx), ErrTimeout)
		abandon := time.NewTimer(abandonAfter)
		defer abandon.Stop()
		select {
		case err = <-result:
		case <-abandon.C:
			w.client.logger.ErrorContext(j.logCtx, "jobq job ignored cancellation, abandoning it", w.attrs(j, "cause", context.Cause(ctx))...)
			err = fmt.Errorf("jobq: job %s abandoned, it did not return within %s of cancellation: %w", j.info.Name, abandonAfter, context.Cause(ctx))
		}
	}
	// Once the timeout has passed the attempt is an ordinary failure,
	// whatever the handler returned: snoozes, RetryAfter and Permanent only
	// contribute their text.
	if timedOut {
		text := "context deadline exceeded"
		if err != nil {
			text = err.Error()
		}
		err = fmt.Errorf("%w after %s: %s", ErrTimeout, j.info.Timeout, text)
	}
	return err
}

// run invokes the handler through the middleware chain, turning panics into errors.
func (w *Worker) run(ctx context.Context, info Info, h *handler, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jobq: job %s panicked: %v\n%s", info.Name, r, debug.Stack())
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
// worker can run it, and dead-letters it once the unknown job timeout has
// passed.
func (w *Worker) unknownJob(ctx context.Context, j *job) {
	cause := fmt.Errorf("jobq: no handler registered for job %q", j.info.Name)
	now := time.Now()
	since := j.msg.UnknownSince
	if since.IsZero() {
		since = now
	}
	delay := unknownJobRetry
	if w.unknownTimeout > 0 {
		left := w.unknownTimeout - now.Sub(since)
		if left <= 0 {
			w.deadLetter(ctx, j, nil, cause)
			return
		}
		delay = min(delay, left)
	}
	w.client.logger.WarnContext(ctx, "jobq no handler for job, putting it back", w.attrs(j, "unknown_since", since, "retry_in", delay)...)
	w.settled(ctx, j, "release", j.lease.Release(ctx, Release{At: now.Add(delay), UnknownSince: since}))
}

// deadLetter fails the job, then runs the failure callbacks of h (nil for
// unknown jobs) and of the worker.
func (w *Worker) deadLetter(ctx context.Context, j *job, h *handler, cause error) {
	w.client.logger.ErrorContext(ctx, "jobq job dead-lettered", w.attrs(j, "error", cause)...)
	if err := j.lease.Fail(ctx, errorText(cause)); err != nil {
		w.settled(ctx, j, "dead-letter", err)
		return
	}
	if h != nil && h.failed != nil {
		w.callback(ctx, j, func() {
			if err := h.failed(ctx, j.msg.Payload, cause); err != nil {
				w.client.logger.WarnContext(ctx, "jobq failure callback skipped, payload does not decode", w.attrs(j, "error", err)...)
			}
		})
	}
	for _, fn := range w.onFailed {
		w.callback(ctx, j, func() { fn(ctx, j.info, j.msg.Payload, cause) })
	}
}

func (w *Worker) callback(ctx context.Context, j *job, fn func()) {
	// Each callback has its own bound: one that uses up the settlement budget
	// must not make the callbacks after it look abandoned.
	cbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				w.client.logger.ErrorContext(ctx, "jobq failure callback panicked", w.attrs(j, "panic", r, "stack", string(debug.Stack()))...)
			}
		}()
		fn()
	}()
	select {
	case <-done:
		return
	case <-cbCtx.Done():
	}
	// The callback used up its time.
	// Stop waiting so Run can return; the callback keeps running unobserved.
	select {
	case <-done:
	default:
		w.client.logger.ErrorContext(ctx, "jobq failure callback ignored cancellation, abandoning it", w.attrs(j)...)
	}
}

// settled logs a failed settlement. The job's lease then expires and it is
// claimed again as a new attempt.
func (w *Worker) settled(ctx context.Context, j *job, op string, err error) {
	if err != nil {
		w.client.logger.WarnContext(ctx, "jobq settlement failed, job will be claimed again after its lease",
			w.attrs(j, "op", op, "error", err)...)
	}
}

// keepAlive extends the lease until the returned function is called. When the
// driver reports the lease lost it calls lost with [ErrLeaseLost] and stops.
func (w *Worker) keepAlive(j *job, lost context.CancelCauseFunc) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(max(j.q.Lease/3, 50*time.Millisecond))
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			ctx, cancel := context.WithTimeout(context.WithoutCancel(j.logCtx), max(j.q.Lease/3, time.Second))
			err := j.lease.Extend(ctx)
			cancel()
			if errors.Is(err, ErrLeaseLost) {
				lost(ErrLeaseLost)
				return
			}
			if err != nil {
				w.client.logger.WarnContext(j.logCtx, "jobq lease extension failed", w.attrs(j, "error", err)...)
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

// deriveLogContext applies the worker's log context function, falling back to
// ctx when it is unset, panics or returns nil.
func (w *Worker) deriveLogContext(ctx context.Context, info Info) (derived context.Context) {
	if w.logContext == nil {
		return ctx
	}
	defer func() {
		if r := recover(); r != nil {
			w.client.logger.ErrorContext(ctx, "jobq log context panicked", "job", info.Name, "id", info.ID, "panic", r, "stack", string(debug.Stack()))
			derived = ctx
		}
	}()
	if derived = w.logContext(ctx, info); derived == nil {
		return ctx
	}
	return derived
}

func (w *Worker) attrs(j *job, extra ...any) []any {
	return append([]any{"queue", j.info.Queue, "job", j.info.Name, "id", j.info.ID, "attempt", j.info.Attempt}, extra...)
}

// maxErrorText bounds the error text stored with a job.
const maxErrorText = 4 << 10

func errorText(err error) string {
	s := err.Error()
	if len(s) > maxErrorText {
		s = s[:maxErrorText]
	}
	return s
}
