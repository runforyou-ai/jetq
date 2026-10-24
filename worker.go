package jetq

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

// Queue configures how one worker consumes one queue. It holds only local
// consumption settings: execution policy is fixed in the job's [Envelope] at
// submission and is never overridden here.
type Queue struct {
	// Name of the queue.
	Name string
	// Concurrency is how many jobs this worker runs at once (default 1).
	Concurrency int
}

// Middleware wraps every execution. The first middleware added is the
// outermost. Use [JobInfo] to inspect the job.
type Middleware func(ctx context.Context, next func(context.Context) error) error

// FailedFunc observes a job that reached the failed state. It runs after the
// failure decision is durable, is best effort, and may run more than once or
// not at all.
type FailedFunc func(ctx context.Context, info Info, payload json.RawMessage, err error)

// Worker runs registered handlers for jobs of its queues on a [Driver].
type Worker struct {
	client          *Client
	queues          []Queue
	handlers        map[string]*handler
	middleware      []Middleware
	onFailed        []FailedFunc
	logContext      func(context.Context, Info) context.Context
	shutdownTimeout time.Duration
	unknownTimeout  time.Duration
	unknownRetry    time.Duration
	settleTimeout   time.Duration
	abandonAfter    time.Duration
	// callbackTimeout bounds every application callback: failure observers and
	// log context functions (default: settleTimeout).
	callbackTimeout time.Duration
	started         atomic.Bool
	scheduler       BackgroundTasks
}

type handler struct {
	run func(ctx context.Context, payload json.RawMessage) error
	// failed runs the handler's failure callback; it returns an error when
	// the callback could not run, for example because the payload does not
	// decode.
	failed func(ctx context.Context, payload json.RawMessage, err error) error
}

// NewWorker returns a worker for the given queues. Register handlers with
// [Handle] or [HandleRaw] before calling [Worker.Run].
func (c *Client) NewWorker(queues ...Queue) *Worker {
	return &Worker{
		client:          c,
		queues:          queues,
		handlers:        map[string]*handler{},
		shutdownTimeout: 30 * time.Second,
		unknownTimeout:  time.Hour,
		unknownRetry:    10 * time.Second,
		settleTimeout:   30 * time.Second,
		abandonAfter:    10 * time.Second,
	}
}

// Use appends middleware; the first one added is the outermost.
func (w *Worker) Use(middleware ...Middleware) {
	w.mustNotBeStarted()
	w.middleware = append(w.middleware, middleware...)
}

// OnFailed registers an observer for jobs that reach the failed state.
func (w *Worker) OnFailed(fn FailedFunc) {
	w.mustNotBeStarted()
	w.onFailed = append(w.onFailed, fn)
}

// SetScheduler makes the worker run sched alongside its queues, so an
// application does not need a separate goroutine for driver extensions such
// as recurring schedules. The scheduler stops when the worker does.
func (w *Worker) SetScheduler(sched BackgroundTasks) {
	w.mustNotBeStarted()
	w.scheduler = sched
}

// BackgroundTasks is a long-running part of a driver's extension, started and
// stopped with the worker. Maintainers and schedulers are the usual ones.
type BackgroundTasks interface {
	// RunUntil blocks until ctx is cancelled and returns ctx.Err().
	RunUntil(ctx context.Context) error
}

// SetLogContext sets a function that derives the context jetq uses for its
// own log records about a job, for example to attach trace or tenant fields.
// Failure observers receive the derived context too. If fn panics or returns
// nil, the original context is used.
func (w *Worker) SetLogContext(fn func(ctx context.Context, info Info) context.Context) {
	w.mustNotBeStarted()
	w.logContext = fn
}

// SetShutdownTimeout sets how long [Worker.Run] waits for running jobs after
// its context is cancelled before cancelling their contexts (default 30s).
// Jobs interrupted this way are released and their budget refunded.
func (w *Worker) SetShutdownTimeout(d time.Duration) {
	w.mustNotBeStarted()
	w.shutdownTimeout = max(d, 0)
}

// SetUnknownTaskTimeout sets how long a job whose task this worker has no
// handler for is deferred before it fails (default 1h), counted from the first
// time a worker found no handler. Until then the job is deferred without
// spending budget, so a worker of a newer version or of another service can
// run it. A non-positive value defers such jobs forever.
func (w *Worker) SetUnknownTaskTimeout(d time.Duration) {
	w.mustNotBeStarted()
	w.unknownTimeout = d
}

func (w *Worker) mustNotBeStarted() {
	if w.started.Load() {
		panic("jetq: worker already started")
	}
}

// HandleOption configures a handler registered with [Handle] or [HandleTask].
type HandleOption[T any] func(*handler)

// OnFailure registers a typed observer for when a job of this task fails.
// If the payload does not decode into T, the observer is skipped and a
// warning is logged.
func OnFailure[T any](fn func(ctx context.Context, job T, err error)) HandleOption[T] {
	return func(h *handler) {
		h.failed = func(ctx context.Context, payload json.RawMessage, err error) error {
			var job T
			if decodeErr := json.Unmarshal(payload, &job); decodeErr != nil {
				return decodeErr
			}
			fn(ctx, job, err)
			return nil
		}
	}
}

// Handle registers fn for the task. It panics if a handler for the same task
// name is already registered or the worker has started.
func Handle[T any](w *Worker, task Task[T], fn func(ctx context.Context, job T) error, opts ...HandleOption[T]) {
	if fn == nil {
		panic("jetq: handler is nil")
	}
	w.register(task.name, newTypedHandler(task.name, fn, opts))
}

// HandleTask registers a typed handler under an explicit, stable task name,
// for consumers that do not share the producer's task declaration. T is any
// JSON-decodable type. Invalid payloads fail permanently. It panics for an
// invalid or duplicate name, a nil handler, or a worker that has started.
func HandleTask[T any](w *Worker, name string, fn func(context.Context, T) error, opts ...HandleOption[T]) {
	if fn == nil {
		panic("jetq: handler is nil")
	}
	if err := ValidateTaskName(name); err != nil {
		panic(err)
	}
	w.register(name, newTypedHandler(name, fn, opts))
}

func newTypedHandler[T any](name string, fn func(context.Context, T) error, opts []HandleOption[T]) *handler {
	h := &handler{run: func(ctx context.Context, payload json.RawMessage) error {
		var job T
		if err := json.Unmarshal(payload, &job); err != nil {
			return Permanent(fmt.Errorf("jetq: decode task %s: %w", name, err))
		}
		return fn(ctx, job)
	}}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// HandleRawOption configures a handler registered with [HandleRaw].
type HandleRawOption func(*handler)

// OnRawFailure registers an observer for when a raw job fails.
func OnRawFailure(fn func(ctx context.Context, payload json.RawMessage, err error)) HandleRawOption {
	return func(h *handler) {
		h.failed = func(ctx context.Context, payload json.RawMessage, err error) error {
			fn(ctx, payload, err)
			return nil
		}
	}
}

// HandleRaw registers fn for the task name, passing the JSON payload
// undecoded. It panics for an invalid or duplicate name, a nil handler, or a
// worker that has started.
func (w *Worker) HandleRaw(name string, fn func(ctx context.Context, payload json.RawMessage) error, opts ...HandleRawOption) {
	if fn == nil {
		panic("jetq: handler is nil")
	}
	if err := ValidateTaskName(name); err != nil {
		panic(err)
	}
	h := &handler{run: fn}
	for _, opt := range opts {
		opt(h)
	}
	w.register(name, h)
}

func (w *Worker) register(name string, h *handler) {
	w.mustNotBeStarted()
	if _, exists := w.handlers[name]; exists {
		panic("jetq: handler already registered for task " + name)
	}
	w.handlers[name] = h
}

// Run consumes the worker's queues until ctx is cancelled. It then stops
// receiving and admitting jobs, waits up to the shutdown timeout for running
// jobs, and returns nil. Handlers that ignore cancellation of their context
// are abandoned a bounded time after it.
//
// Run also starts the driver's maintenance loop when the driver implements
// [Maintainer], so recovery does not depend on the application calling a
// driver-specific method.
func (w *Worker) Run(ctx context.Context) error {
	if !w.started.CompareAndSwap(false, true) {
		return errors.New("jetq: worker already started")
	}
	if len(w.queues) == 0 {
		return errors.New("jetq: worker has no queues")
	}
	queues := make([]Queue, len(w.queues))
	consumers := make([]Consumer, len(w.queues))
	for i, q := range w.queues {
		if q.Concurrency <= 0 {
			q.Concurrency = 1
		}
		if err := ValidateID("queue", q.Name); err != nil {
			return err
		}
		consumer, err := w.client.driver.Consumer(ctx, q.Name)
		if err != nil {
			return fmt.Errorf("jetq: consumer for queue %s: %w", q.Name, err)
		}
		queues[i], consumers[i] = q, consumer
	}
	defer func() {
		for _, c := range consumers {
			_ = c.Close()
		}
	}()

	// Maintenance and the configured scheduler run for the whole life of the
	// worker.
	maintainCtx, stopMaintain := context.WithCancel(context.WithoutCancel(ctx))
	defer stopMaintain()
	var maintenance sync.WaitGroup
	start := func(name string, fn func(context.Context) error) {
		maintenance.Add(1)
		go func() {
			defer maintenance.Done()
			if err := fn(maintainCtx); err != nil && maintainCtx.Err() == nil {
				w.client.logger.ErrorContext(maintainCtx, "jetq "+name+" stopped", "error", err)
			}
		}()
	}
	if m, ok := w.client.driver.(Maintainer); ok {
		start("maintenance", m.Maintain)
	}
	if w.scheduler != nil {
		start("scheduler", w.scheduler.RunUntil)
	}

	// Running jobs get their own context so they survive ctx cancellation
	// until the shutdown timeout.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()
	var running sync.WaitGroup
	var loops sync.WaitGroup
	stop := &stopper{}
	go func() {
		<-ctx.Done()
		stop.stopped.Store(true)
	}()
	for i := range queues {
		loops.Add(1)
		go func() {
			defer loops.Done()
			w.consume(ctx, jobCtx, stop, queues[i], consumers[i], &running)
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
		w.client.logger.WarnContext(ctx, "jetq shutdown timeout, cancelling running jobs", "timeout", w.shutdownTimeout)
		cancelJobs()
		<-done
	}
	stopMaintain()
	maintenance.Wait()
	return nil
}

// stopper marks that the worker stopped admitting jobs.
type stopper struct{ stopped atomic.Bool }

// consume fetches at most as many deliveries as there are free slots, so jobs
// never wait in a local buffer while their lease runs.
func (w *Worker) consume(ctx, jobCtx context.Context, stop *stopper, q Queue, consumer Consumer, running *sync.WaitGroup) {
	slots := make(chan struct{}, q.Concurrency)
	for range q.Concurrency {
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
		if ctx.Err() != nil {
			for range free {
				slots <- struct{}{}
			}
			return
		}
		batch, err := consumer.Receive(ctx, free)
		// Every returned delivery is owned by this worker, also when Receive
		// reports an error: all of them are processed or released. A driver
		// that returns more than asked may exceed the local concurrency for
		// that batch; those deliveries release no slot.
		held := free
		for _, dlv := range batch {
			owns := held > 0
			if owns {
				held--
			}
			running.Add(1)
			go func() {
				defer running.Done()
				if owns {
					defer func() { slots <- struct{}{} }()
				}
				w.process(ctx, jobCtx, stop, q, dlv)
			}()
		}
		for range held {
			slots <- struct{}{}
		}
		if err == nil || ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			failures = 0
			continue
		}
		failures++
		w.client.logger.WarnContext(ctx, "jetq receive failed", "queue", q.Name, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(min(failures, 30)) * 100 * time.Millisecond):
		}
	}
}

// process runs one delivery through the engine.
func (w *Worker) process(runCtx, jobCtx context.Context, stop *stopper, q Queue, dlv Delivery) {
	env := dlv.Envelope()
	state := dlv.State()
	// Settlement and release use their own bounded context: they must finish
	// even when the worker's context is cancelled.
	newSettleCtx := func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(runCtx), w.settleTimeout)
		// Failure observers receive the payload even though settlement runs
		// outside the handler's context.
		return context.WithValue(ctx, payloadKey{}, env.Payload), cancel
	}
	info := Info{
		ID: env.JobID, Name: env.Task, Queue: env.Queue,
		MaxAttempts: env.Policy.MaxAttempts, Timeout: env.Policy.Timeout,
		Snoozes: state.Snoozes, Deliveries: state.Deliveries,
		EnqueuedAt: env.CreatedAt, RunAt: env.RunAt,
		Metadata: env.Metadata.Clone(), ParentJobID: env.ParentJobID,
		// Paths that never admit (recorded decision, cancellation, exhausted
		// budget, unknown handler, shutdown) still report the business
		// execution this delivery stands for: the recorded budget.
		Attempt: state.Budget,
	}
	// The log context is derived after admission, inside the lease window: a
	// slow log context function must not let the lease expire.
	logCtx := runCtx
	logger := w.client.logger

	// A recorded decision is completed without running the handler again.
	if state.Pending != nil {
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		w.settle(settleCtx, dlv, *state.Pending, info, state.Pending.ExecutionID)
		return
	}
	// A cancellation recorded before admission prevents this execution.
	if state.Canceled {
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		w.settle(settleCtx, dlv, Decision{Kind: DecideCancel, Reason: WaitReleased}, info, "")
		return
	}

	h, ok := w.handlers[env.Task]
	if !ok {
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		w.unknownTask(settleCtx, dlv, state, info, logCtx)
		return
	}
	if state.Budget >= env.Policy.MaxAttempts {
		// The budget was spent by an earlier execution whose decision never
		// got recorded. The handler must not run again.
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		dec := Decision{Kind: DecideFail, Error: fmt.Sprintf("jetq: execution budget %d exhausted after redelivery", env.Policy.MaxAttempts)}
		w.settle(settleCtx, dlv, dec, info, "")
		return
	}
	if stop.stopped.Load() {
		// Shutdown started while this delivery was in flight: it never runs.
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		w.release(settleCtx, dlv, info, logCtx)
		return
	}

	adm, err := dlv.Admit(logCtx)
	if err != nil {
		settleCtx, cancelSettle := newSettleCtx()
		defer cancelSettle()
		if errors.Is(err, ErrClaimLost) {
			logger.WarnContext(logCtx, "jetq delivery claim lost before admission", "queue", q.Name, "job", info.Name, "id", info.ID, "error", err)
			_ = dlv.Release(settleCtx)
			return
		}
		// The admission is unknown: putting the delivery back is safe, because
		// an admission that was stored stays spent and is recognized by its
		// execution id on the next delivery.
		logger.WarnContext(logCtx, "jetq admission failed", "queue", q.Name, "job", info.Name, "id", info.ID, "error", err)
		_ = dlv.Release(settleCtx)
		return
	}
	info.Attempt = adm.Attempt

	// The lease covers the handler and the settlement: a slow dead-letter
	// write or failure observer must not let the job be redelivered meanwhile.
	// The lease is renewed for as long as the delivery exists; each phase of
	// a delivery is bounded (handler by its timeout and the abandon window,
	// callbacks by callbackWait, shutdown by the grace period), so a healthy
	// job without a timeout renews until it really ends.
	stopKeepAlive := w.keepAlive(logCtx, dlv)
	defer stopKeepAlive()
	logCtx = w.deriveLogContext(logCtx, info)
	err, cause := w.execute(jobCtx, logCtx, info, env, h)

	settleCtx, cancelSettle := newSettleCtx()
	defer cancelSettle()
	w.finish(settleCtx, logCtx, dlv, info, env, err, cause, adm.ExecutionID)
}

// execute runs one admitted execution under its timeout, returning the
// handler's error and the context's cause once the execution ended. A handler
// that has not returned abandonAfter after its context was cancelled is
// abandoned: it keeps running in the background and the execution fails.
func (w *Worker) execute(jobCtx, logCtx context.Context, info Info, env *Envelope, h *handler) (error, error) {
	ctx, cancel := context.WithCancel(context.WithValue(context.WithValue(jobCtx, infoKey{}, info), payloadKey{}, env.Payload))
	defer cancel()
	if info.Timeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeoutCause(ctx, info.Timeout, ErrTimeout)
		defer cancelTimeout()
	}
	result := make(chan error, 1)
	go func() { result <- w.invoke(ctx, info, h, env.Payload) }()
	var err error
	select {
	case err = <-result:
	case <-ctx.Done():
		abandon := time.NewTimer(w.abandonAfter)
		defer abandon.Stop()
		select {
		case err = <-result:
		case <-abandon.C:
			w.client.logger.ErrorContext(logCtx, "jetq handler ignored cancellation, abandoning it",
				"queue", info.Queue, "task", info.Name, "job", info.ID, "attempt", info.Attempt, "cause", context.Cause(ctx))
			err = fmt.Errorf("jetq: handler of %s abandoned, it did not return within %s of cancellation: %w",
				info.Name, w.abandonAfter, context.Cause(ctx))
		}
	}
	// Once the timeout has passed, also when shutdown followed, the execution
	// is an ordinary failure whatever the handler returned: its result only
	// contributes text, so snoozes, RetryAfter and Permanent do not apply.
	if errors.Is(context.Cause(ctx), ErrTimeout) {
		if err == nil {
			err = context.DeadlineExceeded
		}
		if !errors.Is(err, ErrTimeout) {
			err = fmt.Errorf("%w after %s: %w", ErrTimeout, info.Timeout, err)
		}
	}
	return err, context.Cause(ctx)
}

// invoke calls the handler through the middleware chain, turning panics into
// errors.
func (w *Worker) invoke(ctx context.Context, info Info, h *handler, payload json.RawMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jetq: task %s panicked: %v\n%s", info.Name, r, debug.Stack())
		}
	}()
	next := func(ctx context.Context) error { return h.run(ctx, payload) }
	for i := len(w.middleware) - 1; i >= 0; i-- {
		mw, inner := w.middleware[i], next
		next = func(ctx context.Context) error { return mw(ctx, inner) }
	}
	return next(ctx)
}

// finish classifies an admitted execution's result into a decision.
func (w *Worker) finish(ctx context.Context, logCtx context.Context, dlv Delivery, info Info, env *Envelope, err error, cause error, execID string) {
	logger := w.client.logger
	var snooze *snoozeError
	switch {
	case errors.Is(cause, ErrTimeout):
		// A timed-out execution is an ordinary failure, whatever the handler
		// returned after its context was cancelled: Snooze, RetryAfter and
		// Permanent do not apply to it.
		w.settle(ctx, dlv, w.failureDecision(env, info, err), info, execID)
	case err == nil:
		w.settle(ctx, dlv, Decision{Kind: DecideSucceed}, info, execID)
	case errors.As(err, &snooze):
		w.settle(ctx, dlv, Decision{Kind: DecideSnooze, RunAt: w.client.now().Add(snooze.delay), Reason: WaitSnoozed}, info, execID)
	case IsPermanent(err):
		w.settle(ctx, dlv, Decision{Kind: DecideFail, Error: truncate(err.Error())}, info, execID)
	case cause != nil && !errors.Is(cause, ErrTimeout):
		// Shutdown interrupted the execution: put it back and refund the
		// budget. Not a business failure.
		logger.WarnContext(logCtx, "jetq job interrupted by shutdown, releasing it",
			"queue", info.Queue, "task", info.Name, "job", info.ID, "attempt", info.Attempt, "error", err)
		w.settle(ctx, dlv, Decision{Kind: DecideRelease, Reason: WaitReleased}, info, execID)
	case info.Attempt >= info.MaxAttempts:
		w.settle(ctx, dlv, Decision{Kind: DecideFail, Error: truncate(err.Error())}, info, execID)
	default:
		var retry *retryAfterError
		dec := w.failureDecision(env, info, err)
		if errors.As(err, &retry) {
			dec.RunAt = w.client.now().Add(retry.delay)
		}
		logger.WarnContext(logCtx, "jetq job failed, will retry",
			"queue", info.Queue, "task", info.Name, "job", info.ID, "attempt", info.Attempt,
			"max_attempts", info.MaxAttempts, "retry_in", dec.RunAt.Sub(w.client.now()), "error", err)
		w.settle(ctx, dlv, dec, info, execID)
	}
}

// failureDecision is the retry-or-fail decision of a failed execution.
func (w *Worker) failureDecision(env *Envelope, info Info, err error) Decision {
	if info.Attempt >= info.MaxAttempts {
		return Decision{Kind: DecideFail, Error: truncate(err.Error())}
	}
	delay, delayErr := env.Policy.Backoff.Delay(info.Attempt)
	if delayErr != nil {
		w.client.logger.Error("jetq backoff is unusable, retrying immediately", "task", info.Name, "job", info.ID, "error", delayErr)
		delay = 0
	}
	return Decision{Kind: DecideRetry, RunAt: w.client.now().Add(delay), Error: truncate(err.Error()), Reason: WaitRetry}
}

// unknownTask defers a job whose task this worker cannot run, without
// spending its budget, and fails it once the unknown task timeout passed.
func (w *Worker) unknownTask(ctx context.Context, dlv Delivery, state ExecutionState, info Info, logCtx context.Context) {
	since := state.UnknownSince
	now := w.client.now()
	if since.IsZero() {
		since = now
	}
	cause := fmt.Errorf("jetq: no handler registered for task %q", info.Name)
	delay := w.unknownRetry
	if w.unknownTimeout > 0 {
		left := w.unknownTimeout - now.Sub(since)
		if left <= 0 {
			w.client.logger.ErrorContext(logCtx, "jetq no handler for task, failing it",
				"queue", info.Queue, "task", info.Name, "job", info.ID, "unknown_since", since)
			w.settle(ctx, dlv, Decision{Kind: DecideFail, Error: cause.Error(), Reason: WaitUnknown}, info, "")
			return
		}
		delay = min(delay, left)
	}
	w.client.logger.WarnContext(logCtx, "jetq no handler for task, deferring it",
		"queue", info.Queue, "task", info.Name, "job", info.ID, "unknown_since", since, "retry_in", delay)
	w.settle(ctx, dlv, Decision{Kind: DecideDefer, RunAt: now.Add(delay), Reason: WaitUnknown, UnknownSince: since}, info, "")
}

// settle persists a decision with bounded local retries. A decision that is
// durable but whose cleanup is incomplete is completed by the next delivery
// or by the driver's maintenance.
func (w *Worker) settle(ctx context.Context, dlv Delivery, dec Decision, info Info, execID string) {
	logCtx := w.deriveLogContext(ctx, info)
	if dec.ExecutionID == "" {
		dec.ExecutionID = execID
	}
	dec.DecidedAt = w.client.now()
	if dec.ExecutionID != "" {
		dec.TransitionID = TransitionID(dec.ExecutionID, dec.Kind)
	}
	if dec.TransitionID == "" {
		// No admission: the transition is keyed by the delivery's own
		// identity, which the driver records once.
		dec.TransitionID = fmt.Sprintf("%s.%d.%s", info.ID, info.Deliveries, dec.Kind)
	}
	var status SettleStatus
	var err error
	backoff := 20 * time.Millisecond
	for attempt := 1; attempt <= 4; attempt++ {
		status, err = dlv.Settle(ctx, dec)
		if status == Done || status == RecordedCleanupPending || status == SettleUnknown || ctx.Err() != nil {
			break
		}
		if attempt == 4 {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	logger := w.client.logger
	switch {
	case err == nil && (status == Done || status == RecordedCleanupPending):
		if dec.Kind == DecideFail {
			w.failed(logCtx, info, dec, payloadOf(ctx))
		}
		return
	case status == RecordedCleanupPending:
		// The decision stands; maintenance finishes the follow-up work.
		logger.WarnContext(logCtx, "jetq settlement cleanup pending",
			"queue", info.Queue, "task", info.Name, "job", info.ID, "transition", dec.TransitionID, "error", err)
	case status == SettleUnknown:
		logger.ErrorContext(logCtx, "jetq settlement outcome unknown, recovery will resolve it",
			"queue", info.Queue, "task", info.Name, "job", info.ID, "transition", dec.TransitionID, "error", err)
	default:
		// Not recorded: the delivery is offered again and the same decision
		// is retried with the same transition id.
		logger.ErrorContext(logCtx, "jetq settlement not recorded",
			"queue", info.Queue, "task", info.Name, "job", info.ID, "transition", dec.TransitionID, "error", err)
	}
}

// release gives a delivery back without running the handler.
func (w *Worker) release(ctx context.Context, dlv Delivery, info Info, logCtx context.Context) {
	if err := dlv.Release(ctx); err != nil {
		w.client.logger.WarnContext(logCtx, "jetq release failed", "queue", info.Queue, "task", info.Name, "job", info.ID, "error", err)
	}
}

// failed runs the failure observers after a failure decision is durable. It is
// best effort: a panic or error is logged and never affects settlement.
func (w *Worker) failed(ctx context.Context, info Info, dec Decision, payload json.RawMessage) {
	cause := errors.New(dec.Error)
	// Observers reach the same Info as handlers do, so [JobInfo] reports the
	// attempt, queue and task of the execution that failed.
	obsCtx := context.WithValue(ctx, infoKey{}, info)
	if h, ok := w.handlers[info.Name]; ok && h.failed != nil {
		w.callback(obsCtx, info, "failure observer", func() {
			if err := h.failed(obsCtx, payload, cause); err != nil {
				w.client.logger.WarnContext(ctx, "jetq failure observer skipped", "task", info.Name, "job", info.ID, "error", err)
			}
		})
	}
	for _, fn := range w.onFailed {
		w.callback(obsCtx, info, "failure observer", func() { fn(obsCtx, info, payload, cause) })
	}
}

// callback runs an application callback with a bounded wait. A callback that
// ignores cancellation cannot be stopped; it keeps running in the background
// and its result is ignored, so shutdown and settlement are never blocked by
// it. The leak is logged with the task and job.
func (w *Worker) callback(ctx context.Context, info Info, kind string, fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				w.client.logger.ErrorContext(ctx, "jetq "+kind+" panicked", "task", info.Name, "job", info.ID,
					"panic", r, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
	wait := w.callbackWait()
	select {
	case <-done:
	case <-time.After(wait):
		w.client.logger.ErrorContext(ctx, "jetq "+kind+" did not return in time, continuing without it",
			"task", info.Name, "job", info.ID, "wait", wait)
	}
}

// callbackWait bounds every application callback.
func (w *Worker) callbackWait() time.Duration {
	if w.callbackTimeout > 0 {
		return w.callbackTimeout
	}
	return w.settleTimeout
}

// keepAlive extends the delivery's lease while the handler, settlement and
// failure observers run, until the returned stop function is called. The
// interval is a third of the lease, so a short lease is never crossed by the
// interval itself; a lost claim stops it. Background callbacks abandoned after
// callbackWait no longer renew: their delivery is finished.
func (w *Worker) keepAlive(ctx context.Context, dlv Delivery) func() {
	lease := dlv.LeaseDuration()
	if lease <= 0 {
		return func() {}
	}
	interval := min(max(lease/3, time.Millisecond), 5*time.Second)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				extendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), interval)
				err := dlv.Extend(extendCtx)
				cancel()
				if err == nil {
					continue
				}
				w.client.logger.WarnContext(ctx, "jetq lease extension failed", "error", err)
				if errors.Is(err, ErrClaimLost) {
					return
				}
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
// ctx when it is unset, panics, returns nil or does not return in time. It is
// called while the delivery's lease is renewed.
func (w *Worker) deriveLogContext(ctx context.Context, info Info) context.Context {
	if w.logContext == nil {
		return ctx
	}
	type result struct{ ctx context.Context }
	done := make(chan result, 1)
	go func() {
		var derived context.Context
		defer func() {
			if r := recover(); r != nil {
				w.client.logger.ErrorContext(ctx, "jetq log context panicked", "task", info.Name, "job", info.ID,
					"panic", r, "stack", string(debug.Stack()))
				derived = ctx
			}
			done <- result{derived}
		}()
		derived = w.logContext(ctx, info)
	}()
	wait := w.callbackWait()
	select {
	case r := <-done:
		if r.ctx == nil {
			return ctx
		}
		return r.ctx
	case <-time.After(wait):
		w.client.logger.WarnContext(ctx, "jetq log context did not return in time, using the job context",
			"task", info.Name, "job", info.ID, "wait", wait)
		return ctx
	}
}

// ErrTimeout is the cause of the context cancellation when an execution
// exceeds its timeout, and the error the execution fails with. Read it with
// context.Cause in the handler.
var ErrTimeout = errors.New("jetq: job timed out")

// payloadKey carries the job payload to the handler context.
type payloadKey struct{}

func payloadOf(ctx context.Context) json.RawMessage {
	p, _ := ctx.Value(payloadKey{}).(json.RawMessage)
	return p
}

// maxErrorText bounds error text kept for observers and logs.
const maxErrorText = 4096

func truncate(s string) string {
	if len(s) <= maxErrorText {
		return s
	}
	return s[:maxErrorText]
}
