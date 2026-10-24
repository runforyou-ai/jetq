package natsq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/runforyou-ai/jetq"
)

// receiveWait bounds one Receive call for an empty queue.
var receiveWait = 500 * time.Millisecond

// Consumer delivers jobs of one queue. It is a local view: closing it neither
// deletes shared state nor touches the application's connection.
type Consumer struct {
	d     *Driver
	queue string

	mu     sync.Mutex
	closed bool
	taken  map[string]bool // jobs handed out and not yet settled
	rr     int             // round-robin position of the claim sweep
	sub    *nats.Subscription
}

// wakeSub returns the consumer's wake subscription, creating it once.
func (c *Consumer) wakeSub() (*nats.Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sub != nil {
		return c.sub, nil
	}
	sub, err := c.d.js.Conn().SubscribeSync(c.d.cfg.Namespace + ".wake")
	if err != nil {
		return nil, err
	}
	c.sub = sub
	return sub, nil
}

// Consumer attaches a consumer of queue.
func (d *Driver) Consumer(ctx context.Context, queue string) (jetq.Consumer, error) {
	if err := jetq.ValidateID("queue", queue); err != nil {
		return nil, err
	}
	return &Consumer{d: d, queue: queue, taken: map[string]bool{}}, nil
}

// Close stops local delivery. Jobs it holds stay claimed until their lease
// ends unless they were settled.
func (c *Consumer) Close() error {
	c.mu.Lock()
	sub := c.sub
	c.sub = nil
	c.closed = true
	c.mu.Unlock()
	if sub != nil {
		return sub.Unsubscribe()
	}
	return nil
}

func (c *Consumer) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Receive returns up to max eligible deliveries of the queue. The jobs come
// from the ledger: one queued, unclaimed job of the queue whose run time has
// passed becomes claimed here, so a delivery always belongs to exactly one
// consumer at a time.
func (c *Consumer) Receive(ctx context.Context, max int) ([]jetq.Delivery, error) {
	if c.isClosed() {
		return nil, errors.New("natsq: consumer closed")
	}
	if max <= 0 {
		max = 1
	}
	deadline := time.Now().Add(receiveWait)
	var out []jetq.Delivery
	for len(out) < max && ctx.Err() == nil {
		jobID, err := c.nextCandidate(ctx)
		if err != nil {
			return out, err
		}
		if jobID != "" {
			if dlv := c.tryClaim(ctx, jobID); dlv != nil {
				out = append(out, dlv)
				continue
			}
			continue
		}
		if c.holdsCandidate(ctx) {
			// A job this consumer already delivered is due again (retry,
			// release or lease expiry). Waiting for a wake-up would delay it
			// pointlessly; check again after a short pause.
			select {
			case <-ctx.Done():
				return out, nil
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		// No queued job was visible in this pass. Wait for a wake-up of this
		// queue, but no longer than the next recovery scan.
		if err := c.waitWake(ctx, time.Until(deadline)); err != nil {
			if len(out) > 0 || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return out, nil
			}
			return out, err
		}
		deadline = time.Now().Add(receiveWait)
	}
	return out, nil
}

// waitWake waits for the next wake-up message, at most timeout. It is an
// optimization only: missing it costs the scan interval.
func (c *Consumer) waitWake(ctx context.Context, timeout time.Duration) error {
	timeout = min(timeout, c.d.cfg.ScanInterval)
	if timeout <= 0 {
		return context.DeadlineExceeded
	}
	sub, err := c.wakeSub()
	if err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err = sub.NextMsgWithContext(waitCtx)
	return err
}

// holdsCandidate reports whether a job this consumer delivered is due again.
func (c *Consumer) holdsCandidate(ctx context.Context) bool {
	c.mu.Lock()
	ids := make([]string, 0, len(c.taken))
	for id := range c.taken {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	now := c.d.now()
	for _, id := range ids {
		ent, ok, err := c.d.ledger.get(ctx, jobKey(id))
		if err != nil || !ok {
			continue
		}
		if ent.rec.Env != nil && ent.rec.Env.Queue == c.queue && c.d.deliverable(ent.rec, now) {
			return true
		}
	}
	return false
}

// nextCandidate finds a queued job of this consumer's queue that is eligible
// and not claimed, or whose claim expired. It walks the ledger keys in a
// rotating order so that a busy queue cannot starve later keys.
func (c *Consumer) nextCandidate(ctx context.Context) (string, error) {
	keys, err := c.d.ledger.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("natsq: list ledger: %w", err)
	}
	now := c.d.now()
	n := len(keys)
	c.mu.Lock()
	start := 0
	if n > 0 {
		start = c.rr % n
	}
	c.mu.Unlock()
	for i := range n {
		key := keys[(start+i)%n]
		id, ok := strings.CutPrefix(key, "job.")
		if !ok {
			continue
		}
		ent, found, err := c.d.ledger.get(ctx, key)
		if err != nil {
			return "", err
		}
		if !found || ent.rec.Env == nil || ent.rec.Env.Queue != c.queue || !c.d.deliverable(ent.rec, now) {
			continue
		}
		c.mu.Lock()
		c.rr = start + i + 1
		c.mu.Unlock()
		return id, nil
	}
	return "", nil
}

// deliverable reports whether a record may be delivered now: still awaiting,
// due, and not claimed by a live delivery.
func (d *Driver) deliverable(rec *record, now time.Time) bool {
	if !rec.live() {
		// A pending or suppressed submission never runs.
		return false
	}
	if rec.Canceled && rec.State != jetq.StateCanceled {
		return false
	}
	switch rec.State {
	case jetq.StateAwaiting:
		return rec.RunAt.IsZero() || !rec.RunAt.After(now)
	case jetq.StateRunning:
		// A claim that outlived its lease: the claiming delivery is gone.
		// The job is taken over, its spent admission stays spent.
		return !rec.ClaimUntil.After(now)
	default:
		return false
	}
}

// claim is the compare-and-set that turns a due record into a claimed one.
// The lease is also the admission's deadline: the engine must Admit within it
// or the delivery no longer holds the job.
func (d *Driver) claim(rec *record, claimID string, now time.Time, deliveries int) *record {
	rec.State = jetq.StateRunning
	rec.ClaimID = claimID
	rec.ClaimUntil = now.Add(d.cfg.ClaimLease)
	rec.Deliveries = deliveries
	return rec
}

// takeover prepares a record whose claim expired for a new delivery. The
// execution the lost delivery admitted stays spent: its handler may have run.
// The job is treated as awaiting again so its next run is a retry.
func (d *Driver) takeover(rec *record) {
	rec.State = jetq.StateAwaiting
	rec.Wait = jetq.WaitRetry
	rec.ClaimID = ""
	rec.ClaimUntil = time.Time{}
	rec.RunAt = time.Time{}
}

func (c *Consumer) tryClaim(ctx context.Context, jobID string) jetq.Delivery {
	key := jobKey(jobID)
	claimID := nuid.Next()
	for range 5 {
		ent, found, err := c.d.ledger.get(ctx, key)
		if err != nil || !found || !c.d.deliverable(ent.rec, c.d.now()) {
			return nil
		}
		rec := cloneRecord(ent.rec)
		if rec.State == jetq.StateRunning && !rec.ClaimUntil.After(c.d.now()) {
			c.d.takeover(rec)
		}
		rec.Deliveries++
		c.d.claim(rec, claimID, c.d.now(), rec.Deliveries)
		rev, err := c.d.ledger.put(ctx, key, ent.rev, rec)
		if errors.Is(err, errRevisionMismatch) {
			continue
		}
		if err != nil {
			return nil
		}
		c.mu.Lock()
		c.taken[jobID] = true
		c.mu.Unlock()
		return &delivery{c: c, jobID: jobID, claimID: claimID, rev: rev, rec: stripClaim(rec)}
	}
	return nil
}

// stripClaim returns the record as the ledger holds it, for Delivery.State.
func stripClaim(rec *record) *record {
	c := cloneRecord(rec)
	return c
}

// delivery is one claimed job.
type delivery struct {
	c       *Consumer
	jobID   string
	claimID string
	rev     uint64 // revision right after the claim
	rec     *record

	// mu guards the fields below only; it is never held across a ledger
	// write, so a lease extension and a slow settlement run concurrently.
	admitMu   sync.Mutex // serializes concurrent Admit calls
	settleMu  sync.Mutex // serializes concurrent Settle calls
	mu        sync.Mutex
	admission *jetq.Admission
	// execID is the execution id of this delivery's admission. It survives a
	// failed Admit call, so a retry reuses it instead of spending a second
	// unit of budget.
	execID    string
	admitted  bool
	released  bool
	settled   bool
	settleDec string // transition id of the decision this delivery settled
}

func (d *delivery) Envelope() *jetq.Envelope { return d.rec.Env }

func (d *delivery) LeaseDuration() time.Duration { return d.c.cfg().ClaimLease }

func (c *Consumer) cfg() Config { return c.d.cfg }

// State reports the durable state this delivery knows of: the state at claim
// time, updated by every Admit, Extend and Settle it performs.
func (d *delivery) State() jetq.ExecutionState {
	d.mu.Lock()
	defer d.mu.Unlock()
	rec := d.rec
	st := jetq.ExecutionState{
		Budget:       rec.Budget,
		Snoozes:      rec.Snoozes,
		Deliveries:   rec.Deliveries,
		Canceled:     rec.Canceled,
		UnknownSince: rec.UnknownSince,
	}
	if rec.Pending != nil {
		// A private copy: the engine may hold on to it after the delivery
		// moved on.
		pending := *rec.Pending
		st.Pending = &pending
	}
	if rec.Admitted > 0 {
		st.Admitted = rec.ExecutionID
	}
	return st
}

// reread reads the current record. Every mutation works on the latest
// revision under the CAS, never on the claim-time copy.
func (d *delivery) reread(ctx context.Context) (*ledgerEntry, error) {
	ent, found, err := d.c.d.ledger.get(ctx, jobKey(d.jobID))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("natsq: job %s record disappeared", d.jobID)
	}
	return ent, nil
}

// heldBy reports whether the record still carries this delivery's claim.
func (d *delivery) heldBy(rec *record) bool { return rec.ClaimID == d.claimID }

// Admit records the start of a business execution and spends one unit of
// budget, before the handler runs. The execution id is persisted: a crash
// after this point leaves an admission the recovery counts as spent.
func (d *delivery) Admit(ctx context.Context) (jetq.Admission, error) {
	d.mu.Lock()
	if d.admission != nil {
		adm := *d.admission
		d.mu.Unlock()
		return adm, nil
	}
	if d.released {
		d.mu.Unlock()
		return jetq.Admission{}, errors.New("natsq: delivery released")
	}
	// The execution id is chosen once per delivery and kept, so an Admit whose
	// response was lost is recognized on the retry: the record already names
	// this execution, and no second unit of budget is spent.
	execID := d.execID
	if execID == "" {
		execID = nuid.Next()
		// Remember it before writing: if the write's response is lost, the
		// retry reuses the id and recognizes its own committed admission
		// instead of spending a second unit of budget.
		d.execID = execID
	}
	d.mu.Unlock()
	// One Admit writes for this delivery at a time; the lock is not held
	// across the ledger write.
	d.admitMu.Lock()
	defer d.admitMu.Unlock()
	for range 5 {
		ent, err := d.reread(ctx)
		if err != nil {
			return jetq.Admission{}, err
		}
		rec := cloneRecord(ent.rec)
		if !d.heldBy(rec) || rec.ClaimUntil.Before(d.c.d.now()) {
			return jetq.Admission{}, fmt.Errorf("natsq: admit job %s: %w", d.jobID, jetq.ErrClaimLost)
		}
		if rec.Pending != nil {
			return jetq.Admission{}, fmt.Errorf("natsq: job %s already has a recorded decision %s", d.jobID, rec.Pending.TransitionID)
		}
		if rec.Canceled {
			return jetq.Admission{}, fmt.Errorf("natsq: admit job %s: %w: job was canceled", d.jobID, jetq.ErrClaimLost)
		}
		if rec.ExecutionID == execID && rec.Admitted > 0 {
			// This delivery's admission is already recorded (its response was
			// lost, or Admit was called twice): report it without spending
			// more budget, even at the budget limit.
			adm := jetq.Admission{ExecutionID: execID, Attempt: rec.Budget}
			d.mu.Lock()
			d.admitted = true
			d.rec = stripClaim(rec)
			d.admission = &adm
			d.mu.Unlock()
			return adm, nil
		}
		if rec.Budget >= rec.Env.Policy.MaxAttempts {
			return jetq.Admission{}, fmt.Errorf("natsq: job %s exhausted its budget %d", d.jobID, rec.Budget)
		}
		// Reaching this point means the record does not name this execution
		// yet, so the admission spends one unit of budget.
		rec.Budget++
		rec.Admitted++
		rec.ExecutionID = execID
		rec.ClaimUntil = d.c.d.now().Add(d.c.cfg().ClaimLease)
		rev, err := d.c.d.ledger.put(ctx, jobKey(d.jobID), ent.rev, rec)
		if errors.Is(err, errRevisionMismatch) {
			continue
		}
		if err != nil {
			return jetq.Admission{}, err
		}
		// The hook runs after the admission is durable: a failure injected
		// here simulates a lost response, never a lost write.
		d.c.d.maybeHookAdmit(execID)
		adm := jetq.Admission{ExecutionID: execID, Attempt: rec.Budget}
		d.mu.Lock()
		d.admitted = true
		d.rev = rev
		d.rec = stripClaim(rec)
		d.admission = &adm
		d.mu.Unlock()
		return adm, nil
	}
	return jetq.Admission{}, fmt.Errorf("natsq: admit job %s: %w", d.jobID, errRevisionMismatch)
}

// Extend renews the claim until the next lease. It holds the delivery's lock
// only to read local state, never across the ledger write, so a slow
// settlement does not stop the lease from being renewed.
func (d *delivery) Extend(ctx context.Context) error {
	d.mu.Lock()
	gone := d.released || d.settled
	d.mu.Unlock()
	if gone {
		return fmt.Errorf("natsq: extend job %s: %w", d.jobID, jetq.ErrClaimLost)
	}
	for range 5 {
		ent, err := d.reread(ctx)
		if err != nil {
			return err
		}
		rec := cloneRecord(ent.rec)
		if !d.heldBy(rec) {
			return fmt.Errorf("natsq: extend job %s: %w", d.jobID, jetq.ErrClaimLost)
		}
		rec.ClaimUntil = d.c.d.now().Add(d.c.cfg().ClaimLease)
		rev, err := d.c.d.ledger.put(ctx, jobKey(d.jobID), ent.rev, rec)
		if errors.Is(err, errRevisionMismatch) {
			continue
		}
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.rev = rev
		d.mu.Unlock()
		return nil
	}
	return fmt.Errorf("natsq: extend job %s: %w", d.jobID, errRevisionMismatch)
}

// Release gives the delivery back unrun: the job becomes due again at once
// and its budget is untouched.
func (d *delivery) Release(ctx context.Context) error {
	d.mu.Lock()
	if d.released || d.settled {
		d.mu.Unlock()
		return nil
	}
	d.released = true
	d.mu.Unlock()
	defer d.c.forget(d.jobID)
	return d.c.d.releaseDelivery(ctx, d)
}
