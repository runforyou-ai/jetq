package natsq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/runforyou-ai/jetq"
)

// Settle persists the decision by its transition id and then performs the
// follow-up work. Both steps are idempotent:
//
//   - The decision is written once, under the claim's revision. A retry that
//     finds it recorded resumes the follow-up instead of writing it again.
//   - The budget refund of a snooze or release is applied at most once, keyed
//     by the transition id in the record.
//   - The next execution is a change of the same record's run time, so it
//     cannot be created twice.
//
// A decision whose follow-up cannot finish stays recorded as Pending; the
// next delivery of the job, or the recovery scan, completes it without
// running the handler again.
func (d *delivery) Settle(ctx context.Context, dec jetq.Decision) (jetq.SettleStatus, error) {
	// The lock guards local state for reading and for the final state update;
	// it is never held across a ledger write, so Extend can renew the lease
	// while a settlement is in flight.
	d.mu.Lock()
	released := d.released
	settled := d.settled
	settledDec := d.settleDec
	adm := d.admission
	d.mu.Unlock()
	switch {
	case released:
		// Nothing was decided: the job is still awaiting. Claiming Done would
		// hide that the delivery was given back and the decision lost.
		return jetq.NotRecorded, fmt.Errorf("natsq: settle job %s: %w: delivery was released", d.jobID, jetq.ErrClaimLost)
	case settled:
		if settledDec == transitionOf(dec) {
			// A repeat of the same settlement is idempotent.
			return jetq.Done, nil
		}
		// A delivery settles once. A different decision was never written.
		return jetq.NotRecorded, fmt.Errorf("natsq: settle job %s: delivery already settled with %s", d.jobID, settledDec)
	}
	// Two settlements of one delivery at once: the later one is a repeat or a
	// contradiction, never a second decision.
	d.settleMu.Lock()
	defer d.settleMu.Unlock()

	if dec.DecidedAt.IsZero() {
		dec.DecidedAt = d.c.d.now()
	}
	if dec.TransitionID == "" {
		return jetq.NotRecorded, errors.New("natsq: decision without transition id")
	}
	if adm != nil {
		// The engine's decision belongs to the execution this delivery
		// admitted; its own transition id is "forgotten" here and never used.
		dec.ExecutionID = adm.ExecutionID
		dec.TransitionID = jetq.TransitionID(dec.ExecutionID, dec.Kind)
	}
	d.mu.Lock()
	released, settled, settledDec = d.released, d.settled, d.settleDec
	d.mu.Unlock()
	if released {
		return jetq.NotRecorded, fmt.Errorf("natsq: settle job %s: %w: delivery was released", d.jobID, jetq.ErrClaimLost)
	}
	if settled {
		if settledDec == dec.TransitionID {
			return jetq.Done, nil
		}
		return jetq.NotRecorded, fmt.Errorf("natsq: settle job %s: delivery already settled with %s", d.jobID, settledDec)
	}
	// Without an admission this is a resume of a recorded decision, possibly
	// of an execution an earlier delivery admitted; recordDecision checks
	// that it matches the record's pending decision.
	if err := d.c.d.recordDecision(ctx, d, dec); err != nil {
		switch {
		case errors.Is(err, errClaimLost):
			return jetq.SettleUnknown, fmt.Errorf("natsq: settle job %s: %w", d.jobID, jetq.ErrClaimLost)
		case errors.Is(err, errUnknownOutcome):
			return jetq.SettleUnknown, fmt.Errorf("natsq: settle job %s: %w: %w", d.jobID, jetq.ErrUncertain, err)
		default:
			return jetq.NotRecorded, fmt.Errorf("natsq: settle job %s: %w", d.jobID, err)
		}
	}
	if err := d.c.d.completeDecision(ctx, d, dec); err != nil {
		if errors.Is(err, errUnknownOutcome) {
			return jetq.SettleUnknown, fmt.Errorf("natsq: settle job %s: %w: %w", d.jobID, jetq.ErrUncertain, err)
		}
		// The decision is durable; the next delivery or the scan finishes
		// the cleanup.
		return jetq.RecordedCleanupPending, fmt.Errorf("natsq: settle job %s: %w", d.jobID, err)
	}
	d.mu.Lock()
	d.settled, d.settleDec = true, dec.TransitionID
	d.mu.Unlock()
	d.c.forget(d.jobID)
	return jetq.Done, nil
}

// transitionOf is the transition id a decision carries before the delivery's
// admission is applied to it.
func transitionOf(dec jetq.Decision) string {
	if dec.TransitionID != "" {
		return dec.TransitionID
	}
	if dec.ExecutionID != "" {
		return jetq.TransitionID(dec.ExecutionID, dec.Kind)
	}
	return ""
}

var (
	// errClaimLost: the record no longer carries this delivery's claim.
	errClaimLost = errors.New("natsq: claim lost")
	// errUnknownOutcome: the write failed in a way only recovery can settle.
	errUnknownOutcome = errors.New("natsq: write outcome unknown")
)

// recordDecision appends the decision to the record under the claim's
// revision. When the transition is already applied it does nothing.
func (d *Driver) recordDecision(ctx context.Context, dlv *delivery, dec jetq.Decision) error {
	for range 5 {
		ent, err := dlv.reread(ctx)
		if err != nil {
			return err
		}
		rec := cloneRecord(ent.rec)
		if rec.Pending != nil {
			if rec.Pending.TransitionID == dec.TransitionID {
				return nil // a retry of the same settlement
			}
			return fmt.Errorf("natsq: job %s has a different recorded decision %s", dlv.jobID, rec.Pending.TransitionID)
		}
		already := slices.Contains(rec.Applied, dec.TransitionID)
		if !already && !dlv.heldBy(rec) {
			// This delivery no longer holds the job. A settlement retried
			// after its completion is fine; anything else would apply to a
			// job another delivery now owns.
			return errClaimLost
		}
		if already {
			// Applied earlier: only the follow-up work may be missing. The
			// revision must be the one the application left behind.
			return nil
		}
		pending := dec
		rec.Pending = &pending
		if _, err := d.ledger.put(ctx, jobKey(dlv.jobID), ent.rev, rec); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return errUnknownOutcome
		}
		if h := d.hks.afterDecisionWrite; h != nil {
			if err := h(dec); err != nil {
				// The write is durable but the caller cannot learn that:
				// report the outcome as unknown.
				return errUnknownOutcome
			}
		}
		return nil
	}
	return errUnknownOutcome
}

// completeDecision applies the recorded decision: budget refund, next run
// time, unique release, terminal state, then wake-up. The decision stays
// Pending until all of it is done, so a crash anywhere in between is repaired
// by the next delivery or the recovery scan, and the budget is refunded once.
func (d *Driver) completeDecision(ctx context.Context, dlv *delivery, dec jetq.Decision) error {
	if _, err := d.completeRecord(ctx, jobKey(dlv.jobID), dec); err != nil {
		return err
	}
	return nil
}

// completeRecord applies dec to the job record at key until it is done:
// budget refund, next run time, terminal state, unique release, wake-up, and
// finally clearing the pending marker. It reports whether the record changed.
func (d *Driver) completeRecord(ctx context.Context, key string, dec jetq.Decision) (bool, error) {
	// 1. Apply the decision to the record, keeping it Pending until step 3.
	var rec *record
	for range 5 {
		ent, ok, err := d.ledger.get(ctx, key)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil // cleaned up
		}
		cur := cloneRecord(ent.rec)
		if cur.Pending == nil || cur.Pending.TransitionID != dec.TransitionID {
			// Another writer completed it, or a newer decision replaced it.
			return false, nil
		}
		cur.Decided = append(cur.Decided, *cur.Pending)
		d.applyDecision(cur, cur.Decided[len(cur.Decided)-1], d.now())
		cur.Pending = &cur.Decided[len(cur.Decided)-1]
		if _, err := d.ledger.put(ctx, key, ent.rev, cur); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return false, err
		}
		rec = cur
		break
	}
	if rec == nil {
		return false, errRevisionMismatch
	}
	// 2. Follow-up work outside the job record. The unique lock of a terminal
	// job is released only after its decision is durable, so a crash before
	// the release cannot let a new job overlap an unsettled one.
	// A window lock is held for its window, not for the job's lifetime.
	if rec.State.Terminal() && rec.Env.Unique != nil && rec.Env.Unique.Mode != jetq.UniqueModeWindow {
		if err := d.releaseUnique(ctx, rec.Env, rec.Env.JobID); err != nil {
			return true, fmt.Errorf("natsq: release unique lock of %s: %w", rec.Env.JobID, err)
		}
	}
	if h := d.hks.afterCleanup; h != nil {
		if err := h(dec); err != nil {
			return true, err
		}
	}
	if rec.State == jetq.StateAwaiting {
		if err := d.wakeJob(ctx, rec.Env); err != nil {
			// The scan will wake the job; the decision stands.
			d.cfg.Logger.WarnContext(ctx, "natsq wake publish failed", "job", rec.Env.JobID, "error", err)
		}
	}
	// 3. Clear the pending marker: the decision is complete.
	for range 5 {
		ent, ok, err := d.ledger.get(ctx, key)
		if err != nil {
			return true, err
		}
		if !ok {
			return true, nil
		}
		cur := cloneRecord(ent.rec)
		if cur.Pending == nil || cur.Pending.TransitionID != dec.TransitionID {
			return true, nil
		}
		cur.Pending = nil
		cur.Decided = nil
		if _, err := d.ledger.put(ctx, key, ent.rev, cur); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return true, err
		}
		return true, nil
	}
	return true, errRevisionMismatch
}

// applyDecision updates rec by dec, at most once per transition id. It
// releases the unique lock for terminal decisions.
func (d *Driver) applyDecision(rec *record, dec jetq.Decision, now time.Time) {
	if slices.Contains(rec.Applied, dec.TransitionID) {
		// Only the cleanup is left; the fields are already applied.
		return
	}
	rec.Applied = append(rec.Applied, dec.TransitionID)
	if dec.Kind.Refunds() && dec.ExecutionID != "" && rec.Admitted > 0 {
		rec.Budget--
		rec.Admitted--
	}
	if dec.Error != "" {
		rec.LastError = dec.Error
	}
	rec.ExecutionID = ""
	rec.ClaimID = ""
	rec.ClaimUntil = time.Time{}
	if rec.Canceled && dec.Kind.Reschedules() {
		// A cancellation recorded while the execution ran prevents every
		// future execution, including the retry this decision would schedule.
		rec.State = jetq.StateCanceled
		rec.TerminalAt = now
		rec.RunAt = time.Time{}
		return
	}
	switch dec.Kind {
	case jetq.DecideSucceed:
		rec.State = jetq.StateSucceeded
		rec.TerminalAt = now
	case jetq.DecideFail:
		rec.State = jetq.StateFailed
		rec.TerminalAt = now
	case jetq.DecideRetry:
		rec.State, rec.Wait, rec.RunAt = jetq.StateAwaiting, jetq.WaitRetry, dec.RunAt
	case jetq.DecideSnooze:
		rec.State, rec.Wait, rec.RunAt = jetq.StateAwaiting, jetq.WaitSnoozed, dec.RunAt
		rec.Snoozes++
	case jetq.DecideRelease:
		rec.State, rec.Wait, rec.RunAt = jetq.StateAwaiting, jetq.WaitReleased, dec.RunAt
	case jetq.DecideDefer:
		rec.State, rec.Wait, rec.RunAt = jetq.StateAwaiting, dec.Reason, dec.RunAt
		if rec.Wait == "" {
			rec.Wait = jetq.WaitRetry
		}
		rec.UnknownSince = dec.UnknownSince
	case jetq.DecideCancel:
		rec.State = jetq.StateCanceled
		rec.TerminalAt = now
		rec.RunAt = time.Time{}
	}
}

// releaseDelivery gives a claimed job back without a decision.
func (d *Driver) releaseDelivery(ctx context.Context, dlv *delivery) error {
	for range 5 {
		ent, err := dlv.reread(ctx)
		if err != nil {
			return err
		}
		rec := cloneRecord(ent.rec)
		if !dlv.heldBy(rec) {
			// Someone else owns it now; nothing to release.
			return nil
		}
		// An admission made by this delivery is spent; the engine releases
		// only deliveries that never admitted.
		rec.State = jetq.StateAwaiting
		rec.ClaimID = ""
		rec.ClaimUntil = time.Time{}
		rec.RunAt = time.Time{}
		if rec.Wait == "" {
			rec.Wait = jetq.WaitScheduled
		}
		_, err = d.ledger.put(ctx, jobKey(dlv.jobID), ent.rev, rec)
		if errors.Is(err, errRevisionMismatch) {
			continue
		}
		if err != nil {
			return err
		}
		return d.wakeJob(ctx, rec.Env)
	}
	return errRevisionMismatch
}

// forget drops the job from the consumer's taken set.
func (c *Consumer) forget(jobID string) {
	c.mu.Lock()
	delete(c.taken, jobID)
	c.mu.Unlock()
}

// maybeHookAdmit runs the admission fault hook.
func (d *Driver) maybeHookAdmit(executionID string) {
	if d.hks.afterAdmit != nil {
		d.hks.afterAdmit(executionID)
	}
}
