package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

// Recover runs one pass over the ledger and repairs state a crashed or
// disconnected process left behind:
//
//   - a record with a recorded decision whose cleanup did not finish has the
//     decision applied and the job woken, without running the handler again;
//   - a claimed record whose lease expired becomes awaiting again and is
//     delivered once more; the admission the lost delivery spent stays spent,
//     because its handler may have run;
//   - a submission intent whose job was never stored is completed from the
//     intent, so the job runs and retries report it; an intent whose job was
//     cleaned up is removed;
//   - a unique lock whose hold time passed, whose job is terminal, or whose
//     job was cleaned up is removed; a lock whose job was never stored is
//     completed from the lock instead of being released;
//   - terminal records older than JobRetention are deleted.
//
// Recovery is conservative: a read error or a write that is not a revision
// conflict is reported and the record is left unchanged. Records are never
// deleted or released because a read failed, and an undecodable record is
// left alone.
//
// It returns the number of records it changed and the first error hit after
// the pass; a caller that must not lose records treats any error as "rerun".
func (d *Driver) Recover(ctx context.Context) (int, error) {
	keys, err := d.ledger.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	now := d.now()
	fixed := 0
	var errs []error
	for _, key := range keys {
		var changed bool
		var err error
		switch {
		case strings.HasPrefix(key, "ul."):
			changed, err = d.recoverLock(ctx, key, now)
		case strings.HasPrefix(key, "ek."):
			changed, err = d.recoverIntent(ctx, key, now)
		case strings.HasPrefix(key, "oc."):
			changed, err = d.recoverOccurrence(ctx, key)
		case strings.HasPrefix(key, "job."):
			changed, err = d.recoverJob(ctx, key, now)
		}
		if changed {
			fixed++
		}
		if err != nil && ctx.Err() == nil {
			errs = append(errs, err)
		}
	}
	return fixed, errors.Join(errs...)
}

// recoverOccurrence finishes the submission of a durably claimed schedule
// occurrence whose scheduler died before submitting it. The occurrence's job
// identity is derived from its record, so finishing it here creates the same
// job the scheduler would have created, never a second one. A finished
// occurrence is dropped; the schedule cursor no longer needs it.
func (d *Driver) recoverOccurrence(ctx context.Context, key string) (bool, error) {
	occ, ok, err := d.readOccurrence(ctx, key)
	if err != nil {
		d.cfg.Logger.WarnContext(ctx, "natsq occurrence left for an operator", "key", key, "error", err)
		return false, nil
	}
	if !ok {
		return false, nil
	}
	if _, found, err := d.ledger.get(ctx, jobKey(occ.Job.Envelope.JobID)); err != nil || found {
		// The job exists: the claim is done and is removed with the job.
		return false, err
	}
	// The claim's job was never stored: store the snapshot taken at claim
	// time, whatever the schedule looks like now. The claim is kept until
	// the job's own cleanup removes it.
	res, err := d.Submit(ctx, &occ.Job)
	if err != nil {
		return false, err
	}
	return res.Status == jetq.Accepted, nil
}

// recoverIntent finishes a submission whose outcome was never committed, and
// drops an intent whose accepted job finished and was cleaned up. It never
// creates or activates a job directly: it calls the submission path, which
// decides the outcome against the unique reservation.
func (d *Driver) recoverIntent(ctx context.Context, key string, now time.Time) (bool, error) {
	ent, ok, err := d.ledger.get(ctx, key)
	if err != nil || !ok {
		return false, err
	}
	var in intentRecord
	if err := json.Unmarshal(ent.rec.Data, &in); err != nil {
		return false, nil // left for an operator
	}
	if in.Suppressed {
		// A Suppressed outcome is final for its retention, then the key is
		// free again. Until then the intent answers every retry.
		if d.retentionExpired(in.Since, now) {
			if err := d.ledger.delete(ctx, key, ent.rev); err != nil {
				if errors.Is(err, errRevisionMismatch) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		}
		return false, nil
	}
	jobEnt, jobOK, err := d.ledger.get(ctx, jobKey(in.JobID))
	if err != nil {
		return false, err
	}
	switch {
	case jobOK && jobEnt.rec.Outcome == outcomePending:
		// The submission is in flight or its producer died: finish it through
		// the same commit path. The unique rule decides rejected/accepted.
		res, err := d.submit(ctx, &in.Envelope, in.Fingerprint)
		if err != nil && !errors.Is(err, jetq.ErrEnqueueKeyConflict) {
			return false, err
		}
		if res.Status == jetq.Suppressed {
			return false, nil
		}
		return true, nil
	case jobOK:
		if in.Stored {
			return false, nil
		}
		if err := d.markIntentStored(ctx, &in.Envelope); err != nil && !errors.Is(err, errRevisionMismatch) {
			return false, err
		}
		return true, nil
	case in.Stored:
		// The job finished and was cleaned up: the key is free again.
		if err := d.ledger.delete(ctx, key, ent.rev); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	// No job record and no accepted marker: the submission's outcome was never
	// committed. Re-run the submission path so the unique rule is honoured.
	res, err := d.submit(ctx, &in.Envelope, in.Fingerprint)
	if err != nil && !errors.Is(err, jetq.ErrEnqueueKeyConflict) {
		return false, err
	}
	return res.Status == jetq.Accepted, nil
}

func (d *Driver) recoverJob(ctx context.Context, key string, now time.Time) (bool, error) {
	ent, ok, err := d.ledger.get(ctx, key)
	if err != nil || !ok {
		return false, err
	}
	rec := cloneRecord(ent.rec)
	if !rec.live() {
		return d.recoverDeadJob(ctx, ent, now)
	}
	switch {
	case rec.Pending != nil:
		// Finish the recorded decision through the same path as the
		// settling delivery; the handler does not run again.
		dec := *rec.Pending
		return d.completeRecord(ctx, key, dec)
	case rec.State == jetq.StateRunning && rec.ClaimUntil.Before(now):
		// The claiming process is gone. The job runs again; the lost
		// execution's budget stays spent and is bounded by MaxAttempts.
		d.takeover(rec)
		if _, err := d.ledger.put(ctx, key, ent.rev, rec); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				return false, nil
			}
			return false, err
		}
		_ = d.wakeJob(ctx, rec.Env)
		return true, nil
	case rec.State.Terminal() && d.retentionPassed(rec, now):
		// A schedule claim that depends on the job goes first: while it
		// exists the job (and its intent) is the fact that the occurrence
		// already ran. If removing it fails the job is kept for the next
		// pass, so a claim can never re-create a collected job.
		if err := d.releaseOccurrence(ctx, rec.Env.JobID); err != nil {
			return false, err
		}
		// The auxiliary records go first: an intent or lock that outlives a
		// cleaned-up job could recreate it, and a marked auxiliary record
		// without its job is how a finished submission is recognized.
		if err := d.deleteIntentOf(ctx, rec.Env); err != nil {
			return false, err
		}
		if err := d.releaseUnique(ctx, rec.Env, rec.Env.JobID); err != nil {
			return false, err
		}
		if err := d.ledger.delete(ctx, key, ent.rev); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// recoverDeadJob handles a job record whose submission was never accepted:
// pending records are finished through the submission path, suppressed and
// rejected records are kept for the retention and then removed.
func (d *Driver) recoverDeadJob(ctx context.Context, ent *ledgerEntry, now time.Time) (bool, error) {
	rec := ent.rec
	key := jobKey(rec.Env.JobID)
	if rec.Outcome == outcomePending {
		fingerprint := rec.Fingerprint
		res, err := d.submit(ctx, rec.Env, fingerprint)
		if err != nil && !errors.Is(err, jetq.ErrEnqueueKeyConflict) {
			return false, err
		}
		return res.Status == jetq.Accepted, nil
	}
	if !d.retentionPassed(rec, now) {
		return false, nil
	}
	// A suppressed or rejected occurrence receipt is also the fact that the
	// occurrence was decided; its claim goes first.
	if err := d.releaseOccurrence(ctx, rec.Env.JobID); err != nil {
		return false, err
	}
	if err := d.ledger.delete(ctx, key, ent.rev); err != nil {
		if errors.Is(err, errRevisionMismatch) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// retentionExpired reports whether a record created at since may be deleted.
func (d *Driver) retentionExpired(since, now time.Time) bool {
	if d.cfg.JobRetention < 0 || since.IsZero() {
		return false
	}
	return now.Sub(since) > d.cfg.JobRetention
}

// retentionPassed reports whether a terminal record may be deleted. Failed
// jobs keep their record for the same retention so the failure stays visible.
func (d *Driver) retentionPassed(rec *record, now time.Time) bool {
	if d.cfg.JobRetention < 0 || rec.TerminalAt.IsZero() {
		return false
	}
	return now.Sub(rec.TerminalAt) > d.cfg.JobRetention
}

// recoverLock removes a unique lock that no longer suppresses anything: its
// hold time passed, or its job is terminal. A lock whose job was never stored
// completes that job instead; a lock whose job cannot be read is kept.
func (d *Driver) recoverLock(ctx context.Context, key string, now time.Time) (bool, error) {
	ent, ok, err := d.ledger.get(ctx, key)
	if err != nil || !ok {
		return false, err
	}
	held, err := decodeLockPayload(ent, true)
	if err != nil {
		// Left for an operator: the undecodable lock names no job, so
		// recovery cannot tell whether releasing it is safe.
		return false, nil
	}
	before := held.Stored
	state, err := d.assessLock(ctx, held, now)
	if err != nil {
		return false, err
	}
	if state == lockActive {
		if before {
			return false, nil
		}
		// assessLock may have marked the lock stored.
		after, ok, err := d.ledger.get(ctx, key)
		if err != nil || !ok {
			return false, err
		}
		repaired, err := decodeLockPayload(after, true)
		return err == nil && repaired.Stored, nil
	}
	if err := d.ledger.delete(ctx, key, ent.rev); err != nil {
		if errors.Is(err, errRevisionMismatch) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Maintain implements [jetq.Maintainer]: it runs recovery passes until ctx is
// cancelled, so a worker's normal operation repairs crashed deliveries and
// lost wake-ups without the application calling anything extra.
func (d *Driver) Maintain(ctx context.Context) error {
	ticker := time.NewTicker(d.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		if _, err := d.Recover(ctx); err != nil && ctx.Err() == nil {
			d.cfg.Logger.WarnContext(ctx, "natsq recovery scan failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// releaseOccurrence removes the schedule claim of jobID, if any. Missing is
// success, so the step is idempotent.
func (d *Driver) releaseOccurrence(ctx context.Context, jobID string) error {
	key := occurrenceKey(d.cfg.Namespace, jobID)
	ent, ok, err := d.ledger.get(ctx, key)
	if err != nil || !ok {
		return err
	}
	if err := d.ledger.delete(ctx, key, ent.rev); err != nil && !errors.Is(err, errRevisionMismatch) {
		return fmt.Errorf("natsq: release occurrence of %s: %w", jobID, err)
	}
	if _, still, err := d.ledger.get(ctx, key); err != nil || still {
		if err == nil {
			err = fmt.Errorf("natsq: occurrence of %s changed during release", jobID)
		}
		return err
	}
	return nil
}
