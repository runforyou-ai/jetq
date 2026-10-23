package natsq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/runforyou-ai/jetq"
)

// Submission protocol
//
// A submission writes up to three ledger keys: the intent of its EnqueueKey
// (ek.…), the unique lock (ul.…, a reservation) and the job record (job.…).
// They are separate keys, so no step is atomic with the others. Correctness
// comes from one commit point:
//
//   - A new job record starts with Outcome pending. A pending record is never
//     delivered and holds no budget; it records a submission in flight.
//   - The submission's outcome is decided by a single compare-and-set on the
//     canonical job record: pending to accepted (the job becomes awaiting and
//     is woken) or pending to suppressed (a final result; the record is kept
//     for the documented retention and can never become executable).
//   - Whoever wins that CAS decides the submission. Everyone else, including
//     a helper that found the intent or the reservation, reads the final
//     outcome, finishes only the follow-up work (markers, lock release) and
//     reports it. A loser never creates or activates a job from a stale view.
//   - The intent and the reservation carry the complete prepared job so a
//     helper can finish a pending submission, but a helper must decide the
//     outcome against the unique reservation exactly as the original producer
//     would: active reservation means pending to suppressed.
//   - An accepted submission is recognized by its canonical outcome even after
//     the job record is cleaned up: the intent (Stored) and the lock (Stored)
//     keep that record. A suppressed submission is final regardless.

// intentRecord is the durable submission intent of an EnqueueKey.
type intentRecord struct {
	JobID       string        `json:"job_id"`
	Fingerprint string        `json:"fingerprint"`
	Envelope    jetq.Envelope `json:"envelope"`
	// Stored is set once the canonical outcome is accepted and its job record
	// exists. A marked intent whose job is gone means the job finished and was
	// cleaned up.
	Stored bool `json:"stored"`
	// Suppressed marks a final Suppressed outcome: the submission stored no
	// job, and every retry of this EnqueueKey reports Suppressed for the
	// retention instead of evaluating the unique rule again.
	Suppressed bool `json:"suppressed,omitempty"`
	// SuppressedBy is the job that suppressed the submission.
	SuppressedBy string `json:"suppressed_by,omitempty"`
	// Since is when the intent was created, the retention basis of a
	// suppressed intent.
	Since time.Time `json:"since,omitzero"`
}

// lockRecordPayload is the durable unique reservation of a key.
type lockRecordPayload struct {
	JobID       string          `json:"job_id"`
	Fingerprint string          `json:"fingerprint"`
	Mode        jetq.UniqueMode `json:"mode"`
	Since       time.Time       `json:"since"`
	Until       time.Time       `json:"until"`
	// Envelope is the prepared job the reservation belongs to, so a helper can
	// decide its pending submission.
	Envelope *jetq.Envelope `json:"envelope,omitempty"`
	// Stored is set once its job's outcome is accepted.
	Stored bool `json:"stored"`
}

// Submission outcomes of the canonical job record.
const (
	outcomePending    = "pending"
	outcomeAccepted   = "accepted"
	outcomeSuppressed = "suppressed"
	// outcomeRejected marks a job id that already holds different content.
	outcomeRejected = "rejected"
)

func hashKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s;", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func intentKey(namespace, enqueueKey string) string {
	return "ek." + hashKey(namespace, "enqueue", enqueueKey)
}

func lockKeyFor(namespace string, u *jetq.UniqueRule) string {
	return "ul." + hashKey(namespace, "unique", string(u.Mode), u.Key)
}

// readIntent reads the intent of an EnqueueKey.
func (d *Driver) readIntent(ctx context.Context, enqueueKey string) (*ledgerEntry, *intentRecord, bool, error) {
	ent, ok, err := d.ledger.get(ctx, intentKey(d.cfg.Namespace, enqueueKey))
	if err != nil || !ok {
		return nil, nil, false, err
	}
	var in intentRecord
	if err := json.Unmarshal(ent.rec.Data, &in); err != nil {
		return nil, nil, false, fmt.Errorf("natsq: decode intent of %q: %w", enqueueKey, err)
	}
	return ent, &in, true, nil
}

// claimIntent returns the intent the submission must follow, creating it when
// none exists. Different content under a live intent is a conflict.
func (d *Driver) claimIntent(ctx context.Context, env *jetq.Envelope, fingerprint string) (*intentRecord, error) {
	for range 5 {
		_, held, ok, err := d.readIntent(ctx, env.EnqueueKey)
		if err != nil {
			return nil, err
		}
		if ok {
			if held.Fingerprint != fingerprint {
				return nil, fmt.Errorf("%w: enqueue key belongs to job %s", jetq.ErrEnqueueKeyConflict, held.JobID)
			}
			return held, nil
		}
		in := intentRecord{JobID: env.JobID, Fingerprint: fingerprint, Envelope: *env.Clone(), Since: d.now()}
		data, _ := json.Marshal(in)
		if _, err := d.ledger.put(ctx, intentKey(d.cfg.Namespace, env.EnqueueKey), 0, &record{Data: data}); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue // another retry won; adopt its intent
			}
			return nil, err
		}
		return &in, nil
	}
	return nil, errRevisionMismatch
}

// markIntentSuppressed records the final Suppressed outcome of an EnqueueKey.
// Unlike a marker, a failure here would let a retry evaluate the unique rule
// again, so the caller reports an uncertain outcome.
func (d *Driver) markIntentSuppressed(ctx context.Context, env *jetq.Envelope, holder string) error {
	if env.EnqueueKey == "" {
		return nil
	}
	for range 5 {
		ent, held, ok, err := d.readIntent(ctx, env.EnqueueKey)
		if err != nil || !ok {
			return err
		}
		if held.Suppressed {
			return nil
		}
		held.Suppressed = true
		held.SuppressedBy = holder
		data, _ := json.Marshal(held)
		if _, err := d.ledger.put(ctx, intentKey(d.cfg.Namespace, env.EnqueueKey), ent.rev, &record{Data: data}); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return err
		}
		return nil
	}
	return errRevisionMismatch
}

// markIntentStored records that the intent's job was accepted. It is a repair
// step: failure changes no outcome.
func (d *Driver) markIntentStored(ctx context.Context, env *jetq.Envelope) error {
	if env.EnqueueKey == "" {
		return nil
	}
	for range 5 {
		ent, held, ok, err := d.readIntent(ctx, env.EnqueueKey)
		if err != nil || !ok {
			return err
		}
		if held.JobID != env.JobID || held.Stored {
			return nil
		}
		held.Stored = true
		data, _ := json.Marshal(held)
		if _, err := d.ledger.put(ctx, intentKey(d.cfg.Namespace, env.EnqueueKey), ent.rev, &record{Data: data}); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return err
		}
		return nil
	}
	return errRevisionMismatch
}

// deleteIntentOf removes the intent of a finished job before its record is
// cleaned up. An intent naming another job is left alone.
func (d *Driver) deleteIntentOf(ctx context.Context, env *jetq.Envelope) error {
	if env.EnqueueKey == "" {
		return nil
	}
	for range 5 {
		ent, held, ok, err := d.readIntent(ctx, env.EnqueueKey)
		if err != nil || !ok {
			return err
		}
		if held.JobID != env.JobID {
			return nil
		}
		if err := d.ledger.delete(ctx, intentKey(d.cfg.Namespace, env.EnqueueKey), ent.rev); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return err
		}
		return nil
	}
	return errRevisionMismatch
}

// lockState is what a competitor learns about a held reservation.
type lockState int

const (
	lockActive lockState = iota // suppresses
	lockFree                    // may be replaced
)

// assessLock decides whether a decodable reservation still suppresses. It
// never frees a reservation whose submission may still be decided: it
// finishes that submission through the same commit path a producer uses, so
// the reservation's job either becomes accepted or is suppressed.
func (d *Driver) assessLock(ctx context.Context, held lockRecordPayload, now time.Time) (lockState, error) {
	active, err := d.settleReservation(ctx, held, now)
	if err != nil {
		return lockActive, err
	}
	if active {
		return lockActive, nil
	}
	return lockFree, nil
}

// settleReservation finishes the submission a reservation belongs to without
// ever replacing it. It reports whether the reservation still suppresses.
//
// A job record is the authority once it exists: its outcome says whether the
// reservation is live. Only when the record is gone does this function act,
// and then through the same commit path a producer uses, which checks the
// reservation before it decides. A pending outcome that cannot be decided
// here keeps the reservation until its documented hold time.
func (d *Driver) settleReservation(ctx context.Context, held lockRecordPayload, now time.Time) (bool, error) {
	if !held.Until.IsZero() && !now.Before(held.Until) {
		return false, nil // the documented hold time passed
	}
	ent, ok, err := d.ledger.get(ctx, jobKey(held.JobID))
	if err != nil {
		return true, err
	}
	if ok {
		switch ent.rec.Outcome {
		case outcomeSuppressed, outcomeRejected:
			return false, nil
		case outcomeAccepted:
			// An until-done reservation ends with its job; a window
			// reservation keeps the key for its whole window.
			if ent.rec.State.Terminal() && held.Mode != jetq.UniqueModeWindow {
				return false, nil
			}
			_ = d.markLockStored(ctx, ent.rec.Env)
			return true, nil
		}
		// Pending: deliver it to its own commit path. That path decides it
		// against the reservation, so it cannot activate a suppressed job.
		if held.Envelope != nil {
			_, _ = d.submit(ctx, held.Envelope, held.Fingerprint)
			if again, ok, err := d.ledger.get(ctx, jobKey(held.JobID)); err == nil && ok && again.rec.Outcome != outcomePending {
				return again.rec.Outcome == outcomeAccepted, nil
			}
		}
		return true, nil
	}
	if held.Stored || held.Envelope == nil {
		// Marked, yet gone: the job finished and was cleaned up. Without its
		// envelope the submission cannot be finished either; the documented
		// hold time ends it.
		return false, nil
	}
	// The reservation's submission may still be pending: deliver it to its
	// own commit path, which checks this reservation.
	if _, err := d.submit(ctx, held.Envelope, held.Fingerprint); err != nil && !errors.Is(err, jetq.ErrEnqueueKeyConflict) {
		return true, err
	}
	if again, ok, err := d.ledger.get(ctx, jobKey(held.JobID)); err == nil && ok && again.rec.Outcome != outcomePending {
		return again.rec.Outcome == outcomeAccepted, nil
	}
	return true, nil
}

// markLockStored records that the reservation's job was accepted. It is a
// repair step.
func (d *Driver) markLockStored(ctx context.Context, env *jetq.Envelope) error {
	if env == nil || env.Unique == nil {
		return nil
	}
	key := lockKeyFor(d.cfg.Namespace, env.Unique)
	for range 5 {
		ent, ok, err := d.ledger.get(ctx, key)
		if err != nil || !ok {
			return err
		}
		held, err := decodeLockPayload(ent, ok)
		if err != nil || held.JobID != env.JobID || held.Stored {
			return nil
		}
		held.Stored = true
		data, _ := json.Marshal(held)
		if _, err := d.ledger.put(ctx, key, ent.rev, &record{Data: data}); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return err
		}
		return nil
	}
	return errRevisionMismatch
}

// releaseUnique removes the lock of env when it belongs to jobID.
func (d *Driver) releaseUnique(ctx context.Context, env *jetq.Envelope, jobID string) error {
	if env.Unique == nil {
		return nil
	}
	return d.releaseReservation(ctx, lockKeyFor(d.cfg.Namespace, env.Unique), jobID)
}

// releaseReservation removes the reservation at key when it belongs to jobID.
func (d *Driver) releaseReservation(ctx context.Context, key, jobID string) error {
	for range 5 {
		ent, ok, err := d.ledger.get(ctx, key)
		if err != nil || !ok {
			return err
		}
		held, err := decodeLockPayload(ent, ok)
		if err != nil || held.JobID != jobID {
			return nil
		}
		if err := d.ledger.delete(ctx, key, ent.rev); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return err
		}
		return nil
	}
	return errRevisionMismatch
}

func decodeLockPayload(ent *ledgerEntry, ok bool) (lockRecordPayload, error) {
	var held lockRecordPayload
	if !ok {
		return held, errors.New("no lock")
	}
	err := json.Unmarshal(ent.rec.Data, &held)
	return held, err
}

// takeUnique applies the submission's unique rule for env without committing
// anything. It returns whether the submission is suppressed, the job holding
// the key, and the reservation key the submission now holds when it is not
// suppressed (empty without a unique rule).
//
// A reservation that suppresses is never replaced here; the caller decides
// its own submission's outcome against it. A free reservation is taken over
// under the same key with the submission's own identity.
func (d *Driver) takeUnique(ctx context.Context, env *jetq.Envelope, fingerprint string) (suppressed bool, holder string, err error) {
	if env.Unique == nil {
		return false, "", nil
	}
	key := lockKeyFor(d.cfg.Namespace, env.Unique)
	for range 5 {
		ent, held, ok, err := d.readLock(ctx, key)
		if err != nil {
			return false, "", err
		}
		now := d.now()
		var rev uint64
		if ok {
			if held.JobID == env.JobID && held.Fingerprint == fingerprint {
				return false, "", nil // our own reservation
			}
			state, err := d.assessLock(ctx, held, now)
			if err != nil {
				return false, "", err
			}
			if state == lockActive {
				return true, held.JobID, nil
			}
			rev = ent.rev
		}
		if _, err := d.ledger.put(ctx, key, rev, d.newLock(env, fingerprint, now)); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			return false, "", err
		}
		return false, "", nil
	}
	return false, "", errRevisionMismatch
}

// newLock builds the reservation of a submission.
func (d *Driver) newLock(env *jetq.Envelope, fingerprint string, now time.Time) *record {
	until := now.Add(d.cfg.UniqueUntilDoneMax)
	if env.Unique.Mode == jetq.UniqueModeWindow {
		until = now.Add(env.Unique.Window)
	}
	held, _ := json.Marshal(lockRecordPayload{
		JobID: env.JobID, Fingerprint: fingerprint, Mode: env.Unique.Mode, Since: now, Until: until,
		Envelope: env.Clone(),
	})
	return &record{Data: held}
}

// readLock reads a reservation at key.
func (d *Driver) readLock(ctx context.Context, key string) (*ledgerEntry, lockRecordPayload, bool, error) {
	ent, ok, err := d.ledger.get(ctx, key)
	if err != nil || !ok {
		return nil, lockRecordPayload{}, false, err
	}
	held, err := decodeLockPayload(ent, true)
	if err != nil {
		return nil, lockRecordPayload{}, false, fmt.Errorf("natsq: undecodable unique lock %s: %w", key, err)
	}
	return ent, held, true, nil
}
