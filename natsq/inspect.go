package natsq

import (
	"context"
	"errors"
	"fmt"

	"github.com/runforyou-ai/jetq"
)

// Cancel prevents future executions of an awaiting job. A job whose
// execution is running is not interrupted; it finishes, and cancellation
// applies to the retry the engine would schedule. When the job is already
// terminal, Cancel returns jetq.ErrNotCancelable.
func (d *Driver) Cancel(ctx context.Context, jobID string) error {
	if err := jetq.ValidateID("job id", jobID); err != nil {
		return err
	}
	key := jobKey(jobID)
	for range 5 {
		ent, ok, err := d.ledger.get(ctx, key)
		if err != nil {
			return err
		}
		if !ok || !ent.rec.live() {
			return jetq.ErrNotFound
		}
		rec := cloneRecord(ent.rec)
		switch {
		case rec.State == jetq.StateCanceled:
			return nil
		case rec.State.Terminal():
			return fmt.Errorf("%w: job %s is %s", jetq.ErrNotCancelable, jobID, rec.State)
		case rec.Canceled:
			return nil
		case rec.Pending != nil:
			return fmt.Errorf("natsq: job %s has a decision being applied, retry the cancellation", jobID)
		}
		rec.Canceled = true
		if rec.State == jetq.StateAwaiting {
			rec.State = jetq.StateCanceled
			rec.TerminalAt = d.now()
		}
		if _, err := d.ledger.put(ctx, key, ent.rev, rec); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				continue
			}
			// The cancellation was not recorded: nothing was canceled and
			// no lock may be released.
			return fmt.Errorf("natsq: cancel %s: %w", jobID, err)
		}
		if rec.State == jetq.StateCanceled && rec.Env.Unique != nil {
			// The job will not run; free its key now rather than at cleanup.
			// A failure keeps the job canceled and is reported; the recovery
			// scan releases the lock of a terminal job.
			if err := d.releaseUnique(ctx, rec.Env, jobID); err != nil {
				return fmt.Errorf("natsq: cancel %s: canceled, unique lock release pending: %w", jobID, err)
			}
		}
		return nil
	}
	return fmt.Errorf("natsq: cancel %s: %w", jobID, errRevisionMismatch)
}

// JobState reads a job's current state.
func (d *Driver) JobState(ctx context.Context, jobID string) (jetq.JobState, bool, error) {
	ent, ok, err := d.ledger.get(ctx, jobKey(jobID))
	if err != nil || !ok || !ent.rec.live() {
		// A pending or suppressed submission has no logical job.
		return "", false, err
	}
	return ent.rec.State, true, nil
}
