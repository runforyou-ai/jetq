package natsq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

// leafKey is the sort key of a job id: the ledger lists keys in bucket order,
// but a page is defined by JobID, so listing and paging use the job ids
// themselves.
func jobIDOf(key string) (string, bool) { return strings.CutPrefix(key, "job.") }

// defaultPageSize is the page size used when a query sets none.
const defaultPageSize = 100

// GetJob returns the job with the given id.
func (d *Driver) GetJob(ctx context.Context, jobID string) (jetq.JobSnapshot, bool, error) {
	if err := jetq.ValidateID("job id", jobID); err != nil {
		return jetq.JobSnapshot{}, false, err
	}
	ent, ok, err := d.ledger.get(ctx, jobKey(jobID))
	if err != nil || !ok || !ent.rec.live() {
		// Suppressed and rejected receipts are not logical jobs.
		return jetq.JobSnapshot{}, false, err
	}
	return d.snapshot(ent.rec), true, nil
}

// List returns one page of live jobs matching q, ordered by job id.
func (d *Driver) List(ctx context.Context, q jetq.JobQuery) (jetq.JobPage, error) {
	if q.State != "" && q.FailedOnly {
		return jetq.JobPage{}, errors.New("natsq: JobQuery sets State and FailedOnly")
	}
	if q.Queue != "" {
		if err := jetq.ValidateID("queue", q.Queue); err != nil {
			return jetq.JobPage{}, err
		}
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	keys, err := d.jobKeys(ctx)
	if err != nil {
		return jetq.JobPage{}, err
	}
	var page jetq.JobPage
	for _, id := range keys {
		if q.After != "" && id <= q.After {
			continue
		}
		ent, ok, err := d.ledger.get(ctx, jobKey(id))
		if err != nil {
			return page, err
		}
		if !ok || !ent.rec.live() {
			continue
		}
		if q.Queue != "" && ent.rec.Env.Queue != q.Queue {
			continue
		}
		if q.FailedOnly && ent.rec.State != jetq.StateFailed {
			continue
		}
		if q.State != "" && ent.rec.State != q.State {
			continue
		}
		if len(page.Jobs) == limit {
			// Another matching job exists, so advertise the next page.
			page.LastJobID = page.Jobs[limit-1].Envelope.JobID
			break
		}
		page.Jobs = append(page.Jobs, d.snapshot(ent.rec))
	}
	return page, nil
}

// Stats counts live jobs per queue and terminal state.
func (d *Driver) Stats(ctx context.Context) (jetq.Stats, error) {
	keys, err := d.jobKeys(ctx)
	if err != nil {
		return jetq.Stats{}, err
	}
	counts := map[string]*jetq.QueueStats{}
	var stats jetq.Stats
	now := d.now()
	for _, id := range keys {
		ent, ok, err := d.ledger.get(ctx, jobKey(id))
		if err != nil {
			return stats, err
		}
		if !ok || !ent.rec.live() {
			continue
		}
		rec := ent.rec
		switch rec.State {
		case jetq.StateSucceeded:
			stats.Succeeded++
		case jetq.StateCanceled:
			stats.Canceled++
		case jetq.StateFailed:
			queueCount(counts, rec.Env.Queue).Failed++
			continue
		}
		q := queueCount(counts, rec.Env.Queue)
		switch {
		case rec.State == jetq.StateRunning:
			q.Running++
		case rec.RunAt.IsZero() || !rec.RunAt.After(now):
			q.Ready++
		default:
			q.Delayed++
		}
	}
	for _, q := range counts {
		stats.Queues = append(stats.Queues, *q)
	}
	sort.Slice(stats.Queues, func(i, j int) bool { return stats.Queues[i].Queue < stats.Queues[j].Queue })
	return stats, nil
}

func queueCount(counts map[string]*jetq.QueueStats, queue string) *jetq.QueueStats {
	q, ok := counts[queue]
	if !ok {
		q = &jetq.QueueStats{Queue: queue}
		counts[queue] = q
	}
	return q
}

// jobKeys lists the ledger's job ids in order.
func (d *Driver) jobKeys(ctx context.Context) ([]string, error) {
	keys, err := d.ledger.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("natsq: list ledger: %w", err)
	}
	ids := make([]string, 0, len(keys))
	for _, key := range keys {
		if id, ok := jobIDOf(key); ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// snapshot copies a record into the neutral view. The envelope is a deep copy
// and RunAt reports the job's next execution time, so a caller never sees a
// ledger revision or a shared envelope.
func (d *Driver) snapshot(rec *record) jetq.JobSnapshot {
	snap := jetq.JobSnapshot{
		State:        rec.State,
		Wait:         rec.Wait,
		Budget:       rec.Budget,
		Deliveries:   rec.Deliveries,
		Snoozes:      rec.Snoozes,
		LastError:    rec.LastError,
		Canceled:     rec.Canceled,
		UnknownSince: rec.UnknownSince,
		ParentJobID:  rec.Env.ParentJobID,
		CreatedAt:    rec.Env.CreatedAt,
		UpdatedAt:    rec.UpdatedAt,
	}
	if !rec.State.Terminal() {
		snap.RunAt = rec.RunAt
		snap.Attempts = rec.Budget
	}
	if rec.Env.Policy.MaxAttempts > 0 {
		snap.Attempts = rec.Env.Policy.MaxAttempts
	}
	if rec.Pending != nil {
		pending := *rec.Pending
		snap.PendingDecision = &pending
	}
	snap.Envelope = *rec.Env.Clone()
	return snap
}
