package jetq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Stats is a snapshot of the queue system.
type Stats struct {
	// Queues has one entry per queue that a worker has consumed, sorted by name.
	Queues []QueueStats
	// Delayed is the number of pending delayed jobs across all queues.
	Delayed uint64
	// Schedules is the number of installed recurring schedules.
	Schedules int
}

// QueueStats describes one queue.
type QueueStats struct {
	Queue string
	// Waiting is the number of jobs ready to run, including jobs waiting for a retry.
	Waiting uint64
	// Running is the number of jobs delivered to a worker and not yet settled.
	Running int
	// Redelivered is the number of running jobs that are being retried.
	Redelivered int
	// Dead is the number of dead-lettered jobs still kept for this queue.
	Dead uint64
}

// Stats reports per-queue counts. Queues appear once a worker has created their consumer.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	dead := map[string]uint64{}
	deadInfo, err := c.dead.Info(ctx, jetstream.WithSubjectFilter(c.deadSubject(">")))
	if err != nil {
		return stats, fmt.Errorf("jetq: dead-letter stats: %w", err)
	}
	for subject, n := range deadInfo.State.Subjects {
		dead[strings.TrimPrefix(subject, c.deadSubject(""))] = n
	}

	consumers := c.stream.ListConsumers(ctx)
	for info := range consumers.Info() {
		queue, ok := strings.CutPrefix(info.Name, "jetq-")
		if !ok || info.Config.FilterSubject != c.queueSubject(queue) {
			continue
		}
		stats.Queues = append(stats.Queues, QueueStats{
			Queue:       queue,
			Waiting:     info.NumPending,
			Running:     info.NumAckPending,
			Redelivered: info.NumRedelivered,
			Dead:        dead[queue],
		})
	}
	if err := consumers.Err(); err != nil {
		return stats, fmt.Errorf("jetq: list consumers: %w", err)
	}
	slices.SortFunc(stats.Queues, func(a, b QueueStats) int { return strings.Compare(a.Queue, b.Queue) })

	delayed, err := c.stream.Info(ctx, jetstream.WithSubjectFilter(c.delaySubject(">")))
	if err != nil {
		return stats, fmt.Errorf("jetq: delayed stats: %w", err)
	}
	for _, n := range delayed.State.Subjects {
		stats.Delayed += n
	}
	schedules, err := c.stream.Info(ctx, jetstream.WithSubjectFilter(c.cronSubject(">")))
	if err != nil {
		return stats, fmt.Errorf("jetq: schedule stats: %w", err)
	}
	stats.Schedules = len(schedules.State.Subjects)
	return stats, nil
}

// DeadLetter is a job that ran out of attempts or failed permanently.
type DeadLetter struct {
	// Sequence identifies the entry in the dead-letter stream; pass it as
	// [DeadLetterQuery.Before] to page backwards.
	Sequence   uint64
	ID         string
	Name       string
	Queue      string
	Attempts   int
	Error      string
	FailedAt   time.Time
	EnqueuedAt time.Time
	Header     nats.Header
	Payload    []byte
}

// DeadLetterQuery selects dead letters, newest first.
type DeadLetterQuery struct {
	// Queue limits results to one queue; empty means all queues.
	Queue string
	// Before only returns entries with a lower sequence; zero means from the newest.
	Before uint64
	// Limit is the maximum number of entries (default 50).
	Limit int
}

// DeadLetters returns dead-lettered jobs, newest first.
func (c *Client) DeadLetters(ctx context.Context, query DeadLetterQuery) ([]DeadLetter, error) {
	if query.Limit <= 0 {
		query.Limit = 50
	}
	info, err := c.dead.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("jetq: dead letters: %w", err)
	}
	last := info.State.LastSeq
	if query.Before > 0 && query.Before-1 < last {
		last = query.Before - 1
	}
	subject := ""
	if query.Queue != "" {
		subject = c.deadSubject(query.Queue)
	}

	var out []DeadLetter
	for seq := last; seq >= info.State.FirstSeq && seq > 0 && len(out) < query.Limit; seq-- {
		msg, err := c.dead.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("jetq: dead letter %d: %w", seq, err)
		}
		if subject != "" && msg.Subject != subject {
			continue
		}
		h := msg.Header
		entry := DeadLetter{
			Sequence: msg.Sequence,
			ID:       h.Get(HeaderID),
			Name:     h.Get(HeaderJob),
			Queue:    h.Get(HeaderQueue),
			Error:    h.Get(HeaderError),
			Header:   h,
			Payload:  msg.Data,
		}
		entry.Attempts, _ = strconv.Atoi(h.Get(HeaderAttempts))
		entry.FailedAt, _ = time.Parse(time.RFC3339Nano, h.Get(HeaderFailedAt))
		entry.EnqueuedAt, _ = time.Parse(time.RFC3339Nano, h.Get(HeaderEnqueuedAt))
		out = append(out, entry)
	}
	return out, nil
}
