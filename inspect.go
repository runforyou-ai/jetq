package jetq

import (
	"context"
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
	// Delayed is the number of pending delayed jobs across all queues,
	// including jobs waiting for a retry or a snooze.
	Delayed uint64
	// Schedules is the number of installed recurring schedules.
	Schedules int
}

// QueueStats describes one queue, as counted by its JetStream consumer.
type QueueStats struct {
	Queue string
	// Ready is the number of jobs not yet delivered to any worker.
	Ready uint64
	// InFlight is the number of delivered jobs that are not settled yet, mostly
	// running jobs. Jobs waiting for a retry or a snooze count as [Stats.Delayed].
	InFlight int
	// Redelivered is the number of in-flight jobs that have been delivered more
	// than once, after a worker crash or a failed settlement.
	Redelivered int
	// Dead is the number of dead-lettered jobs still kept for this queue.
	Dead uint64
}

// Stats reports per-queue counts. Queues appear once a worker has created their consumer.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	dead := map[string]uint64{}
	deadInfo, err := c.info(ctx, c.cfg.deadName, jetstream.WithSubjectFilter(c.deadSubject(">")))
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
			Ready:       info.NumPending,
			InFlight:    info.NumAckPending,
			Redelivered: info.NumRedelivered,
			Dead:        dead[queue],
		})
	}
	if err := consumers.Err(); err != nil {
		return stats, fmt.Errorf("jetq: list consumers: %w", err)
	}
	slices.SortFunc(stats.Queues, func(a, b QueueStats) int { return strings.Compare(a.Queue, b.Queue) })

	delayed, err := c.info(ctx, c.cfg.streamName, jetstream.WithSubjectFilter(c.delaySubject(">")))
	if err != nil {
		return stats, fmt.Errorf("jetq: delayed stats: %w", err)
	}
	for _, n := range delayed.State.Subjects {
		stats.Delayed += n
	}
	schedules, err := c.info(ctx, c.cfg.streamName, jetstream.WithSubjectFilter(c.cronSubject(">")))
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
//
// Without a queue filter it reads the newest Limit entries below the cursor in
// one batch. With a queue filter it reads that queue's dead letters in batches
// up to the cursor, so its cost grows with the number of dead letters kept for
// that queue.
func (c *Client) DeadLetters(ctx context.Context, query DeadLetterQuery) ([]DeadLetter, error) {
	if query.Limit <= 0 {
		query.Limit = 50
	}
	if query.Queue != "" {
		if err := validName("queue", query.Queue); err != nil {
			return nil, err
		}
		return c.queueDeadLetters(ctx, query)
	}
	info, err := c.info(ctx, c.cfg.deadName)
	if err != nil {
		return nil, fmt.Errorf("jetq: dead letters: %w", err)
	}
	first, last := info.State.FirstSeq, info.State.LastSeq
	if query.Before > 0 && query.Before-1 < last {
		last = query.Before - 1
	}
	var out []DeadLetter
	// Read the window of sequences just below the cursor; deleted entries leave
	// gaps, so step further back until enough entries are found.
	for end := last; end > 0 && end >= first && len(out) < query.Limit; {
		start := first
		if need := uint64(query.Limit - len(out)); end-first >= need {
			start = end - need + 1
		}
		batch, err := c.readDeadLetters(ctx, start, end)
		if err != nil {
			return nil, fmt.Errorf("jetq: dead letters: %w", err)
		}
		for i := len(batch) - 1; i >= 0 && len(out) < query.Limit; i-- {
			out = append(out, batch[i])
		}
		end = start - 1
	}
	return out, nil
}

// readDeadLetters reads the dead letters with sequences from start to end in
// one pass of an ordered consumer, oldest first.
func (c *Client) readDeadLetters(ctx context.Context, start, end uint64) ([]DeadLetter, error) {
	consumer, err := c.js.OrderedConsumer(ctx, c.cfg.deadName, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{c.deadSubject(">")},
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       start,
		InactiveThreshold: 30 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	var out []DeadLetter
	if info.NumPending == 0 {
		return out, nil
	}
	messages, err := consumer.Messages(jetstream.PullMaxMessages(int(min(end-start+1, 256))))
	if err != nil {
		return nil, err
	}
	defer messages.Stop()
	for {
		msg, err := messages.Next(jetstream.NextContext(ctx))
		if err != nil {
			return nil, err
		}
		meta, err := msg.Metadata()
		if err != nil {
			return nil, err
		}
		if meta.Sequence.Stream > end {
			return out, nil
		}
		out = append(out, deadLetter(meta.Sequence.Stream, msg.Headers(), msg.Data()))
		if meta.Sequence.Stream == end || meta.NumPending == 0 {
			return out, nil
		}
	}
}

// queueDeadLetters reads one queue's dead letters below the cursor through an
// ordered consumer, keeping the newest query.Limit entries.
func (c *Client) queueDeadLetters(ctx context.Context, query DeadLetterQuery) ([]DeadLetter, error) {
	consumer, err := c.js.OrderedConsumer(ctx, c.cfg.deadName, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{c.deadSubject(query.Queue)},
		InactiveThreshold: 30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("jetq: dead letters: %w", err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("jetq: dead letters: %w", err)
	}
	var window []DeadLetter
	if info.NumPending == 0 {
		return window, nil
	}
	messages, err := consumer.Messages(jetstream.PullMaxMessages(256))
	if err != nil {
		return nil, fmt.Errorf("jetq: dead letters: %w", err)
	}
	defer messages.Stop()
	// Stop after the entries that existed when the query started, even while new dead letters arrive.
	for remaining := info.NumPending; remaining > 0; remaining-- {
		msg, err := messages.Next(jetstream.NextContext(ctx))
		if err != nil {
			return nil, fmt.Errorf("jetq: dead letters: %w", err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			return nil, fmt.Errorf("jetq: dead letters: %w", err)
		}
		if query.Before > 0 && meta.Sequence.Stream >= query.Before {
			break
		}
		window = append(window, deadLetter(meta.Sequence.Stream, msg.Headers(), msg.Data()))
		if len(window) > query.Limit {
			window = window[1:]
		}
		if meta.NumPending == 0 {
			break
		}
	}
	slices.Reverse(window)
	return window, nil
}

func deadLetter(seq uint64, h nats.Header, data []byte) DeadLetter {
	entry := DeadLetter{
		Sequence: seq,
		ID:       h.Get(HeaderID),
		Name:     h.Get(HeaderJob),
		Queue:    h.Get(HeaderQueue),
		Error:    h.Get(HeaderError),
		Header:   h,
		Payload:  data,
	}
	entry.Attempts, _ = strconv.Atoi(h.Get(HeaderAttempts))
	entry.FailedAt, _ = time.Parse(time.RFC3339Nano, h.Get(HeaderFailedAt))
	entry.EnqueuedAt, _ = time.Parse(time.RFC3339Nano, h.Get(HeaderEnqueuedAt))
	return entry
}
