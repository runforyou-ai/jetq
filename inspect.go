package jobq

import (
	"context"
	"time"
)

// Stats is a snapshot of the queues. Counts may be approximate.
type Stats struct {
	// Queues has one entry per known queue, sorted by name.
	Queues []QueueStats
	// Schedules is the number of recurring schedules.
	Schedules int
}

// QueueStats counts the jobs of one queue.
type QueueStats struct {
	Queue string
	// Ready jobs are due and wait for a worker.
	Ready int64
	// Scheduled jobs wait for their run time, a retry or a snooze.
	Scheduled int64
	// Running jobs are claimed by a worker.
	Running int64
	// Dead jobs were dead-lettered and are still kept.
	Dead int64
}

// DeadLetter is a job that ran out of attempts or failed permanently.
type DeadLetter struct {
	Message  *Message
	Attempts int
	Error    string
	FailedAt time.Time
}

// DeadLetterQuery selects dead letters, newest first.
type DeadLetterQuery struct {
	// Queue limits results to one queue; empty means all queues.
	Queue string
	// Cursor continues from [DeadLetterPage.Next]; empty starts at the newest.
	Cursor string
	// Limit is the maximum number of entries (default 50).
	Limit int
}

// DeadLetterPage is one page of dead letters.
type DeadLetterPage struct {
	DeadLetters []DeadLetter
	// Next is the cursor of the following page, empty on the last page.
	Next string
}

func (c *Client) inspector() (Inspector, error) {
	i, ok := c.driver.(Inspector)
	if !ok {
		return nil, ErrUnsupported
	}
	return i, nil
}

// Stats counts jobs per queue. It returns [ErrUnsupported] when the driver is
// not an [Inspector].
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	i, err := c.inspector()
	if err != nil {
		return Stats{}, err
	}
	return i.Stats(ctx)
}

// DeadLetters lists dead-lettered jobs, newest first.
func (c *Client) DeadLetters(ctx context.Context, q DeadLetterQuery) (DeadLetterPage, error) {
	i, err := c.inspector()
	if err != nil {
		return DeadLetterPage{}, err
	}
	if q.Limit <= 0 {
		q.Limit = 50
	}
	if q.Queue != "" {
		if err := validName("queue", q.Queue); err != nil {
			return DeadLetterPage{}, err
		}
	}
	return i.DeadLetters(ctx, q)
}

// RetryDead puts the dead-lettered job id back on its queue with fresh attempts.
func (c *Client) RetryDead(ctx context.Context, id string) error {
	i, err := c.inspector()
	if err != nil {
		return err
	}
	return i.RetryDead(ctx, id)
}

// DeleteDead removes the dead-lettered job id.
func (c *Client) DeleteDead(ctx context.Context, id string) error {
	i, err := c.inspector()
	if err != nil {
		return err
	}
	return i.DeleteDead(ctx, id)
}
