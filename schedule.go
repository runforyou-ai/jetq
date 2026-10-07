package jetq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Schedule is a recurring job fired by the NATS server. Create it with [Cron]
// and install the full set with [Client.SyncSchedules].
type Schedule struct {
	key      string
	spec     string
	timeZone string
	job      Job
	opts     []EnqueueOption
}

// Cron declares a recurring job. key identifies the schedule and must be
// unique; spec is a standard five-field cron expression ("0 2 * * *"), a
// six-field expression with leading seconds, or a descriptor such as
// "@hourly", "@daily" or "@every 5m". Only [OnQueue], [MaxAttempts] and
// [WithHeader] apply to scheduled jobs.
func Cron(key, spec string, job Job, opts ...EnqueueOption) Schedule {
	return Schedule{key: key, spec: spec, job: job, opts: opts}
}

// In evaluates the cron expression in the named IANA time zone (default UTC).
// It does not apply to "@every" schedules. Requires nats-server 2.14 or later.
func (s Schedule) In(timeZone string) Schedule {
	s.timeZone = timeZone
	return s
}

// SyncSchedules makes the server-side schedules match schedules exactly:
// missing ones are created, changed ones are replaced and schedules no longer
// listed are removed. Unchanged schedules are left alone so their timing is
// not reset. Call it at startup from any number of instances; every instance
// must pass the same set.
func (c *Client) SyncSchedules(ctx context.Context, schedules ...Schedule) error {
	desired := make(map[string]*nats.Msg, len(schedules))
	for _, s := range schedules {
		msg, err := c.scheduleMsg(s)
		if err != nil {
			return err
		}
		if _, exists := desired[msg.Subject]; exists {
			return fmt.Errorf("jetq: duplicate schedule key %q", s.key)
		}
		desired[msg.Subject] = msg
	}

	info, err := c.info(ctx, c.cfg.streamName, jetstream.WithSubjectFilter(c.cronSubject(">")))
	if err != nil {
		return fmt.Errorf("jetq: list schedules: %w", err)
	}
	for subject := range info.State.Subjects {
		if _, keep := desired[subject]; keep {
			continue
		}
		if err := c.stream.Purge(ctx, jetstream.WithPurgeSubject(subject)); err != nil {
			return fmt.Errorf("jetq: remove schedule %s: %w", subject, err)
		}
		c.cfg.logger.InfoContext(ctx, "jetq schedule removed", "subject", subject)
	}

	for subject, msg := range desired {
		current, err := c.stream.GetLastMsgForSubject(ctx, subject)
		if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return fmt.Errorf("jetq: read schedule %s: %w", subject, err)
		}
		if current != nil && sameSchedule(current, msg) {
			continue
		}
		// Publishing on the schedule subject replaces the previous schedule (implicit rollup).
		if _, err := c.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(c.cfg.streamName)); err != nil {
			return fmt.Errorf("jetq: install schedule %s: %w", subject, err)
		}
		c.cfg.logger.InfoContext(ctx, "jetq schedule installed", "subject", subject, "spec", msg.Header.Get(headerSchedule))
	}
	return nil
}

func (c *Client) scheduleMsg(s Schedule) (*nats.Msg, error) {
	if err := validName("schedule", s.key); err != nil {
		return nil, err
	}
	o := enqueueOptions{queue: DefaultQueue}
	for _, opt := range s.opts {
		opt(&o)
	}
	if o.unique != "" || o.lock != "" || o.delay != 0 || !o.at.IsZero() || o.id != "" {
		return nil, fmt.Errorf("jetq: schedule %q: Unique, UniqueUntilDone, Delay, At and JobID do not apply to schedules", s.key)
	}
	if err := validName("queue", o.queue); err != nil {
		return nil, err
	}
	spec, err := normalizeCron(s.spec)
	if err != nil {
		return nil, fmt.Errorf("jetq: schedule %q: %w", s.key, err)
	}
	if s.timeZone != "" {
		if strings.HasPrefix(spec, "@every") {
			return nil, fmt.Errorf("jetq: schedule %q: time zones do not apply to @every", s.key)
		}
		if _, err := time.LoadLocation(s.timeZone); err != nil {
			return nil, fmt.Errorf("jetq: schedule %q: %w", s.key, err)
		}
	}
	name := s.job.JobName()
	if name == "" {
		return nil, fmt.Errorf("jetq: schedule %q: job name is empty", s.key)
	}
	data, err := encodeJob(s.job)
	if err != nil {
		return nil, fmt.Errorf("jetq: schedule %q: %w", s.key, err)
	}

	msg := &nats.Msg{Subject: c.cronSubject(s.key), Data: data, Header: nats.Header{}}
	for key, values := range o.header {
		if reservedHeader(key) {
			return nil, fmt.Errorf("jetq: header %q is reserved", key)
		}
		msg.Header[key] = values
	}
	msg.Header.Set(HeaderJob, name)
	if o.maxAttempts > 0 {
		msg.Header.Set(HeaderMaxAttempts, strconv.Itoa(o.maxAttempts))
	}
	msg.Header.Set(headerSchedule, spec)
	msg.Header.Set(headerScheduleTarget, c.queueSubject(o.queue))
	if s.timeZone != "" {
		msg.Header.Set(headerScheduleTimeZone, s.timeZone)
	}
	return msg, nil
}

// normalizeCron converts a five-field cron expression to the six-field form
// NATS expects; descriptors and six-field expressions are passed through.
func normalizeCron(spec string) (string, error) {
	spec = strings.TrimSpace(spec)
	if every, ok := strings.CutPrefix(spec, "@every "); ok {
		d, err := time.ParseDuration(strings.TrimSpace(every))
		if err != nil || d < time.Second {
			return "", fmt.Errorf("invalid interval %q: use a duration of at least 1s", every)
		}
		return "@every " + d.String(), nil
	}
	if strings.HasPrefix(spec, "@at ") {
		return "", errors.New("use Delay or At for one-off jobs")
	}
	if strings.HasPrefix(spec, "@") {
		switch spec {
		case "@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly":
			return spec, nil
		}
		return "", fmt.Errorf("unknown cron descriptor %q", spec)
	}
	switch len(strings.Fields(spec)) {
	case 5:
		return "0 " + strings.Join(strings.Fields(spec), " "), nil
	case 6:
		return strings.Join(strings.Fields(spec), " "), nil
	default:
		return "", fmt.Errorf("invalid cron expression %q", spec)
	}
}

// sameSchedule reports whether the stored schedule message equals msg,
// ignoring headers the server or publish options add on their own.
func sameSchedule(current *jetstream.RawStreamMsg, msg *nats.Msg) bool {
	if !bytes.Equal(current.Data, msg.Data) {
		return false
	}
	return maps.EqualFunc(scheduleHeaders(current.Header), scheduleHeaders(msg.Header), slices.Equal)
}

func scheduleHeaders(h nats.Header) nats.Header {
	out := nats.Header{}
	for key, values := range h {
		if key == jetstream.MsgRollup || strings.HasPrefix(key, "Nats-Expected-") {
			continue
		}
		out[key] = values
	}
	return out
}
