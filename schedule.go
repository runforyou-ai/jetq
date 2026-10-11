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

// Schedule describes a recurring job fired by the NATS server.
// Every instance installs the same full set with Client.SyncSchedules.
// Names are stable protocol identifiers independent of Go payload types.
type Schedule struct {
	Key string
	// Cron is a five- or six-field cron expression or a descriptor such as @hourly or @every 5m.
	Cron string
	// Timezone is an IANA time zone, default UTC. It does not apply to @every.
	Timezone string
	Name     string
	Payload  any
	// Queue defaults to DefaultQueue.
	Queue string
	// MaxAttempts overrides the worker queue limit when positive.
	MaxAttempts int
	// Timeout overrides the queue timeout when non-nil; zero means no limit.
	Timeout *time.Duration
	Header  nats.Header
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
			return fmt.Errorf("jetq: duplicate schedule key %q", s.Key)
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
	if err := validName("schedule", s.Key); err != nil {
		return nil, err
	}
	queue := s.Queue
	if queue == "" {
		queue = DefaultQueue
	}
	if s.MaxAttempts < 0 {
		return nil, fmt.Errorf("jetq: schedule %q: max attempts must not be negative", s.Key)
	}
	if s.Timeout != nil && *s.Timeout < 0 {
		return nil, fmt.Errorf("jetq: schedule %q: timeout must not be negative", s.Key)
	}
	if err := validName("queue", queue); err != nil {
		return nil, err
	}
	spec, err := normalizeCron(s.Cron)
	if err != nil {
		return nil, fmt.Errorf("jetq: schedule %q: %w", s.Key, err)
	}
	if s.Timezone != "" {
		if strings.HasPrefix(spec, "@every") {
			return nil, fmt.Errorf("jetq: schedule %q: time zones do not apply to @every", s.Key)
		}
		if _, err := time.LoadLocation(s.Timezone); err != nil {
			return nil, fmt.Errorf("jetq: schedule %q: %w", s.Key, err)
		}
	}
	name := s.Name
	if name == "" {
		return nil, fmt.Errorf("jetq: schedule %q: job name is empty", s.Key)
	}
	data, err := encodePayload(name, s.Payload)
	if err != nil {
		return nil, fmt.Errorf("jetq: schedule %q: %w", s.Key, err)
	}

	msg := &nats.Msg{Subject: c.cronSubject(s.Key), Data: data, Header: nats.Header{}}
	for key, values := range s.Header {
		if reservedHeader(key) {
			return nil, fmt.Errorf("jetq: header %q is reserved", key)
		}
		msg.Header[key] = values
	}
	msg.Header.Set(HeaderJob, name)
	if s.MaxAttempts > 0 {
		msg.Header.Set(HeaderMaxAttempts, strconv.Itoa(s.MaxAttempts))
	}
	if s.Timeout != nil {
		msg.Header.Set(HeaderTimeout, s.Timeout.String())
	}
	msg.Header.Set(headerSchedule, spec)
	msg.Header.Set(headerScheduleTarget, c.queueSubject(queue))
	if s.Timezone != "" {
		msg.Header.Set(headerScheduleTimeZone, s.Timezone)
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
