package jobq

import (
	"context"
	"fmt"
	"time"
)

// Schedule describes a recurring job. Every instance passes the same full set
// to [Client.SyncSchedules].
type Schedule struct {
	// Key uniquely identifies the schedule; jobs it fires get ids starting
	// with "<key>-".
	Key string
	// Cron is the schedule, as accepted by [ParseCron].
	Cron string
	// Timezone is an IANA time zone, default UTC. It does not apply to @every.
	Timezone string
	// Job is the job to enqueue. Use [RawJob] for runtime names.
	Job Job
	// Options apply to every fired job. JobID, Delay, At, Unique and
	// UniqueFor are not allowed.
	Options []EnqueueOption
}

// SyncSchedules makes the driver's recurring schedules match schedules
// exactly: missing ones are created, changed ones replaced and schedules no
// longer listed removed. Unchanged schedules keep their timing. Call it at
// startup from any number of instances, each passing the same set. It returns
// [ErrUnsupported] when the driver is not a [Scheduler].
func (c *Client) SyncSchedules(ctx context.Context, schedules ...Schedule) error {
	scheduler, ok := c.driver.(Scheduler)
	if !ok {
		return ErrUnsupported
	}
	specs := make([]ScheduleSpec, 0, len(schedules))
	seen := map[string]bool{}
	for _, s := range schedules {
		spec, err := s.spec()
		if err != nil {
			return err
		}
		if seen[s.Key] {
			return fmt.Errorf("jobq: duplicate schedule %q", s.Key)
		}
		seen[s.Key] = true
		specs = append(specs, spec)
	}
	return scheduler.SyncSchedules(ctx, specs)
}

func (s Schedule) spec() (ScheduleSpec, error) {
	if err := validName("schedule key", s.Key); err != nil {
		return ScheduleSpec{}, err
	}
	cron, err := ParseCron(s.Cron)
	if err != nil {
		return ScheduleSpec{}, fmt.Errorf("jobq: schedule %q: %w", s.Key, err)
	}
	if s.Timezone != "" {
		if cron.Every() > 0 {
			return ScheduleSpec{}, fmt.Errorf("jobq: schedule %q: time zones do not apply to @every", s.Key)
		}
		if _, err := time.LoadLocation(s.Timezone); err != nil {
			return ScheduleSpec{}, fmt.Errorf("jobq: schedule %q: %w", s.Key, err)
		}
	}
	if s.Job == nil {
		return ScheduleSpec{}, fmt.Errorf("jobq: schedule %q: job is nil", s.Key)
	}
	var o enqueueOptions
	for _, opt := range s.Options {
		opt(&o)
	}
	if o.id != "" || o.delay != 0 || !o.at.IsZero() || o.unique != "" {
		return ScheduleSpec{}, fmt.Errorf("jobq: schedule %q: JobID, Delay, At and unique keys are not allowed", s.Key)
	}
	msg, err := NewMessage(s.Job.JobName(), s.Job, s.Options...)
	if err != nil {
		return ScheduleSpec{}, fmt.Errorf("jobq: schedule %q: %w", s.Key, err)
	}
	msg.ID, msg.EnqueuedAt = "", time.Time{}
	return ScheduleSpec{Key: s.Key, Cron: cron.String(), Timezone: s.Timezone, Message: msg}, nil
}
