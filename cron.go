package jobq

import (
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

// Cron is a parsed recurring schedule.
type Cron struct {
	spec  string
	every time.Duration
	// Bit sets of allowed seconds, minutes, hours, days of month, months
	// (1-12) and days of week (0-6, Sunday first).
	sec, min, hour, dom, month, dow uint64
	domStar, dowStar                bool
}

// ParseCron parses a five-field (minute first) or six-field (second first)
// cron expression, a descriptor (@yearly, @annually, @monthly, @weekly,
// @daily, @midnight, @hourly) or "@every <duration>" of at least a second.
// Fields accept *, numbers, ranges, steps and lists; month and weekday names
// are not supported.
func ParseCron(spec string) (Cron, error) {
	spec = strings.TrimSpace(spec)
	if every, ok := strings.CutPrefix(spec, "@every "); ok {
		d, err := time.ParseDuration(strings.TrimSpace(every))
		if err != nil || d < time.Second {
			return Cron{}, fmt.Errorf("jobq: invalid interval %q: use a duration of at least 1s", every)
		}
		d = d.Truncate(time.Second)
		return Cron{spec: "@every " + d.String(), every: d}, nil
	}
	expr := spec
	switch spec {
	case "@yearly", "@annually":
		expr = "0 0 0 1 1 *"
	case "@monthly":
		expr = "0 0 0 1 * *"
	case "@weekly":
		expr = "0 0 0 * * 0"
	case "@daily", "@midnight":
		expr = "0 0 0 * * *"
	case "@hourly":
		expr = "0 0 * * * *"
	default:
		if strings.HasPrefix(spec, "@") {
			return Cron{}, fmt.Errorf("jobq: unknown cron descriptor %q", spec)
		}
	}
	fields := strings.Fields(expr)
	if len(fields) == 5 {
		fields = append([]string{"0"}, fields...)
	}
	if len(fields) != 6 {
		return Cron{}, fmt.Errorf("jobq: invalid cron expression %q: use five or six fields", spec)
	}
	c := Cron{spec: spec}
	if strings.HasPrefix(spec, "@") {
		c.spec = spec
	} else {
		c.spec = strings.Join(fields, " ")
	}
	ranges := [6][2]int{{0, 59}, {0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}
	sets := [6]*uint64{&c.sec, &c.min, &c.hour, &c.dom, &c.month, &c.dow}
	for i, field := range fields {
		set, err := parseField(field, ranges[i][0], ranges[i][1])
		if err != nil {
			return Cron{}, fmt.Errorf("jobq: invalid cron expression %q: %w", spec, err)
		}
		*sets[i] = set
	}
	if c.dow&(1<<7) != 0 {
		c.dow = c.dow&^(1<<7) | 1
	}
	c.domStar = fields[3] == "*" || fields[3] == "?"
	c.dowStar = fields[5] == "*" || fields[5] == "?"
	return c, nil
}

func parseField(field string, lo, hi int) (uint64, error) {
	var set uint64
	for part := range strings.SplitSeq(field, ",") {
		rng, stepText, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n <= 0 {
				return 0, fmt.Errorf("bad step %q", part)
			}
			step = n
		}
		var from, to int
		switch {
		case rng == "*" || rng == "?":
			from, to = lo, hi
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			x, err1 := strconv.Atoi(a)
			y, err2 := strconv.Atoi(b)
			if err1 != nil || err2 != nil || x > y {
				return 0, fmt.Errorf("bad range %q", part)
			}
			from, to = x, y
		default:
			n, err := strconv.Atoi(rng)
			if err != nil {
				return 0, fmt.Errorf("bad value %q", part)
			}
			from, to = n, n
			if hasStep {
				to = hi
			}
		}
		if from < lo || to > hi {
			return 0, fmt.Errorf("value out of range %d-%d in %q", lo, hi, part)
		}
		if step > hi-lo+1 {
			return 0, fmt.Errorf("step %d is larger than the field range in %q", step, part)
		}
		for v := from; v <= to; v += step {
			set |= 1 << v
		}
	}
	if set == 0 {
		return 0, errors.New("empty field")
	}
	return set, nil
}

// String returns the normalized expression: six fields, a descriptor or
// "@every <duration>".
func (c Cron) String() string { return c.spec }

// Every returns the interval of an "@every" schedule, or zero.
func (c Cron) Every() time.Duration { return c.every }

// Next returns the first occurrence strictly after t in loc. For "@every"
// schedules occurrences are aligned to multiples of the interval since the
// Unix epoch. It returns the zero time when there is none within five years.
func (c Cron) Next(t time.Time, loc *time.Location) time.Time {
	if c.every > 0 {
		n := t.Unix()/int64(c.every/time.Second) + 1
		return time.Unix(n*int64(c.every/time.Second), 0).In(t.Location())
	}
	if loc == nil {
		loc = time.UTC
	}
	t = t.In(loc).Truncate(time.Second).Add(time.Second)
	limit := t.AddDate(5, 0, 0)
	last := t.Add(-time.Second)
	for t.Before(limit) {
		// time.Date maps a wall time that does not exist (a DST gap) to an
		// earlier instant. Never go backwards or stand still: skip an hour.
		if !t.After(last) {
			t = last.Add(time.Hour)
		}
		last = t
		if c.month&(1<<int(t.Month())) == 0 {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, loc)
			continue
		}
		if !c.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, loc)
			continue
		}
		if c.hour&(1<<t.Hour()) == 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, loc)
			continue
		}
		if c.min&(1<<t.Minute()) == 0 {
			t = t.Truncate(time.Minute).Add(time.Minute)
			continue
		}
		if c.sec&(1<<t.Second()) == 0 {
			next := nextBit(c.sec, t.Second())
			if next < 0 {
				t = t.Truncate(time.Minute).Add(time.Minute)
			} else {
				t = t.Add(time.Duration(next-t.Second()) * time.Second)
			}
			continue
		}
		return t
	}
	return time.Time{}
}

// dayMatches follows cron: when both day fields are restricted, either matches.
func (c Cron) dayMatches(t time.Time) bool {
	dom := c.dom&(1<<t.Day()) != 0
	dow := c.dow&(1<<int(t.Weekday())) != 0
	switch {
	case c.domStar && c.dowStar:
		return true
	case c.domStar:
		return dow
	case c.dowStar:
		return dom
	default:
		return dom || dow
	}
}

func nextBit(set uint64, after int) int {
	rest := set >> (after + 1)
	if rest == 0 {
		return -1
	}
	return after + 1 + bits.TrailingZeros64(rest)
}
