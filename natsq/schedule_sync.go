package natsq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/runforyou-ai/jetq"
)

// ScheduleVersion is the configuration version of one owner's schedule set.
// Zero means the owner has no set. Firing never changes it: only a sync that
// changes the definitions does.
type ScheduleVersion uint64

// ErrVersionConflict is returned by [Driver.SyncSchedules] when the caller's
// expected version is not the held one and the definitions differ. Nothing is
// written.
var ErrVersionConflict = errors.New("natsq: schedule set version conflict")

// SyncSchedules replaces the owner's schedule set with schedules, but only on
// top of the version the caller last saw. This is what keeps rolling deploys
// safe: a process still running an old set cannot delete schedules a newer
// deploy added, because its expected version is stale.
//
//   - expected 0 creates the set; it conflicts when the owner already holds a
//     different set.
//   - a nonzero expected updates the set only when it is the held version.
//   - syncing the definitions that are already held succeeds with the held
//     version whatever expected is, so a retry or a restart is idempotent.
//
// On success the new (or unchanged) version is returned; pass it to the next
// sync. A schedule whose definition is unchanged keeps its cursor, so adding
// or changing one schedule never skips or re-fires another. Another owner's
// schedules are never touched.
func (d *Driver) SyncSchedules(ctx context.Context, owner string, expected ScheduleVersion, schedules ...Schedule) (ScheduleVersion, error) {
	if err := validateOwner(owner); err != nil {
		return 0, err
	}
	desired := make([]scheduleRecord, 0, len(schedules))
	seen := map[string]bool{}
	for _, s := range schedules {
		rec, err := d.compileSchedule(s)
		if err != nil {
			return 0, err
		}
		if seen[s.Key] {
			return 0, fmt.Errorf("natsq: duplicate schedule key %q", s.Key)
		}
		seen[s.Key] = true
		desired = append(desired, rec)
	}
	sort.Slice(desired, func(i, j int) bool { return desired[i].Key < desired[j].Key })
	key := manifestKey(d.cfg.Namespace, owner)
	for range 10 {
		ent, held, ok, err := d.readManifest(ctx, owner)
		if err != nil {
			return 0, err
		}
		if !ok {
			if expected != 0 {
				return 0, fmt.Errorf("%w: owner %s has no schedule set, expected version %d", ErrVersionConflict, owner, expected)
			}
			m := scheduleManifest{Owner: owner, Version: 1, Schedules: desired}
			if _, err := d.ledger.put(ctx, key, 0, &record{Data: mustJSON(m)}); err != nil {
				if errors.Is(err, errRevisionMismatch) {
					continue // created concurrently; decide against it
				}
				return 0, err
			}
			return 1, nil
		}
		current := ScheduleVersion(held.Version)
		if sameDefinitions(held.Schedules, desired) {
			return current, nil
		}
		if expected != current {
			return 0, fmt.Errorf("%w: owner %s holds version %d, expected %d", ErrVersionConflict, owner, current, expected)
		}
		m := scheduleManifest{Owner: owner, Version: held.Version + 1, Schedules: keepCursors(held.Schedules, desired)}
		if _, err := d.ledger.put(ctx, key, ent.rev, &record{Data: mustJSON(m)}); err != nil {
			if errors.Is(err, errRevisionMismatch) {
				// A cursor refresh or another sync wrote; re-decide. A
				// concurrent sync bumps the version and then conflicts.
				continue
			}
			return 0, err
		}
		return current + 1, nil
	}
	return 0, errRevisionMismatch
}

// CurrentScheduleVersion returns the owner's held configuration version, 0 when it
// has none. It is for management tools that deliberately take over a set; a
// worker must not read it and then overwrite a set it did not write.
func (d *Driver) CurrentScheduleVersion(ctx context.Context, owner string) (ScheduleVersion, error) {
	if err := validateOwner(owner); err != nil {
		return 0, err
	}
	_, m, ok, err := d.readManifest(ctx, owner)
	if err != nil || !ok {
		return 0, err
	}
	return ScheduleVersion(m.Version), nil
}

func validateOwner(owner string) error {
	if owner == "" {
		return errors.New("natsq: schedule owner is required")
	}
	return jetq.ValidateID("schedule owner", owner)
}

// definition is a schedule without its runtime cursor.
func definition(s scheduleRecord) string {
	s.Next = time.Time{}
	return string(mustJSON(s))
}

// sameDefinitions reports whether two sorted sets define the same schedules.
func sameDefinitions(a, b []scheduleRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if definition(a[i]) != definition(b[i]) {
			return false
		}
	}
	return true
}

// keepCursors returns desired with the held cursor of every schedule whose key
// and definition are unchanged, so a due occurrence that was not fired yet is
// still fired after the sync.
func keepCursors(held, desired []scheduleRecord) []scheduleRecord {
	byKey := make(map[string]scheduleRecord, len(held))
	for _, h := range held {
		byKey[h.Key] = h
	}
	out := make([]scheduleRecord, len(desired))
	for i, s := range desired {
		if h, ok := byKey[s.Key]; ok && definition(h) == definition(s) {
			s.Next = h.Next
		}
		out[i] = s
	}
	return out
}
