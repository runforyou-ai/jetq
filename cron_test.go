package jobq_test

import (
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
)

func TestCronNext(t *testing.T) {
	t.Parallel()
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 31, 23, 59, 30, 0, time.UTC)
	tests := []struct {
		spec string
		loc  *time.Location
		from time.Time
		want time.Time
	}{
		{"* * * * *", nil, base, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)},
		{"*/15 * * * * *", nil, base, time.Date(2026, 1, 31, 23, 59, 45, 0, time.UTC)},
		{"0 2 * * *", nil, base, time.Date(2026, 2, 1, 2, 0, 0, 0, time.UTC)},
		{"0 2 * * *", shanghai, base, time.Date(2026, 2, 2, 2, 0, 0, 0, shanghai)}, // already 07:59 on Feb 1 there
		{"0 0 29 2 *", nil, base, time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC)},
		{"0 9 * * 1-5", nil, time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC), time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)}, // Saturday -> Monday
		{"0 0 1 * 0", nil, time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)},      // first Sunday wins over the 1st
		{"0 0 * * 7", nil, time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)},      // 7 is Sunday
		{"@hourly", nil, base, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)},
		{"@every 1m", nil, base, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)},
		{"5,10-12 * * * * *", nil, time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC), time.Date(2026, 1, 1, 0, 0, 11, 0, time.UTC)},
	}
	for _, tt := range tests {
		c, err := jobq.ParseCron(tt.spec)
		if err != nil {
			t.Fatalf("%s: %v", tt.spec, err)
		}
		if got := c.Next(tt.from, tt.loc); !got.Equal(tt.want) {
			t.Errorf("%s from %s = %s, want %s", tt.spec, tt.from, got, tt.want)
		}
	}
}

func TestParseCronErrors(t *testing.T) {
	t.Parallel()
	for _, spec := range []string{"", "* * * *", "60 * * * *", "* * * * * * *", "@every 10ms", "@sometimes", "*/0 * * * *", "5-1 * * * *", "MON * * * *"} {
		if _, err := jobq.ParseCron(spec); err == nil {
			t.Errorf("%q accepted", spec)
		}
	}
	c, err := jobq.ParseCron("0  2 * * *")
	if err != nil || c.String() != "0 0 2 * * *" {
		t.Fatalf("normalized = %q, %v", c.String(), err)
	}
}
