package jobq_test

import (
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
)

func TestReviewCronSpringForward(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	c, err := jobq.ParseCron("0 2 * * *")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan time.Time, 1)
	go func() { done <- c.Next(time.Date(2026, 3, 8, 0, 0, 0, 0, loc), loc) }()
	select {
	case next := <-done:
		t.Log(next)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Cron.Next stuck on spring-forward day")
	}
}
