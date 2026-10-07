package jetq_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

func (WelcomeEmail) JobName() string { return "welcome-email" }

type DailyReport struct{}

func (DailyReport) JobName() string { return "daily-report" }

func Example() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()

	q, err := jetq.New(ctx, js)
	if err != nil {
		log.Fatal(err)
	}

	// Producer side: enqueue after the database transaction has committed.
	_, err = q.Enqueue(ctx, WelcomeEmail{UserID: 42},
		jetq.OnQueue("mail"),
		jetq.Delay(10*time.Minute),
		jetq.Unique("welcome-42"),
	)
	if err != nil && !errors.Is(err, jetq.ErrDuplicate) {
		log.Fatal(err)
	}

	// Recurring jobs are fired by the NATS server; every instance syncs the same set.
	if err := q.SyncSchedules(ctx,
		jetq.Cron("daily-report", "0 2 * * *", DailyReport{}).In("Asia/Shanghai"),
	); err != nil {
		log.Fatal(err)
	}

	// Worker side.
	w := q.NewWorker(
		jetq.Queue{Name: "default"},
		jetq.Queue{Name: "mail", Concurrency: 8, MaxAttempts: 5},
	)
	jetq.Handle(w, func(ctx context.Context, job WelcomeEmail) error {
		info, _ := jetq.JobInfo(ctx)
		fmt.Println("sending welcome email", job.UserID, "attempt", info.Attempt)
		return nil
	}, jetq.OnFailure(func(ctx context.Context, job WelcomeEmail, err error) {
		log.Printf("welcome email for %d failed: %v", job.UserID, err)
	}))
	jetq.Handle(w, func(ctx context.Context, job DailyReport) error { return nil })

	if err := w.Run(ctx); err != nil { // blocks until ctx is cancelled
		log.Fatal(err)
	}
}
