package jobq_test

import (
	"context"
	"fmt"
	"time"

	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/memq"
)

type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

func (WelcomeEmail) JobName() string { return "welcome-email" }

func Example() {
	driver := memq.New() // or natsq.New(ctx, js), pgq.New(ctx, pool)
	defer driver.Close()
	q := jobq.New(driver)

	w := q.NewWorker(jobq.Queue{Name: "mail", Concurrency: 8, MaxAttempts: 5, Timeout: time.Minute})
	done := make(chan struct{})
	jobq.Handle(w, func(ctx context.Context, job WelcomeEmail) error {
		info, _ := jobq.JobInfo(ctx)
		fmt.Println("welcome", job.UserID, "attempt", info.Attempt)
		close(done)
		return nil
	})
	ctx, stop := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = w.Run(ctx)
	}()

	_, err := q.Enqueue(context.Background(), WelcomeEmail{UserID: 42},
		jobq.OnQueue("mail"),
		jobq.Unique("welcome-42"),
	)
	if err != nil {
		panic(err)
	}
	<-done
	stop()
	<-stopped
	// Output: welcome 42 attempt 1
}
