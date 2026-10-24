package jetq_test

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
	"github.com/runforyou-ai/jetq/natsq"
)

// WelcomeEmail is an ordinary struct: no methods, no backend types.
type WelcomeEmail struct {
	UserID int64 `json:"user_id"`
}

// openExample provisions and opens a driver on a default local NATS server.
func openExample(ctx context.Context) *natsq.Driver {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	// Deployment step: create the namespace's resources.
	cfg := natsq.Config{Namespace: "billing"}
	if _, err := natsq.Provision(ctx, js, cfg); err != nil {
		log.Fatal(err)
	}
	driver, err := natsq.Open(ctx, js, cfg)
	if err != nil {
		log.Fatal(err)
	}
	return driver
}

// Example shows the flow: provision once, open a driver, submit typed jobs
// with a producer, and run them with a worker that finds handlers by task
// name. The producer and the worker are separate clients and can run in
// separate processes.
func Example() {
	ctx := context.Background()
	driver := openExample(ctx)
	defer driver.Close()

	// Producer side. The task does not name a queue, so the same task can be
	// submitted to several queues; the policy is fixed at submission.
	producer, err := jetq.NewClient(driver,
		jetq.WithDefaultQueue("mail"),
		jetq.WithMaxPayloadBytes(256<<10),
	)
	if err != nil {
		log.Fatal(err)
	}
	welcome := jetq.MustTask[WelcomeEmail]("welcome-email", jetq.Policy{
		MaxAttempts: 5,
		Timeout:     jetq.TimeoutOf(30 * time.Second),
	})
	// Prepare once, then submit: a retry of the same prepared value keeps the
	// job id and the absolute run time.
	job, err := jetq.PrepareTask(producer, welcome, WelcomeEmail{UserID: 42}, jetq.Submission{
		EnqueueKey: "welcome-42",
		Delay:      10 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	res, err := producer.SubmitPrepared(ctx, job)
	if err != nil {
		if errors.Is(err, jetq.ErrEnqueueKeyConflict) {
			log.Printf("welcome-42 is already held by different content")
		}
		log.Fatal(err)
	}
	log.Printf("job %s accepted: %v", res.JobID, res.Status == jetq.Accepted)

	// Raw and dynamically named tasks share the same path.
	raw := jetq.Raw("legacy-import")
	if _, err := producer.SubmitRaw(ctx, raw, json.RawMessage(`{"id": 7}`), jetq.Submission{Queue: "imports"}); err != nil {
		log.Fatal(err)
	}

	// Worker side. Consumers only need the task name; a shared Go declaration
	// is optional.
	worker := producer.NewWorker(
		jetq.Queue{Name: "mail", Concurrency: 8},
		jetq.Queue{Name: "imports"},
	)
	jetq.Handle(worker, welcome, func(ctx context.Context, job WelcomeEmail) error {
		info, _ := jetq.JobInfo(ctx)
		log.Printf("sending welcome email to %d, attempt %d of %d", job.UserID, info.Attempt, info.MaxAttempts)
		return nil
	}, jetq.OnFailure(func(ctx context.Context, job WelcomeEmail, err error) {
		log.Printf("welcome email for %d failed: %v", job.UserID, err)
	}))
	jetq.HandleTask(worker, "legacy-import", func(ctx context.Context, raw json.RawMessage) error {
		log.Printf("importing %s", raw)
		return nil
	})

	// Run blocks until ctx is cancelled, then stops taking jobs, finishes the
	// running ones within its grace period and keeps the driver's recovery
	// loop going meanwhile.
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

// ExampleClient_PrepareRedrive shows how to retry a redrive that uses a
// relative delay: prepare once, then repeat the submit.
func ExampleClient_PrepareRedrive() {
	ctx := context.Background()
	driver := openExample(ctx)
	defer driver.Close()
	client, err := jetq.NewClient(driver)
	if err != nil {
		log.Fatal(err)
	}

	failedID := "failed-job-id"
	replacement, err := client.PrepareRedrive(ctx, failedID,
		jetq.RedriveDelay(time.Hour),
		jetq.RedriveEnqueueKey("redrive:"+failedID),
	)
	if err != nil {
		if errors.Is(err, jetq.ErrNotRedrivable) {
			log.Printf("%s is not failed", failedID)
		}
		log.Fatal(err)
	}
	// Every retry submits the same prepared value: one replacement, one run
	// time.
	res, err := client.SubmitPrepared(ctx, replacement)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("replacement %s: %v", res.JobID, res.Status == jetq.Accepted)
}

// ExampleDriver_syncSchedules shows the owner/version protocol: a sync states
// the version it expects, so an old deployment cannot delete a newer set.
func ExampleDriver_SyncSchedules() {
	ctx := context.Background()
	driver := openExample(ctx)
	defer driver.Close()

	daily := natsq.Schedule{
		Key:      "daily",
		Cron:     "0 2 * * *",
		Timezone: "Asia/Shanghai",
		Task:     "invoice.close",
		Payload:  json.RawMessage(`{"region": "cn"}`),
		Queue:    "billing",
	}
	version, err := driver.SyncSchedules(ctx, "billing", 0, daily)
	if errors.Is(err, natsq.ErrVersionConflict) {
		// Another process changed the set; take its version and re-apply.
		version, err = driver.CurrentScheduleVersion(ctx, "billing")
		if err == nil {
			version, err = driver.SyncSchedules(ctx, "billing", version, daily)
		}
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("schedule set at version %d", version)
}
