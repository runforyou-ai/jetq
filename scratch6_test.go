package jetq_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
)

func TestScratchRawPointerAndScheduleJobID(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	// Pointer RawJob: what payload ends up in the queue?
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 1})
	got := make(chan string, 2)
	w.HandleRaw("legacy.action", func(ctx context.Context, payload json.RawMessage) error {
		got <- string(payload)
		return nil
	})
	start(t, w)

	if _, err := c.Enqueue(ctx, &jetq.RawJob{Name: "legacy.action", Payload: json.RawMessage(`{"N":7}`)}); err != nil {
		t.Fatal(err)
	}
	if p := wait(t, got, 5*time.Second); p != `{"N":7}` {
		t.Errorf("pointer RawJob payload = %q", p)
	}

	// Direct json.Marshal of RawJob now that MarshalJSON is gone.
	b, err := json.Marshal(jetq.RawJob{Name: "n", Payload: json.RawMessage(`{"a":1}`)})
	t.Logf("json.Marshal(RawJob) = %s (err=%v)", b, err)

	// JobID on a cron schedule: silently ignored?
	err = c.SyncSchedules(ctx, jetq.Cron("nightly", "@daily", report{}, jetq.JobID("fixed-id")))
	t.Logf("Cron with JobID err = %v", err)
	_ = ctx
}
