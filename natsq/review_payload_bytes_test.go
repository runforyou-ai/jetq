package natsq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
)

func reviewPrettyPayload() json.RawMessage {
	return json.RawMessage("{\n  \"html\": \"<x>\", \"n\": 1\n}")
}
func TestReviewDriverPreservesRawJSONBytes(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "payload-bytes"))
	p := prepare(t, "raw", "q", nil)
	p.Envelope.Payload = reviewPrettyPayload()
	p.Fingerprint = p.Envelope.Fingerprint()
	mustSubmit(t, d, p)
	c, _ := d.Consumer(context.Background(), "q")
	got := receive(t, c).Envelope().Payload
	if string(got) != string(p.Envelope.Payload) {
		t.Fatalf("driver changed raw JSON bytes: got=%q want=%q", got, p.Envelope.Payload)
	}
}
func TestReviewSchedulePreservesPrettyRawJSONBytes(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "schedule-pretty"))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d.hks.now = func() time.Time { return base }
	ctx := context.Background()
	if _, e := d.SyncSchedules(ctx, "owner", 0, Schedule{Key: "pretty", Cron: "@every 1s", Task: "pretty", Payload: reviewPrettyPayload(), Queue: "q"}); e != nil {
		t.Fatal(e)
	}
	r := d.Scheduler("owner")
	r.Now = func() time.Time { return base.Add(time.Second) }
	if e := r.Fire(ctx); e != nil {
		t.Fatal(e)
	}
	page, e := d.List(ctx, jetq.JobQuery{Limit: 10})
	if e != nil || len(page.Jobs) != 1 {
		t.Fatal(page, e)
	}
	if got := string(page.Jobs[0].Envelope.Payload); got != string(reviewPrettyPayload()) {
		t.Fatalf("schedule changed raw JSON bytes: got=%q want=%q", got, reviewPrettyPayload())
	}
}
