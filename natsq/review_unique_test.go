package natsq

import (
	"context"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
)

func TestReviewWindowSurvivesSuccessfulJob(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "windowreview"))
	ctx := context.Background()
	u := &jetq.UniqueRule{Mode: jetq.UniqueModeWindow, Key: "window-key", Window: time.Hour}
	mustSubmit(t, d, prepare(t, "window-first", "q", u))
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	a, err := dlv.Admit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideSucceed, TransitionID: jetq.TransitionID(a.ExecutionID, jetq.DecideSucceed), ExecutionID: a.ExecutionID})
	if err != nil {
		t.Fatal(err)
	}
	r, err := d.Submit(ctx, prepare(t, "window-second", "q", u))
	if err != nil || r.Status != jetq.Suppressed {
		t.Fatalf("window released before expiration: %+v %v", r, err)
	}
}
func TestReviewSuppressedIntentDoesNotRunAfterRecovery(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "suppressreview"))
	ctx := context.Background()
	u := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "one-at-a-time"}
	mustSubmit(t, d, prepare(t, "holder", "q", u))
	candidate := prepare(t, "suppressed", "q", u)
	candidate.Envelope.EnqueueKey = "suppressed-intent"
	candidate.Fingerprint = candidate.Envelope.Fingerprint()
	r, err := d.Submit(ctx, candidate)
	if err != nil || r.Status != jetq.Suppressed {
		t.Fatalf("submit=%+v %v", r, err)
	}
	if _, err := d.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if st, ok, err := d.JobState(ctx, "suppressed"); err != nil || ok {
		t.Fatalf("suppressed submission became executable during recovery: state=%v exists=%v err=%v", st, ok, err)
	}
}
