package natsq

import (
	"context"
	"testing"

	"github.com/runforyou-ai/jetq"
)

func TestReviewSuppressedEnqueueKeySurvivesNewCandidateID(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "suppressed-replay"))
	ctx := context.Background()
	u := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "shared"}
	mustSubmit(t, d, prepare(t, "holder", "q", u))
	mk := func(id string) *jetq.PreparedJob {
		p := prepare(t, id, "q", u)
		p.Envelope.EnqueueKey = "stable-intent"
		p.Fingerprint = p.Envelope.Fingerprint()
		return p
	}
	r, e := d.Submit(ctx, mk("suppressed-first"))
	if e != nil || r.Status != jetq.Suppressed {
		t.Fatalf("first outcome=%+v %v", r, e)
	}
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	a, e := dlv.Admit(ctx)
	if e != nil {
		t.Fatal(e)
	}
	_, e = dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideSucceed, ExecutionID: a.ExecutionID, TransitionID: jetq.TransitionID(a.ExecutionID, jetq.DecideSucceed)})
	if e != nil {
		t.Fatal(e)
	}
	r, e = d.Submit(ctx, mk("suppressed-retry"))
	if e != nil || r.Status != jetq.Suppressed || r.ExistingJobID != "holder" {
		t.Fatalf("same EnqueueKey changed final outcome after holder completed: %+v %v", r, e)
	}
}
