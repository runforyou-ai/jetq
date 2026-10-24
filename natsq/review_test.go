package natsq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetq"
)

type reviewFailMarker struct {
	jetstream.KeyValue
	prefix string
}

func (k reviewFailMarker) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	if strings.HasPrefix(key, k.prefix) {
		return 0, errors.New("injected marker unavailable")
	}
	return k.KeyValue.Update(ctx, key, value, rev)
}

func TestReviewUniqueMarkerFailurePreservesActiveJob(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "reviewunique")
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	kv := d.ledger.kv
	d.ledger.kv = reviewFailMarker{KeyValue: kv, prefix: "ul."}
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "account-1"}
	first := prepare(t, "review-first", "q", unique)
	r, err := d.Submit(ctx, first)
	if err != nil || r.Status != jetq.Accepted {
		t.Fatalf("first=%+v %v", r, err)
	}
	d.ledger.kv = kv
	now := time.Now().Add(time.Minute)
	d.hks.now = func() time.Time { return now }
	r, err = d.Submit(ctx, prepare(t, "review-second", "q", unique))
	if err != nil || r.Status != jetq.Suppressed || r.ExistingJobID != "review-first" {
		t.Fatalf("active unique job lost protection: %+v %v", r, err)
	}
}

func TestReviewIntentMarkerFailureKeepsAcceptedJob(t *testing.T) {
	s := startServer(t)
	cfg := testConfig(t, "reviewintent")
	d := openDriver(t, s, cfg)
	ctx := context.Background()
	kv := d.ledger.kv
	d.ledger.kv = reviewFailMarker{KeyValue: kv, prefix: "ek."}
	first := prepare(t, "review-first", "q", nil)
	first.Envelope.EnqueueKey = "intent-1"
	first.Fingerprint = first.Envelope.Fingerprint()
	r, err := d.Submit(ctx, first)
	if err != nil || r.Status != jetq.Accepted {
		t.Fatalf("first=%+v %v", r, err)
	}
	d.ledger.kv = kv
	now := time.Now().Add(time.Minute)
	d.hks.now = func() time.Time { return now }
	second := prepare(t, "review-second", "q", nil)
	second.Envelope.EnqueueKey = "intent-1"
	second.Fingerprint = second.Envelope.Fingerprint()
	r, err = d.Submit(ctx, second)
	if err != nil || r.Status != jetq.AlreadyAccepted || r.JobID != "review-first" {
		t.Fatalf("accepted intent replaced: %+v %v", r, err)
	}
}
