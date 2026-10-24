package natsq

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetq"
)

type reviewPauseIntent struct {
	jetstream.KeyValue
	once             sync.Once
	entered, release chan struct{}
}

func (k *reviewPauseIntent) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	r, e := k.KeyValue.Create(ctx, key, value, opts...)
	if e == nil && strings.HasPrefix(key, "ek.") {
		k.once.Do(func() { close(k.entered); <-k.release })
	}
	return r, e
}
func TestReviewConcurrentRecoveryRespectsUnique(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "intent-race"))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	unique := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "one-job"}
	mustSubmit(t, d, prepare(t, "owner", "q", unique))
	pause := &reviewPauseIntent{KeyValue: d.ledger.kv, entered: make(chan struct{}), release: make(chan struct{})}
	d.ledger.kv = pause
	candidate := prepare(t, "candidate", "q", unique)
	candidate.Envelope.EnqueueKey = "candidate-intent"
	candidate.Fingerprint = candidate.Envelope.Fingerprint()
	type answer struct {
		res jetq.EnqueueResult
		err error
	}
	done := make(chan answer, 1)
	go func() { r, e := d.Submit(ctx, candidate); done <- answer{r, e} }()
	select {
	case <-pause.entered:
	case <-ctx.Done():
		close(pause.release)
		t.Fatal("intent was not persisted")
	}
	_, err := d.Recover(ctx)
	if err != nil {
		t.Error(err)
	}
	if st, ok, e := d.JobState(ctx, "candidate"); e != nil || ok {
		t.Errorf("recovery bypassed active unique owner: state=%v exists=%v error=%v", st, ok, e)
	}
	close(pause.release)
	select {
	case a := <-done:
		if a.err != nil || a.res.Status != jetq.Suppressed {
			t.Errorf("paused producer outcome=%+v error=%v; want Suppressed", a.res, a.err)
		}
	case <-ctx.Done():
		t.Fatal("producer did not finish")
	}
}
