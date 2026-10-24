package natsq

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetq"
)

type reviewStallDecision struct {
	jetstream.KeyValue
	once             sync.Once
	entered, release chan struct{}
}

func (k *reviewStallDecision) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	var r record
	if json.Unmarshal(value, &r) == nil && r.Pending != nil {
		k.once.Do(func() { close(k.entered); <-k.release })
	}
	return k.KeyValue.Update(ctx, key, value, rev)
}
func TestReviewExtendNotBlockedBySettlement(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "settle-renew"))
	ctx := context.Background()
	mustSubmit(t, d, prepare(t, "job", "q", nil))
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	a, e := dlv.Admit(ctx)
	if e != nil {
		t.Fatal(e)
	}
	stall := &reviewStallDecision{KeyValue: d.ledger.kv, entered: make(chan struct{}), release: make(chan struct{})}
	d.ledger.kv = stall
	var unblock sync.Once
	defer unblock.Do(func() { close(stall.release) })
	settled := make(chan struct{})
	go func() {
		defer close(settled)
		_, _ = dlv.Settle(ctx, jetq.Decision{Kind: jetq.DecideSucceed, TransitionID: jetq.TransitionID(a.ExecutionID, jetq.DecideSucceed), ExecutionID: a.ExecutionID})
	}()
	select {
	case <-stall.entered:
	case <-time.After(time.Second):
		t.Fatal("decision write did not start")
	}
	extendCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	extended := make(chan error, 1)
	go func() { extended <- dlv.Extend(extendCtx) }()
	select {
	case err := <-extended:
		if err != nil {
			t.Errorf("healthy lease renewal failed during delayed settlement: %v", err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Error("Extend blocked behind settlement mutex instead of renewing the lease")
	}
	unblock.Do(func() { close(stall.release) })
	select {
	case <-settled:
	case <-time.After(time.Second):
		t.Error("settlement did not finish")
	}
}
