package natsq

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type reviewLoseAdmissionResponse struct {
	jetstream.KeyValue
	once sync.Once
}

func (k *reviewLoseAdmissionResponse) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	n, e := k.KeyValue.Update(ctx, key, value, rev)
	var r record
	if e == nil && json.Unmarshal(value, &r) == nil && r.ExecutionID != "" && r.Budget > 0 && r.Pending == nil {
		lost := false
		k.once.Do(func() { lost = true })
		if lost {
			return 0, context.DeadlineExceeded
		}
	}
	return n, e
}
func TestReviewAdmissionResponseLossAtBudgetLimit(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "admit-response"))
	ctx := context.Background()
	p := prepare(t, "job", "q", nil)
	p.Envelope.Policy.MaxAttempts = 1
	p.Fingerprint = p.Envelope.Fingerprint()
	mustSubmit(t, d, p)
	c, _ := d.Consumer(ctx, "q")
	dlv := receive(t, c)
	d.ledger.kv = &reviewLoseAdmissionResponse{KeyValue: d.ledger.kv}
	if _, e := dlv.Admit(ctx); e == nil {
		t.Fatal("first admission response should be lost")
	}
	a, e := dlv.Admit(ctx)
	if e != nil || a.Attempt != 1 || a.ExecutionID == "" {
		t.Fatalf("same claim cannot recover its committed admission at budget limit: %+v %v", a, e)
	}
	ent, ok, e := d.ledger.get(ctx, jobKey("job"))
	if e != nil || !ok || ent.rec.Budget != 1 {
		t.Fatalf("admission budget changed on retry: %+v %v %v", ent, ok, e)
	}
}
