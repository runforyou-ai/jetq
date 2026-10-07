package jetq_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
)

func TestScratchQueueFilterMultipleBatches(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	js := c.JetStream()
	const n = 600
	for i := 0; i < n; i++ {
		msg := &nats.Msg{
			Subject: "jetq.dead.other",
			Data:    []byte(`{"i":` + strconv.Itoa(i) + `}`),
			Header:  nats.Header{},
		}
		msg.Header.Set("Jetq-Id", fmt.Sprint(i))
		msg.Header.Set("Jetq-Job", "other-job")
		msg.Header.Set("Jetq-Queue", "other")
		msg.Header.Set("Jetq-Attempts", "1")
		msg.Header.Set("Jetq-Failed-At", time.Now().UTC().Format(time.RFC3339Nano))
		msg.Header.Set("Jetq-Error", "boom")
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithExpectStream(c.DeadLetterStreamName())); err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	out, err := c.DeadLetters(ctx, jetq.DeadLetterQuery{Queue: "other", Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("limit=50 got %d entries in %s", len(out), time.Since(start))
	if len(out) > 0 {
		t.Logf("first seq=%d payload=%s, last seq=%d payload=%s",
			out[0].Sequence, out[0].Payload, out[len(out)-1].Sequence, out[len(out)-1].Payload)
	}
	// Newest first should be sequences 600..551.
	for i := 1; i < len(out); i++ {
		if out[i-1].Sequence <= out[i].Sequence {
			t.Fatalf("not newest first: %d then %d", out[i-1].Sequence, out[i].Sequence)
		}
	}
	if len(out) == 50 && out[0].Sequence != n {
		t.Fatalf("expected newest seq %d, got %d", n, out[0].Sequence)
	}
}
