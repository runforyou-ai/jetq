package jetq_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/runforyou-ai/jetq"
)

// plainInput is shared application data without queue methods.
type plainInput struct {
	Value string `json:"value"`
}

func TestNamedPayloads(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default"})
	got := make(chan string, 2)
	failed := make(chan string, 1)
	jetq.HandleNamed(w, "plain.success", func(ctx context.Context, input plainInput) error {
		info, _ := jetq.JobInfo(ctx)
		got <- info.Name + ":" + input.Value
		return nil
	})
	jetq.HandleNamed(w, "plain.failure", func(ctx context.Context, input plainInput) error {
		return jetq.Permanent(errors.New("failed"))
	}, jetq.OnFailure(func(ctx context.Context, input plainInput, err error) { failed <- input.Value + ":" + err.Error() }))
	start(t, w)
	for _, name := range []string{"plain.success", "plain.failure"} {
		if _, err := c.EnqueueNamed(t.Context(), name, plainInput{Value: "payload"}); err != nil {
			t.Fatal(err)
		}
	}
	if v := wait(t, got, 5*time.Second); v != "plain.success:payload" {
		t.Fatal(v)
	}
	if v := wait(t, failed, 5*time.Second); v != "payload:failed" {
		t.Fatal(v)
	}
}

func TestNamedRawPayloadAndValidation(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default"})
	got := make(chan string, 1)
	w.HandleRaw("raw", func(ctx context.Context, data json.RawMessage) error { got <- string(data); return nil })
	start(t, w)
	raw := json.RawMessage("{ \"value\" : 1234567890123456789 }")
	if _, err := c.EnqueueNamed(t.Context(), "raw", raw); err != nil {
		t.Fatal(err)
	}
	if v := wait(t, got, 5*time.Second); v != string(raw) {
		t.Fatalf("raw payload changed: %s", v)
	}
	for _, payload := range []any{json.RawMessage(`{`), make(chan int)} {
		if _, err := c.EnqueueNamed(t.Context(), "raw", payload); err == nil {
			t.Fatal("invalid payload accepted")
		}
	}
	if _, err := c.EnqueueNamed(t.Context(), "", plainInput{}); err == nil {
		t.Fatal("empty name accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.EnqueueNamed(ctx, "raw", plainInput{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled enqueue: %v", err)
	}
}

func TestNamedDecodeFailure(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "default", MaxAttempts: 3})
	failed := make(chan jetq.Info, 1)
	jetq.HandleNamed(w, "plain", func(context.Context, plainInput) error { t.Error("invalid input reached handler"); return nil })
	w.OnFailed(func(ctx context.Context, info jetq.Info, _ []byte, err error) {
		if !jetq.IsPermanent(err) {
			t.Errorf("decode error was not permanent: %v", err)
		}
		failed <- info
	})
	start(t, w)
	if _, err := c.EnqueueNamed(t.Context(), "plain", json.RawMessage(`{"value":123}`)); err != nil {
		t.Fatal(err)
	}
	if info := wait(t, failed, 5*time.Second); info.Attempt != 1 {
		t.Fatalf("decode was retried: %+v", info)
	}
}

func TestNamedScheduleOptions(t *testing.T) {
	c := newClient(t)
	w := c.NewWorker(jetq.Queue{Name: "reports", Timeout: time.Second})
	got := make(chan jetq.Info, 4)
	jetq.HandleNamed(w, "plain.report", func(ctx context.Context, input plainInput) error {
		if input.Value != "scheduled" {
			t.Errorf("payload: %+v", input)
		}
		info, _ := jetq.JobInfo(ctx)
		got <- info
		return nil
	})
	start(t, w)
	timeout := time.Duration(0)
	schedule := jetq.Schedule{Key: "plain", Cron: "@every 1s", Name: "plain.report", Payload: plainInput{Value: "scheduled"}, Queue: "reports", MaxAttempts: 7, Timeout: &timeout, Header: nats.Header{"Traceparent": []string{"trace"}}}
	if err := c.SyncSchedules(t.Context(), schedule); err != nil {
		t.Fatal(err)
	}
	info := wait(t, got, 5*time.Second)
	if info.Name != "plain.report" || info.MaxAttempts != 7 || info.Timeout != 0 || info.Header.Get("Traceparent") != "trace" {
		t.Fatalf("schedule options: %+v", info)
	}
	if err := c.SyncSchedules(t.Context()); err != nil {
		t.Fatal(err)
	}
	negative := -time.Second
	for _, s := range []jetq.Schedule{
		{Key: "bad", Cron: "@hourly", Name: "plain", MaxAttempts: -1},
		{Key: "bad", Cron: "@hourly", Name: "plain", Timeout: &negative},
		{Key: "bad", Cron: "@hourly", Name: "plain", Header: nats.Header{"Jetq-Job": []string{"override"}}},
		{Key: "bad", Cron: "@hourly", Name: "plain", Payload: json.RawMessage(`{`)},
		{Key: "bad", Cron: "@hourly"},
	} {
		if err := c.SyncSchedules(t.Context(), s); err == nil {
			t.Fatalf("invalid schedule accepted: %+v", s)
		}
	}
}
