package jetq_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/runforyou-ai/jetq"
	"github.com/runforyou-ai/jetq/jetqtest"
)

func TestAttachPreservesResources(t *testing.T) {
	s := jetqtest.Start(t)
	ctx := context.Background()
	opts := []jetq.Option{jetq.WithMemoryStorage(), jetq.WithDuplicateWindow(time.Hour)}
	if _, err := jetq.Attach(ctx, s.JetStream, opts...); err == nil {
		t.Fatal("missing resources accepted")
	}
	r, err := jetq.ResourceConfigs(opts...)
	if err != nil {
		t.Fatal(err)
	}
	r.Work.MaxBytes = 8 << 20
	for _, c := range []jetstream.StreamConfig{r.Work, r.Dead} {
		if _, err := s.JetStream.CreateStream(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.JetStream.CreateKeyValue(ctx, r.State); err != nil {
		t.Fatal(err)
	}
	stream, err := s.JetStream.Stream(ctx, r.Work.Name)
	if err != nil {
		t.Fatal(err)
	}
	before, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, err := jetq.Attach(ctx, s.JetStream, jetq.WithDuplicateWindow(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	after, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Config, after.Config) {
		t.Fatal("Attach changed configuration")
	}
	if _, err := c.Enqueue(ctx, sendEmail{To: "a"}, jetq.UniqueUntilDone("attach")); err != nil {
		t.Fatal(err)
	}
	if _, err := jetq.Attach(ctx, s.JetStream, jetq.WithDuplicateWindow(2*time.Hour)); err == nil {
		t.Fatal("short duplicate window accepted")
	}
}

func TestMsgIDResumesUncertainEnqueue(t *testing.T) {
	for _, unique := range []bool{false, true} {
		for _, stored := range []bool{false, true} {
			t.Run(fmt.Sprintf("unique=%t/stored=%t", unique, stored), func(t *testing.T) {
				c, f := newFlakyClient(t)
				opts := []jetq.EnqueueOption{jetq.JobID("stable-job"), jetq.MsgID("stable-publish")}
				if unique {
					opts = append(opts, jetq.UniqueUntilDone("mail"))
				}
				f.fail(stored, false)
				if _, err := c.Enqueue(context.Background(), sendEmail{To: "a"}, opts...); !errors.Is(err, jetq.ErrUncertain) {
					t.Fatalf("uncertain error = %v", err)
				}
				if _, err := c.Enqueue(context.Background(), sendEmail{To: "a"}, opts...); err != nil {
					t.Fatal(err)
				}
				if _, err := c.Enqueue(context.Background(), sendEmail{To: "a"}, opts...); err != nil {
					t.Fatal(err)
				}
				ran, counts := runCount(t, c)
				wait(t, ran, 5*time.Second)
				expectNothing(t, ran, 100*time.Millisecond)
				if counts.Load() != 1 {
					t.Fatalf("runs = %d", counts.Load())
				}
			})
		}
	}
}
