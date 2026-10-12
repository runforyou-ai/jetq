package jetq_test

import (
	"context"
	"crypto/sha256"
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

func TestMsgIDSettledJobDoesNotReacquireUniqueLock(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(fmt.Sprintf("lostAck=%t", lostAck), func(t *testing.T) {
			c, f := newFlakyClient(t)
			ctx := context.Background()
			opts := []jetq.EnqueueOption{jetq.JobID("settled"), jetq.MsgID("settled-publish"), jetq.UniqueUntilDone("mail")}
			if _, err := c.Enqueue(ctx, sendEmail{To: "first"}, opts...); err != nil {
				t.Fatal(err)
			}
			ran, _ := runCount(t, c)
			wait(t, ran, 5*time.Second)
			bucket, err := f.KeyValue(ctx, "JETQ_STATE")
			if err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("%x", sha256.Sum256([]byte("mail")))
			eventually(t, func() bool { _, err := bucket.Get(ctx, key); return errors.Is(err, jetstream.ErrKeyNotFound) })
			if lostAck {
				f.fail(true)
			}
			if _, err := c.Enqueue(ctx, sendEmail{To: "first"}, opts...); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Enqueue(ctx, sendEmail{To: "second"}, jetq.UniqueUntilDone("mail")); err != nil {
				t.Fatal(err)
			}
			if to := wait(t, ran, 5*time.Second); to != "second" {
				t.Fatalf("unexpected job %q", to)
			}
		})
	}
}

func TestMsgIDCancellationSurvivesRetry(t *testing.T) {
	for _, unique := range []bool{false, true} {
		t.Run(fmt.Sprint(unique), func(t *testing.T) {
			s := jetqtest.Start(t)
			ctx := context.Background()
			c, err := jetq.New(ctx, s.JetStream, jetq.WithMemoryStorage(), jetq.WithDuplicateWindow(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			opts := []jetq.EnqueueOption{jetq.JobID("cancelled"), jetq.MsgID("cancelled-publication"), jetq.At(time.Now().Add(time.Hour))}
			if unique {
				opts = append(opts, jetq.UniqueUntilDone("mail"))
			}
			if _, err = c.Enqueue(ctx, sendEmail{To: "cancelled"}, opts...); err != nil {
				t.Fatal(err)
			}
			if err = c.Cancel(ctx, "cancelled"); err != nil {
				t.Fatal(err)
			}
			for _, pause := range []time.Duration{0, 1100 * time.Millisecond} {
				time.Sleep(pause)
				if _, err = c.Enqueue(ctx, sendEmail{To: "cancelled"}, opts...); err != nil {
					t.Fatal(err)
				}
				// An expired absolute time still identifies the same cancelled publication.
				if _, err = c.Enqueue(ctx, sendEmail{To: "cancelled"}, jetq.JobID("cancelled"), jetq.MsgID("cancelled-publication")); err != nil {
					t.Fatal(err)
				}
			}
			ran, _ := runCount(t, c)
			if _, err = c.Enqueue(ctx, sendEmail{To: "next"}, jetq.UniqueUntilDone("mail")); err != nil {
				t.Fatal(err)
			}
			if to := wait(t, ran, 5*time.Second); to != "next" {
				t.Fatalf("cancelled job resurrected: %s", to)
			}
			expectNothing(t, ran, 100*time.Millisecond)
		})
	}
}

func TestMsgIDPreservesActiveLockAfterConflict(t *testing.T) {
	c, _ := newFlakyClient(t)
	ctx := context.Background()
	at := time.Now().Add(time.Hour)
	opts := []jetq.EnqueueOption{jetq.JobID("pending"), jetq.MsgID("publication"), jetq.UniqueUntilDone("mail"), jetq.At(at)}
	if _, err := c.Enqueue(ctx, sendEmail{}, opts...); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, sendEmail{}, append(opts, jetq.MsgID("different"))...); !errors.Is(err, jetq.ErrJobIDInUse) {
		t.Fatalf("conflict: %v", err)
	}
	if _, err := c.Enqueue(ctx, sendEmail{}, jetq.UniqueUntilDone("mail")); !errors.Is(err, jetq.ErrDuplicate) {
		t.Fatalf("active lock was lost: %v", err)
	}
	if _, err := c.Enqueue(ctx, sendEmail{}, append(opts, jetq.Delay(time.Second))...); err == nil {
		t.Fatal("relative schedule accepted with MsgID")
	}
}

func TestAttachRejectsIncompatibleWorkStream(t *testing.T) {
	for _, kind := range []string{"delete", "ack", "retention"} {
		t.Run(kind, func(t *testing.T) {
			s := jetqtest.Start(t)
			ctx := context.Background()
			opts := []jetq.Option{jetq.WithMemoryStorage(), jetq.WithDuplicateWindow(time.Second)}
			resources, err := jetq.ResourceConfigs(opts...)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "delete":
				resources.Work.DenyDelete = true
			case "ack":
				resources.Work.NoAck = true
			case "retention":
				resources.Work.MaxAge = time.Minute
			}
			for _, config := range []jetstream.StreamConfig{resources.Work, resources.Dead} {
				if _, err = s.JetStream.CreateStream(ctx, config); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.JetStream.CreateKeyValue(ctx, resources.State); err != nil {
				t.Fatal(err)
			}
			if _, err = jetq.Attach(ctx, s.JetStream, opts...); err == nil {
				t.Fatal("incompatible work stream accepted")
			}
		})
	}
}
