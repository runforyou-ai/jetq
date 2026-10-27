package natsq

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jobq"
)

type reviewNoAck struct{ jetstream.Msg }

func (reviewNoAck) Ack() error   { return nil }
func ReviewDropAck(l jobq.Lease) { x := l.(*jobLease); x.nmsg = reviewNoAck{x.nmsg} }
func ReviewPublishOccurrence(ctx context.Context, d *Driver, m *jobq.Message) error {
	e := newEnvelope(m)
	e.Schedule = "s"
	e.Gen = m.ID
	_, err := d.publish(ctx, m, e, time.Time{})
	return err
}
func ReviewStageRequeue(ctx context.Context, l jobq.Lease) error {
	x := l.(*jobLease)
	s, rev, err := x.d.getState(ctx, x.msg.ID)
	if err != nil {
		return err
	}
	// State after the requeue protocol records its intent, before the publish.
	s.Next = jobq.NewID()
	s.Running = false
	s.LeaseUntil = time.Time{}
	s.LeaseStart = time.Time{}
	_, err = x.d.putState(ctx, x.msg.ID, rev, s)
	return err
}
func ReviewUniqueTTL(d *Driver, ttl time.Duration) { d.cfg.uniqueLockTTL = ttl }

type reviewUncertainPublisher struct{ jetstream.JetStream }

func (reviewUncertainPublisher) PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, context.DeadlineExceeded
}
func ReviewRetryPublishTimeout(ctx context.Context, d *Driver, l jobq.Lease) error {
	original := d.js
	d.js = reviewUncertainPublisher{original}
	defer func() { d.js = original }()
	return l.Retry(ctx, time.Now(), "retry")
}

// ReviewStageRequeuePublished leaves the durable state of a requeue that
// crashed after publishing the new copy and before switching the record:
// the record names the new generation as Next and the new copy is stored.
func ReviewStageRequeuePublished(ctx context.Context, l jobq.Lease) error {
	x := l.(*jobLease)
	s, rev, err := x.d.getState(ctx, x.msg.ID)
	if err != nil {
		return err
	}
	env := newEnvelope(x.msg)
	env.Gen = jobq.NewID()
	s.Next = env.Gen
	s.Running = false
	s.LeaseUntil, s.LeaseStart = time.Time{}, time.Time{}
	if _, err := x.d.putState(ctx, x.msg.ID, rev, s); err != nil {
		return err
	}
	_, err = x.d.publish(ctx, x.msg, env, time.Time{})
	return err
}
