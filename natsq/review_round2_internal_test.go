package natsq

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type reviewPausedPublisher struct {
	jetstream.JetStream
	entered, resume chan struct{}
}

func (p reviewPausedPublisher) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	close(p.entered)
	select {
	case <-p.resume:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return p.JetStream.PublishMsg(ctx, m, opts...)
}
func ReviewPausePublish(d *Driver) (<-chan struct{}, func(), func()) {
	original := d.js
	entered, resume := make(chan struct{}), make(chan struct{})
	d.js = reviewPausedPublisher{original, entered, resume}
	return entered, func() { close(resume) }, func() { d.js = original }
}
