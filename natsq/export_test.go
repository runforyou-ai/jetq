package natsq

import (
	"context"

	"github.com/runforyou-ai/jobq"
)

// PublishCopy stores another copy of msg with an unrelated generation, as a
// publish whose outcome was unknown would leave behind.
func PublishCopy(ctx context.Context, d *Driver, msg *jobq.Message) error {
	env := newEnvelope(msg)
	env.Gen = jobq.NewID()
	_, err := d.publish(ctx, msg, env, msg.RunAt)
	return err
}
