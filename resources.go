package jetq

import (
	"context"
	"fmt"
	"slices"

	"github.com/nats-io/nats.go/jetstream"
)

// Resources describes the streams and state bucket required by a queue.
type Resources struct {
	Work  jetstream.StreamConfig
	Dead  jetstream.StreamConfig
	State jetstream.KeyValueConfig
}

// ResourceConfigs returns queue resource definitions without contacting NATS.
func ResourceConfigs(opts ...Option) (Resources, error) {
	c, err := resolveConfig(opts...)
	if err != nil {
		return Resources{}, err
	}
	return Resources{
		Work:  jetstream.StreamConfig{Name: c.streamName, Description: "jetq jobs", Subjects: []string{c.prefix + ".q.>", c.prefix + ".at.>", c.prefix + ".cron.>"}, Retention: jetstream.WorkQueuePolicy, Storage: c.storage, Replicas: c.replicas, Duplicates: c.duplicates, MaxMsgSize: c.maxJobBytes, AllowMsgSchedules: true, AllowRollup: true},
		Dead:  jetstream.StreamConfig{Name: c.deadName, Description: "jetq dead-lettered jobs", Subjects: []string{c.prefix + ".dead.>"}, Retention: jetstream.LimitsPolicy, Storage: c.storage, Replicas: c.replicas, MaxAge: c.deadMaxAge},
		State: jetstream.KeyValueConfig{Bucket: c.streamName + "_STATE", Description: "jetq unique job locks and cancelled delayed jobs", TTL: c.uniqueTTL, Storage: c.storage, Replicas: c.replicas},
	}, nil
}

// Attach opens provisioned resources without changing stream or bucket configuration.
// It validates queue semantics and minimum retention, while preserving deployment
// choices such as storage, replicas, placement and capacity.
func Attach(ctx context.Context, js jetstream.JetStream, opts ...Option) (*Client, error) {
	cfg, err := resolveConfig(opts...)
	if err != nil {
		return nil, err
	}
	resources, err := ResourceConfigs(opts...)
	if err != nil {
		return nil, err
	}
	work, err := js.Stream(ctx, cfg.streamName)
	if err != nil {
		return nil, fmt.Errorf("jetq: attach work stream: %w", err)
	}
	for _, want := range []jetstream.StreamConfig{resources.Work, resources.Dead} {
		stream, err := js.Stream(ctx, want.Name)
		if err != nil {
			return nil, fmt.Errorf("jetq: attach %s: %w", want.Name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return nil, err
		}
		got := info.Config
		a, b := slices.Clone(got.Subjects), slices.Clone(want.Subjects)
		slices.Sort(a)
		slices.Sort(b)
		if !slices.Equal(a, b) || got.Retention != want.Retention || got.Sealed || got.DenyPurge {
			return nil, fmt.Errorf("jetq: incompatible stream %s", want.Name)
		}
		if want.Name == cfg.streamName {
			if !got.AllowMsgSchedules || !got.AllowRollup || got.Duplicates < cfg.duplicates {
				return nil, fmt.Errorf("jetq: incompatible scheduling or deduplication on %s", want.Name)
			}
			cfg.duplicates = got.Duplicates
		} else if got.MaxAge != 0 && (cfg.deadMaxAge == 0 || got.MaxAge < cfg.deadMaxAge) {
			return nil, fmt.Errorf("jetq: insufficient retention on %s", want.Name)
		}
	}
	state, err := js.KeyValue(ctx, resources.State.Bucket)
	if err != nil {
		return nil, fmt.Errorf("jetq: attach state: %w", err)
	}
	status, err := state.Status(ctx)
	if err != nil {
		return nil, err
	}
	if status.TTL() != 0 && (cfg.uniqueTTL == 0 || status.TTL() < cfg.uniqueTTL) {
		return nil, fmt.Errorf("jetq: insufficient state TTL")
	}
	return &Client{js: js, stream: work, cfg: cfg, state: state}, nil
}
