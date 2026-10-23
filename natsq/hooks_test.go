package natsq

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

// faultKV delegates to a real key-value store and injects failures.
type faultKV struct {
	jetstream.KeyValue
	// createCommitted: Create writes the key, then reports an error, as a
	// lost response does.
	createCommitted func(key string) bool
}

func (f *faultKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	rev, err := f.KeyValue.Create(ctx, key, value, opts...)
	if err == nil && f.createCommitted != nil && f.createCommitted(key) {
		return 0, context.DeadlineExceeded
	}
	return rev, err
}

var errInjected = errors.New("injected failure")
