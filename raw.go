package jetq

import (
	"context"
	"encoding/json"
)

// RawJob is a job whose name and JSON payload are only known at runtime, for
// example when bridging an existing task system. Enqueue it like any other job
// and handle it with [Worker.HandleRaw]. Payload must be valid JSON (empty
// means null) and is stored and delivered byte for byte.
type RawJob struct {
	Name    string
	Payload json.RawMessage
}

// JobName returns the job name.
func (j RawJob) JobName() string { return j.Name }

// MarshalJSON returns the payload ("null" when empty), so a RawJob encodes
// as its payload wherever it is marshalled.
func (j RawJob) MarshalJSON() ([]byte, error) {
	if len(j.Payload) == 0 {
		return []byte("null"), nil
	}
	return j.Payload, nil
}

// RawHandleOption configures a handler registered with [Worker.HandleRaw].
type RawHandleOption func(*handler)

// OnRawFailure registers a callback for when a raw job is dead-lettered.
func OnRawFailure(fn func(ctx context.Context, payload json.RawMessage, err error)) RawHandleOption {
	return func(h *handler) {
		h.failed = func(ctx context.Context, payload []byte, err error) { fn(ctx, payload, err) }
	}
}

// HandleRaw registers fn for jobs named name, passing the JSON payload
// undecoded. It panics if a handler for name is already registered or the
// worker has started.
func (w *Worker) HandleRaw(name string, fn func(ctx context.Context, payload json.RawMessage) error, opts ...RawHandleOption) {
	h := &handler{run: func(ctx context.Context, payload []byte) error { return fn(ctx, payload) }}
	for _, opt := range opts {
		opt(h)
	}
	w.register(name, h)
}
