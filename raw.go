package jetq

import (
	"context"
	"encoding/json"
)

// RawJob is a job whose name and JSON payload are only known at runtime, for
// example when bridging an existing task system. Enqueue it like any other job
// and handle it with [Worker.HandleRaw].
type RawJob struct {
	Name    string
	Payload json.RawMessage
}

// JobName returns the job name.
func (j RawJob) JobName() string { return j.Name }

// MarshalJSON returns the payload unchanged ("null" when empty).
func (j RawJob) MarshalJSON() ([]byte, error) {
	if len(j.Payload) == 0 {
		return []byte("null"), nil
	}
	return j.Payload, nil
}

// HandleRaw registers fn for jobs named name, passing the JSON payload
// undecoded. onFailure, if not nil, is called when such a job is
// dead-lettered. It panics if a handler for name is already registered or the
// worker has started.
func (w *Worker) HandleRaw(name string, fn func(ctx context.Context, payload json.RawMessage) error, onFailure func(ctx context.Context, payload json.RawMessage, err error)) {
	h := &handler{run: func(ctx context.Context, payload []byte) error { return fn(ctx, payload) }}
	if onFailure != nil {
		h.failed = func(ctx context.Context, payload []byte, err error) { onFailure(ctx, payload, err) }
	}
	w.register(name, h)
}
