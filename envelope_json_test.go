package jetq

import (
	"encoding/json"
	"testing"
	"time"
)

// TestEnvelopeJSONKeepsPayloadBytes: values and pointers of Envelope and
// PreparedJob round-trip through JSON with the payload byte for byte and the
// fingerprint still valid.
func TestEnvelopeJSONKeepsPayloadBytes(t *testing.T) {
	raw := json.RawMessage("{\n  \"b\": \"<&>\",\t\"a\": [1, 2]\n}")
	p, err := Prepare("pretty", raw, Submission{JobID: "j", EnqueueKey: "k"}, Policy{}, Policy{}, time.Unix(1, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]any{"prepared value": *p, "prepared pointer": p} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var got PreparedJob
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if string(got.Envelope.Payload) != string(raw) || got.Envelope.Fingerprint() != p.Fingerprint || got.Fingerprint != p.Fingerprint {
			t.Errorf("%s: payload=%q", name, got.Envelope.Payload)
		}
	}
	for name, v := range map[string]any{"envelope value": p.Envelope, "envelope pointer": &p.Envelope} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var got Envelope
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if string(got.Payload) != string(raw) || got.Fingerprint() != p.Fingerprint {
			t.Errorf("%s: payload=%q", name, got.Payload)
		}
	}
}
