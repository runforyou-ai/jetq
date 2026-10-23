package jetq

import (
	"encoding/json"
	"testing"
	"time"
)

func TestReviewPreparedJSONPreservesPayloadAndFingerprint(t *testing.T) {
	raw := json.RawMessage("{\n  \"html\": \"<x>\", \"n\": 1\n}")
	p, e := Prepare("pretty", raw, Submission{JobID: "raw", EnqueueKey: "raw-key"}, Policy{}, Policy{}, time.Now(), nil)
	if e != nil {
		t.Fatal(e)
	}
	b, e := json.Marshal(p)
	if e != nil {
		t.Fatal(e)
	}
	var restored PreparedJob
	if e = json.Unmarshal(b, &restored); e != nil {
		t.Fatal(e)
	}
	if string(restored.Envelope.Payload) != string(raw) || restored.Envelope.Fingerprint() != restored.Fingerprint {
		t.Fatalf("persisted preparation changed raw bytes/fingerprint: payload=%q valid_fingerprint=%v", restored.Envelope.Payload, restored.Envelope.Fingerprint() == restored.Fingerprint)
	}
}
