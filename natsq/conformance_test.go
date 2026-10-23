package natsq_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/runforyou-ai/jetq"
	"github.com/runforyou-ai/jetq/drivertest"
	"github.com/runforyou-ai/jetq/jetqtest"
)

// TestConformance runs the neutral driver suite against a real NATS server.
// Every backend runs the same suite; NATS-specific faults stay in the package
// tests.
func TestConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T, name string) (jetq.Driver, func()) {
		// Namespaces are derived from the subtest name so parallel pages of
		// the suite never share one.
		ns := "conf-" + shortName(name)
		s := jetqtest.Start(t, ns)
		return s.Driver, nil
	}, drivertest.Options{ClaimTimeout: 5 * time.Second})
}

// TestConformanceRedrive runs the redrive suite, which needs inspection.
func TestConformanceRedrive(t *testing.T) {
	drivertest.Redrive(t, func(t *testing.T, name string) (jetq.Driver, func()) {
		return jetqtest.Start(t, "conf-redrive-"+shortName(name)).Driver, nil
	}, drivertest.Options{ClaimTimeout: 5 * time.Second})
}

// TestEndToEndInspectorSnapshot replaces the former single-purpose budget
// probe: the E2E checks use the neutral snapshot instead.
func TestEndToEndSnapshot(t *testing.T) {
	s := jetqtest.Start(t, "e2e-snapshot")
	ctx := context.Background()
	producer := s.Client(t)
	job, err := producer.Prepare("probe", json.RawMessage(`1`), jetq.Submission{})
	if err != nil {
		t.Fatal(err)
	}
	if res, err := producer.SubmitPrepared(ctx, job); err != nil || res.Status != jetq.Accepted {
		t.Fatalf("submit = %+v, %v", res, err)
	}
	snap, found, err := producer.Inspect(ctx, job.Envelope.JobID)
	if err != nil || !found || snap.State != jetq.StateAwaiting || snap.Envelope.Task != "probe" {
		t.Fatalf("snapshot = %+v, %v, %v", snap, found, err)
	}
}

func shortName(name string) string {
	var out []byte
	for _, r := range name {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			out = append(out, byte(r))
		}
	}
	if len(out) > 40 {
		out = out[len(out)-40:]
	}
	return string(out)
}
