package natsq

import (
	"context"
	"fmt"
	"testing"

	"github.com/runforyou-ai/jetq"
)

// Benchmarks of the receive path, to show that claim cost grows with the
// number of retained records rather than with runnable ones. They are
// micro-benchmarks on one in-memory server: they do not measure throughput,
// latency or stability of a production deployment.
//
// Run with:
//
//	go test -run '^$' -bench Receive -benchtime 50x ./natsq
func benchLedger(b *testing.B, retained int) (*testServer, *Driver) {
	b.Helper()
	s := startServer(b)
	d := openDriver(b, s, testConfig(b, fmt.Sprintf("bench-%d", retained)))
	ctx := context.Background()
	client, err := jetq.NewClient(d)
	if err != nil {
		b.Fatal(err)
	}
	for i := range retained {
		sub := jetq.Submission{JobID: fmt.Sprintf("done-%06d", i), Queue: "other"}
		if _, err := client.Submit(ctx, "bench", map[string]int{"n": i}, sub); err != nil {
			b.Fatal(err)
		}
	}
	return s, d
}

// BenchmarkReceiveEmptyLedger measures a receive against an empty ledger.
func BenchmarkReceiveEmptyLedger(b *testing.B) { benchmarkReceive(b, 0) }

// BenchmarkReceiveThousandRetained measures a receive against 1,000 retained
// records of another queue; nothing is ever claimable, so the number reflects
// the scan of retained records.
func BenchmarkReceiveThousandRetained(b *testing.B) { benchmarkReceive(b, 1000) }

func benchmarkReceive(b *testing.B, retained int) {
	b.Helper()
	ctx := context.Background()
	_, d := benchLedger(b, retained)
	c, err := d.Consumer(ctx, "q")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	b.ResetTimer()
	for range b.N {
		if _, err := c.Receive(ctx, 1); err != nil {
			b.Fatal(err)
		}
	}
}
