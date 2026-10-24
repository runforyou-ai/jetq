// Package jetqtest starts a throwaway in-process NATS server with JetStream
// and provisions a jetq namespace on it, for tests of code that uses jetq.
package jetqtest

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/runforyou-ai/jetq"
	"github.com/runforyou-ai/jetq/natsq"
)

// Server is a running test NATS server with a provisioned jetq namespace.
type Server struct {
	*server.Server
	// Conn is a connection to the server, closed when the test ends.
	Conn *nats.Conn
	// JetStream is a JetStream context on Conn.
	JetStream jetstream.JetStream
	// Driver is attached to the namespace's resources.
	Driver *natsq.Driver
}

// Start runs a JetStream-enabled NATS server on a temporary store, provisions
// a namespace with the given name on it, and shuts everything down when the
// test ends. The namespace keeps no terminal records for long, so tests that
// resubmit a finished job id are not affected by cleanup.
func Start(tb testing.TB, namespace string) *Server {
	tb.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  tb.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		tb.Fatalf("jetqtest: create server: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		tb.Fatal("jetqtest: server not ready")
	}
	tb.Cleanup(srv.Shutdown)

	conn, err := nats.Connect(srv.ClientURL())
	if err != nil {
		tb.Fatalf("jetqtest: connect: %v", err)
	}
	tb.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		tb.Fatalf("jetqtest: jetstream: %v", err)
	}
	cfg := Config(namespace)
	if _, err := natsq.Provision(context.Background(), js, cfg); err != nil {
		tb.Fatalf("jetqtest: provision: %v", err)
	}
	driver, err := natsq.Open(context.Background(), js, cfg)
	if err != nil {
		tb.Fatalf("jetqtest: open: %v", err)
	}
	return &Server{Server: srv, Conn: conn, JetStream: js, Driver: driver}
}

// Config returns the namespace configuration used by [Start]: in-memory
// storage, a short claim lease and a short scan interval so tests see
// recovery quickly.
func Config(namespace string) natsq.Config {
	return natsq.Config{
		Namespace:    namespace,
		Memory:       true,
		ClaimLease:   2 * time.Second,
		ScanInterval: 100 * time.Millisecond,
	}
}

// Client returns a producer on the server's driver.
func (s *Server) Client(tb testing.TB, opts ...jetq.ClientOption) *jetq.Client {
	tb.Helper()
	opts = append([]jetq.ClientOption{jetq.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))}, opts...)
	c, err := jetq.NewClient(s.Driver, opts...)
	if err != nil {
		tb.Fatalf("jetqtest: client: %v", err)
	}
	return c
}
