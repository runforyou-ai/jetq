// Package jetqtest starts a throwaway in-process NATS server with JetStream
// for tests of code that uses jetq.
package jetqtest

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Server is a running test NATS server.
type Server struct {
	*server.Server
	// Conn is a connection to the server, closed when the test ends.
	Conn *nats.Conn
	// JetStream is a JetStream context on Conn.
	JetStream jetstream.JetStream
}

// Start runs a JetStream-enabled NATS server on a random port with storage in
// a temporary directory and shuts it down when the test ends.
func Start(tb testing.TB) *Server {
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
	return &Server{Server: srv, Conn: conn, JetStream: js}
}
