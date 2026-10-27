package memq_test

import (
	"testing"
	"time"

	"github.com/runforyou-ai/jobq"
	"github.com/runforyou-ai/jobq/drivertest"
	"github.com/runforyou-ai/jobq/memq"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	drivertest.Run(t, func(*testing.T) jobq.Driver { return memq.New() },
		drivertest.Options{Resolution: 200 * time.Millisecond, CancelReady: true})
}
