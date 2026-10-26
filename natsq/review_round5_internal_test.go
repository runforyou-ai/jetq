package natsq

import (
	"context"

	"github.com/runforyou-ai/jobq"
)

func ReviewR5SeedDead(ctx context.Context, d *Driver, count int) (string, error) {
	first := ""
	for i := 0; i < count; i++ {
		m, err := jobq.NewMessage("x", i, jobq.OnQueue("q"))
		if err != nil {
			return "", err
		}
		if i == 0 {
			first = m.ID
		}
		if err := d.storeDead(ctx, m, 1, "failed", jobq.NewID()); err != nil {
			return "", err
		}
	}
	return first, nil
}
