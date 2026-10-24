package natsq

import (
	"context"
	"fmt"
	"testing"

	"github.com/runforyou-ai/jetq"
)

func TestReviewInspectorHidesSuppressedReceipt(t *testing.T) {
	s := startServer(t)
	d := openDriver(t, s, testConfig(t, "inspect-suppressed"))
	ctx := context.Background()
	u := &jetq.UniqueRule{Mode: jetq.UniqueModeUntilDone, Key: "only"}
	mustSubmit(t, d, prepare(t, "holder", "q", u))
	r, e := d.Submit(ctx, prepare(t, "suppressed", "q", u))
	if e != nil || r.Status != jetq.Suppressed {
		t.Fatal(r, e)
	}
	_, ok, e := d.GetJob(ctx, "suppressed")
	if e != nil || ok {
		t.Fatalf("suppressed receipt leaked as logical job: exists=%v error=%v", ok, e)
	}
}
func TestReviewInspectorLastPageHasNoCursor(t *testing.T) {
	for _, limit := range []int{1, 10} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			s := startServer(t)
			d := openDriver(t, s, testConfig(t, "inspect-page"))
			ctx := context.Background()
			mustSubmit(t, d, prepare(t, "a", "wanted", nil))
			mustSubmit(t, d, prepare(t, "z", "other", nil))
			page, e := d.List(ctx, jetq.JobQuery{Queue: "wanted", Limit: limit})
			if e != nil || len(page.Jobs) != 1 || page.LastJobID != "" {
				t.Fatalf("final filtered page must not advertise another page: count=%d cursor=%q error=%v", len(page.Jobs), page.LastJobID, e)
			}
		})
	}
}
