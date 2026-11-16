package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/journal"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type stubWebhookGov struct {
	lvl atomic.Int32
}

func (g *stubWebhookGov) Level() governor.Level {
	return governor.Level(g.lvl.Load())
}

// TestDeliverer_PacingSaatYellowDanRed memverifikasi pacing 200ms per lane saat governor berada pada level >= Yellow dan tanpa jeda saat Green.
func TestDeliverer_PacingSaatYellowDanRed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		jr := newFakeJournal()
		tc := NewTargetCache()
		tc.Set("sales", Target{
			URL:         "https://example.invalid/hook",
			Secret:      "whsec_a2V5MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM=",
			Lanes:       1,
			Format:      "waapi",
			MaxAttempts: 3,
		})

		var mu sync.Mutex
		var deliveredAt []time.Time
		client := &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				mu.Lock()
				deliveredAt = append(deliveredAt, time.Now())
				mu.Unlock()
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("ok")),
					Header:     make(http.Header),
				}, nil
			}),
		}

		gov := &stubWebhookGov{}
		gov.lvl.Store(int32(governor.Yellow))

		svc := newServiceForTest(jr, tc, nil, nil, client, nil, Options{
			CommitEvery:    1,
			CommitInterval: time.Second,
			AttemptTimeout: 2 * time.Second,
			IdleWait:       10 * time.Second,
			Governor:       gov,
		})

		ctx, cancel := context.WithCancel(context.Background())
		svc.Start(ctx, []string{"sales"})
		synctest.Wait()

		for i := 1; i <= 3; i++ {
			jr.appendEvent(journal.Event{
				InstanceID: "sales",
				Type:       "message",
				Chat:       "6281234@s.whatsapp.net",
				Payload:    []byte(`{"ok":true}`),
			})
		}

		synctest.Wait()
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()

		mu.Lock()
		nEarly := len(deliveredAt)
		mu.Unlock()
		if nEarly != 1 {
			t.Fatalf("saat Yellow di t=50ms baru 1 event seharusnya terkirim (terkena pacing 200ms), didapat %d", nEarly)
		}

		// Lewati pacing 200ms kedua di level Red -> pacing tetap aktif saat Red
		gov.lvl.Store(int32(governor.Red))
		time.Sleep(450 * time.Millisecond)
		synctest.Wait()

		mu.Lock()
		ts := append([]time.Time(nil), deliveredAt...)
		mu.Unlock()
		if len(ts) != 3 {
			t.Fatalf("ketiga event diharapkan sudah terkirim pada t=500ms, didapat %d", len(ts))
		}
		if d1 := ts[1].Sub(ts[0]); d1 < 200*time.Millisecond {
			t.Fatalf("jarak event 1->2 saat Yellow diharapkan >= 200ms, didapat %v", d1)
		}
		if d2 := ts[2].Sub(ts[1]); d2 < 200*time.Millisecond {
			t.Fatalf("jarak event 2->3 saat Red diharapkan >= 200ms, didapat %v", d2)
		}

		// Kembalikan ke Green: batch event berikutnya langsung terkirim tanpa jeda 200ms
		gov.lvl.Store(int32(governor.Green))
		for i := 4; i <= 6; i++ {
			jr.appendEvent(journal.Event{
				InstanceID: "sales",
				Type:       "message",
				Chat:       "6281234@s.whatsapp.net",
				Payload:    []byte(`{"ok":true}`),
			})
		}
		synctest.Wait()

		mu.Lock()
		tsAll := append([]time.Time(nil), deliveredAt...)
		mu.Unlock()
		if len(tsAll) != 6 {
			t.Fatalf("saat Green seluruh 3 event baru harus langsung terkirim tanpa sleep 200ms, didapat total %d", len(tsAll))
		}
		if gap := tsAll[5].Sub(tsAll[3]); gap >= 200*time.Millisecond {
			t.Fatalf("saat Green tidak boleh ada pacing 200ms, jarak didapat %v", gap)
		}

		cancel()
		svc.Stop()
	})
}

var _ = httptest.NewRecorder
