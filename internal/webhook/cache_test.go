package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

// TestTargetCacheDanKirimV2 memastikan TargetCache melayani Service v2 tanpa query ke store dan mengirim header X-WA-Event-Id.
func TestTargetCacheDanKirimV2(t *testing.T) {
	received := make(chan struct {
		eventID string
		attempt string
		body    map[string]any
	}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		received <- struct {
			eventID string
			attempt string
			body    map[string]any
		}{
			eventID: r.Header.Get("X-WA-Event-Id"),
			attempt: r.Header.Get("X-WA-Delivery-Attempt"),
			body:    body,
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    2,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	tc := NewTargetCache()
	tc.Set("sales", Target{
		URL:       srv.URL,
		Secret:    "whsec_test",
		EventsCSV: "message",
		Format:    "waapi",
		Lanes:     2,
	})

	var fallbackCalls atomic.Int64
	svc := NewService(j, tc, NewDeadStore(j.DB()), srv.Client(), nil, Options{
		CommitEvery: 1,
		OnPause: func(context.Context, string, string) error {
			fallbackCalls.Add(1)
			return nil
		},
	})
	svc.Start(ctx, []string{"sales"})
	defer svc.Stop()

	res, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "628111@s.whatsapp.net",
		TS:         1794360100,
		Payload:    []byte(`{"id":"M77","text":"halo cache"}`),
	})
	if err != nil {
		t.Fatalf("journal.Append gagal: %v", err)
	}

	select {
	case got := <-received:
		if got.eventID != "1" || res.Seq != 1 || got.attempt != "1" {
			t.Fatalf("header X-WA-Event-Id / Attempt tidak sesuai: %+v seq=%d", got, res.Seq)
		}
		if got.body["v"] != float64(2) || got.body["id"] != "1" || got.body["event"] != "message" {
			t.Fatalf("envelope v2 webhook tidak sesuai: %+v", got.body)
		}
		if fallbackCalls.Load() != 0 {
			t.Fatalf("tidak boleh ada fallback pause, terpanggil %d kali", fallbackCalls.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webhook v2 tidak terkirim tepat waktu")
	}
}
