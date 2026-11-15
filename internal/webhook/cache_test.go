package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
)

// TestTargetCacheDanEnqueueEvent memastikan TargetCache melayani EnqueueEvent tanpa fallback DB dan mengirim header X-WA-Event-Id.
func TestTargetCacheDanEnqueueEvent(t *testing.T) {
	received := make(chan struct {
		eventID string
		body    map[string]any
	}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		received <- struct {
			eventID string
			body    map[string]any
		}{eventID: r.Header.Get("X-WA-Event-Id"), body: body}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tc := NewTargetCache()
	tc.Set("sales", Target{
		URL:       srv.URL,
		Secret:    "sec",
		EventsCSV: "message",
		Format:    "waapi",
	})

	var fallbackCalls atomic.Int64
	d := NewDispatcher(nil, func(ctx context.Context, instanceID string) (string, string, string, error) {
		fallbackCalls.Add(1)
		return "", "", "", nil
	}, 1)
	d.SetTargetCache(tc)

	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)
	defer func() {
		cancel()
		d.Stop()
	}()

	d.EnqueueEvent(journal.Event{
		Seq:        77,
		InstanceID: "sales",
		Type:       "message",
		TS:         1794360100,
		Payload:    []byte(`{"id":"M77","text":"halo cache"}`),
	})

	select {
	case got := <-received:
		if got.eventID != "77" {
			t.Fatalf("X-WA-Event-Id diharapkan '77', didapat %q", got.eventID)
		}
		if got.body["v"] != float64(2) || got.body["id"] != "77" || got.body["event"] != "message" {
			t.Fatalf("envelope v2 webhook tidak sesuai: %+v", got.body)
		}
		if fallbackCalls.Load() != 0 {
			t.Fatalf("fallback resolver tidak boleh dipanggil saat cache hit, dipanggil %d kali", fallbackCalls.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webhook EnqueueEvent tidak terkirim tepat waktu")
	}
}
