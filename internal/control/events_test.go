package control

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

// TestControlEventsEndpoint menguji endpoint internal /control/instances/{id}/events termasuk 404, filter, dan notifikasi wait.
func TestControlEventsEndpoint(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	if err := st.CreateInstance(ctx, store.Instance{
		ID:            "sales",
		Status:        store.StatusCreated,
		WebhookSecret: "sec",
		WebhookEvents: store.DefaultWebhookEvents,
		RatePerMin:    20,
		Settings:      store.Defaults(),
	}); err != nil {
		t.Fatalf("CreateInstance gagal: %v", err)
	}

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns: 3, CacheKiB: 512, IdleTimeout: time.Minute, TxLock: "immediate", Sync: "NORMAL",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	srv := NewServer(cfg, st, nil, nil, nil, WithJournal(j))
	h := srv.Handler()

	// Instance tidak dikenal harus mengembalikan 404.
	req404 := httptest.NewRequest(http.MethodGet, "/control/instances/unknown/events", nil)
	rec404 := httptest.NewRecorder()
	h.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("status 404 diharapkan untuk instance tidak dikenal, didapat %d", rec404.Code)
	}

	if _, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "6281@s.whatsapp.net",
		WAID:       "C1",
		TS:         1794360100,
		Payload:    []byte(`{"id":"C1","text":"pertama"}`),
	}); err != nil {
		t.Fatal(err)
	}

	req1 := httptest.NewRequest(http.MethodGet, "/control/instances/sales/events?after=0&types=message", nil)
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("status 200 diharapkan, didapat %d (%s)", rec1.Code, rec1.Body.String())
	}
	var p1 struct {
		Events    []map[string]any `json:"events"`
		NextAfter int64            `json:"next_after"`
		HasMore   bool             `json:"has_more"`
	}
	if err := json.Unmarshal(rec1.Body.Bytes(), &p1); err != nil {
		t.Fatal(err)
	}
	if len(p1.Events) != 1 || p1.NextAfter != 1 || p1.HasMore {
		t.Fatalf("respons tidak sesuai: len=%d next=%d has_more=%v", len(p1.Events), p1.NextAfter, p1.HasMore)
	}

	// Uji long-poll wait=2 bangkit segera setelah ada event baru.
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reqWait := httptest.NewRequest(http.MethodGet, "/control/instances/sales/events?after=1&wait=2", nil)
		recWait := httptest.NewRecorder()
		h.ServeHTTP(recWait, reqWait)
		done <- recWait
	}()

	time.Sleep(50 * time.Millisecond)
	if _, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "6281@s.whatsapp.net",
		WAID:       "C2",
		TS:         1794360101,
		Payload:    []byte(`{"id":"C2","text":"kedua"}`),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case recWait := <-done:
		if recWait.Code != http.StatusOK {
			t.Fatalf("status wait diharapkan 200, didapat %d", recWait.Code)
		}
		var p2 struct {
			Events    []map[string]any `json:"events"`
			NextAfter int64            `json:"next_after"`
		}
		if err := json.Unmarshal(recWait.Body.Bytes(), &p2); err != nil {
			t.Fatal(err)
		}
		if len(p2.Events) != 1 || p2.NextAfter != 2 {
			t.Fatalf("event hasil wait tidak sesuai: %+v", p2)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("long-poll control tidak bangun setelah event baru masuk")
	}
}
