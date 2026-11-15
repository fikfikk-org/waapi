package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// TestEventsPullAPI menguji pagination after/limit, filter types, has_more, scope 403 instance lain, dan envelope v2.
func TestEventsPullAPI(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs gagal: %v", err)
	}

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: 60 * time.Second,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j))
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	defer waMgr.Close()

	if _, err := waMgr.Create(ctx, "sales", 20, false); err != nil {
		t.Fatalf("waMgr.Create gagal: %v", err)
	}
	if _, err := waMgr.Create(ctx, "other", 20, false); err != nil {
		t.Fatalf("waMgr.Create other gagal: %v", err)
	}

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatalf("auth.NewManager gagal: %v", err)
	}
	salesKey, _, err := authMgr.Create(ctx, "sales-key", "sales")
	if err != nil {
		t.Fatalf("authMgr.Create sales gagal: %v", err)
	}
	otherKey, _, err := authMgr.Create(ctx, "other-key", "other")
	if err != nil {
		t.Fatalf("authMgr.Create other gagal: %v", err)
	}

	// Isi 3 message + 1 receipt pada sales
	for i := 1; i <= 3; i++ {
		_, err := j.Append(ctx, &journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       "6281@s.whatsapp.net",
			WAID:       fmt.Sprintf("MSG-%d", i),
			TS:         1794360100 + int64(i),
			Payload:    []byte(fmt.Sprintf(`{"id":"MSG-%d","text":"halo"}`, i)),
		})
		if err != nil {
			t.Fatalf("Append message gagal: %v", err)
		}
	}
	_, err = j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "receipt",
		Chat:       "6281@s.whatsapp.net",
		TS:         1794360104,
		Payload:    []byte(`{"type":"read"}`),
	})
	if err != nil {
		t.Fatalf("Append receipt gagal: %v", err)
	}

	srv := NewHandler(authMgr, waMgr, WithJournal(j))

	// 1. Key untuk instance lain harus 403
	req403 := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/events", nil)
	req403.Header.Set("X-API-Key", otherKey)
	rec403 := httptest.NewRecorder()
	srv.ServeHTTP(rec403, req403)
	if rec403.Code != http.StatusForbidden {
		t.Fatalf("diharapkan 403 untuk key instance lain, didapat %d (%s)", rec403.Code, rec403.Body.String())
	}

	type pullResp struct {
		Events    []map[string]any `json:"events"`
		NextAfter int64            `json:"next_after"`
		HasMore   bool             `json:"has_more"`
	}

	// 2. Page 1: after=0&limit=2&types=message
	req1 := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/events?after=0&limit=2&types=message", nil)
	req1.Header.Set("X-API-Key", salesKey)
	rec1 := httptest.NewRecorder()
	srv.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("status page 1 diharapkan 200, didapat %d (%s)", rec1.Code, rec1.Body.String())
	}

	var p1 pullResp
	if err := json.Unmarshal(rec1.Body.Bytes(), &p1); err != nil {
		t.Fatalf("decode respons page 1 gagal: %v", err)
	}
	if len(p1.Events) != 2 || !p1.HasMore || p1.NextAfter != 2 {
		t.Fatalf("page 1 tidak sesuai: len=%d, has_more=%v, next_after=%d", len(p1.Events), p1.HasMore, p1.NextAfter)
	}
	if p1.Events[0]["v"] != float64(2) || p1.Events[0]["id"] != "1" || p1.Events[0]["event"] != "message" || p1.Events[0]["instance"] != "sales" {
		t.Fatalf("envelope v2 tidak sesuai: %+v", p1.Events[0])
	}

	// 3. Page 2: after=2&limit=2&types=message -> sisa 1 message, receipt (seq 4) dilewati namun next_after maju ke head=4 dan has_more=false
	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v2/instances/sales/events?after=%d&limit=2&types=message", p1.NextAfter), nil)
	req2.Header.Set("X-API-Key", salesKey)
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("status page 2 diharapkan 200, didapat %d (%s)", rec2.Code, rec2.Body.String())
	}

	var p2 pullResp
	if err := json.Unmarshal(rec2.Body.Bytes(), &p2); err != nil {
		t.Fatalf("decode respons page 2 gagal: %v", err)
	}
	if len(p2.Events) != 1 || p2.HasMore || p2.NextAfter != 4 {
		t.Fatalf("page 2 tidak sesuai: len=%d, has_more=%v, next_after=%d", len(p2.Events), p2.HasMore, p2.NextAfter)
	}
	if p2.Events[0]["id"] != "3" {
		t.Fatalf("event terakhir page 2 harus ber-id '3', didapat %v", p2.Events[0]["id"])
	}
}
