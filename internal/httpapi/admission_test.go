package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type atomicGovernor struct {
	lvl atomic.Int32
}

func (g *atomicGovernor) Set(l governor.Level) {
	g.lvl.Store(int32(l))
}

func (g *atomicGovernor) Level() governor.Level {
	return governor.Level(g.lvl.Load())
}

// TestAdmission_Red503 memverifikasi endpoint berat ditolak 503 memory_pressure saat level Red sedangkan endpoint ringan tetap lolos.
func TestAdmission_Red503(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	apiKey, _, err := authMgr.Create(ctx, "adm", "")
	if err != nil {
		t.Fatal(err)
	}

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns:    2,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j))
	if err != nil {
		t.Fatal(err)
	}
	defer waMgr.Close()

	if _, err := waMgr.Create(ctx, "sales", 60, false); err != nil {
		t.Fatal(err)
	}

	uploads := media.NewUploadStore(j.DB())
	pipe := media.NewPipeline(media.Limits{TmpDir: cfg.TmpDir()})

	exec := &apiFakeExecutor{
		paired: map[string]bool{"sales": true},
		ready:  map[string]bool{"sales": false},
	}
	outboxSvc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{})
	gov := &atomicGovernor{}
	gov.Set(governor.Red)

	srv := NewServer(
		authMgr,
		waMgr,
		WithDB(st.DB),
		WithJournal(j),
		WithOutbox(outboxSvc),
		WithMedia(pipe, uploads),
		WithGovernor(gov),
	)

	doReq := func(method, target, body string) *httptest.ResponseRecorder {
		var rBody *strings.Reader
		if body != "" {
			rBody = strings.NewReader(body)
		} else {
			rBody = strings.NewReader("")
		}
		req := httptest.NewRequest(method, target, rBody)
		req.Header.Set("X-API-Key", apiKey)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec
	}

	assertRed503 := func(name, method, target, body string) {
		t.Helper()
		rec := doReq(method, target, body)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s saat Red diharapkan 503, didapat %d (%s)", name, rec.Code, rec.Body.String())
		}
		if ra := rec.Header().Get("Retry-After"); ra != "15" {
			t.Fatalf("%s saat Red harus menyertakan Retry-After: 15, didapat %q", name, ra)
		}
		var eb ErrorBody
		if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil || eb.Error.Code != "memory_pressure" {
			t.Fatalf("%s saat Red harus berkode memory_pressure, didapat %s", name, rec.Body.String())
		}
	}

	// 1. Seluruh endpoint berat wajib ditolak 503 memory_pressure saat Red
	assertRed503("v1 media", http.MethodPost, "/v1/instances/sales/messages/media", "")
	assertRed503("v1 media-url", http.MethodPost, "/v1/instances/sales/messages/media-url", `{"to":"6281234","type":"image","url":"https://example.com/a.jpg"}`)
	assertRed503("v1 download", http.MethodPost, "/v1/instances/sales/media/download", `{"direct_path":"/v/1","media_key":"AAAA"}`)
	assertRed503("v2 upload", http.MethodPost, "/v2/instances/sales/media?kind=image", "")
	assertRed503("v2 download", http.MethodPost, "/v2/instances/sales/media/download", `{"direct_path":"/v/1","media_key":"AAAA"}`)
	assertRed503("v2 event media", http.MethodGet, "/v2/instances/sales/events/1/media", "")
	assertRed503("v2 media.url", http.MethodPost, "/v2/instances/sales/messages", `{"to":"628123456","type":"image","media":{"url":"https://example.com/a.jpg"}}`)
	assertRed503("start pair", http.MethodPost, "/v1/instances/sales/pair", `{}`)
	assertRed503("sse stream", http.MethodGet, "/v2/instances/sales/events/stream", "")

	// /readyz -> 503 not_ready
	recReady := httptest.NewRecorder()
	srv.ServeHTTP(recReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recReady.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz saat Red diharapkan 503, didapat %d", recReady.Code)
	}

	// 2. Endpoint ringan + pesan teks + media dengan upload_id TIDAK boleh diblokir memory_pressure
	for _, tc := range []struct {
		name   string
		method string
		target string
		body   string
	}{
		{name: "healthz", method: http.MethodGet, target: "/healthz"},
		{name: "list instances", method: http.MethodGet, target: "/v1/instances"},
		{name: "get events", method: http.MethodGet, target: "/v2/instances/sales/events"},
		{name: "text message", method: http.MethodPost, target: "/v2/instances/sales/messages", body: `{"to":"628123456","type":"text","text":"halo"}`},
		{name: "upload_id message", method: http.MethodPost, target: "/v2/instances/sales/messages", body: `{"to":"628123456","type":"image","media":{"upload_id":"up_missing"}}`},
	} {
		rec := doReq(tc.method, tc.target, tc.body)
		if rec.Code == http.StatusServiceUnavailable {
			var eb ErrorBody
			_ = json.Unmarshal(rec.Body.Bytes(), &eb)
			if eb.Error.Code == "memory_pressure" {
				t.Fatalf("endpoint ringan %s tidak boleh ditolak 503 memory_pressure saat Red (%s)", tc.name, rec.Body.String())
			}
		}
	}

	// Pastikan text message benar-benar lolos 202
	recText := doReq(http.MethodPost, "/v2/instances/sales/messages", `{"to":"628123456","type":"text","text":"aman"}`)
	if recText.Code != http.StatusAccepted {
		t.Fatalf("kirim pesan teks saat Red diharapkan 202, didapat %d (%s)", recText.Code, recText.Body.String())
	}

	// 3. Saat turun ke Yellow, admission kembali terbuka dan /readyz 200
	gov.Set(governor.Yellow)
	recReadyYellow := httptest.NewRecorder()
	srv.ServeHTTP(recReadyYellow, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recReadyYellow.Code != http.StatusOK {
		t.Fatalf("/readyz saat Yellow diharapkan 200, didapat %d", recReadyYellow.Code)
	}

	// Kirim media.url saat Yellow lolos admission -> masuk ke outbox (202)
	recYellowURL := doReq(http.MethodPost, "/v2/instances/sales/messages", `{"to":"628123456","type":"image","media":{"url":"https://example.com/a.jpg"}}`)
	if recYellowURL.Code != http.StatusAccepted {
		t.Fatalf("enqueue media.url saat Yellow diharapkan 202, didapat %d (%s)", recYellowURL.Code, recYellowURL.Body.String())
	}
	_ = bytes.TrimSpace(recYellowURL.Body.Bytes())
}
