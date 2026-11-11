package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/httpapi"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

// TestEndToEndControlAndPublicAPI menguji alur pembuatan key, instance, edit, dan autentikasi API publik.
func TestEndToEndControlAndPublicAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	cfg := &config.Config{
		DataDir:          tmpDir,
		HTTPAddr:         "127.0.0.1:0",
		MemoryLimitBytes: 80 << 20,
		MaxConns:         16,
		MediaMaxBytes:    16 << 20,
		DocumentMaxBytes: 64 << 20,
		WebhookWorkers:   2,
	}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs gagal: %v", err)
	}

	st, err := store.Open(ctx, filepath.Join(tmpDir, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatalf("auth.NewManager gagal: %v", err)
	}

	wh := webhook.NewDispatcher(st, func(ctx context.Context, instanceID string) (string, string, string, error) {
		inst, err := st.GetInstance(ctx, instanceID)
		if err != nil {
			return "", "", "", err
		}
		return inst.WebhookURL, inst.WebhookSecret, inst.WebhookEvents, nil
	}, 2)
	wh.Start(ctx)
	defer func() {
		cancel()
		wh.Stop()
	}()

	waMgr, err := wa.NewManager(ctx, cfg, st, wh)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	defer waMgr.Close()

	// Buat instance via manager (setara waapi add sales)
	instStatus, err := waMgr.Create(ctx, "sales", 25, true)
	if err != nil {
		t.Fatalf("waMgr.Create gagal: %v", err)
	}
	if instStatus.ID != "sales" || instStatus.RatePerMin != 25 {
		t.Fatalf("status instance tidak sesuai: %+v", instStatus)
	}

	// Edit instance (setara waapi edit sales --rate 40)
	newRate := 40
	updated, err := waMgr.Update(ctx, "sales", store.InstanceUpdate{RatePerMin: &newRate})
	if err != nil || updated.RatePerMin != 40 {
		t.Fatalf("waMgr.Update gagal: %+v, err=%v", updated, err)
	}

	// Buat API key ter-scope ke instance sales
	rawKey, keyMeta, err := authMgr.Create(ctx, "backend-sales", "sales")
	if err != nil {
		t.Fatalf("authMgr.Create gagal: %v", err)
	}

	pubHandler := httpapi.NewHandler(authMgr, waMgr)

	// 1. Request tanpa X-API-Key harus ditolak 401
	req1 := httptest.NewRequest(http.MethodGet, "/v1/instances/sales", nil)
	rec1 := httptest.NewRecorder()
	pubHandler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Fatalf("diharapkan 401 tanpa key, didapat %d", rec1.Code)
	}

	// 2. Request dengan X-API-Key valid harus mengembalikan 200
	req2 := httptest.NewRequest(http.MethodGet, "/v1/instances/sales", nil)
	req2.Header.Set("X-API-Key", rawKey)
	rec2 := httptest.NewRecorder()
	pubHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("diharapkan 200 dengan key valid, didapat %d: %s", rec2.Code, rec2.Body.String())
	}

	// 3. Request ke instance lain di luar scope harus ditolak 403
	_, _ = waMgr.Create(ctx, "support", 20, true)
	req3 := httptest.NewRequest(http.MethodGet, "/v1/instances/support", nil)
	req3.Header.Set("X-API-Key", rawKey)
	rec3 := httptest.NewRecorder()
	pubHandler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusForbidden {
		t.Fatalf("diharapkan 403 untuk instance di luar scope, didapat %d", rec3.Code)
	}

	// 4. Revoke key -> request berikutnya harus langsung 401
	if err := authMgr.Revoke(ctx, keyMeta.ID); err != nil {
		t.Fatalf("authMgr.Revoke gagal: %v", err)
	}
	req4 := httptest.NewRequest(http.MethodGet, "/v1/instances/sales", nil)
	req4.Header.Set("X-API-Key", rawKey)
	rec4 := httptest.NewRecorder()
	pubHandler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusUnauthorized {
		t.Fatalf("diharapkan 401 setelah key dicabut, didapat %d", rec4.Code)
	}

	// 5. Pastikan endpoint control doctor berjalan
	ctrlSrv := control.NewServer(cfg, st, authMgr, waMgr, wh)
	req5 := httptest.NewRequest(http.MethodGet, "/control/doctor", nil)
	rec5 := httptest.NewRecorder()
	ctrlSrv.Handler().ServeHTTP(rec5, req5)
	if rec5.Code != http.StatusOK {
		t.Fatalf("diharapkan 200 dari /control/doctor, didapat %d", rec5.Code)
	}
}
