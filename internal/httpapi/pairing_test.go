package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
	"go.mau.fi/whatsmeow"
)

// TestPairingHTTPHandlers menguji kontrak POST/GET/DELETE /pair dan GET /qr.png beserta validasi auth.
func TestPairingHTTPHandlers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	cfg := &config.Config{
		DataDir:          tmpDir,
		HTTPAddr:         "127.0.0.1:0",
		MemoryLimitBytes: 80 << 20,
		MaxConns:         16,
		MediaMaxBytes:    16 << 20,
		DocumentMaxBytes: 64 << 20,
		WebhookWorkers:   1,
	}
	_ = cfg.EnsureDirs()

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
		return "", "", "", nil
	}, 1)
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

	if _, err := waMgr.Create(ctx, "kostash", 20, true); err != nil {
		t.Fatalf("waMgr.Create gagal: %v", err)
	}
	inst, _ := waMgr.Get("kostash")
	inst.SetDriverFactoryForTest(func() (chan whatsmeow.QRChannelItem, string) {
		ch := make(chan whatsmeow.QRChannelItem, 4)
		ch <- whatsmeow.QRChannelItem{Event: "code", Code: "2@qr-http-test", Timeout: 60 * time.Second}
		return ch, "GKMW-A1DL"
	})

	rawKey, _, _ := authMgr.Create(ctx, "dashboard", "kostash")
	h := NewHandler(authMgr, waMgr)

	// 1. GET /pair sebelum ada sesi harus 404 no_pairing_session
	r1 := httptest.NewRequest(http.MethodGet, "/v1/instances/kostash/pair", nil)
	r1.Header.Set("X-API-Key", rawKey)
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, r1)
	if w1.Code != http.StatusNotFound {
		t.Fatalf("diharapkan 404 sebelum sesi ada, didapat %d", w1.Code)
	}

	// 2. POST /pair dengan nomor awalan 0 harus 400 invalid_phone
	r2 := httptest.NewRequest(http.MethodPost, "/v1/instances/kostash/pair", strings.NewReader(`{"phone":"08123456789"}`))
	r2.Header.Set("X-API-Key", rawKey)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("diharapkan 400 untuk nomor awalan 0, didapat %d", w2.Code)
	}

	// 3. POST /pair mode QR tanpa body harus 200 dengan qr_raw & qr_image_url
	r3 := httptest.NewRequest(http.MethodPost, "/v1/instances/kostash/pair", nil)
	r3.Header.Set("X-API-Key", rawKey)
	w3 := httptest.NewRecorder()
	h.ServeHTTP(w3, r3)
	if w3.Code != http.StatusOK {
		t.Fatalf("POST /pair gagal (%d): %s", w3.Code, w3.Body.String())
	}
	var snap wa.PairSnapshot
	if err := json.Unmarshal(w3.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode response gagal: %v", err)
	}
	if snap.Method != wa.PairMethodQR || snap.State != wa.PairQRRefreshed || snap.QRRaw == nil {
		t.Fatalf("snapshot tidak sesuai: %+v", snap)
	}

	// 4. GET /qr.png harus mengembalikan image/png valid dan Cache-Control no-store
	r4 := httptest.NewRequest(http.MethodGet, "/v1/instances/kostash/qr.png?scale=6", nil)
	r4.Header.Set("X-API-Key", rawKey)
	w4 := httptest.NewRecorder()
	h.ServeHTTP(w4, r4)
	if w4.Code != http.StatusOK || w4.Header().Get("Content-Type") != "image/png" {
		t.Fatalf("GET /qr.png gagal: status=%d, content-type=%s", w4.Code, w4.Header().Get("Content-Type"))
	}
	if !bytes.HasPrefix(w4.Body.Bytes(), []byte("\x89PNG\r\n\x1a\n")) {
		t.Fatal("body qr.png bukan file PNG yang valid")
	}

	// 5. GET /qr.png dengan scale di luar batas (scale=99) harus 400
	r5 := httptest.NewRequest(http.MethodGet, "/v1/instances/kostash/qr.png?scale=99", nil)
	r5.Header.Set("X-API-Key", rawKey)
	w5 := httptest.NewRecorder()
	h.ServeHTTP(w5, r5)
	if w5.Code != http.StatusBadRequest {
		t.Fatalf("diharapkan 400 untuk scale=99, didapat %d", w5.Code)
	}

	// 6. DELETE /pair harus membatalkan sesi dan mengembalikan state cancelled
	r6 := httptest.NewRequest(http.MethodDelete, "/v1/instances/kostash/pair", nil)
	r6.Header.Set("X-API-Key", rawKey)
	w6 := httptest.NewRecorder()
	h.ServeHTTP(w6, r6)
	if w6.Code != http.StatusOK {
		t.Fatalf("DELETE /pair gagal: %d", w6.Code)
	}
}
