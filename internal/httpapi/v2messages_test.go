package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type apiFakeExecutor struct {
	mu      sync.Mutex
	ready   map[string]bool
	paired  map[string]bool
	nextSeq int
	execFn  func(ctx context.Context, job *outbox.Job) (outbox.ExecResult, error)
}

func (f *apiFakeExecutor) Ready(instanceID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready[instanceID]
}

func (f *apiFakeExecutor) NewMessageID(instanceID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.paired[instanceID] {
		return "", outbox.ErrNotPaired
	}
	f.nextSeq++
	return fmt.Sprintf("3EB0API%06d", f.nextSeq), nil
}

func (f *apiFakeExecutor) Execute(ctx context.Context, job *outbox.Job) (outbox.ExecResult, error) {
	f.mu.Lock()
	fn := f.execFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, job)
	}
	return outbox.ExecResult{ServerTS: time.Now()}, nil
}

func setupV2MessagesServer(t *testing.T, exec *apiFakeExecutor, opts ...ServerOption) (http.Handler, *outbox.Service, string) {
	t.Helper()
	ctx := context.Background()
	tmp := t.TempDir()

	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatalf("auth.NewManager gagal: %v", err)
	}
	waMgr, err := wa.NewManager(ctx, cfg, st, nil)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })

	if _, err := waMgr.Create(ctx, "sales", 60, false); err != nil {
		t.Fatalf("waMgr.Create gagal: %v", err)
	}
	if _, err := waMgr.Create(ctx, "unpaired", 60, false); err != nil {
		t.Fatalf("waMgr.Create unpaired gagal: %v", err)
	}

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns:    2,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	outSvc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{
		DisableSendJitter: true,
	})
	t.Cleanup(outSvc.Stop)

	key, _, err := authMgr.Create(ctx, "api-client", "")
	if err != nil {
		t.Fatalf("authMgr.Create gagal: %v", err)
	}

	allOpts := append([]ServerOption{
		WithDB(st.DB),
		WithJournal(j),
		WithGovernor(governor.AlwaysGreen),
		WithOutbox(outSvc),
	}, opts...)
	return NewHandler(authMgr, waMgr, allOpts...), outSvc, key
}

// TestV2MessagesAPI_Kontrak memverifikasi 202, replay 200, 422 idempotency mismatch, 400 invalid_spec, GET, DELETE, dan wait=sent.
func TestV2MessagesAPI_Kontrak(t *testing.T) {
	exec := &apiFakeExecutor{
		ready:  map[string]bool{"sales": false},
		paired: map[string]bool{"sales": true},
	}
	h, outSvc, key := setupV2MessagesServer(t, exec)
	srv := httptest.NewServer(h)
	defer srv.Close()

	doReq := func(method, path string, headers map[string]string, body any) (*http.Response, []byte) {
		t.Helper()
		var buf bytes.Buffer
		if body != nil {
			_ = json.NewEncoder(&buf).Encode(body)
		}
		req, err := http.NewRequest(method, srv.URL+path, &buf)
		if err != nil {
			t.Fatalf("NewRequest gagal: %v", err)
		}
		req.Header.Set("X-API-Key", key)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("HTTP %s %s gagal: %v", method, path, err)
		}
		defer resp.Body.Close()
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		return resp, raw.Bytes()
	}

	payload := map[string]any{
		"to":     "6289876543210",
		"type":   "text",
		"text":   "Pesanan #1024 sudah dikirim.",
		"typing": true,
	}

	// 1. 202 Accepted dengan field lengkap.
	resp, raw := doReq(http.MethodPost, "/v2/instances/sales/messages", map[string]string{
		"Idempotency-Key": "order-1024-shipped",
	}, payload)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("diharapkan 202, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	var accepted struct {
		ID          string `json:"id"`
		WAMessageID string `json:"wa_message_id"`
		Status      string `json:"status"`
		To          string `json:"to"`
		NotBefore   int64  `json:"not_before"`
		StatusURL   string `json:"status_url"`
	}
	if err := json.Unmarshal(raw, &accepted); err != nil {
		t.Fatalf("decode 202 gagal: %v", err)
	}
	if accepted.ID == "" || accepted.WAMessageID == "" || accepted.Status != "queued" ||
		accepted.To != "6289876543210@s.whatsapp.net" || accepted.NotBefore <= 0 ||
		accepted.StatusURL != "/v2/instances/sales/messages/"+accepted.ID {
		t.Fatalf("payload 202 tidak lengkap/sesuai: %+v", accepted)
	}

	// 2. 200 OK untuk replay idempoten identik.
	resp, raw = doReq(http.MethodPost, "/v2/instances/sales/messages", map[string]string{
		"Idempotency-Key": "order-1024-shipped",
	}, payload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay idempoten harus 200, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	var replayed struct {
		ID          string `json:"id"`
		WAMessageID string `json:"wa_message_id"`
	}
	_ = json.Unmarshal(raw, &replayed)
	if replayed.ID != accepted.ID || replayed.WAMessageID != accepted.WAMessageID {
		t.Fatalf("replay mengembalikan ID berbeda: %+v != %+v", replayed, accepted)
	}

	// 3. 422 idempotency_key_reused jika payload berbeda.
	payloadDiff := map[string]any{
		"to":   "6289876543210",
		"type": "text",
		"text": "Pesanan dibatalkan",
	}
	resp, raw = doReq(http.MethodPost, "/v2/instances/sales/messages", map[string]string{
		"Idempotency-Key": "order-1024-shipped",
	}, payloadDiff)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("beda payload harus 422, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	var errEnv ErrorBody
	_ = json.Unmarshal(raw, &errEnv)
	if errEnv.Error.Code != "idempotency_key_reused" {
		t.Fatalf("kode error 422 tidak sesuai: %+v", errEnv)
	}

	// 4. 400 invalid_spec untuk tipe non-text di Batch 5.
	resp, raw = doReq(http.MethodPost, "/v2/instances/sales/messages", nil, map[string]any{
		"to":   "6289876543210",
		"type": "image",
		"text": "belum aktif",
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("tipe selain text harus 400, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	_ = json.Unmarshal(raw, &errEnv)
	if errEnv.Error.Code != "invalid_spec" {
		t.Fatalf("kode error tipe non-text harus invalid_spec, dapat %q", errEnv.Error.Code)
	}

	// 5. GET status pesan.
	resp, raw = doReq(http.MethodGet, accepted.StatusURL, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status pesan harus 200, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	var detail map[string]any
	_ = json.Unmarshal(raw, &detail)
	for _, field := range []string{"id", "wa_message_id", "instance", "to", "type", "status", "origin", "attempts", "created_at", "receipts", "error"} {
		if _, ok := detail[field]; !ok {
			t.Fatalf("field %q wajib ada di GET /v2/.../messages/{id}: %s", field, string(raw))
		}
	}

	// 6. DELETE membatalkan job queued (200), lalu DELETE kedua gagal (409 message_not_cancellable).
	resp, raw = doReq(http.MethodDelete, accepted.StatusURL, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE pertama harus 200, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	resp, raw = doReq(http.MethodDelete, accepted.StatusURL, nil, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("DELETE kedua harus 409, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	_ = json.Unmarshal(raw, &errEnv)
	if errEnv.Error.Code != "message_not_cancellable" {
		t.Fatalf("kode error DELETE ulang harus message_not_cancellable, dapat %q", errEnv.Error.Code)
	}

	// 7. Instance belum di-pairing -> 409 instance_not_paired.
	resp, raw = doReq(http.MethodPost, "/v2/instances/unpaired/messages", nil, map[string]any{
		"to":   "6289876543210",
		"type": "text",
		"text": "uji unpaired",
	})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("unpaired harus 409, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	_ = json.Unmarshal(raw, &errEnv)
	if errEnv.Error.Code != "instance_not_paired" {
		t.Fatalf("kode error unpaired harus instance_not_paired, dapat %q", errEnv.Error.Code)
	}

	// 8. wait=sent mengembalikan 200 saat scheduler aktif menyelesaikan pengiriman.
	exec.mu.Lock()
	exec.ready["sales"] = true
	exec.mu.Unlock()
	schedCtx, cancelSched := context.WithCancel(context.Background())
	defer cancelSched()
	outSvc.StartScheduler(schedCtx, "sales")
	outSvc.NotifyReady("sales")

	resp, raw = doReq(http.MethodPost, "/v2/instances/sales/messages", nil, map[string]any{
		"to":   "6289876543210",
		"type": "text",
		"text": "tunggu sent",
		"wait": "sent",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("wait=sent harus mengembalikan 200, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	_ = json.Unmarshal(raw, &detail)
	if detail["status"] != "sent" {
		t.Fatalf("status setelah wait=sent harus sent, dapat %v", detail["status"])
	}
}

// TestV1TextCompat memverifikasi endpoint v1 /messages/text kompatibel dengan respons 200, 503 saat belum Ready, dan 504 + details saat wait habis.
func TestV1TextCompat(t *testing.T) {
	gate := make(chan struct{})
	exec := &apiFakeExecutor{
		ready:  map[string]bool{"sales": false},
		paired: map[string]bool{"sales": true},
	}
	h, outSvc, key := setupV2MessagesServer(t, exec, withV1WaitTimeout(50*time.Millisecond))
	srv := httptest.NewServer(h)
	defer srv.Close()

	postText := func(text string) (*http.Response, []byte) {
		t.Helper()
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(map[string]any{
			"to":   "6289876543210",
			"text": text,
		})
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/instances/sales/messages/text", &buf)
		req.Header.Set("X-API-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("POST v1 text gagal: %v", err)
		}
		defer resp.Body.Close()
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		return resp, raw.Bytes()
	}

	// 1. Belum Ready -> 503 instance_not_connected.
	resp, raw := postText("belum siap")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("belum Ready harus 503, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	var errEnv ErrorBody
	_ = json.Unmarshal(raw, &errEnv)
	if errEnv.Error.Code != "instance_not_connected" {
		t.Fatalf("kode error belum Ready harus instance_not_connected, dapat %q", errEnv.Error.Code)
	}

	// 2. Ready dan cepat -> 200 {message_id, to, timestamp}.
	exec.mu.Lock()
	exec.ready["sales"] = true
	exec.mu.Unlock()
	schedCtx, cancelSched := context.WithCancel(context.Background())
	defer cancelSched()
	outSvc.StartScheduler(schedCtx, "sales")
	outSvc.NotifyReady("sales")

	resp, raw = postText("halo v1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("kirim v1 sukses harus 200, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	var v1Res wa.SendResult
	if err := json.Unmarshal(raw, &v1Res); err != nil {
		t.Fatalf("decode SendResult gagal: %v", err)
	}
	if v1Res.MessageID == "" || v1Res.To != "6289876543210@s.whatsapp.net" || v1Res.Timestamp <= 0 {
		t.Fatalf("bentuk respons SendResult v1 tidak sesuai: %+v", v1Res)
	}

	// 3. Wait habis -> 504 send_timeout dengan details.id dan details.status_url.
	exec.mu.Lock()
	exec.execFn = func(ctx context.Context, _ *outbox.Job) (outbox.ExecResult, error) {
		select {
		case <-gate:
			return outbox.ExecResult{ServerTS: time.Now()}, nil
		case <-ctx.Done():
			return outbox.ExecResult{}, ctx.Err()
		}
	}
	exec.mu.Unlock()

	resp, raw = postText("lambat")
	close(gate)
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("wait timeout harus 504, dapat %d body=%s", resp.StatusCode, string(raw))
	}
	var timeoutBody struct {
		Error struct {
			Code    string         `json:"code"`
			Details map[string]any `json:"details"`
		} `json:"error"`
	}
	_ = json.Unmarshal(raw, &timeoutBody)
	if timeoutBody.Error.Code != "send_timeout" {
		t.Fatalf("kode error timeout harus send_timeout, dapat %q", timeoutBody.Error.Code)
	}
	if id, _ := timeoutBody.Error.Details["id"].(string); id == "" {
		t.Fatalf("details.id wajib diisi pada 504 send_timeout: %s", string(raw))
	}
	if su, _ := timeoutBody.Error.Details["status_url"].(string); su == "" {
		t.Fatalf("details.status_url wajib diisi pada 504 send_timeout: %s", string(raw))
	}
}
