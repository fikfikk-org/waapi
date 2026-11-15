package control

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

func setupControlV2Test(t *testing.T, webhookSrvURL string) (
	context.Context,
	*store.Store,
	*journal.Journal,
	*webhook.DeadStore,
	*Server,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()

	st, err := store.Open(ctx, cfg.AppDBPath())
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	j, err := journal.Open(ctx, cfg.JournalDBPath(), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: time.Minute,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	tc := webhook.NewTargetCache()
	_ = tc.Load(ctx, st)
	dead := webhook.NewDeadStore(j.DB())
	whSvc := webhook.NewService(j, tc, dead, &http.Client{Timeout: 5 * time.Second}, nil, webhook.Options{Store: st})
	whSvc.Start(ctx, nil)
	t.Cleanup(whSvc.Stop)

	waMgr, err := wa.NewManager(ctx, cfg, st, nil,
		wa.WithJournal(j),
		wa.WithTargetCache(tc),
		wa.WithWebhookService(whSvc),
	)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })

	if _, err := waMgr.Create(ctx, "sales", 20, false); err != nil {
		t.Fatalf("waMgr.Create gagal: %v", err)
	}
	if webhookSrvURL != "" {
		events := "*"
		if _, err := waMgr.Update(ctx, "sales", store.InstanceUpdate{
			WebhookURL:    &webhookSrvURL,
			WebhookEvents: &events,
		}); err != nil {
			t.Fatalf("waMgr.Update gagal: %v", err)
		}
	}

	srv := NewServer(cfg, st, nil, waMgr, nil, WithJournal(j), WithWebhookService(whSvc, dead))
	return ctx, st, j, dead, srv
}

// TestControlEventsStreamSSE memverifikasi streaming SSE pada control socket termasuk resume via Last-Event-ID.
func TestControlEventsStreamSSE(t *testing.T) {
	ctx, _, j, _, srv := setupControlV2Test(t, "")

	r1, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "62811@s.whatsapp.net",
		WAID:       "SSE1",
		TS:         1794360100,
		Payload:    []byte(`{"id":"SSE1","text":"satu"}`),
	})
	if err != nil {
		t.Fatalf("Append seq1 gagal: %v", err)
	}
	seq1 := r1.Seq

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Buka stream dari after=0 dan baca event pertama.
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, ts.URL+"/control/instances/sales/events/stream?after=0", nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("GET stream gagal: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status 200 diharapkan, didapat %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type harus text/event-stream, didapat %q", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(lines) > 0 {
				break
			}
			continue
		}
		lines = append(lines, line)
	}
	frame1 := strings.Join(lines, "\n")
	if !strings.Contains(frame1, "id: 1") || !strings.Contains(frame1, `"id":"1"`) || !strings.Contains(frame1, "SSE1") {
		t.Fatalf("frame SSE pertama tidak sesuai: %q", frame1)
	}
	_ = resp.Body.Close()

	// Tambahkan event kedua lalu sambung ulang memakai header Last-Event-ID.
	if _, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "receipt",
		Chat:       "62811@s.whatsapp.net",
		WAID:       "SSE2",
		TS:         1794360105,
		Payload:    []byte(`{"id":"SSE2"}`),
	}); err != nil {
		t.Fatalf("Append seq2 gagal: %v", err)
	}

	req2, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, ts.URL+"/control/instances/sales/events/stream", nil)
	req2.Header.Set("Last-Event-ID", "1")
	resp2, err := ts.Client().Do(req2)
	if err != nil {
		t.Fatalf("GET stream resume gagal: %v", err)
	}
	defer resp2.Body.Close()

	scanner2 := bufio.NewScanner(resp2.Body)
	lines2 := make([]string, 0, 4)
	for scanner2.Scan() {
		line := scanner2.Text()
		if line == "" {
			if len(lines2) > 0 {
				break
			}
			continue
		}
		lines2 = append(lines2, line)
	}
	frame2 := strings.Join(lines2, "\n")
	if seq1 != 1 || !strings.Contains(frame2, "id: 2") || !strings.Contains(frame2, "event: receipt") {
		t.Fatalf("frame SSE resume tidak sesuai: %q", frame2)
	}
}

// TestControlWebhookResumeDLQDanDoctor menguji endpoint resume, DLQ ls/replay/purge, webhook_from, dan doctor --deep webhook.
func TestControlWebhookResumeDLQDanDoctor(t *testing.T) {
	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer targetSrv.Close()

	ctx, st, j, dead, srv := setupControlV2Test(t, "")
	h := srv.Handler()

	// Sisipkan 2 event ke journal sebelum webhook dipasang agar bisa diuji replay retained & DLQ.
	r1, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales", Type: "message", Chat: "62811@s.whatsapp.net", WAID: "M1", TS: 1794360100,
		Payload: []byte(`{"id":"M1","text":"satu"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	seq1 := r1.Seq
	r2, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales", Type: "message", Chat: "62812@s.whatsapp.net", WAID: "M2", TS: 1794360101,
		Payload: []byte(`{"id":"M2","text":"dua"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	seq2 := r2.Seq

	// Validasi nilai webhook_from yang tidak valid -> 400.
	reqBad := httptest.NewRequest(http.MethodPatch, "/control/instances/sales", strings.NewReader(`{"webhook_from":"invalid"}`))
	recBad := httptest.NewRecorder()
	h.ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("webhook_from tidak valid harus 400, didapat %d", recBad.Code)
	}

	// Pasang webhook dengan webhook_from=retained -> cursor tetap di 0 agar retained event ikut terkirim.
	patchBody, _ := json.Marshal(map[string]any{
		"webhook_url":    targetSrv.URL,
		"webhook_events": "*",
		"webhook_from":   "retained",
	})
	reqSet := httptest.NewRequest(http.MethodPatch, "/control/instances/sales", bytes.NewReader(patchBody))
	recSet := httptest.NewRecorder()
	h.ServeHTTP(recSet, reqSet)
	if recSet.Code != http.StatusOK {
		t.Fatalf("PATCH webhook_from=retained harus 200, didapat %d: %s", recSet.Code, recSet.Body.String())
	}

	// Set webhook_paused="gone" di app.db lalu panggil /webhook/resume -> harus terhapus.
	cur, err := st.GetInstance(ctx, "sales")
	if err != nil {
		t.Fatal(err)
	}
	pausedSettings := cur.Settings
	pausedSettings.WebhookPaused = "gone"
	if _, err := st.UpdateInstance(ctx, "sales", store.InstanceUpdate{Settings: &pausedSettings}); err != nil {
		t.Fatal(err)
	}
	reqResume := httptest.NewRequest(http.MethodPost, "/control/instances/sales/webhook/resume", nil)
	recResume := httptest.NewRecorder()
	h.ServeHTTP(recResume, reqResume)
	if recResume.Code != http.StatusOK {
		t.Fatalf("POST /webhook/resume harus 200, didapat %d: %s", recResume.Code, recResume.Body.String())
	}
	afterResume, _ := st.GetInstance(ctx, "sales")
	if afterResume.Settings.WebhookPaused != "" {
		t.Fatalf("webhook_paused harus kosong setelah resume, didapat %q", afterResume.Settings.WebhookPaused)
	}

	// Isi 2 entri ke DLQ lalu uji GET, replay 1 entri, dan DELETE purge sisanya.
	_ = dead.Record(ctx, webhook.DeadEntry{InstanceID: "sales", Seq: seq1, Attempts: 3, LastStatus: 404, LastError: "HTTP 404"})
	_ = dead.Record(ctx, webhook.DeadEntry{InstanceID: "sales", Seq: seq2, Attempts: 3, LastStatus: 500, LastError: "HTTP 500"})

	reqList := httptest.NewRequest(http.MethodGet, "/control/instances/sales/webhook/dlq", nil)
	recList := httptest.NewRecorder()
	h.ServeHTTP(recList, reqList)
	if recList.Code != http.StatusOK {
		t.Fatalf("GET /webhook/dlq harus 200, didapat %d", recList.Code)
	}
	var listed []webhook.DeadEntry
	if err := json.Unmarshal(recList.Body.Bytes(), &listed); err != nil || len(listed) != 2 {
		t.Fatalf("daftar DLQ harus 2 entri, didapat %d (err=%v)", len(listed), err)
	}

	// Uji /control/doctor?deep=1 menyertakan bagian webhook sebelum DLQ dibersihkan.
	reqDoc := httptest.NewRequest(http.MethodGet, "/control/doctor?deep=1", nil)
	recDoc := httptest.NewRecorder()
	h.ServeHTTP(recDoc, reqDoc)
	if recDoc.Code != http.StatusOK {
		t.Fatalf("GET /control/doctor?deep=1 harus 200, didapat %d", recDoc.Code)
	}
	var rep obs.DeepReport
	if err := json.Unmarshal(recDoc.Body.Bytes(), &rep); err != nil {
		t.Fatalf("decode DeepReport gagal: %v", err)
	}
	if len(rep.Webhook) != 1 || rep.Webhook[0].InstanceID != "sales" || rep.Webhook[0].DLQ != 2 {
		t.Fatalf("laporan webhook pada doctor --deep tidak sesuai: %+v", rep.Webhook)
	}

	// Replay seq1 saja -> seq1 hilang, seq2 tetap.
	reqReplay := httptest.NewRequest(http.MethodPost, "/control/instances/sales/webhook/dlq/replay", strings.NewReader(`{"seq":1}`))
	recReplay := httptest.NewRecorder()
	h.ServeHTTP(recReplay, reqReplay)
	if recReplay.Code != http.StatusOK {
		t.Fatalf("POST /webhook/dlq/replay harus 200, didapat %d: %s", recReplay.Code, recReplay.Body.String())
	}
	var repRes webhook.ReplayResult
	if err := json.Unmarshal(recReplay.Body.Bytes(), &repRes); err != nil || repRes.Replayed != 1 {
		t.Fatalf("hasil replay harus 1, didapat %+v (err=%v)", repRes, err)
	}

	// Purge sisa DLQ -> purged=1.
	reqPurge := httptest.NewRequest(http.MethodDelete, "/control/instances/sales/webhook/dlq", nil)
	recPurge := httptest.NewRecorder()
	h.ServeHTTP(recPurge, reqPurge)
	if recPurge.Code != http.StatusOK {
		t.Fatalf("DELETE /webhook/dlq harus 200, didapat %d", recPurge.Code)
	}
	var purgeRes struct {
		Purged int64 `json:"purged"`
	}
	if err := json.Unmarshal(recPurge.Body.Bytes(), &purgeRes); err != nil || purgeRes.Purged != 1 {
		t.Fatalf("hasil purge harus 1, didapat %+v (err=%v)", purgeRes, err)
	}
}
