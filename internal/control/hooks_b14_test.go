package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/hooks"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// TestControlHooks_AddTrustedEnableDisableLogsDanTest memverifikasi manajemen hook via control plane, flag trusted=1, enable/disable, audit, logs, dan dry-run file.
func TestControlHooks_AddTrustedEnableDisableLogsDanTest(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, _ := store.Open(ctx, cfg.AppDBPath())
	defer st.Close()
	j, _ := journal.Open(ctx, cfg.JournalDBPath())
	defer j.Close()
	authMgr, _ := auth.NewManager(ctx, st)
	waMgr, _ := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j))
	defer waMgr.Close()
	_, _ = waMgr.Create(ctx, "sales", 60, false)

	auditLog := auth.NewAuditLog(st.DB)
	hooksEng := hooks.NewEngine(hooks.EngineDeps{AppStore: st, Journal: j})
	srv := NewServer(cfg, st, authMgr, waMgr, nil, WithJournal(j), WithAuditLog(auditLog), WithHooks(hooksEng))
	h := srv.Handler()

	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// 1. Control plane add rule dengan http://127.0.0.1 diizinkan dan menghasilkan trusted=true
	recAdd := call(http.MethodPost, "/control/instances/sales/hooks", `{
		"id":"local-cb","name":"Local Callback","on":["message"],
		"do":[{"action":"http","url":"http://127.0.0.1:9001/hook","timeout_ms":1000}]
	}`)
	if recAdd.Code != http.StatusCreated {
		t.Fatalf("POST /control/hooks gagal: %d %s", recAdd.Code, recAdd.Body.String())
	}
	saved, err := st.GetHook(ctx, "sales", "local-cb")
	if err != nil || !saved.Trusted || !saved.Enabled {
		t.Fatalf("hook dari control plane wajib trusted=true & enabled=true: %+v err=%v", saved, err)
	}

	// 2. Disable -> Enable
	if rec := call(http.MethodPost, "/control/instances/sales/hooks/local-cb/disable", ""); rec.Code != http.StatusOK {
		t.Fatalf("disable gagal: %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(http.MethodPost, "/control/instances/sales/hooks/local-cb/enable", ""); rec.Code != http.StatusOK {
		t.Fatalf("enable gagal: %d %s", rec.Code, rec.Body.String())
	}

	// 3. Dry-run ad-hoc dari CLI --file rule.json (trusted localhost diperbolehkan)
	recDry := call(http.MethodPost, "/control/instances/sales/hooks/test", `{
		"rule":{"id":"adhoc","on":["message"],"do":[{"action":"reply","text":"Hai {{push_name}}"}]},
		"event":{"event":"message","instance":"sales","data":{"chat":"628111@s.whatsapp.net","from":"628111@s.whatsapp.net","push_name":"Tono","type":"text","text":"tes"}}
	}`)
	if recDry.Code != http.StatusOK {
		t.Fatalf("POST /control/hooks/test gagal: %d %s", recDry.Code, recDry.Body.String())
	}
	var dry hooks.DryRunResult
	if err := json.Unmarshal(recDry.Body.Bytes(), &dry); err != nil || !dry.Matched || len(dry.Actions) != 1 || dry.Actions[0].Text != "Hai Tono" {
		t.Fatalf("hasil dry-run control plane tidak sesuai: %+v", dry)
	}

	// 4. Logs hook_runs
	_ = hooks.NewSQLiteRunStore(j.DB()).UpsertRun(ctx, hooks.RunRecord{
		InstanceID: "sales", Seq: 1, HookID: "local-cb", Status: "done", At: time.Now().UnixMilli(),
	})
	recLogs := call(http.MethodGet, "/control/instances/sales/hooks/logs?hook=local-cb&since=1h", "")
	if recLogs.Code != http.StatusOK || !bytes.Contains(recLogs.Body.Bytes(), []byte("local-cb")) {
		t.Fatalf("GET /control/hooks/logs gagal: %d %s", recLogs.Code, recLogs.Body.String())
	}

	// 5. Delete & Audit check
	if rec := call(http.MethodDelete, "/control/instances/sales/hooks/local-cb", ""); rec.Code != http.StatusOK {
		t.Fatalf("delete gagal: %d %s", rec.Code, rec.Body.String())
	}
	entries, _ := auditLog.List(ctx, auth.AuditQuery{Limit: 10})
	if len(entries) < 3 {
		t.Fatalf("aksi mutasi hooks di control plane wajib dicatat di audit_log, didapat %d", len(entries))
	}
}
