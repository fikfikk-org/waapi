package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/hooks"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// TestV2HooksAPI_CRUDDryRunDanTrustedGuard memastikan CRUD /v2/instances/{id}/hooks*, dry-run /test, dan penurunan trusted=0 pada REST API.
func TestV2HooksAPI_CRUDDryRunDanTrustedGuard(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j))
	if err != nil {
		t.Fatal(err)
	}
	defer waMgr.Close()
	if _, err := waMgr.Create(ctx, "sales", 60, false); err != nil {
		t.Fatal(err)
	}

	hooksEng := hooks.NewEngine(hooks.EngineDeps{AppStore: st, Journal: j})
	srv := NewServer(authMgr, waMgr, WithDB(st.DB), WithJournal(j), WithHooks(hooksEng))

	hooksScopes, _ := auth.ParseScopes("hooks:admin")
	rawKey, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{Name: "hooks-key", Scopes: hooksScopes})
	if err != nil {
		t.Fatal(err)
	}

	doReq := func(method, path, body string) *httptest.ResponseRecorder {
		var rd *strings.Reader
		if body != "" {
			rd = strings.NewReader(body)
		} else {
			rd = strings.NewReader("")
		}
		req := httptest.NewRequest(method, path, rd)
		req.Header.Set("X-API-Key", rawKey)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec
	}

	// 1. POST dengan HTTP localhost via REST API wajib ditolak meski payload mencantumkan trusted=true.
	recBad := doReq(http.MethodPost, "/v2/instances/sales/hooks", `{
		"id":"ssrf-try","trusted":true,"on":["message"],
		"do":[{"action":"http","url":"http://127.0.0.1:9000/hook"}]
	}`)
	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("REST API membuat http://127.0.0.1 harus ditolak 400, didapat %d (%s)", recBad.Code, recBad.Body.String())
	}

	// 2. POST rule valid HTTPS -> 201 Created
	recCreate := doReq(http.MethodPost, "/v2/instances/sales/hooks", `{
		"id":"faq-jam","name":"FAQ","on":["message"],
		"when":{"field":"text","op":"icontains","value":"buka"},
		"do":[{"action":"reply","text":"Halo {{push_name}}"}]
	}`)
	if recCreate.Code != http.StatusCreated {
		t.Fatalf("POST hook valid diharapkan 201, didapat %d (%s)", recCreate.Code, recCreate.Body.String())
	}

	// 3. POST duplikat -> 409
	recDup := doReq(http.MethodPost, "/v2/instances/sales/hooks", `{"id":"faq-jam","on":["message"],"do":[{"action":"mark_read"}]}`)
	if recDup.Code != http.StatusConflict {
		t.Fatalf("POST hook duplikat diharapkan 409, didapat %d (%s)", recDup.Code, recDup.Body.String())
	}

	// 4. GET list & GET detail
	recList := doReq(http.MethodGet, "/v2/instances/sales/hooks", "")
	if recList.Code != http.StatusOK || !strings.Contains(recList.Body.String(), "faq-jam") {
		t.Fatalf("GET hooks gagal: %d %s", recList.Code, recList.Body.String())
	}
	recGet := doReq(http.MethodGet, "/v2/instances/sales/hooks/faq-jam", "")
	if recGet.Code != http.StatusOK || !strings.Contains(recGet.Body.String(), "FAQ") {
		t.Fatalf("GET hook detail gagal: %d %s", recGet.Code, recGet.Body.String())
	}

	// 5. Dry-run via REST /test
	recTest := doReq(http.MethodPost, "/v2/instances/sales/hooks/faq-jam/test", `{
		"event":{"event":"message","instance":"sales","data":{"chat":"628111@s.whatsapp.net","from":"628111@s.whatsapp.net","push_name":"Budi","type":"text","text":"Jam buka?"}}
	}`)
	if recTest.Code != http.StatusOK {
		t.Fatalf("POST /hooks/{id}/test diharapkan 200, didapat %d (%s)", recTest.Code, recTest.Body.String())
	}
	var dry hooks.DryRunResult
	if err := json.Unmarshal(recTest.Body.Bytes(), &dry); err != nil || !dry.Matched || len(dry.Actions) != 1 || dry.Actions[0].Text != "Halo Budi" {
		t.Fatalf("respons dry-run REST tidak sesuai: %s (%+v)", recTest.Body.String(), dry)
	}

	// 6. PUT terhadap rule trusted=1 dari DB wajib menurunkan trusted=0
	_ = st.UpsertHook(ctx, store.HookRecord{
		InstanceID: "sales", ID: "from-cli", Trusted: true, Enabled: true, Priority: 10,
		Spec: `{"id":"from-cli","trusted":true,"on":["message"],"do":[{"action":"http","url":"http://127.0.0.1:8088/cb"}]}`,
	})
	recPutLoopback := doReq(http.MethodPut, "/v2/instances/sales/hooks/from-cli", `{
		"on":["message"],"do":[{"action":"http","url":"http://127.0.0.1:8088/cb"}]
	}`)
	if recPutLoopback.Code != http.StatusBadRequest {
		t.Fatalf("PUT REST terhadap rule trusted lama tidak boleh tetap diizinkan ke localhost: %d (%s)", recPutLoopback.Code, recPutLoopback.Body.String())
	}
	recPutHTTPS := doReq(http.MethodPut, "/v2/instances/sales/hooks/from-cli", `{
		"on":["message"],"do":[{"action":"reply","text":"diperbarui"}]
	}`)
	if recPutHTTPS.Code != http.StatusOK {
		t.Fatalf("PUT REST valid diharapkan 200, didapat %d (%s)", recPutHTTPS.Code, recPutHTTPS.Body.String())
	}
	saved, err := st.GetHook(ctx, "sales", "from-cli")
	if err != nil || saved.Trusted {
		t.Fatalf("PUT lewat REST wajib menurunkan trusted=0, got=%+v err=%v", saved, err)
	}

	// 6b. POST ai_reply saat WAAPI_AI_BASE_URL kosong wajib ditolak seketika
	recAIDisabled := doReq(http.MethodPost, "/v2/instances/sales/hooks", `{
		"id":"cs-ai-disabled","on":["message"],"do":[{"action":"ai_reply","system":"CS"}]
	}`)
	if recAIDisabled.Code != http.StatusBadRequest || !strings.Contains(recAIDisabled.Body.String(), "WAAPI_AI_BASE_URL") {
		t.Fatalf("POST ai_reply tanpa WAAPI_AI_BASE_URL wajib 400: %d (%s)", recAIDisabled.Code, recAIDisabled.Body.String())
	}

	// 7. DELETE
	recDel := doReq(http.MethodDelete, "/v2/instances/sales/hooks/faq-jam", "")
	if recDel.Code != http.StatusOK {
		t.Fatalf("DELETE hook gagal: %d %s", recDel.Code, recDel.Body.String())
	}
}
