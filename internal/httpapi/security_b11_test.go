package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/policy"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

func setupSecurityServer(t *testing.T, opts ...ServerOption) (*Server, *auth.Manager, *store.Store, *wa.Manager) {
	t.Helper()
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 2, CacheKiB: 512, TxLock: "immediate"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })

	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })

	if _, err := waMgr.Create(ctx, "sales", 60, false); err != nil {
		t.Fatal(err)
	}
	if inst, ok := waMgr.Get("sales"); ok {
		// Gunakan driver pairing tiruan agar pengujian rute pair tidak membuka koneksi WebSocket nyata.
		inst.SetDriverFactoryForTest(func() (chan whatsmeow.QRChannelItem, string) {
			ch := make(chan whatsmeow.QRChannelItem, 4)
			ch <- whatsmeow.QRChannelItem{Event: "code", Code: "2@qr-scope-test", Timeout: 60 * time.Second}
			return ch, "ABCD-1234"
		})
	}

	allOpts := append([]ServerOption{
		WithDB(st.DB),
		WithJournal(j),
		WithAuditLog(auth.NewAuditLog(st.DB)),
	}, opts...)
	srv := NewServer(authMgr, waMgr, allOpts...)
	return srv, authMgr, st, waMgr
}

func materialiseRoutePath(pattern string) (method, urlPath string) {
	parts := strings.SplitN(pattern, " ", 2)
	method = parts[0]
	p := parts[1]
	p = strings.ReplaceAll(p, "{id}", "sales")
	p = strings.ReplaceAll(p, "{seq}", "1")
	p = strings.ReplaceAll(p, "{msg_id}", "msg_1")
	p = strings.ReplaceAll(p, "{jid}", "6281234@s.whatsapp.net")
	p = strings.ReplaceAll(p, "{wa_id}", "3EB0TEST")
	if strings.Contains(p, "/groups/6281234@s.whatsapp.net") {
		p = strings.ReplaceAll(p, "6281234@s.whatsapp.net", "120363000111@g.us")
	}
	return method, p
}

// TestRouteScopeMatrix mengiterasi seluruh route terdaftar dan memverifikasi penegakan 403 scope_denied saat scope tidak dimiliki key.
func TestRouteScopeMatrix(t *testing.T) {
	ctx := context.Background()
	srv, authMgr, _, _ := setupSecurityServer(t)

	for _, rt := range srv.Routes() {
		// Key dengan semua scope KECUALI rt.Scope harus ditolak 403 scope_denied
		missingScopes, err := auth.ParseScopes("*")
		if err != nil {
			t.Fatal(err)
		}
		// Hapus bit scope milik route ini
		withoutTarget := removeScopeBit(t, missingScopes, rt.Scope)
		rawWithout, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{
			Name:       "no-" + string(rt.Scope),
			Scopes:     withoutTarget,
			RatePerSec: 1000,
		})
		if err != nil {
			t.Fatalf("buat key tanpa scope %q gagal: %v", rt.Scope, err)
		}

		method, target := materialiseRoutePath(rt.Pattern)
		reqDeny := httptest.NewRequest(method, target, strings.NewReader("{}"))
		reqDeny.Header.Set("X-API-Key", rawWithout)
		recDeny := httptest.NewRecorder()
		srv.ServeHTTP(recDeny, reqDeny)

		if recDeny.Code != http.StatusForbidden {
			t.Fatalf("route %s tanpa scope %q diharapkan 403, didapat %d (%s)", rt.Pattern, rt.Scope, recDeny.Code, recDeny.Body.String())
		}
		var eb ErrorBody
		if err := json.Unmarshal(recDeny.Body.Bytes(), &eb); err != nil || eb.Error.Code != "scope_denied" {
			t.Fatalf("route %s tanpa scope %q harus berkode scope_denied, didapat %s", rt.Pattern, rt.Scope, recDeny.Body.String())
		}

		// Key dengan scope yang tepat tidak boleh terkena 403 scope_denied maupun 401
		rawWith, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{
			Name:       "with-" + string(rt.Scope),
			Scopes:     rt.ScopeToSet(),
			RatePerSec: 1000,
		})
		if err != nil {
			t.Fatalf("buat key dengan scope %q gagal: %v", rt.Scope, err)
		}
		reqCtx, cancelReq := context.WithTimeout(ctx, 50*time.Millisecond)
		reqAllow := httptest.NewRequest(method, target, strings.NewReader("{}")).WithContext(reqCtx)
		reqAllow.Header.Set("X-API-Key", rawWith)
		recAllow := httptest.NewRecorder()
		srv.ServeHTTP(recAllow, reqAllow)
		cancelReq()
		if recAllow.Code == http.StatusUnauthorized || recAllow.Code == http.StatusForbidden {
			t.Fatalf("route %s dengan scope %q tidak boleh 401/403, didapat %d (%s)", rt.Pattern, rt.Scope, recAllow.Code, recAllow.Body.String())
		}
	}

	// Kasus khusus invite-link?reset=true butuh groups:write
	grpReadOnly, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{
		Name:   "grp-read",
		Scopes: parseScopesOrFail(t, "groups:read"),
	})
	if err != nil {
		t.Fatal(err)
	}
	reqReset := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/groups/120363000111@g.us/invite-link?reset=true", nil)
	reqReset.Header.Set("X-API-Key", grpReadOnly)
	recReset := httptest.NewRecorder()
	srv.ServeHTTP(recReset, reqReset)
	if recReset.Code != http.StatusForbidden {
		t.Fatalf("invite-link?reset=true tanpa groups:write diharapkan 403 scope_denied, didapat %d (%s)", recReset.Code, recReset.Body.String())
	}
}

func (r RouteInfo) ScopeToSet() auth.ScopeSet {
	s, _ := auth.ParseScopes(string(r.Scope))
	return s
}

func parseScopesOrFail(t *testing.T, csv string) auth.ScopeSet {
	t.Helper()
	s, err := auth.ParseScopes(csv)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func removeScopeBit(t *testing.T, all auth.ScopeSet, drop auth.Scope) auth.ScopeSet {
	t.Helper()
	var kept []string
	for _, sc := range []auth.Scope{
		auth.ScopeInstancesRead, auth.ScopePair, auth.ScopeMessagesSend, auth.ScopeMessagesRead,
		auth.ScopeMediaWrite, auth.ScopeEventsRead, auth.ScopeGroupsRead, auth.ScopeGroupsWrite,
		auth.ScopeHealthRead, auth.ScopeSettingsAdmin, auth.ScopeHooksAdmin, auth.ScopeMCP,
	} {
		if sc == drop {
			continue
		}
		if all.Has(sc) {
			kept = append(kept, string(sc))
		}
	}
	s, err := auth.ParseScopes(strings.Join(kept, ","))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestBearerHeader memverifikasi header Authorization: Bearer diperlakukan setara dengan X-API-Key.
func TestBearerHeader(t *testing.T) {
	ctx := context.Background()
	srv, authMgr, _, _ := setupSecurityServer(t)

	raw, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{Name: "bearer-test"})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/instances", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Authorization: Bearer valid diharapkan 200, didapat %d (%s)", rec.Code, rec.Body.String())
	}
}

// TestKeyExpired401 memastikan API key kedaluwarsa mengembalikan HTTP 401 dengan code key_expired.
func TestKeyExpired401(t *testing.T) {
	ctx := context.Background()
	srv, authMgr, _, _ := setupSecurityServer(t)

	raw, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{
		Name:      "expired-key",
		ExpiresAt: time.Now().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/instances", nil)
	req.Header.Set("X-API-Key", raw)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("key kedaluwarsa diharapkan 401, didapat %d", rec.Code)
	}
	var eb ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil || eb.Error.Code != "key_expired" {
		t.Fatalf("key kedaluwarsa harus berkode key_expired, didapat %s", rec.Body.String())
	}
}

// TestRateLimitPerKey429 memverifikasi penegakan token bucket per API key mengembalikan 429 rate_limited + Retry-After: 1.
func TestRateLimitPerKey429(t *testing.T) {
	ctx := context.Background()
	srv, authMgr, _, _ := setupSecurityServer(t)

	raw, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{
		Name:       "slow-key",
		RatePerSec: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	var got429 bool
	for range 10 {
		req := httptest.NewRequest(http.MethodGet, "/v1/instances", nil)
		req.Header.Set("X-API-Key", raw)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code == http.StatusTooManyRequests {
			if rec.Header().Get("Retry-After") != "1" {
				t.Fatalf("429 rate_limited harus menyertakan Retry-After: 1, didapat %q", rec.Header().Get("Retry-After"))
			}
			var eb ErrorBody
			if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil || eb.Error.Code != "rate_limited" {
				t.Fatalf("429 harus berkode rate_limited, didapat %s", rec.Body.String())
			}
			got429 = true
			break
		}
	}
	if !got429 {
		t.Fatal("burst request di atas batas RatePerSec=1 seharusnya memicu 429 rate_limited")
	}
}

// TestRequestID_Propagasi memverifikasi header X-Request-Id dipertahankan bila valid, dibuat hex 16-char bila kosong, dan disematkan pada error envelope.
func TestRequestID_Propagasi(t *testing.T) {
	srv, _, _, _ := setupSecurityServer(t)

	// 1. Header masuk valid -> dipakai ulang
	req1 := httptest.NewRequest(http.MethodGet, "/v1/instances", nil)
	req1.Header.Set("X-Request-Id", "trace_abc.123456")
	rec1 := httptest.NewRecorder()
	srv.ServeHTTP(rec1, req1)
	if rec1.Header().Get("X-Request-Id") != "trace_abc.123456" {
		t.Fatalf("X-Request-Id masuk seharusnya diteruskan, didapat %q", rec1.Header().Get("X-Request-Id"))
	}
	var eb1 ErrorBody
	if err := json.Unmarshal(rec1.Body.Bytes(), &eb1); err != nil || eb1.Error.RequestID != "trace_abc.123456" {
		t.Fatalf("error.request_id harus terisi trace_abc.123456, didapat %s", rec1.Body.String())
	}

	// 2. Header tidak dikirim atau tidak valid -> buat baru 16 karakter hex
	req2 := httptest.NewRequest(http.MethodGet, "/v1/instances", nil)
	req2.Header.Set("X-Request-Id", "bad id!")
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, req2)
	genID := rec2.Header().Get("X-Request-Id")
	if len(genID) != 16 {
		t.Fatalf("request id buatan harus sepanjang 16 hex char, didapat %q", genID)
	}
	var eb2 ErrorBody
	if err := json.Unmarshal(rec2.Body.Bytes(), &eb2); err != nil || eb2.Error.RequestID != genID {
		t.Fatalf("error.request_id harus cocok dengan header %q, didapat %s", genID, rec2.Body.String())
	}
}

type failingOutboxExec struct{}

func (failingOutboxExec) Ready(string) bool                  { return true }
func (failingOutboxExec) NewMessageID(string) (string, error) { return "3EB0FAIL01", nil }
func (failingOutboxExec) Execute(context.Context, *outbox.Job) (outbox.ExecResult, error) {
	return outbox.ExecResult{}, errors.New("rahasia-internal-socket-timeout-10.0.0.5")
}

// TestError5xx_TanpaDetailInternal memastikan respons HTTP 5xx tidak pernah membocorkan pesan error internal ke klien publik.
func TestError5xx_TanpaDetailInternal(t *testing.T) {
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

	authMgr, _ := auth.NewManager(ctx, st)
	j, _ := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 2, CacheKiB: 512, TxLock: "immediate"})
	defer j.Close()

	waMgr, _ := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j))
	defer waMgr.Close()
	_, _ = waMgr.Create(ctx, "sales", 60, false)

	ob := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, failingOutboxExec{}, nil, nil, outbox.Options{DisableSendJitter: true})
	srv := NewServer(authMgr, waMgr, WithDB(st.DB), WithJournal(j), WithOutbox(ob))

	raw, _, err := authMgr.Create(ctx, "admin", "")
	if err != nil {
		t.Fatal(err)
	}

	// Uji writeMappedError default (502 upstream_error)
	recDirect := httptest.NewRecorder()
	recDirect.Header().Set("X-Request-Id", "test-req-502-direct")
	writeMappedError(recDirect, errors.New("rahasia-koneksi-postgres-password"))
	if recDirect.Code != http.StatusBadGateway {
		t.Fatalf("diharapkan 502, didapat %d", recDirect.Code)
	}
	if strings.Contains(recDirect.Body.String(), "rahasia-koneksi-postgres-password") {
		t.Fatalf("body 502 membocorkan detail error internal: %s", recDirect.Body.String())
	}

	// Uji writeOutboxWaitError (ErrSendFailed -> 502 send_failed tanpa membocorkan ref.ErrorMsg internal)
	ob.StartScheduler(ctx, "sales")
	defer ob.Stop()
	reqSend := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages",
		strings.NewReader(`{"to":"628123456","type":"text","text":"tes","wait":"sent"}`))
	reqSend.Header.Set("X-API-Key", raw)
	recSend := httptest.NewRecorder()
	srv.ServeHTTP(recSend, reqSend)
	if recSend.Code != http.StatusBadGateway {
		t.Fatalf("wait=sent gagal permanen diharapkan 502, didapat %d (%s)", recSend.Code, recSend.Body.String())
	}
	if strings.Contains(recSend.Body.String(), "10.0.0.5") || strings.Contains(recSend.Body.String(), "rahasia-internal") {
		t.Fatalf("body 502 outbox membocorkan detail internal: %s", recSend.Body.String())
	}

	// Uji writePairingError default (502 pairing_failed tanpa detail err.Error())
	recPair := httptest.NewRecorder()
	writePairingError(recPair, errors.New("rahasia-noise-handshake-token"))
	if recPair.Code != http.StatusBadGateway {
		t.Fatalf("writePairingError default diharapkan 502, didapat %d", recPair.Code)
	}
	if strings.Contains(recPair.Body.String(), "rahasia-noise") {
		t.Fatalf("body 502 pairing membocorkan detail internal: %s", recPair.Body.String())
	}
}

type destructiveFakeGroupDriver struct{}

func (destructiveFakeGroupDriver) ListGroups(context.Context, string) ([]wa.GroupSummary, error) {
	return nil, nil
}
func (destructiveFakeGroupDriver) CreateGroup(context.Context, string, wa.CreateGroupRequest) (wa.GroupDetail, error) {
	return wa.GroupDetail{}, nil
}
func (destructiveFakeGroupDriver) GetGroup(context.Context, string, types.JID) (wa.GroupDetail, error) {
	return wa.GroupDetail{}, nil
}
func (destructiveFakeGroupDriver) PatchGroup(context.Context, string, types.JID, wa.PatchGroupRequest) (wa.GroupDetail, error) {
	return wa.GroupDetail{}, nil
}
func (destructiveFakeGroupDriver) UpdateGroupParticipants(context.Context, string, types.JID, wa.UpdateParticipantsRequest) ([]wa.ParticipantResult, error) {
	return nil, nil
}
func (destructiveFakeGroupDriver) GetGroupInviteLink(context.Context, string, types.JID, bool) (string, error) {
	return "https://chat.whatsapp.com/test", nil
}
func (destructiveFakeGroupDriver) JoinGroup(context.Context, string, string) (types.JID, error) {
	return types.EmptyJID, nil
}
func (destructiveFakeGroupDriver) LeaveGroup(context.Context, string, types.JID) error {
	return nil
}
func (destructiveFakeGroupDriver) ListGroupRequests(context.Context, string, types.JID) ([]wa.GroupJoinRequestItem, error) {
	return nil, nil
}
func (destructiveFakeGroupDriver) HandleGroupRequests(context.Context, string, types.JID, wa.GroupRequestsAction) ([]wa.ParticipantResult, error) {
	return nil, nil
}

// TestV2Settings_GetPatchDanDestructiveGroup memverifikasi endpoint GET/PATCH /settings, pencatatan audit settings.update, serta CheckDestructive pada grup.
func TestV2Settings_GetPatchDanDestructiveGroup(t *testing.T) {
	ctx := context.Background()
	var enf *policy.Enforcer

	srv, authMgr, st, _ := setupSecurityServer(t, withGroupDriver(destructiveFakeGroupDriver{}))
	enf = policy.New(authMgr, nil)
	srv.policyEnforcer = enf

	denyDestr := auth.DefaultPolicy()
	denyDestr.Destructive = "deny"
	rawKey, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{
		Name:       "settings-and-groups",
		Scopes:     parseScopesOrFail(t, "settings:admin,groups:write"),
		Policy:     denyDestr,
		RatePerSec: 100,
	})
	if err != nil {
		t.Fatal(err)
	}

	// GET settings -> langsung objek InstanceSettings
	reqGet := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/settings", nil)
	reqGet.Header.Set("X-API-Key", rawKey)
	recGet := httptest.NewRecorder()
	srv.ServeHTTP(recGet, reqGet)
	if recGet.Code != http.StatusOK {
		t.Fatalf("GET settings diharapkan 200, didapat %d (%s)", recGet.Code, recGet.Body.String())
	}
	var gotSet store.InstanceSettings
	if err := json.Unmarshal(recGet.Body.Bytes(), &gotSet); err != nil || gotSet.Timezone != "Asia/Jakarta" {
		t.Fatalf("GET settings harus mengembalikan store.InstanceSettings di level atas JSON, didapat %s", recGet.Body.String())
	}

	// PATCH settings -> perbarui & catat audit settings.update
	reqPatch := httptest.NewRequest(http.MethodPatch, "/v2/instances/sales/settings",
		strings.NewReader(`{"recycle_at":"04:00","webhook_lanes":8}`))
	reqPatch.Header.Set("X-API-Key", rawKey)
	recPatch := httptest.NewRecorder()
	srv.ServeHTTP(recPatch, reqPatch)
	if recPatch.Code != http.StatusOK {
		t.Fatalf("PATCH settings diharapkan 200, didapat %d (%s)", recPatch.Code, recPatch.Body.String())
	}
	var patched store.InstanceSettings
	if err := json.Unmarshal(recPatch.Body.Bytes(), &patched); err != nil || patched.RecycleAt != "04:00" || patched.WebhookLanes != 8 {
		t.Fatalf("PATCH settings tidak menerapkan perubahan: %s", recPatch.Body.String())
	}

	entries, err := auth.NewAuditLog(st.DB).List(ctx, auth.AuditQuery{ActionPattern: "settings.update"})
	if err != nil || len(entries) != 1 || entries[0].Target != "sales" {
		t.Fatalf("PATCH settings harus mencatat audit settings.update, didapat %+v (err=%v)", entries, err)
	}

	// CheckDestructive pada remove peserta dan leave grup -> 403 destructive_denied
	reqRemove := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/groups/120363000111@g.us/participants",
		strings.NewReader(`{"action":"remove","participants":["628123"]}`))
	reqRemove.Header.Set("X-API-Key", rawKey)
	recRemove := httptest.NewRecorder()
	srv.ServeHTTP(recRemove, reqRemove)
	if recRemove.Code != http.StatusForbidden {
		t.Fatalf("remove peserta dengan destructive=deny diharapkan 403, didapat %d (%s)", recRemove.Code, recRemove.Body.String())
	}

	reqLeave := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/groups/120363000111@g.us/leave", nil)
	reqLeave.Header.Set("X-API-Key", rawKey)
	recLeave := httptest.NewRecorder()
	srv.ServeHTTP(recLeave, reqLeave)
	if recLeave.Code != http.StatusForbidden {
		t.Fatalf("leave grup dengan destructive=deny diharapkan 403, didapat %d (%s)", recLeave.Code, recLeave.Body.String())
	}
}
