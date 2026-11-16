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

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"go.mau.fi/whatsmeow/types"
)

type fakeGroupDriver struct {
	mu           sync.Mutex
	summaries    []wa.GroupSummary
	detail       wa.GroupDetail
	partResults  []wa.ParticipantResult
	inviteLink   string
	joinedJID    types.JID
	reqItems     []wa.GroupJoinRequestItem
	reqResults   []wa.ParticipantResult
	err          error
	calls        []string
	lastReset    bool
	lastJoinCode string
}

func (f *fakeGroupDriver) record(name string) {
	f.mu.Lock()
	f.calls = append(f.calls, name)
	f.mu.Unlock()
}

func (f *fakeGroupDriver) ListGroups(_ context.Context, _ string) ([]wa.GroupSummary, error) {
	f.record("list")
	if f.err != nil {
		return nil, f.err
	}
	return f.summaries, nil
}

func (f *fakeGroupDriver) CreateGroup(_ context.Context, _ string, req wa.CreateGroupRequest) (wa.GroupDetail, error) {
	f.record("create")
	if len([]rune(req.Name)) == 0 || len([]rune(req.Name)) > 25 || len(req.Participants) > 50 {
		return wa.GroupDetail{}, wa.NewGroupError(http.StatusBadRequest, "invalid_request", "validasi grup gagal", 0)
	}
	if f.err != nil {
		return wa.GroupDetail{}, f.err
	}
	return f.detail, nil
}

func (f *fakeGroupDriver) GetGroup(_ context.Context, _ string, _ types.JID) (wa.GroupDetail, error) {
	f.record("get")
	if f.err != nil {
		return wa.GroupDetail{}, f.err
	}
	return f.detail, nil
}

func (f *fakeGroupDriver) PatchGroup(_ context.Context, _ string, _ types.JID, _ wa.PatchGroupRequest) (wa.GroupDetail, error) {
	f.record("patch")
	if f.err != nil {
		return wa.GroupDetail{}, f.err
	}
	return f.detail, nil
}

func (f *fakeGroupDriver) UpdateGroupParticipants(_ context.Context, _ string, _ types.JID, _ wa.UpdateParticipantsRequest) ([]wa.ParticipantResult, error) {
	f.record("participants")
	if f.err != nil {
		return nil, f.err
	}
	return f.partResults, nil
}

func (f *fakeGroupDriver) GetGroupInviteLink(_ context.Context, _ string, _ types.JID, reset bool) (string, error) {
	f.record("invite-link")
	f.mu.Lock()
	f.lastReset = reset
	f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	return f.inviteLink, nil
}

func (f *fakeGroupDriver) JoinGroup(_ context.Context, _ string, codeOrLink string) (types.JID, error) {
	f.record("join")
	f.mu.Lock()
	f.lastJoinCode = codeOrLink
	f.mu.Unlock()
	if f.err != nil {
		return types.EmptyJID, f.err
	}
	return f.joinedJID, nil
}

func (f *fakeGroupDriver) LeaveGroup(_ context.Context, _ string, _ types.JID) error {
	f.record("leave")
	return f.err
}

func (f *fakeGroupDriver) ListGroupRequests(_ context.Context, _ string, _ types.JID) ([]wa.GroupJoinRequestItem, error) {
	f.record("list-requests")
	if f.err != nil {
		return nil, f.err
	}
	return f.reqItems, nil
}

func (f *fakeGroupDriver) HandleGroupRequests(_ context.Context, _ string, _ types.JID, _ wa.GroupRequestsAction) ([]wa.ParticipantResult, error) {
	f.record("handle-requests")
	if f.err != nil {
		return nil, f.err
	}
	return f.reqResults, nil
}

type fakeContactDriver struct {
	mu      sync.Mutex
	profile wa.ContactProfile
	err     error
	lastJID string
}

func (f *fakeContactDriver) GetContactProfile(_ context.Context, _ string, jid string) (wa.ContactProfile, error) {
	f.mu.Lock()
	f.lastJID = jid
	f.mu.Unlock()
	if f.err != nil {
		return wa.ContactProfile{}, f.err
	}
	return f.profile, nil
}

func setupB8HTTPServer(t *testing.T, gd groupTransport, cd contactTransport) (http.Handler, string) {
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

	if err := st.CreateInstance(ctx, store.Instance{ID: "sales", Status: store.StatusConnected}); err != nil {
		t.Fatalf("CreateInstance gagal: %v", err)
	}
	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatalf("auth.NewManager gagal: %v", err)
	}
	rawKey, _, err := authMgr.Create(ctx, "test", "")
	if err != nil {
		t.Fatalf("auth.Create gagal: %v", err)
	}
	waMgr, err := wa.NewManager(ctx, cfg, st, nil)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })

	srv := NewServer(authMgr, waMgr, withGroupDriver(gd), withContactDriver(cd))
	return srv, rawKey
}

// TestV2GroupsEndpoints memverifikasi endpoint grup v2 termasuk status 201, PATCH details.applied, requests, invite-link, join, leave, dan 1024 truncation.
func TestV2GroupsEndpoints(t *testing.T) {
	grpJID := "12036301122334455@g.us"
	parts1024 := make([]wa.GroupParticipantView, 1024)
	for i := range parts1024 {
		parts1024[i] = wa.GroupParticipantView{JID: fmt.Sprintf("62810000%04d@s.whatsapp.net", i)}
	}
	drv := &fakeGroupDriver{
		summaries: []wa.GroupSummary{
			{JID: grpJID, Name: "Tim Sales", Size: 15, IsAdmin: true, CreatedAt: 1794360100},
		},
		detail: wa.GroupDetail{
			JID:                   grpJID,
			Name:                  "Tim Sales",
			Participants:          parts1024,
			ParticipantsTruncated: true,
		},
		partResults: []wa.ParticipantResult{{JID: "628111@s.whatsapp.net", Status: "ok"}},
		inviteLink:  "https://chat.whatsapp.com/XYZ999",
		joinedJID:   types.NewJID("12036301122334455", types.GroupServer),
		reqItems:    []wa.GroupJoinRequestItem{{JID: "628999@s.whatsapp.net", RequestedAt: 1794360000}},
		reqResults:  []wa.ParticipantResult{{JID: "628999@s.whatsapp.net", Status: "ok"}},
	}
	srv, key := setupB8HTTPServer(t, drv, &fakeContactDriver{})

	doReq := func(method, path, body string) *httptest.ResponseRecorder {
		var rBody *bytes.Reader
		if body != "" {
			rBody = bytes.NewReader([]byte(body))
		} else {
			rBody = bytes.NewReader(nil)
		}
		req := httptest.NewRequest(method, path, rBody)
		req.Header.Set("X-API-Key", key)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		return rec
	}

	// GET list groups
	rec := doReq(http.MethodGet, "/v2/instances/sales/groups", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /groups diharapkan 200, didapat %d (%s)", rec.Code, rec.Body.String())
	}

	// POST create group (0 peserta diperbolehkan -> 201)
	recCreate := doReq(http.MethodPost, "/v2/instances/sales/groups", `{"name":"Grup Baru","participants":[]}`)
	if recCreate.Code != http.StatusCreated {
		t.Fatalf("POST /groups diharapkan 201, didapat %d (%s)", recCreate.Code, recCreate.Body.String())
	}

	// GET detail group -> participants_truncated: true
	recGet := doReq(http.MethodGet, "/v2/instances/sales/groups/"+grpJID, "")
	if recGet.Code != http.StatusOK {
		t.Fatalf("GET /groups/{jid} diharapkan 200, didapat %d", recGet.Code)
	}
	var gotDetail wa.GroupDetail
	if err := json.Unmarshal(recGet.Body.Bytes(), &gotDetail); err != nil || !gotDetail.ParticipantsTruncated || len(gotDetail.Participants) != 1024 {
		t.Fatalf("detail group / truncation tidak sesuai: %+v err=%v", gotDetail, err)
	}

	// PATCH gagal parsial harus mengembalikan error.details.applied
	drv.err = &wa.GroupError{
		Status:  http.StatusForbidden,
		Code:    "group_forbidden",
		Message: "ditolak server",
		Details: map[string]any{"applied": []string{"name"}},
	}
	recPatch := doReq(http.MethodPatch, "/v2/instances/sales/groups/"+grpJID, `{"name":"Baru","description":"Desc"}`)
	if recPatch.Code != http.StatusForbidden {
		t.Fatalf("PATCH gagal diharapkan 403, didapat %d (%s)", recPatch.Code, recPatch.Body.String())
	}
	var errBody ErrorBody
	if err := json.Unmarshal(recPatch.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("decode ErrorBody gagal: %v", err)
	}
	if errBody.Error.Code != "group_forbidden" || errBody.Error.Details["applied"] == nil {
		t.Fatalf("details.applied pada PATCH error tidak sesuai: %+v", errBody)
	}
	drv.err = nil

	// GET & POST requests, invite-link, join, leave
	if recReq := doReq(http.MethodGet, "/v2/instances/sales/groups/"+grpJID+"/requests", ""); recReq.Code != http.StatusOK {
		t.Fatalf("GET /requests diharapkan 200, didapat %d", recReq.Code)
	}
	if recReqPost := doReq(http.MethodPost, "/v2/instances/sales/groups/"+grpJID+"/requests", `{"action":"approve","participants":["628999"]}`); recReqPost.Code != http.StatusOK {
		t.Fatalf("POST /requests diharapkan 200, didapat %d (%s)", recReqPost.Code, recReqPost.Body.String())
	}
	if recLink := doReq(http.MethodGet, "/v2/instances/sales/groups/"+grpJID+"/invite-link?reset=true", ""); recLink.Code != http.StatusOK || !drv.lastReset {
		t.Fatalf("GET /invite-link?reset=true gagal: code=%d reset=%v", recLink.Code, drv.lastReset)
	}
	if recJoin := doReq(http.MethodPost, "/v2/instances/sales/groups/join", `{"link":"https://chat.whatsapp.com/ABC999"}`); recJoin.Code != http.StatusOK {
		t.Fatalf("POST /groups/join diharapkan 200, didapat %d", recJoin.Code)
	}
	if recLeave := doReq(http.MethodPost, "/v2/instances/sales/groups/"+grpJID+"/leave", ""); recLeave.Code != http.StatusNoContent {
		t.Fatalf("POST /leave diharapkan 204, didapat %d", recLeave.Code)
	}

	// Validasi input HTTP 400 sebelum memanggil driver
	for _, tc := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPatch, "/v2/instances/sales/groups/bukan-grup", `{"name":"x"}`},
		{http.MethodPatch, "/v2/instances/sales/groups/" + grpJID, `{}`},
		{http.MethodGet, "/v2/instances/sales/groups/" + grpJID + "/invite-link?reset=bukanbool", ""},
		{http.MethodPost, "/v2/instances/sales/groups/join", `{}`},
	} {
		recBad := doReq(tc.method, tc.path, tc.body)
		if recBad.Code != http.StatusBadRequest {
			t.Fatalf("%s %s diharapkan 400, didapat %d (%s)", tc.method, tc.path, recBad.Code, recBad.Body.String())
		}
	}
}

// TestV2ContactsEndpoint memverifikasi GET /v2/instances/{id}/contacts/{jid} mengembalikan profil kontak dengan picture null dan verified_name string.
func TestV2ContactsEndpoint(t *testing.T) {
	cd := &fakeContactDriver{
		profile: wa.ContactProfile{
			JID:          "123456@lid",
			PN:           "628123456789@s.whatsapp.net",
			LID:          "123456@lid",
			PushName:     "Budi",
			VerifiedName: "PT Maju",
			Picture:      nil,
		},
	}
	srv, key := setupB8HTTPServer(t, &fakeGroupDriver{}, cd)

	req := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/contacts/123456@lid", nil)
	req.Header.Set("X-API-Key", key)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /contacts/{jid} diharapkan 200, didapat %d (%s)", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode profil kontak gagal: %v", err)
	}
	if pic, exists := body["picture"]; !exists || pic != nil {
		t.Fatalf("field picture harus bernilai JSON null, didapat exists=%v val=%v", exists, pic)
	}
	if body["verified_name"] != "PT Maju" || body["pn"] != "628123456789@s.whatsapp.net" {
		t.Fatalf("field profil kontak tidak sesuai: %+v", body)
	}
}
