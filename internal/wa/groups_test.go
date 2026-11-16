package wa

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type fakeGroupClient struct {
	mu          sync.Mutex
	serverCalls int
	joined      []*types.GroupInfo
	groupInfo   *types.GroupInfo
	createRes   *types.GroupInfo
	partRes     []types.GroupParticipant
	inviteLink  string
	joinRes     types.JID
	reqList     []types.GroupParticipantRequest
	reqUpdate   []types.GroupParticipant
	err         error
	setNameErr  error
	setDescErr  error
	patchCalls  []string
}

func (f *fakeGroupClient) hit() {
	f.mu.Lock()
	f.serverCalls++
	f.mu.Unlock()
}

func (f *fakeGroupClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.serverCalls
}

func (f *fakeGroupClient) GetJoinedGroups(_ context.Context) ([]*types.GroupInfo, error) {
	f.hit()
	if f.err != nil {
		return nil, f.err
	}
	return f.joined, nil
}

func (f *fakeGroupClient) GetGroupInfo(_ context.Context, _ types.JID) (*types.GroupInfo, error) {
	f.hit()
	if f.err != nil {
		return nil, f.err
	}
	return f.groupInfo, nil
}

func (f *fakeGroupClient) CreateGroup(_ context.Context, _ whatsmeow.ReqCreateGroup) (*types.GroupInfo, error) {
	f.hit()
	if f.err != nil {
		return nil, f.err
	}
	return f.createRes, nil
}

func (f *fakeGroupClient) UpdateGroupParticipants(_ context.Context, _ types.JID, _ []types.JID, _ whatsmeow.ParticipantChange) ([]types.GroupParticipant, error) {
	f.hit()
	if f.err != nil {
		return nil, f.err
	}
	return f.partRes, nil
}

func (f *fakeGroupClient) SetGroupName(_ context.Context, _ types.JID, _ string) error {
	f.hit()
	f.mu.Lock()
	f.patchCalls = append(f.patchCalls, "name")
	f.mu.Unlock()
	if f.setNameErr != nil {
		return f.setNameErr
	}
	return f.err
}

func (f *fakeGroupClient) SetGroupDescription(_ context.Context, _ types.JID, _ string) error {
	f.hit()
	f.mu.Lock()
	f.patchCalls = append(f.patchCalls, "description")
	f.mu.Unlock()
	if f.setDescErr != nil {
		return f.setDescErr
	}
	return f.err
}

func (f *fakeGroupClient) SetGroupAnnounce(_ context.Context, _ types.JID, _ bool) error {
	f.hit()
	f.mu.Lock()
	f.patchCalls = append(f.patchCalls, "announce")
	f.mu.Unlock()
	return f.err
}

func (f *fakeGroupClient) SetGroupLocked(_ context.Context, _ types.JID, _ bool) error {
	f.hit()
	f.mu.Lock()
	f.patchCalls = append(f.patchCalls, "locked")
	f.mu.Unlock()
	return f.err
}

func (f *fakeGroupClient) SetGroupJoinApprovalMode(_ context.Context, _ types.JID, _ bool) error {
	f.hit()
	f.mu.Lock()
	f.patchCalls = append(f.patchCalls, "join_approval")
	f.mu.Unlock()
	return f.err
}

func (f *fakeGroupClient) GetGroupInviteLink(_ context.Context, _ types.JID, _ bool) (string, error) {
	f.hit()
	if f.err != nil {
		return "", f.err
	}
	return f.inviteLink, nil
}

func (f *fakeGroupClient) JoinGroupWithLink(_ context.Context, _ string) (types.JID, error) {
	f.hit()
	if f.err != nil {
		return types.EmptyJID, f.err
	}
	return f.joinRes, nil
}

func (f *fakeGroupClient) LeaveGroup(_ context.Context, _ types.JID) error {
	f.hit()
	return f.err
}

func (f *fakeGroupClient) GetGroupRequestParticipants(_ context.Context, _ types.JID) ([]types.GroupParticipantRequest, error) {
	f.hit()
	if f.err != nil {
		return nil, f.err
	}
	return f.reqList, nil
}

func (f *fakeGroupClient) UpdateGroupRequestParticipants(_ context.Context, _ types.JID, _ []types.JID, _ whatsmeow.ParticipantRequestChange) ([]types.GroupParticipant, error) {
	f.hit()
	if f.err != nil {
		return nil, f.err
	}
	return f.reqUpdate, nil
}

type stubGroupGovernor struct {
	createErr error
	addErr    error
	calls     int
}

func (g *stubGroupGovernor) AllowGroupCreate(_ context.Context, _ string, _ []string) error {
	g.calls++
	return g.createErr
}

func (g *stubGroupGovernor) AllowParticipantAdd(_ context.Context, _ string, _ []string) error {
	g.calls++
	return g.addErr
}

func newTestGroupManager(fg groupClient, gov GroupGovernor) (*Manager, types.JID) {
	grpJID := types.NewJID("12036301122334455", types.GroupServer)
	inst := &Instance{
		id:         "sales",
		jid:        "628111111111@s.whatsapp.net",
		checkLRU:   NewLRU[string, NumberCheckResult](2048, 24*time.Hour),
		contactLRU: NewLRU[string, ContactProfile](256, time.Hour),
		groupCli:   fg,
	}
	mgr := &Manager{
		instances:    map[string]*Instance{"sales": inst},
		groupListLRU: NewLRU[string, []GroupSummary](128, 60*time.Second),
		groupGov:     gov,
	}
	inst.mgr = mgr
	return mgr, grpJID
}

// TestGroups_PemetaanErrorIQ memverifikasi tabel pemetaan *whatsmeow.IQError ke HTTP status & error code.
func TestGroups_PemetaanErrorIQ(t *testing.T) {
	cases := []struct {
		code       int
		wantStatus int
		wantCode   string
	}{
		{code: 401, wantStatus: http.StatusForbidden, wantCode: "group_forbidden"},
		{code: 403, wantStatus: http.StatusForbidden, wantCode: "group_forbidden"},
		{code: 404, wantStatus: http.StatusNotFound, wantCode: "group_not_found"},
		{code: 406, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{code: 429, wantStatus: http.StatusTooManyRequests, wantCode: "rate_limited"},
		{code: 500, wantStatus: http.StatusBadGateway, wantCode: "upstream_error"},
		{code: 410, wantStatus: http.StatusBadGateway, wantCode: "upstream_error"},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("iq-%d", tc.code), func(t *testing.T) {
			rawErr := fmt.Errorf("bungkus: %w", &whatsmeow.IQError{Code: tc.code, Text: "test-iq"})
			mapped := MapGroupIQError(rawErr)
			var ge *GroupError
			if !errors.As(mapped, &ge) {
				t.Fatalf("diharapkan *GroupError, didapat %T (%v)", mapped, mapped)
			}
			if ge.Status != tc.wantStatus || ge.Code != tc.wantCode {
				t.Fatalf("kode IQ %d: diharapkan (%d, %s), didapat (%d, %s)", tc.code, tc.wantStatus, tc.wantCode, ge.Status, ge.Code)
			}
		})
	}
}

// TestGroups_ValidasiSebelumPanggilServer memastikan nama 26 karakter atau 51 peserta ditolak 400 tanpa memanggil fake client.
func TestGroups_ValidasiSebelumPanggilServer(t *testing.T) {
	fg := &fakeGroupClient{}
	mgr, grpJID := newTestGroupManager(fg, nil)

	// Nama 26 karakter pada CreateGroup.
	_, err := mgr.CreateGroup(context.Background(), "sales", CreateGroupRequest{
		Name:         strings.Repeat("a", 26),
		Participants: []string{"6281234567890"},
	})
	var ge *GroupError
	if !errors.As(err, &ge) || ge.Status != http.StatusBadRequest {
		t.Fatalf("nama 26 karakter harus mengembalikan 400 GroupError, didapat %v", err)
	}

	// 51 peserta pada CreateGroup.
	parts51 := make([]string, 51)
	for idx := range parts51 {
		parts51[idx] = fmt.Sprintf("628123456%04d", idx)
	}
	_, err = mgr.CreateGroup(context.Background(), "sales", CreateGroupRequest{
		Name:         "Valid Group",
		Participants: parts51,
	})
	if !errors.As(err, &ge) || ge.Status != http.StatusBadRequest {
		t.Fatalf("51 peserta harus mengembalikan 400 GroupError, didapat %v", err)
	}

	// 51 peserta pada UpdateGroupParticipants.
	_, err = mgr.UpdateGroupParticipants(context.Background(), "sales", grpJID, UpdateParticipantsRequest{
		Action:       "add",
		Participants: parts51,
	})
	if !errors.As(err, &ge) || ge.Status != http.StatusBadRequest {
		t.Fatalf("51 peserta pada update harus mengembalikan 400, didapat %v", err)
	}

	// PATCH kosong {} harus mengembalikan 400.
	_, err = mgr.PatchGroup(context.Background(), "sales", grpJID, PatchGroupRequest{})
	if !errors.As(err, &ge) || ge.Status != http.StatusBadRequest {
		t.Fatalf("PATCH kosong harus mengembalikan 400, didapat %v", err)
	}

	if fg.callCount() != 0 {
		t.Fatalf("fake groupClient seharusnya tidak pernah dipanggil untuk input invalid, terpanggil %d kali", fg.callCount())
	}
}

// TestGroups_HasilPerPeserta memverifikasi pemetaan GroupParticipant.Error menjadi string status yang tepat.
func TestGroups_HasilPerPeserta(t *testing.T) {
	fg := &fakeGroupClient{
		partRes: []types.GroupParticipant{
			{JID: types.NewJID("628111", types.DefaultUserServer), Error: 0},
			{JID: types.NewJID("628222", types.DefaultUserServer), Error: 403},
			{JID: types.NewJID("628333", types.DefaultUserServer), Error: 409},
			{JID: types.NewJID("628444", types.DefaultUserServer), Error: 404},
			{JID: types.NewJID("628555", types.DefaultUserServer), Error: 500},
		},
	}
	mgr, grpJID := newTestGroupManager(fg, nil)

	res, err := mgr.UpdateGroupParticipants(context.Background(), "sales", grpJID, UpdateParticipantsRequest{
		Action:       "add",
		Participants: []string{"62811100", "62822200", "62833300", "62844400", "62855500"},
	})
	if err != nil {
		t.Fatalf("UpdateGroupParticipants gagal: %v", err)
	}
	want := []string{"ok", "privacy_requires_invite", "already_member", "not_on_whatsapp", "error_500"}
	if len(res) != len(want) {
		t.Fatalf("panjang hasil tidak sesuai: %+v", res)
	}
	for idx, w := range want {
		if res[idx].Status != w {
			t.Fatalf("indeks %d: diharapkan status %q, didapat %q", idx, w, res[idx].Status)
		}
	}
}

// TestGroups_GovernorDipanggil memverifikasi GroupGovernor dipanggil dan penolakan 429/403 diteruskan tanpa memanggil server WA.
func TestGroups_GovernorDipanggil(t *testing.T) {
	fg := &fakeGroupClient{}
	gov429 := &stubGroupGovernor{
		createErr: NewGroupError(http.StatusTooManyRequests, "rate_limited", "kuota grup habis", 60),
		addErr:    NewGroupError(http.StatusForbidden, "guardian_restricted", "akun dibatasi", 0),
	}
	mgr, grpJID := newTestGroupManager(fg, gov429)

	_, err := mgr.CreateGroup(context.Background(), "sales", CreateGroupRequest{
		Name:         "Grup Uji",
		Participants: []string{"6281234567890"},
	})
	var ge *GroupError
	if !errors.As(err, &ge) || ge.Status != http.StatusTooManyRequests || ge.Code != "rate_limited" {
		t.Fatalf("penolakan governor 429 harus diteruskan, didapat %v", err)
	}

	_, err = mgr.UpdateGroupParticipants(context.Background(), "sales", grpJID, UpdateParticipantsRequest{
		Action:       "add",
		Participants: []string{"6281234567890"},
	})
	if !errors.As(err, &ge) || ge.Status != http.StatusForbidden || ge.Code != "guardian_restricted" {
		t.Fatalf("penolakan governor 403 harus diteruskan, didapat %v", err)
	}
	if gov429.calls != 2 {
		t.Fatalf("governor harus dipanggil 2 kali, didapat %d", gov429.calls)
	}
	if fg.callCount() != 0 {
		t.Fatalf("fake groupClient seharusnya tidak dipanggil saat governor menolak, didapat %d", fg.callCount())
	}
}

// TestGroups_CacheRingkasanPatchDanTruncation memverifikasi cache list 60s, PATCH berurutan dengan details.applied, 0 peserta create, requests, dan pemotongan 1024 peserta.
func TestGroups_CacheRingkasanPatchDanTruncation(t *testing.T) {
	many := make([]types.GroupParticipant, 1030)
	for idx := range many {
		many[idx] = types.GroupParticipant{JID: types.NewJID(fmt.Sprintf("62812000%04d", idx), types.DefaultUserServer)}
	}
	fg := &fakeGroupClient{
		joined: []*types.GroupInfo{
			{
				JID:          types.NewJID("12036301122334455", types.GroupServer),
				GroupName:    types.GroupName{Name: "Sales"},
				Participants: []types.GroupParticipant{{JID: types.NewJID("628111111111", types.DefaultUserServer), IsAdmin: true}},
			},
		},
		groupInfo: &types.GroupInfo{
			JID:          types.NewJID("12036301122334455", types.GroupServer),
			GroupName:    types.GroupName{Name: "Sales"},
			Participants: many,
		},
		createRes: &types.GroupInfo{
			JID:       types.NewJID("12036309999999999", types.GroupServer),
			GroupName: types.GroupName{Name: "Grup Kosong"},
		},
		reqList: []types.GroupParticipantRequest{
			{JID: types.NewJID("628999", types.DefaultUserServer), RequestedAt: time.Unix(1794360100, 0)},
		},
		reqUpdate: []types.GroupParticipant{
			{JID: types.NewJID("628999", types.DefaultUserServer), Error: 409},
		},
	}
	mgr, grpJID := newTestGroupManager(fg, nil)

	// ListGroups pertama memanggil server; panggilan kedua berasal dari cache 60s.
	list1, err := mgr.ListGroups(context.Background(), "sales")
	if err != nil || len(list1) != 1 || !list1[0].IsAdmin {
		t.Fatalf("ListGroups pertama gagal: res=%+v err=%v", list1, err)
	}
	list2, err := mgr.ListGroups(context.Background(), "sales")
	if err != nil || len(list2) != 1 {
		t.Fatalf("ListGroups kedua gagal: err=%v", err)
	}
	if fg.callCount() != 1 {
		t.Fatalf("panggilan GetJoinedGroups seharusnya di-cache (1 kali), didapat %d", fg.callCount())
	}

	// CreateGroup dengan 0 peserta diperbolehkan dan menginvalidasi cache list.
	created, err := mgr.CreateGroup(context.Background(), "sales", CreateGroupRequest{Name: "Grup Kosong"})
	if err != nil || created.Name != "Grup Kosong" {
		t.Fatalf("CreateGroup 0 peserta gagal: res=%+v err=%v", created, err)
	}
	if _, err := mgr.ListGroups(context.Background(), "sales"); err != nil {
		t.Fatalf("ListGroups setelah invalidasi gagal: %v", err)
	}

	// Detail > 1024 peserta harus dipotong menjadi 1024 dengan ParticipantsTruncated=true.
	detail, err := mgr.GetGroup(context.Background(), "sales", grpJID)
	if err != nil || len(detail.Participants) != 1024 || !detail.ParticipantsTruncated {
		t.Fatalf("pemotongan 1024 peserta tidak sesuai: len=%d trunc=%v err=%v", len(detail.Participants), detail.ParticipantsTruncated, err)
	}

	// PATCH parsial: name sukses, description gagal 403 -> details.applied == ["name"].
	fg.setDescErr = &whatsmeow.IQError{Code: 403, Text: "forbidden"}
	newName := "Nama Baru"
	newDesc := "Desc Baru"
	lock := true
	_, err = mgr.PatchGroup(context.Background(), "sales", grpJID, PatchGroupRequest{
		Name:        &newName,
		Description: &newDesc,
		Locked:      &lock,
	})
	var ge *GroupError
	if !errors.As(err, &ge) || ge.Status != http.StatusForbidden {
		t.Fatalf("PATCH gagal description harus 403 GroupError, didapat %v", err)
	}
	applied, _ := ge.Details["applied"].([]string)
	if len(applied) != 1 || applied[0] != "name" {
		t.Fatalf("details.applied diharapkan ['name'], didapat %+v", ge.Details)
	}
	fg.setDescErr = nil

	// Join requests GET & POST.
	reqs, err := mgr.ListGroupRequests(context.Background(), "sales", grpJID)
	if err != nil || len(reqs) != 1 || reqs[0].RequestedAt != 1794360100 {
		t.Fatalf("ListGroupRequests gagal: %+v err=%v", reqs, err)
	}
	reqRes, err := mgr.HandleGroupRequests(context.Background(), "sales", grpJID, GroupRequestsAction{
		Action:       "approve",
		Participants: []string{"62899900"},
	})
	if err != nil || len(reqRes) != 1 || reqRes[0].Status != "already_member" {
		t.Fatalf("HandleGroupRequests gagal: %+v err=%v", reqRes, err)
	}
}
