package wa

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

const (
	maxGroupNameRunes          = 25
	maxGroupBatchParticipants  = 50
	maxGroupDetailParticipants = 1024
)

// groupClient adalah subset *whatsmeow.Client yang dipakai API grup (signature persis versi pin).
type groupClient interface {
	GetJoinedGroups(ctx context.Context) ([]*types.GroupInfo, error)
	GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error)
	CreateGroup(ctx context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error)
	UpdateGroupParticipants(ctx context.Context, jid types.JID, changes []types.JID, action whatsmeow.ParticipantChange) ([]types.GroupParticipant, error)
	SetGroupName(ctx context.Context, jid types.JID, name string) error
	SetGroupDescription(ctx context.Context, jid types.JID, description string) error
	SetGroupAnnounce(ctx context.Context, jid types.JID, announce bool) error
	SetGroupLocked(ctx context.Context, jid types.JID, locked bool) error
	SetGroupJoinApprovalMode(ctx context.Context, jid types.JID, mode bool) error
	GetGroupInviteLink(ctx context.Context, jid types.JID, reset bool) (string, error)
	JoinGroupWithLink(ctx context.Context, code string) (types.JID, error)
	LeaveGroup(ctx context.Context, jid types.JID) error
	GetGroupRequestParticipants(ctx context.Context, jid types.JID) ([]types.GroupParticipantRequest, error)
	UpdateGroupRequestParticipants(ctx context.Context, jid types.JID, changes []types.JID, action whatsmeow.ParticipantRequestChange) ([]types.GroupParticipant, error)
}

// GroupGovernor dipanggil sebelum membuat grup atau menambah peserta (diisi Guardian di Batch 9).
type GroupGovernor interface {
	AllowGroupCreate(ctx context.Context, instanceID string, participants []string) error
	AllowParticipantAdd(ctx context.Context, instanceID string, participants []string) error
}

type noopGroupGovernor struct{}

func (noopGroupGovernor) AllowGroupCreate(context.Context, string, []string) error    { return nil }
func (noopGroupGovernor) AllowParticipantAdd(context.Context, string, []string) error { return nil }

// WithGroupGovernor memasang GroupGovernor kustom pada Manager untuk kontrol laju pembuatan/penambahan grup.
func WithGroupGovernor(gov GroupGovernor) ManagerOption {
	return func(m *Manager) {
		if gov != nil {
			m.groupGov = gov
		}
	}
}

// GroupError membawa status HTTP, kode mesin, retry_after, dan detail opsional untuk respons endpoint grup & kontak.
type GroupError struct {
	Status     int
	Code       string
	Message    string
	RetryAfter int
	Details    map[string]any
	Err        error
}

// NewGroupError membuat *GroupError terstruktur untuk pemetaan respons HTTP.
func NewGroupError(status int, code, message string, retryAfter int) *GroupError {
	return &GroupError{
		Status:     status,
		Code:       code,
		Message:    message,
		RetryAfter: retryAfter,
	}
}

func (e *GroupError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Code
}

func (e *GroupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// HTTPStatus mengembalikan kode status HTTP untuk pemetaan error terstruktur.
func (e *GroupError) HTTPStatus() int { return e.Status }

// ErrorCode mengembalikan kode mesin error API.
func (e *GroupError) ErrorCode() string { return e.Code }

// ErrorRetryAfter mengembalikan durasi tunggu dalam detik bila ada.
func (e *GroupError) ErrorRetryAfter() int { return e.RetryAfter }

// ErrorDetails mengembalikan metadata terstruktur tambahan bila ada.
func (e *GroupError) ErrorDetails() map[string]any { return e.Details }

// GroupSummary adalah ringkasan grup tanpa daftar peserta untuk endpoint daftar grup.
type GroupSummary struct {
	JID       string `json:"jid"`
	Name      string `json:"name"`
	Size      int    `json:"size"`
	IsAdmin   bool   `json:"is_admin"`
	Announce  bool   `json:"announce"`
	Locked    bool   `json:"locked"`
	CreatedAt int64  `json:"created_at"`
}

// GroupParticipantView merangkum peran satu peserta pada respons detail grup.
type GroupParticipantView struct {
	JID          string `json:"jid"`
	PN           string `json:"pn,omitempty"`
	IsAdmin      bool   `json:"is_admin"`
	IsSuperAdmin bool   `json:"is_super_admin"`
}

// GroupDetail memuat metadata grup lengkap dengan batas maksimal 1024 peserta.
type GroupDetail struct {
	JID                   string                 `json:"jid"`
	Name                  string                 `json:"name"`
	Description           string                 `json:"description"`
	Owner                 string                 `json:"owner"`
	CreatedAt             int64                  `json:"created_at"`
	Announce              bool                   `json:"announce"`
	Locked                bool                   `json:"locked"`
	JoinApproval          bool                   `json:"join_approval"`
	EphemeralSeconds      uint32                 `json:"ephemeral_seconds"`
	Participants          []GroupParticipantView `json:"participants"`
	ParticipantsTruncated bool                   `json:"participants_truncated,omitempty"`
}

// CreateGroupRequest mendeskripsikan payload pembuatan grup baru.
type CreateGroupRequest struct {
	Name         string   `json:"name"`
	Participants []string `json:"participants"`
}

// PatchGroupRequest mendeskripsikan perubahan metadata grup secara berurutan.
type PatchGroupRequest struct {
	Name         *string `json:"name"`
	Description  *string `json:"description"`
	Announce     *bool   `json:"announce"`
	Locked       *bool   `json:"locked"`
	JoinApproval *bool   `json:"join_approval"`
}

// UpdateParticipantsRequest mendeskripsikan mutasi daftar peserta grup.
type UpdateParticipantsRequest struct {
	Action       string   `json:"action"`
	Participants []string `json:"participants"`
}

// ParticipantResult merangkum status hasil operasi per peserta grup atau permintaan bergabung.
type ParticipantResult struct {
	JID    string `json:"jid"`
	Status string `json:"status"`
}

// GroupJoinRequestItem merangkum satu permintaan bergabung yang menunggu persetujuan admin grup.
type GroupJoinRequestItem struct {
	JID         string `json:"jid"`
	RequestedAt int64  `json:"requested_at"`
}

// GroupRequestsAction mendeskripsikan aksi persetujuan atau penolakan permintaan bergabung grup.
type GroupRequestsAction struct {
	Action       string   `json:"action"`
	Participants []string `json:"participants"`
}

// MapGroupIQError memetakan error dari whatsmeow (*whatsmeow.IQError atau sentinel grup) ke *GroupError sesuai kontrak v2.
func MapGroupIQError(err error) error {
	if err == nil {
		return nil
	}
	var ge *GroupError
	if errors.As(err, &ge) {
		return ge
	}
	if errors.Is(err, ErrNotConnected) || errors.Is(err, store.ErrNotFound) || errors.Is(err, ErrInvalidRecipient) || errors.Is(err, jid.ErrInvalidRecipient) {
		return err
	}

	var iqErr *whatsmeow.IQError
	if errors.As(err, &iqErr) {
		switch iqErr.Code {
		case 401, 403:
			return &GroupError{Status: http.StatusForbidden, Code: "group_forbidden", Message: "akses ke grup ditolak oleh WhatsApp", Err: err}
		case 404:
			return &GroupError{Status: http.StatusNotFound, Code: "group_not_found", Message: "grup tidak ditemukan", Err: err}
		case 406:
			return &GroupError{Status: http.StatusBadRequest, Code: "invalid_request", Message: "permintaan ditolak oleh WhatsApp (406)", Err: err}
		case 429:
			return &GroupError{Status: http.StatusTooManyRequests, Code: "rate_limited", Message: "permintaan terlalu cepat ke server WhatsApp", Err: err}
		default:
			return &GroupError{Status: http.StatusBadGateway, Code: "upstream_error", Message: "server WhatsApp mengembalikan galat", Err: err}
		}
	}

	switch {
	case errors.Is(err, whatsmeow.ErrNotInGroup), errors.Is(err, whatsmeow.ErrGroupInviteLinkUnauthorized):
		return &GroupError{Status: http.StatusForbidden, Code: "group_forbidden", Message: err.Error(), Err: err}
	case errors.Is(err, whatsmeow.ErrGroupNotFound):
		return &GroupError{Status: http.StatusNotFound, Code: "group_not_found", Message: err.Error(), Err: err}
	case errors.Is(err, whatsmeow.ErrInviteLinkInvalid):
		return &GroupError{Status: http.StatusBadRequest, Code: "invalid_request", Message: err.Error(), Err: err}
	default:
		return &GroupError{Status: http.StatusBadGateway, Code: "upstream_error", Message: "server WhatsApp mengembalikan galat", Err: err}
	}
}

func (m *Manager) activeGroupGovernor() GroupGovernor {
	if m != nil && m.groupGov != nil {
		return m.groupGov
	}
	return noopGroupGovernor{}
}

func (m *Manager) invalidateGroupListCache(instanceID string) {
	if m != nil && m.groupListLRU != nil {
		m.groupListLRU.Delete(instanceID)
	}
}

func (m *Manager) groupTransport(instanceID string) (groupClient, *Instance, error) {
	inst, ok := m.Get(instanceID)
	if !ok {
		return nil, nil, store.ErrNotFound
	}
	if inst.groupCli != nil {
		return inst.groupCli, inst, nil
	}
	cli := inst.getClient()
	if cli == nil || !cli.IsConnected() {
		return nil, inst, ErrNotConnected
	}
	return cli, inst, nil
}

// ListGroups mengembalikan ringkasan seluruh grup yang diikuti instance dengan cache 60 detik tanpa daftar peserta.
func (m *Manager) ListGroups(ctx context.Context, instanceID string) ([]GroupSummary, error) {
	if m == nil {
		return nil, store.ErrNotFound
	}
	_, _, err := m.groupTransport(instanceID)
	if err != nil {
		return nil, err
	}
	if m.groupListLRU != nil {
		if cached, ok := m.groupListLRU.Get(instanceID); ok {
			out := make([]GroupSummary, len(cached))
			copy(out, cached)
			return out, nil
		}
	}

	gcli, inst, err := m.groupTransport(instanceID)
	if err != nil {
		return nil, err
	}
	raw, err := gcli.GetJoinedGroups(ctx)
	if err != nil {
		return nil, MapGroupIQError(err)
	}

	ownJIDs := inst.ownUserJIDs()
	summaries := make([]GroupSummary, 0, len(raw))
	for _, g := range raw {
		if g == nil {
			continue
		}
		summaries = append(summaries, buildGroupSummary(g, ownJIDs))
	}

	if m.groupListLRU != nil {
		cp := make([]GroupSummary, len(summaries))
		copy(cp, summaries)
		m.groupListLRU.Set(instanceID, cp)
	}
	return summaries, nil
}

// CreateGroup memvalidasi input, memeriksa GroupGovernor, lalu membuat grup WhatsApp baru.
func (m *Manager) CreateGroup(ctx context.Context, instanceID string, req CreateGroupRequest) (GroupDetail, error) {
	name := strings.TrimSpace(req.Name)
	nameRunes := utf8.RuneCountInString(name)
	if nameRunes == 0 || nameRunes > maxGroupNameRunes {
		return GroupDetail{}, NewGroupError(http.StatusBadRequest, "invalid_request", "nama grup wajib 1 sampai 25 karakter", 0)
	}
	if len(req.Participants) > maxGroupBatchParticipants {
		return GroupDetail{}, NewGroupError(http.StatusBadRequest, "invalid_request", "jumlah peserta awal maksimal 50", 0)
	}

	partJIDs, partStrs, err := parseUserParticipants(req.Participants, true)
	if err != nil {
		return GroupDetail{}, err
	}

	if _, ok := m.Get(instanceID); !ok {
		return GroupDetail{}, store.ErrNotFound
	}
	if err := m.activeGroupGovernor().AllowGroupCreate(ctx, instanceID, partStrs); err != nil {
		return GroupDetail{}, err
	}

	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return GroupDetail{}, err
	}
	created, err := gcli.CreateGroup(ctx, whatsmeow.ReqCreateGroup{
		Name:         name,
		Participants: partJIDs,
	})
	if err != nil {
		return GroupDetail{}, MapGroupIQError(err)
	}
	m.invalidateGroupListCache(instanceID)
	return buildGroupDetail(created), nil
}

// GetGroup mengambil detail metadata sebuah grup dengan batas maksimal 1024 peserta.
func (m *Manager) GetGroup(ctx context.Context, instanceID string, groupJID types.JID) (GroupDetail, error) {
	if err := validateGroupJID(groupJID); err != nil {
		return GroupDetail{}, err
	}
	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return GroupDetail{}, err
	}
	info, err := gcli.GetGroupInfo(ctx, groupJID)
	if err != nil {
		return GroupDetail{}, MapGroupIQError(err)
	}
	return buildGroupDetail(info), nil
}

// PatchGroup menerapkan perubahan metadata grup secara berurutan dan mengembalikan details.applied jika gagal di tengah.
func (m *Manager) PatchGroup(ctx context.Context, instanceID string, groupJID types.JID, req PatchGroupRequest) (GroupDetail, error) {
	if err := validateGroupJID(groupJID); err != nil {
		return GroupDetail{}, err
	}
	if req.Name == nil && req.Description == nil && req.Announce == nil && req.Locked == nil && req.JoinApproval == nil {
		return GroupDetail{}, NewGroupError(http.StatusBadRequest, "invalid_request", "minimal satu field pembaruan grup wajib diisi", 0)
	}
	var trimmedName string
	if req.Name != nil {
		trimmedName = strings.TrimSpace(*req.Name)
		n := utf8.RuneCountInString(trimmedName)
		if n == 0 || n > maxGroupNameRunes {
			return GroupDetail{}, NewGroupError(http.StatusBadRequest, "invalid_request", "nama grup wajib 1 sampai 25 karakter", 0)
		}
	}

	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return GroupDetail{}, err
	}

	type patchStep struct {
		field string
		fn    func() error
	}
	var steps []patchStep
	if req.Name != nil {
		steps = append(steps, patchStep{"name", func() error { return gcli.SetGroupName(ctx, groupJID, trimmedName) }})
	}
	if req.Description != nil {
		desc := *req.Description
		steps = append(steps, patchStep{"description", func() error { return gcli.SetGroupDescription(ctx, groupJID, desc) }})
	}
	if req.Announce != nil {
		v := *req.Announce
		steps = append(steps, patchStep{"announce", func() error { return gcli.SetGroupAnnounce(ctx, groupJID, v) }})
	}
	if req.Locked != nil {
		v := *req.Locked
		steps = append(steps, patchStep{"locked", func() error { return gcli.SetGroupLocked(ctx, groupJID, v) }})
	}
	if req.JoinApproval != nil {
		v := *req.JoinApproval
		steps = append(steps, patchStep{"join_approval", func() error { return gcli.SetGroupJoinApprovalMode(ctx, groupJID, v) }})
	}

	applied := make([]string, 0, len(steps))
	for _, step := range steps {
		if err := step.fn(); err != nil {
			if len(applied) > 0 {
				m.invalidateGroupListCache(instanceID)
			}
			return GroupDetail{}, attachAppliedDetails(MapGroupIQError(err), applied)
		}
		applied = append(applied, step.field)
	}

	m.invalidateGroupListCache(instanceID)
	info, err := gcli.GetGroupInfo(ctx, groupJID)
	if err != nil {
		return GroupDetail{}, attachAppliedDetails(MapGroupIQError(err), applied)
	}
	return buildGroupDetail(info), nil
}

// UpdateGroupParticipants memvalidasi aksi dan peserta (1..50), memeriksa governor saat add, lalu mengembalikan status per peserta.
func (m *Manager) UpdateGroupParticipants(ctx context.Context, instanceID string, groupJID types.JID, req UpdateParticipantsRequest) ([]ParticipantResult, error) {
	if err := validateGroupJID(groupJID); err != nil {
		return nil, err
	}
	var waAction whatsmeow.ParticipantChange
	switch strings.ToLower(strings.TrimSpace(req.Action)) {
	case "add":
		waAction = whatsmeow.ParticipantChangeAdd
	case "remove":
		waAction = whatsmeow.ParticipantChangeRemove
	case "promote":
		waAction = whatsmeow.ParticipantChangePromote
	case "demote":
		waAction = whatsmeow.ParticipantChangeDemote
	default:
		return nil, NewGroupError(http.StatusBadRequest, "invalid_request", "action harus add, remove, promote, atau demote", 0)
	}

	partJIDs, partStrs, err := parseUserParticipants(req.Participants, false)
	if err != nil {
		return nil, err
	}

	if _, ok := m.Get(instanceID); !ok {
		return nil, store.ErrNotFound
	}
	if waAction == whatsmeow.ParticipantChangeAdd {
		if err := m.activeGroupGovernor().AllowParticipantAdd(ctx, instanceID, partStrs); err != nil {
			return nil, err
		}
	}

	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return nil, err
	}
	res, err := gcli.UpdateGroupParticipants(ctx, groupJID, partJIDs, waAction)
	if err != nil {
		return nil, MapGroupIQError(err)
	}
	m.invalidateGroupListCache(instanceID)
	return mapParticipantResults(res), nil
}

// GetGroupInviteLink mengambil atau mereset tautan undangan grup.
func (m *Manager) GetGroupInviteLink(ctx context.Context, instanceID string, groupJID types.JID, reset bool) (string, error) {
	if err := validateGroupJID(groupJID); err != nil {
		return "", err
	}
	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return "", err
	}
	link, err := gcli.GetGroupInviteLink(ctx, groupJID, reset)
	if err != nil {
		return "", MapGroupIQError(err)
	}
	return link, nil
}

// JoinGroup bergabung ke grup menggunakan kode atau URL undangan WhatsApp.
func (m *Manager) JoinGroup(ctx context.Context, instanceID string, codeOrLink string) (types.JID, error) {
	code := extractInviteCode(codeOrLink)
	if code == "" || strings.ContainsAny(code, " \t\r\n/") {
		return types.EmptyJID, NewGroupError(http.StatusBadRequest, "invalid_request", "kode atau tautan undangan grup tidak valid", 0)
	}
	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return types.EmptyJID, err
	}
	joined, err := gcli.JoinGroupWithLink(ctx, code)
	if err != nil {
		return types.EmptyJID, MapGroupIQError(err)
	}
	m.invalidateGroupListCache(instanceID)
	return joined, nil
}

// LeaveGroup mengeluarkan instance dari grup yang ditentukan.
func (m *Manager) LeaveGroup(ctx context.Context, instanceID string, groupJID types.JID) error {
	if err := validateGroupJID(groupJID); err != nil {
		return err
	}
	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return err
	}
	if err := gcli.LeaveGroup(ctx, groupJID); err != nil {
		return MapGroupIQError(err)
	}
	m.invalidateGroupListCache(instanceID)
	return nil
}

// ListGroupRequests mengambil daftar permintaan bergabung grup yang sedang menunggu persetujuan.
func (m *Manager) ListGroupRequests(ctx context.Context, instanceID string, groupJID types.JID) ([]GroupJoinRequestItem, error) {
	if err := validateGroupJID(groupJID); err != nil {
		return nil, err
	}
	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return nil, err
	}
	reqs, err := gcli.GetGroupRequestParticipants(ctx, groupJID)
	if err != nil {
		return nil, MapGroupIQError(err)
	}
	out := make([]GroupJoinRequestItem, 0, len(reqs))
	for _, r := range reqs {
		var ts int64
		if !r.RequestedAt.IsZero() {
			ts = r.RequestedAt.Unix()
		}
		out = append(out, GroupJoinRequestItem{
			JID:         r.JID.ToNonAD().String(),
			RequestedAt: ts,
		})
	}
	return out, nil
}

// HandleGroupRequests menyetujui atau menolak daftar permintaan bergabung ke grup.
func (m *Manager) HandleGroupRequests(ctx context.Context, instanceID string, groupJID types.JID, req GroupRequestsAction) ([]ParticipantResult, error) {
	if err := validateGroupJID(groupJID); err != nil {
		return nil, err
	}
	var waAction whatsmeow.ParticipantRequestChange
	switch strings.ToLower(strings.TrimSpace(req.Action)) {
	case "approve":
		waAction = whatsmeow.ParticipantChangeApprove
	case "reject":
		waAction = whatsmeow.ParticipantChangeReject
	default:
		return nil, NewGroupError(http.StatusBadRequest, "invalid_request", "action harus approve atau reject", 0)
	}
	partJIDs, _, err := parseUserParticipants(req.Participants, false)
	if err != nil {
		return nil, err
	}

	gcli, _, err := m.groupTransport(instanceID)
	if err != nil {
		return nil, err
	}
	res, err := gcli.UpdateGroupRequestParticipants(ctx, groupJID, partJIDs, waAction)
	if err != nil {
		return nil, MapGroupIQError(err)
	}
	return mapParticipantResults(res), nil
}

func validateGroupJID(g types.JID) error {
	if g.IsEmpty() || g.Server != types.GroupServer || g.User == "" {
		return NewGroupError(http.StatusBadRequest, "invalid_request", "JID grup harus berakhiran @g.us", 0)
	}
	return nil
}

func parseUserParticipants(raw []string, allowEmpty bool) ([]types.JID, []string, error) {
	if !allowEmpty && len(raw) == 0 {
		return nil, nil, NewGroupError(http.StatusBadRequest, "invalid_request", "participants wajib berisi 1 sampai 50 anggota", 0)
	}
	if len(raw) > maxGroupBatchParticipants {
		return nil, nil, NewGroupError(http.StatusBadRequest, "invalid_request", "participants maksimal 50 anggota", 0)
	}

	jids := make([]types.JID, 0, len(raw))
	strs := make([]string, 0, len(raw))
	for _, item := range raw {
		parsed, err := jid.ParseRecipient(item)
		if err != nil {
			return nil, nil, err
		}
		nonAD := parsed.ToNonAD()
		if nonAD.Server != types.DefaultUserServer && nonAD.Server != types.HiddenUserServer {
			return nil, nil, NewGroupError(http.StatusBadRequest, "invalid_request", "peserta harus berupa nomor telepon atau JID pengguna", 0)
		}
		jids = append(jids, nonAD)
		strs = append(strs, nonAD.String())
	}
	return jids, strs, nil
}

func extractInviteCode(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		if u, err := url.Parse(s); err == nil {
			path := strings.Trim(u.Path, "/")
			if idx := strings.LastIndex(path, "/"); idx >= 0 {
				path = path[idx+1:]
			}
			return strings.TrimSpace(path)
		}
	}
	s = strings.TrimPrefix(s, whatsmeow.InviteLinkPrefix)
	if idx := strings.IndexByte(s, '?'); idx >= 0 {
		s = s[:idx]
	}
	return strings.TrimSpace(strings.Trim(s, "/"))
}

func attachAppliedDetails(err error, applied []string) error {
	if err == nil {
		return nil
	}
	var ge *GroupError
	if errors.As(err, &ge) {
		copied := *ge
		cp := make([]string, len(applied))
		copy(cp, applied)
		copied.Details = map[string]any{"applied": cp}
		return &copied
	}
	return err
}

func mapParticipantResults(raw []types.GroupParticipant) []ParticipantResult {
	out := make([]ParticipantResult, 0, len(raw))
	for _, p := range raw {
		out = append(out, ParticipantResult{
			JID:    p.JID.ToNonAD().String(),
			Status: participantStatus(p.Error),
		})
	}
	return out
}

func participantStatus(code int) string {
	switch code {
	case 0:
		return "ok"
	case 403:
		return "privacy_requires_invite"
	case 409:
		return "already_member"
	case 404:
		return "not_on_whatsapp"
	default:
		return "error_" + strconv.Itoa(code)
	}
}

func (i *Instance) ownUserJIDs() []types.JID {
	var out []types.JID
	if cli := i.getClient(); cli != nil && cli.Store != nil {
		if cli.Store.ID != nil && !cli.Store.ID.IsEmpty() {
			out = append(out, cli.Store.ID.ToNonAD())
		}
		if !cli.Store.LID.IsEmpty() {
			out = append(out, cli.Store.LID.ToNonAD())
		}
	}
	i.mu.RLock()
	rawJID := i.jid
	i.mu.RUnlock()
	if rawJID != "" {
		if parsed, err := types.ParseJID(rawJID); err == nil && !parsed.IsEmpty() {
			out = append(out, parsed.ToNonAD())
		}
	}
	return out
}

func buildGroupSummary(g *types.GroupInfo, ownJIDs []types.JID) GroupSummary {
	size := len(g.Participants)
	if g.ParticipantCount > size {
		size = g.ParticipantCount
	}
	var isAdmin bool
	for _, p := range g.Participants {
		if !p.IsAdmin && !p.IsSuperAdmin {
			continue
		}
		for _, own := range ownJIDs {
			if (!p.JID.IsEmpty() && p.JID.ToNonAD() == own) ||
				(!p.PhoneNumber.IsEmpty() && p.PhoneNumber.ToNonAD() == own) ||
				(!p.LID.IsEmpty() && p.LID.ToNonAD() == own) {
				isAdmin = true
				break
			}
		}
		if isAdmin {
			break
		}
	}
	var created int64
	if !g.GroupCreated.IsZero() {
		created = g.GroupCreated.Unix()
	}
	return GroupSummary{
		JID:       g.JID.ToNonAD().String(),
		Name:      g.Name,
		Size:      size,
		IsAdmin:   isAdmin,
		Announce:  g.IsAnnounce,
		Locked:    g.IsLocked,
		CreatedAt: created,
	}
}

func buildGroupDetail(g *types.GroupInfo) GroupDetail {
	if g == nil {
		return GroupDetail{Participants: []GroupParticipantView{}}
	}
	owner := jidStringOrEmpty(g.OwnerPN)
	if owner == "" {
		owner = jidStringOrEmpty(g.OwnerJID)
	}
	var created int64
	if !g.GroupCreated.IsZero() {
		created = g.GroupCreated.Unix()
	}
	var ephem uint32
	if g.IsEphemeral {
		ephem = g.DisappearingTimer
	}

	limit := len(g.Participants)
	truncated := false
	if limit > maxGroupDetailParticipants {
		limit = maxGroupDetailParticipants
		truncated = true
	}
	parts := make([]GroupParticipantView, 0, limit)
	for _, p := range g.Participants[:limit] {
		pnStr := jidStringOrEmpty(p.PhoneNumber)
		if pnStr == "" && p.JID.Server == types.DefaultUserServer {
			pnStr = p.JID.ToNonAD().String()
		}
		parts = append(parts, GroupParticipantView{
			JID:          p.JID.ToNonAD().String(),
			PN:           pnStr,
			IsAdmin:      p.IsAdmin,
			IsSuperAdmin: p.IsSuperAdmin,
		})
	}

	return GroupDetail{
		JID:                   g.JID.ToNonAD().String(),
		Name:                  g.Name,
		Description:           g.Topic,
		Owner:                 owner,
		CreatedAt:             created,
		Announce:              g.IsAnnounce,
		Locked:                g.IsLocked,
		JoinApproval:          g.IsJoinApprovalRequired,
		EphemeralSeconds:      ephem,
		Participants:          parts,
		ParticipantsTruncated: truncated,
	}
}

var _ = fmt.Sprintf
