package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/fikfikk-org/waapi/internal/wa"
	"go.mau.fi/whatsmeow/types"
)

type v2JoinGroupRequest struct {
	Code string `json:"code"`
	Link string `json:"link"`
}

func (s *Server) ensureGroupDriver(w http.ResponseWriter) bool {
	if s.groupDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan grup belum aktif", 0)
		return false
	}
	return true
}

func parseGroupJIDParam(w http.ResponseWriter, r *http.Request) (types.JID, bool) {
	raw := strings.TrimSpace(r.PathValue("jid"))
	parsed, err := types.ParseJID(raw)
	if err != nil || parsed.IsEmpty() || parsed.Server != types.GroupServer || parsed.User == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "JID grup harus berakhiran @g.us", 0)
		return types.EmptyJID, false
	}
	return parsed.ToNonAD(), true
}

func decodeGroupJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid", 0)
		return false
	}
	return true
}

// handleV2ListGroups mengembalikan ringkasan daftar grup yang diikuti instance.
func (s *Server) handleV2ListGroups(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	groups, err := s.groupDriver.ListGroups(r.Context(), r.PathValue("id"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": groups})
}

// handleV2CreateGroup membuat grup WhatsApp baru dan mengembalikan detailnya dengan status 201.
func (s *Server) handleV2CreateGroup(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	var req wa.CreateGroupRequest
	if !decodeGroupJSON(w, r, 32<<10, &req) {
		return
	}
	detail, err := s.groupDriver.CreateGroup(r.Context(), r.PathValue("id"), req)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, detail)
}

// handleV2GetGroup mengembalikan detail metadata dan peserta grup (maksimal 1024 peserta).
func (s *Server) handleV2GetGroup(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	grpJID, ok := parseGroupJIDParam(w, r)
	if !ok {
		return
	}
	detail, err := s.groupDriver.GetGroup(r.Context(), r.PathValue("id"), grpJID)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// handleV2PatchGroup menerapkan perubahan metadata grup berurutan dan mengembalikan details.applied jika gagal di tengah.
func (s *Server) handleV2PatchGroup(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	grpJID, ok := parseGroupJIDParam(w, r)
	if !ok {
		return
	}
	var req wa.PatchGroupRequest
	if !decodeGroupJSON(w, r, 32<<10, &req) {
		return
	}
	if req.Name == nil && req.Description == nil && req.Announce == nil && req.Locked == nil && req.JoinApproval == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "minimal satu field pembaruan grup wajib diisi", 0)
		return
	}
	detail, err := s.groupDriver.PatchGroup(r.Context(), r.PathValue("id"), grpJID, req)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// handleV2GroupParticipants menambah, menghapus, mempromosikan, atau menurunkan hak admin peserta grup.
func (s *Server) handleV2GroupParticipants(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	grpJID, ok := parseGroupJIDParam(w, r)
	if !ok {
		return
	}
	var req wa.UpdateParticipantsRequest
	if !decodeGroupJSON(w, r, 32<<10, &req) {
		return
	}
	results, err := s.groupDriver.UpdateGroupParticipants(r.Context(), r.PathValue("id"), grpJID, req)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// handleV2GroupInviteLink mengambil atau mereset tautan undangan grup.
func (s *Server) handleV2GroupInviteLink(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	grpJID, ok := parseGroupJIDParam(w, r)
	if !ok {
		return
	}
	var reset bool
	if rawReset := strings.TrimSpace(r.URL.Query().Get("reset")); rawReset != "" {
		parsedReset, err := strconv.ParseBool(rawReset)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "parameter reset harus bernilai true atau false", 0)
			return
		}
		reset = parsedReset
	}
	link, err := s.groupDriver.GetGroupInviteLink(r.Context(), r.PathValue("id"), grpJID, reset)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"link": link})
}

// handleV2JoinGroup menggabungkan instance ke grup melalui kode atau URL undangan.
func (s *Server) handleV2JoinGroup(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	var req v2JoinGroupRequest
	if !decodeGroupJSON(w, r, 16<<10, &req) {
		return
	}
	codeOrLink := strings.TrimSpace(req.Code)
	if codeOrLink == "" {
		codeOrLink = strings.TrimSpace(req.Link)
	}
	if codeOrLink == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "code atau link undangan wajib diisi", 0)
		return
	}
	joined, err := s.groupDriver.JoinGroup(r.Context(), r.PathValue("id"), codeOrLink)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"jid": joined.ToNonAD().String()})
}

// handleV2LeaveGroup mengeluarkan instance dari grup dan membalas 204 No Content.
func (s *Server) handleV2LeaveGroup(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	grpJID, ok := parseGroupJIDParam(w, r)
	if !ok {
		return
	}
	if err := s.groupDriver.LeaveGroup(r.Context(), r.PathValue("id"), grpJID); err != nil {
		writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleV2ListGroupRequests mengambil daftar permintaan bergabung yang menunggu persetujuan admin grup.
func (s *Server) handleV2ListGroupRequests(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	grpJID, ok := parseGroupJIDParam(w, r)
	if !ok {
		return
	}
	items, err := s.groupDriver.ListGroupRequests(r.Context(), r.PathValue("id"), grpJID)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"requests": items})
}

// handleV2HandleGroupRequests menyetujui atau menolak daftar permintaan bergabung ke grup.
func (s *Server) handleV2HandleGroupRequests(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if !s.ensureGroupDriver(w) {
		return
	}
	grpJID, ok := parseGroupJIDParam(w, r)
	if !ok {
		return
	}
	var req wa.GroupRequestsAction
	if !decodeGroupJSON(w, r, 32<<10, &req) {
		return
	}
	results, err := s.groupDriver.HandleGroupRequests(r.Context(), r.PathValue("id"), grpJID, req)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}
