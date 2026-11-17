package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/wa"
	"go.mau.fi/whatsmeow/types"
)

const maxMarkReadIDs = 50

type v2MarkReadRequest struct {
	MessageIDs []string `json:"message_ids"`
	Sender     string   `json:"sender"`
	Played     bool     `json:"played"`
}

type v2PresenceRequest struct {
	State string `json:"state"`
}

type v2SubscribePresenceRequest struct {
	JID string `json:"jid"`
}

// handleV2MarkRead mengirim tanda terima read atau played untuk maksimal 50 ID pesan sekaligus (204 No Content).
func (s *Server) handleV2MarkRead(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.chatDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan chat belum aktif", 0)
		return
	}

	chatJID, err := jid.ParseRecipient(r.PathValue("jid"))
	if err != nil {
		writeMappedError(w, err)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	var req v2MarkReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid", 0)
		return
	}
	if len(req.MessageIDs) == 0 || len(req.MessageIDs) > maxMarkReadIDs {
		writeError(w, http.StatusBadRequest, "invalid_request", "message_ids harus berisi 1 sampai 50 ID pesan", 0)
		return
	}

	ids := make([]types.MessageID, 0, len(req.MessageIDs))
	for _, raw := range req.MessageIDs {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "message_ids tidak boleh memuat elemen kosong", 0)
			return
		}
		ids = append(ids, types.MessageID(trimmed))
	}

	var senderJID types.JID
	if rawSender := strings.TrimSpace(req.Sender); rawSender != "" {
		parsed, pErr := jid.ParseRecipient(rawSender)
		if pErr != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "format sender tidak valid", 0)
			return
		}
		senderJID = parsed
	}

	if err := s.chatDriver.MarkRead(r.Context(), r.PathValue("id"), chatJID, senderJID, ids, req.Played); err != nil {
		writeMappedError(w, err)
		return
	}
	if s.ctxStore != nil {
		// No-op apabila context_store mati; mereset unread ke 0 jika aktif.
		_ = s.ctxStore.MarkChatRead(r.Context(), r.PathValue("id"), chatJID.String())
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleV2Presence mengatur status kehadiran global instance (available/unavailable).
func (s *Server) handleV2Presence(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.chatDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan chat belum aktif", 0)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	var req v2PresenceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid", 0)
		return
	}

	var state types.Presence
	switch strings.ToLower(strings.TrimSpace(req.State)) {
	case "available":
		state = types.PresenceAvailable
	case "unavailable":
		state = types.PresenceUnavailable
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", "state harus 'available' atau 'unavailable'", 0)
		return
	}

	if err := s.chatDriver.SendPresence(r.Context(), r.PathValue("id"), state); err != nil {
		writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleV2ChatPresence mengirim indikator mengetik/merekam suara ke sebuah percakapan.
func (s *Server) handleV2ChatPresence(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.chatDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan chat belum aktif", 0)
		return
	}

	chatJID, err := jid.ParseRecipient(r.PathValue("jid"))
	if err != nil {
		writeMappedError(w, err)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	var req v2PresenceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid", 0)
		return
	}

	var (
		state types.ChatPresence
		media types.ChatPresenceMedia
	)
	switch strings.ToLower(strings.TrimSpace(req.State)) {
	case "composing":
		state = types.ChatPresenceComposing
		media = types.ChatPresenceMediaText
	case "recording":
		state = types.ChatPresenceComposing
		media = types.ChatPresenceMediaAudio
	case "paused":
		state = types.ChatPresencePaused
		media = types.ChatPresenceMediaText
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", "state harus 'composing', 'recording', atau 'paused'", 0)
		return
	}

	if err := s.chatDriver.SendChatPresence(r.Context(), r.PathValue("id"), chatJID, state, media); err != nil {
		writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleV2SubscribePresence mendaftarkan pemantauan presence JID target (maks 100 per instance).
func (s *Server) handleV2SubscribePresence(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.chatDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan chat belum aktif", 0)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	var req v2SubscribePresenceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid", 0)
		return
	}

	targetJID, err := jid.ParseRecipient(req.JID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "jid tujuan tidak valid", 0)
		return
	}

	if err := s.chatDriver.SubscribePresence(r.Context(), r.PathValue("id"), targetJID); err != nil {
		writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleV2GetPollTally mengembalikan agregasi suara polling berdasarkan chat wajib dan wa_id pesan polling.
func (s *Server) handleV2GetPollTally(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.pollStore == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan polling belum aktif", 0)
		return
	}

	chatRaw := strings.TrimSpace(r.URL.Query().Get("chat"))
	if chatRaw == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "parameter query chat wajib diisi", 0)
		return
	}
	chatJID, err := jid.ParseRecipient(chatRaw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "parameter query chat tidak valid", 0)
		return
	}

	tally, err := s.pollStore.GetTally(r.Context(), r.PathValue("id"), chatJID.String(), r.PathValue("wa_id"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tally)
}
