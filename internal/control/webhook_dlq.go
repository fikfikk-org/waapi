package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/fikfikk-org/waapi/internal/store"
)

type dlqReplayReq struct {
	All  bool    `json:"all"`
	Seq  int64   `json:"seq"`
	Seqs []int64 `json:"seqs"`
}

func (s *Server) handleResumeWebhook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cur, err := s.store.GetInstance(r.Context(), id)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	next := cur.Settings
	next.WebhookPaused = ""
	st, err := s.wa.Update(r.Context(), id, store.InstanceUpdate{Settings: &next})
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleListWebhookDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dead == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("dead-letter queue webhook belum aktif"))
		return
	}
	id := r.PathValue("id")
	if _, err := s.store.GetInstance(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			limit = v
		}
	}
	items, err := s.dead.List(r.Context(), id, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleReplayWebhookDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dead == nil || s.webhookSvc == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("service webhook v2 belum aktif"))
		return
	}
	id := r.PathValue("id")
	if _, err := s.store.GetInstance(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}

	var req dlqReplayReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	var seqs []int64
	switch {
	case req.All:
		var err error
		seqs, err = s.dead.Seqs(r.Context(), id, 500)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
	case req.Seq > 0:
		seqs = []int64{req.Seq}
	case len(req.Seqs) > 0:
		seqs = req.Seqs
	default:
		writeErr(w, http.StatusBadRequest, errors.New("tentukan all=true atau seq/seqs untuk replay"))
		return
	}

	res, err := s.webhookSvc.Replay(r.Context(), id, seqs)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handlePurgeWebhookDLQ(w http.ResponseWriter, r *http.Request) {
	if s.dead == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("dead-letter queue webhook belum aktif"))
		return
	}
	id := r.PathValue("id")
	if _, err := s.store.GetInstance(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}
	purged, err := s.dead.Purge(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "purged", "purged": purged})
}
