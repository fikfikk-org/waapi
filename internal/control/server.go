// Package control menyediakan kanal komunikasi privat antara CLI dan daemon melalui Unix Domain Socket.
package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

// Server melayani seluruh perintah administratif dari CLI lewat control.sock.
type Server struct {
	cfg       *config.Config
	store     *store.Store
	auth      *auth.Manager
	wa        *wa.Manager
	webhook   *webhook.Dispatcher
	startedAt time.Time
}

// NewServer membangun Server control plane baru.
func NewServer(cfg *config.Config, st *store.Store, a *auth.Manager, w *wa.Manager, wh *webhook.Dispatcher) *Server {
	return &Server{
		cfg:       cfg,
		store:     st,
		auth:      a,
		wa:        w,
		webhook:   wh,
		startedAt: time.Now(),
	}
}

// Listen membuka Unix Domain Socket dengan permission 0660 agar group waapi dapat menjalankan CLI tanpa sudo.
func (s *Server) Listen() (net.Listener, error) {
	sock := s.cfg.ControlSocket()
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return nil, fmt.Errorf("listen unix socket %s: %w", sock, err)
	}
	// Permission 0660 mengizinkan user waapi dan anggota group waapi mengakses control socket
	_ = os.Chmod(sock, 0o660)
	return ln, nil
}

// Handler mengembalikan http.Handler untuk seluruh endpoint /control/*.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /control/doctor", s.handleDoctor)

	mux.HandleFunc("GET /control/keys", s.handleListKeys)
	mux.HandleFunc("POST /control/keys", s.handleCreateKey)
	mux.HandleFunc("DELETE /control/keys/{id}", s.handleRevokeKey)

	mux.HandleFunc("GET /control/instances", s.handleListInstances)
	mux.HandleFunc("POST /control/instances", s.handleCreateInstance)
	mux.HandleFunc("GET /control/instances/{id}", s.handleGetInstance)
	mux.HandleFunc("PATCH /control/instances/{id}", s.handleUpdateInstance)
	mux.HandleFunc("POST /control/instances/{id}/pair", s.handlePairInstance)
	mux.HandleFunc("POST /control/instances/{id}/restart", s.handleRestartInstance)
	mux.HandleFunc("POST /control/instances/{id}/stop", s.handleStopInstance)
	mux.HandleFunc("POST /control/instances/{id}/logout", s.handleLogoutInstance)
	mux.HandleFunc("DELETE /control/instances/{id}", s.handleDeleteInstance)
	mux.HandleFunc("POST /control/instances/{id}/webhook/test", s.handleTestWebhook)

	return mux
}

func (s *Server) handleDoctor(w http.ResponseWriter, r *http.Request) {
	outbox, _ := s.store.CountOutbox(r.Context())
	counts := s.wa.CountByStatus(r.Context())
	rep := obs.Collect(s.startedAt, s.cfg.AppDBPath(), s.cfg.WADBPath(), outbox, counts)
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.store.ListAPIKeys(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, keys)
}

type createKeyReq struct {
	Name       string `json:"name"`
	InstanceID string `json:"instance_id"`
}

type createKeyResp struct {
	Key  string       `json:"key"`
	Meta store.APIKey `json:"meta"`
}

func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	var req createKeyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeErr(w, http.StatusBadRequest, errors.New("nama key wajib diisi"))
		return
	}
	raw, meta, err := s.auth.Create(r.Context(), strings.TrimSpace(req.Name), strings.TrimSpace(req.InstanceID))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, createKeyResp{Key: raw, Meta: meta})
}

func (s *Server) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.auth.Revoke(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, http.StatusNotFound, errors.New("api key tidak ditemukan atau sudah dicabut"))
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked", "id": id})
}

func (s *Server) handleListInstances(w http.ResponseWriter, r *http.Request) {
	list, err := s.wa.ListStatuses(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

type createInstanceReq struct {
	ID         string `json:"id"`
	RatePerMin int    `json:"rate_per_min"`
	AutoStart  bool   `json:"auto_start"`
}

func (s *Server) handleCreateInstance(w http.ResponseWriter, r *http.Request) {
	var req createInstanceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.RatePerMin <= 0 {
		req.RatePerMin = 20
	}
	st, err := s.wa.Create(r.Context(), strings.TrimSpace(req.ID), req.RatePerMin, req.AutoStart)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, st)
}

func (s *Server) handleGetInstance(w http.ResponseWriter, r *http.Request) {
	st, err := s.wa.Status(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

type updateInstanceReq struct {
	WebhookURL    *string `json:"webhook_url"`
	WebhookEvents *string `json:"webhook_events"`
	RatePerMin    *int    `json:"rate_per_min"`
	AutoStart     *bool   `json:"auto_start"`
	RotateSecret  bool    `json:"rotate_secret"`
}

func (s *Server) handleUpdateInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req updateInstanceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	up := store.InstanceUpdate{
		WebhookURL:    req.WebhookURL,
		WebhookEvents: req.WebhookEvents,
		RatePerMin:    req.RatePerMin,
		AutoStart:     req.AutoStart,
	}
	if req.RotateSecret {
		sec, err := auth.GenerateSecret()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		up.WebhookSecret = &sec
	}
	st, err := s.wa.Update(r.Context(), id, up)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleRestartInstance(w http.ResponseWriter, r *http.Request) {
	st, err := s.wa.Restart(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleStopInstance(w http.ResponseWriter, r *http.Request) {
	st, err := s.wa.Stop(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleLogoutInstance(w http.ResponseWriter, r *http.Request) {
	st, err := s.wa.Logout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleDeleteInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.wa.Delete(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

func (s *Server) handleTestWebhook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	code, err := s.webhook.SendTest(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "delivered", "http_status": code})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeStatusErr(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, errors.New("instance tidak ditemukan"))
		return
	}
	writeErr(w, http.StatusBadRequest, err)
}

