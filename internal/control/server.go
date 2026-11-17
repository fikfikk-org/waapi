// Package control menyediakan kanal komunikasi privat antara CLI dan daemon melalui Unix Domain Socket.
package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/guardian"
	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/mcpserver"
	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

type peerUIDKey struct{}

// ServerOption mengatur opsi dependensi tambahan untuk Server control plane.
type ServerOption func(*Server)

// WithJournal memasang Journal v2 ke control plane untuk inspeksi event dan diagnostik DB.
func WithJournal(j *journal.Journal) ServerOption {
	return func(s *Server) {
		if j != nil {
			s.journal = j
		}
	}
}

// WithWebhookService memasang service webhook v2 untuk test, replay DLQ, dan kontrol target.
func WithWebhookService(ws *webhook.Service, dead *webhook.DeadStore) ServerOption {
	return func(s *Server) {
		s.webhookSvc = ws
		if dead != nil {
			s.dead = dead
		}
	}
}

// WithOutbox memasang service durable outbox untuk pengiriman pesan melalui CLI control plane.
func WithOutbox(svc *outbox.Service) ServerOption {
	return func(s *Server) {
		if svc != nil {
			s.outbox = svc
		}
	}
}

type healthProvider interface {
	Health(ctx context.Context, instanceID string) (guardian.HealthReport, error)
}

// WithHealthProvider memasang Guardian ke control plane untuk perintah CLI waapi health.
func WithHealthProvider(hp healthProvider) ServerOption {
	return func(s *Server) {
		if hp != nil {
			s.health = hp
		}
	}
}

// WithGovernor memasang sumber level tekanan memori Governor agar dilaporkan oleh waapi doctor.
func WithGovernor(src governor.Source) ServerOption {
	return func(s *Server) {
		if src != nil {
			s.gov = src
		}
	}
}

// WithAuditLog memasang pencatat AuditLog ke control plane.
func WithAuditLog(al *auth.AuditLog) ServerOption {
	return func(s *Server) {
		if al != nil {
			s.audit = al
		}
	}
}

// WithContextStore memasang Store context store ke control plane untuk perintah waapi context.
func WithContextStore(cs *ctxstore.Store) ServerOption {
	return func(s *Server) {
		if cs != nil {
			s.ctxStore = cs
		}
	}
}

// WithMCP memasang server MCP ke control plane untuk endpoint /control/mcp.
func WithMCP(ms *mcpserver.Server) ServerOption {
	return func(s *Server) {
		if ms != nil {
			s.mcpSrv = ms
		}
	}
}

// Server melayani seluruh perintah administratif dari CLI lewat control.sock.
type Server struct {
	cfg        *config.Config
	store      *store.Store
	auth       *auth.Manager
	wa         *wa.Manager
	webhook    *webhook.Dispatcher
	webhookSvc *webhook.Service
	dead       *webhook.DeadStore
	journal    *journal.Journal
	outbox     *outbox.Service
	health     healthProvider
	gov        governor.Source
	audit      *auth.AuditLog
	ctxStore   *ctxstore.Store
	mcpSrv     *mcpserver.Server
	startedAt  time.Time
	version    string
}

// NewServer membangun Server control plane baru.
func NewServer(cfg *config.Config, st *store.Store, a *auth.Manager, w *wa.Manager, wh *webhook.Dispatcher, opts ...ServerOption) *Server {
	s := &Server{
		cfg:       cfg,
		store:     st,
		auth:      a,
		wa:        w,
		webhook:   wh,
		gov:       governor.AlwaysGreen,
		startedAt: time.Now(),
		version:   "dev",
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.journal == nil && w != nil {
		s.journal = w.Journal()
	}
	if s.dead == nil && s.journal != nil && s.journal.DB() != nil {
		s.dead = webhook.NewDeadStore(s.journal.DB())
	}
	if s.audit == nil && st != nil && st.DB != nil {
		s.audit = auth.NewAuditLog(st.DB)
	}
	if s.ctxStore == nil && w != nil {
		s.ctxStore = w.ContextStore()
	}
	return s
}

// SetVersion menyetel string versi aplikasi untuk dilaporkan oleh diagnostik.
func (s *Server) SetVersion(v string) {
	if strings.TrimSpace(v) != "" {
		s.version = strings.TrimSpace(v)
	}
}

// Listen membuka Unix Domain Socket secara atomik (listen tmp -> chmod 0660 -> rename) tanpa jendela izin terbuka.
func (s *Server) Listen() (net.Listener, error) {
	sock := s.cfg.ControlSocket()
	tmpSock := sock + ".tmp"
	_ = os.Remove(tmpSock)

	ln, err := net.Listen("unix", tmpSock)
	if err != nil {
		return nil, fmt.Errorf("listen unix socket %s: %w", sock, err)
	}
	if ul, ok := ln.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}
	if runtime.GOOS != "windows" {
		// Setel izin 0660 sebelum rename agar tidak ada jeda izin umask proses.
		if err := os.Chmod(tmpSock, 0o660); err != nil {
			_ = ln.Close()
			_ = os.Remove(tmpSock)
			return nil, fmt.Errorf("chmod control socket %s: %w", tmpSock, err)
		}
	}
	_ = os.Remove(sock)
	if err := os.Rename(tmpSock, sock); err != nil {
		_ = ln.Close()
		_ = os.Remove(tmpSock)
		return nil, fmt.Errorf("rename control socket %s: %w", sock, err)
	}
	return ln, nil
}

// Handler mengembalikan http.Handler untuk seluruh endpoint /control/*.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /control/doctor", s.handleDoctor)
	mux.HandleFunc("GET /control/audit", s.handleListAudit)
	mux.HandleFunc("POST /control/backup", s.handleBackup)

	mux.HandleFunc("GET /control/keys", s.handleListKeys)
	mux.HandleFunc("POST /control/keys", s.handleCreateKey)
	mux.HandleFunc("GET /control/keys/{id}", s.handleGetKey)
	mux.HandleFunc("POST /control/keys/{id}/rotate", s.handleRotateKey)
	mux.HandleFunc("DELETE /control/keys/{id}", s.handleRevokeKey)

	mux.HandleFunc("GET /control/instances", s.handleListInstances)
	mux.HandleFunc("POST /control/instances", s.handleCreateInstance)
	mux.HandleFunc("GET /control/instances/{id}", s.handleGetInstance)
	mux.HandleFunc("PATCH /control/instances/{id}", s.handleUpdateInstance)
	mux.HandleFunc("POST /control/instances/{id}/pair", s.handlePairInstance)
	mux.HandleFunc("POST /control/instances/{id}/restart", s.handleRestartInstance)
	mux.HandleFunc("POST /control/instances/{id}/recycle", s.handleRecycleInstance)
	mux.HandleFunc("POST /control/instances/{id}/stop", s.handleStopInstance)
	mux.HandleFunc("POST /control/instances/{id}/logout", s.handleLogoutInstance)
	mux.HandleFunc("DELETE /control/instances/{id}", s.handleDeleteInstance)
	mux.HandleFunc("POST /control/instances/{id}/webhook/test", s.handleTestWebhook)
	mux.HandleFunc("POST /control/instances/{id}/webhook/resume", s.handleResumeWebhook)
	mux.HandleFunc("GET /control/instances/{id}/webhook/dlq", s.handleListWebhookDLQ)
	mux.HandleFunc("POST /control/instances/{id}/webhook/dlq/replay", s.handleReplayWebhookDLQ)
	mux.HandleFunc("DELETE /control/instances/{id}/webhook/dlq", s.handlePurgeWebhookDLQ)
	mux.HandleFunc("GET /control/instances/{id}/events", s.handleGetEvents)
	mux.HandleFunc("GET /control/instances/{id}/events/stream", s.handleStreamEvents)
	mux.HandleFunc("GET /control/instances/{id}/health", s.handleGetHealth)
	mux.HandleFunc("POST /control/instances/{id}/messages", s.handleSendMessage)
	mux.HandleFunc("POST /control/instances/{id}/context", s.handleContextToggle)
	mux.HandleFunc("POST /control/instances/{id}/context/purge", s.handleContextPurge)
	mux.HandleFunc("/control/mcp", s.handleMCP)

	// Ekspos pprof hanya melalui Unix Domain Socket control plane, tidak pernah di port HTTP publik
	pprofMux := http.NewServeMux()
	pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
	pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/control/debug/pprof/", http.StripPrefix("/control", pprofMux))

	return mux
}

func (s *Server) handleDoctor(w http.ResponseWriter, r *http.Request) {
	outbox, _ := s.store.CountOutbox(r.Context())
	counts := s.wa.CountByStatus(r.Context())
	govLevel := governor.Green.String()
	if s.gov != nil {
		govLevel = s.gov.Level().String()
	}
	if r.URL.Query().Get("deep") == "1" {
		dbs := []obs.DBInput{
			{Name: "app.db", DB: s.store.DB, Path: s.cfg.AppDBPath()},
		}
		if waDB := s.wa.WADB(); waDB != nil {
			dbs = append(dbs, obs.DBInput{Name: "wa.db", DB: waDB, Path: s.cfg.WADBPath()})
		}
		if s.journal != nil && s.journal.DB() != nil {
			dbs = append(dbs, obs.DBInput{Name: "journal.db", DB: s.journal.DB(), Path: s.cfg.JournalDBPath()})
		}
		rep := obs.CollectDeep(r.Context(), s.startedAt, s.cfg.AppDBPath(), s.cfg.WADBPath(), outbox, counts, dbs, s.version)
		rep.GovernorLevel = govLevel
		rep.Doctor.GovernorLevel = govLevel
		if s.journal != nil && s.store != nil {
			if instances, err := s.store.ListInstances(r.Context()); err == nil {
				whReports := make([]obs.WebhookInstanceReport, 0, len(instances))
				for _, inst := range instances {
					head, _ := s.journal.HeadSeq(r.Context(), inst.ID)
					minCur, lanes, _ := s.journal.ConsumerCursorMin(r.Context(), "webhook", inst.ID)
					var lag int64
					var oldestSec int64
					if lanes > 0 || strings.TrimSpace(inst.WebhookURL) != "" {
						activeMin, _ := s.journal.MinCursor(r.Context(), inst.ID)
						minCur = activeMin
						if head > activeMin {
							lag = head - activeMin
							if age, aErr := s.journal.OldestUndeliveredAge(r.Context(), inst.ID, activeMin); aErr == nil {
								oldestSec = int64(age.Seconds())
							}
						}
					}
					var dlqCount int
					if s.dead != nil {
						dlqCount, _ = s.dead.Count(r.Context(), inst.ID)
					}
					dropped, _ := obs.MetricJournalDroppedUndeliveredTotal.Lookup(inst.ID)
					whReports = append(whReports, obs.WebhookInstanceReport{
						InstanceID:         inst.ID,
						HeadSeq:            head,
						MinCursor:          minCur,
						Lag:                lag,
						OldestUndeliveredS: oldestSec,
						DLQ:                dlqCount,
						Paused:             inst.Settings.WebhookPaused,
						DroppedUndelivered: int64(dropped),
					})
				}
				rep.Webhook = whReports
			}
		}
		writeJSON(w, http.StatusOK, rep)
		return
	}
	rep := obs.Collect(s.startedAt, s.cfg.AppDBPath(), s.cfg.WADBPath(), outbox, counts)
	rep.GovernorLevel = govLevel
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
	Name       string          `json:"name"`
	InstanceID string          `json:"instance_id"`
	Scopes     string          `json:"scopes"`
	ExpiresAt  int64           `json:"expires_at"`
	ExpiresIn  string          `json:"expires_in"`
	RatePerSec int             `json:"rate_per_sec"`
	Policy     *auth.KeyPolicy `json:"policy"`
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

	scopes := auth.DefaultScopes()
	if strings.TrimSpace(req.Scopes) != "" {
		parsed, err := auth.ParseScopes(req.Scopes)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		scopes = parsed
	}

	var expiresAt time.Time
	if strings.TrimSpace(req.ExpiresIn) != "" {
		dur, err := time.ParseDuration(strings.TrimSpace(req.ExpiresIn))
		if err != nil || dur <= 0 {
			writeErr(w, http.StatusBadRequest, errors.New("durasi expires_in harus bernilai positif"))
			return
		}
		expiresAt = time.Now().Add(dur)
	} else if req.ExpiresAt > 0 {
		expiresAt = time.Unix(req.ExpiresAt, 0)
	}

	var pol auth.KeyPolicy
	if req.Policy != nil {
		pol = *req.Policy
	}

	raw, meta, err := s.auth.CreateWithOptions(r.Context(), auth.CreateKeyRequest{
		Name:       strings.TrimSpace(req.Name),
		InstanceID: strings.TrimSpace(req.InstanceID),
		Scopes:     scopes,
		ExpiresAt:  expiresAt,
		RatePerSec: req.RatePerSec,
		Policy:     pol,
	})
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	s.recordAudit(r.Context(), "key.create", meta.ID,
		fmt.Sprintf("name=%s instance=%s scopes=%s", meta.Name, meta.InstanceID, meta.Scopes))
	writeJSON(w, http.StatusCreated, createKeyResp{Key: raw, Meta: meta})
}

func (s *Server) handleGetKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	k, err := s.store.GetAPIKey(r.Context(), id)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, k)
}

type rotateKeyReq struct {
	Grace string `json:"grace"`
}

func (s *Server) handleRotateKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req rotateKeyReq
	_ = json.NewDecoder(r.Body).Decode(&req)

	grace := 24 * time.Hour
	if strings.TrimSpace(req.Grace) != "" {
		dur, err := time.ParseDuration(strings.TrimSpace(req.Grace))
		if err != nil || dur < 0 {
			writeErr(w, http.StatusBadRequest, errors.New("durasi grace tidak valid"))
			return
		}
		grace = dur
	}

	raw, meta, err := s.auth.Rotate(r.Context(), id, grace)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	s.recordAudit(r.Context(), "key.rotate", id, fmt.Sprintf("new_id=%s grace=%s", meta.ID, grace))
	writeJSON(w, http.StatusOK, createKeyResp{Key: raw, Meta: meta})
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
	s.recordAudit(r.Context(), "key.revoke", id, "")
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked", "id": id})
}

func (s *Server) handleListAudit(w http.ResponseWriter, r *http.Request) {
	if s.audit == nil {
		writeJSON(w, http.StatusOK, []auth.AuditEntry{})
		return
	}
	qp := r.URL.Query()
	q := auth.AuditQuery{
		ActionPattern: strings.TrimSpace(qp.Get("action")),
	}
	if rawSince := strings.TrimSpace(qp.Get("since")); rawSince != "" {
		dur, err := time.ParseDuration(rawSince)
		if err != nil || dur <= 0 {
			writeErr(w, http.StatusBadRequest, errors.New("parameter since harus durasi Go yang valid, mis. 24h"))
			return
		}
		q.Since = time.Now().Add(-dur)
	}
	if rawLimit := strings.TrimSpace(qp.Get("limit")); rawLimit != "" {
		n, err := strconv.Atoi(rawLimit)
		if err != nil || n <= 0 {
			writeErr(w, http.StatusBadRequest, errors.New("parameter limit harus bilangan bulat positif"))
			return
		}
		q.Limit = n
	}
	entries, err := s.audit.List(r.Context(), q)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) recordAudit(ctx context.Context, action, target, detail string) {
	if s == nil || s.audit == nil {
		return
	}
	actor := "control"
	if uid, ok := ctx.Value(peerUIDKey{}).(uint32); ok {
		actor = "control:uid=" + strconv.FormatUint(uint64(uid), 10)
	}
	aCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := s.audit.Write(aCtx, actor, action, target, detail); err != nil {
		slog.Warn("gagal menulis audit control plane", "action", action, "target", target, "err", err)
	}
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
	s.recordAudit(r.Context(), "instance.create", st.ID, fmt.Sprintf("rate=%d auto_start=%v", st.RatePerMin, st.AutoStart))
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
	WebhookURL    *string         `json:"webhook_url"`
	WebhookEvents *string         `json:"webhook_events"`
	WebhookFrom   string          `json:"webhook_from"`
	RatePerMin    *int            `json:"rate_per_min"`
	AutoStart     *bool           `json:"auto_start"`
	RotateSecret  bool            `json:"rotate_secret"`
	Settings      json.RawMessage `json:"settings"`
}

func (s *Server) handleUpdateInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req updateInstanceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.WebhookFrom != "" && req.WebhookFrom != "now" && req.WebhookFrom != "retained" {
		writeErr(w, http.StatusBadRequest, errors.New("webhook_from harus 'now' atau 'retained'"))
		return
	}
	up := store.InstanceUpdate{
		WebhookURL:    req.WebhookURL,
		WebhookEvents: req.WebhookEvents,
		WebhookFrom:   req.WebhookFrom,
		RatePerMin:    req.RatePerMin,
		AutoStart:     req.AutoStart,
	}
	if len(bytes.TrimSpace(req.Settings)) > 0 {
		cur, err := s.store.GetInstance(r.Context(), id)
		if err != nil {
			writeStatusErr(w, err)
			return
		}
		merged, err := cur.Settings.Merge(req.Settings)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		up.Settings = &merged
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
	s.recordAudit(r.Context(), "instance.update", id, fmt.Sprintf("rotate_secret=%v", req.RotateSecret))
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleRestartInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, err := s.wa.Restart(r.Context(), id)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	s.recordAudit(r.Context(), "instance.restart", id, "")
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleRecycleInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, err := s.wa.Recycle(r.Context(), id)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	s.recordAudit(r.Context(), "instance.recycle", id, "")
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleStopInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, err := s.wa.Stop(r.Context(), id)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	s.recordAudit(r.Context(), "instance.stop", id, "")
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleLogoutInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, err := s.wa.Logout(r.Context(), id)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	s.recordAudit(r.Context(), "instance.logout", id, "")
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleDeleteInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.wa.Delete(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}
	if s.ctxStore != nil {
		_ = s.ctxStore.Purge(r.Context(), id, "")
	}
	s.recordAudit(r.Context(), "instance.delete", id, "")
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

type contextToggleReq struct {
	Enabled bool `json:"enabled"`
	Purge   bool `json:"purge"`
}

func (s *Server) handleContextToggle(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req contextToggleReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cur, err := s.store.GetInstance(r.Context(), id)
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	cur.Settings.ContextStore = req.Enabled
	if _, err := s.wa.UpdateSettings(r.Context(), id, cur.Settings); err != nil {
		writeStatusErr(w, err)
		return
	}
	if req.Purge && s.ctxStore != nil {
		if err := s.ctxStore.Purge(r.Context(), id, ""); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
	}
	action := "context.disable"
	if req.Enabled {
		action = "context.enable"
	}
	s.recordAudit(r.Context(), action, id, fmt.Sprintf("purge=%t", req.Purge))
	writeJSON(w, http.StatusOK, map[string]any{
		"instance":      id,
		"context_store": req.Enabled,
		"purged":        req.Purge,
	})
}

type contextPurgeReq struct {
	Chat string `json:"chat"`
}

func (s *Server) handleContextPurge(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetInstance(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}
	var req contextPurgeReq
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
	}
	chatCanon := strings.TrimSpace(req.Chat)
	if chatCanon != "" && !strings.HasSuffix(chatCanon, "@lid") {
		parsed, err := jid.ParseRecipient(chatCanon)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		chatCanon = parsed.String()
	}
	if s.ctxStore != nil {
		if err := s.ctxStore.Purge(r.Context(), id, chatCanon); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
	}
	s.recordAudit(r.Context(), "context.purge", id, fmt.Sprintf("chat=%s", chatCanon))
	writeJSON(w, http.StatusOK, map[string]any{
		"instance": id,
		"chat":     chatCanon,
		"purged":   true,
	})
}

func (s *Server) handleTestWebhook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var (
		code int
		err  error
	)
	if s.webhookSvc != nil {
		code, err = s.webhookSvc.SendTest(r.Context(), id)
	} else if s.webhook != nil {
		code, err = s.webhook.SendTest(r.Context(), id)
	} else {
		err = errors.New("service webhook belum aktif")
	}
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	s.recordAudit(r.Context(), "webhook.test", id, strconv.Itoa(code))
	writeJSON(w, http.StatusOK, map[string]any{"status": "delivered", "http_status": code})
}

func (s *Server) handleGetHealth(w http.ResponseWriter, r *http.Request) {
	if s.health == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("layanan kesehatan akun belum aktif"))
		return
	}
	rep, err := s.health.Health(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStatusErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	if s.mcpSrv == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("layanan MCP belum aktif"))
		return
	}
	s.mcpSrv.ControlHandler().ServeHTTP(w, r)
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
