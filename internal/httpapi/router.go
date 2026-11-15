package httpapi

import (
	"context"
	"database/sql"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type ctxKey int

const keyMetaCtx ctxKey = 1

// RouteInfo mendeskripsikan pola HTTP dan scope yang dibutuhkan oleh sebuah endpoint API.
type RouteInfo struct {
	Pattern string
	Scope   auth.Scope
}

// ServerOption mengatur opsi dependensi opsional untuk Server HTTP publik.
type ServerOption func(*Server)

// WithDB menyetel koneksi app.db yang dipakai untuk pengecekan kesiapan /readyz.
func WithDB(db *sql.DB) ServerOption {
	return func(s *Server) {
		s.appDB = db
	}
}

// WithGovernor menyetel sumber level tekanan memori untuk pengecekan /readyz.
func WithGovernor(src governor.Source) ServerOption {
	return func(s *Server) {
		if src != nil {
			s.gov = src
		}
	}
}

// Server menyatukan router HTTP publik dengan auth manager dan instance manager.
type Server struct {
	auth    *auth.Manager
	wa      *wa.Manager
	appDB   *sql.DB
	gov     governor.Source
	mux     *http.ServeMux
	handler http.Handler
	routes  []RouteInfo
}

// NewServer membangun Server HTTP publik dengan pencatatan scope per route.
func NewServer(a *auth.Manager, w *wa.Manager, opts ...ServerOption) *Server {
	s := &Server{
		auth: a,
		wa:   w,
		gov:  governor.AlwaysGreen,
		mux:  http.NewServeMux(),
	}
	if a != nil && a.Store() != nil {
		s.appDB = a.Store().DB
	} else if w != nil && w.Store() != nil {
		s.appDB = w.Store().DB
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.handle("GET /v1/instances", auth.ScopeInstancesRead, s.withAuth(s.handleListInstances))
	s.handle("GET /v1/instances/{id}", auth.ScopeInstancesRead, s.withInstanceAuth(s.handleGetInstance))
	s.handle("POST /v1/instances/{id}/pair", auth.ScopePair, s.withInstanceAuth(s.handleStartPair))
	s.handle("GET /v1/instances/{id}/pair", auth.ScopePair, s.withInstanceAuth(s.handleGetPair))
	s.handle("DELETE /v1/instances/{id}/pair", auth.ScopePair, s.withInstanceAuth(s.handleCancelPair))
	s.handle("GET /v1/instances/{id}/qr.png", auth.ScopePair, s.withInstanceAuth(s.handleQRPNG))
	s.handle("POST /v1/instances/{id}/messages/text", auth.ScopeMessagesSend, s.withInstanceAuth(s.handleSendText))
	s.handle("POST /v1/instances/{id}/messages/media", auth.ScopeMessagesSend, s.withInstanceAuth(s.handleSendMedia))
	s.handle("POST /v1/instances/{id}/messages/media-url", auth.ScopeMessagesSend, s.withInstanceAuth(s.handleSendMediaURL))
	s.handle("POST /v1/instances/{id}/contacts/check", auth.ScopeMessagesRead, s.withInstanceAuth(s.handleCheckNumbers))
	s.handle("POST /v1/instances/{id}/media/download", auth.ScopeMessagesRead, s.withInstanceAuth(s.handleDownloadMedia))

	s.handler = recoverMiddleware(s.mux)
	return s
}

// NewHandler membangun http.Handler untuk seluruh endpoint publik (/healthz, /readyz, dan /v1/*).
func NewHandler(a *auth.Manager, w *wa.Manager, opts ...ServerOption) http.Handler {
	return NewServer(a, w, opts...)
}

// ServeHTTP meneruskan permintaan HTTP ke router publik yang sudah dibungkus middleware.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// Routes mengembalikan daftar route terdaftar beserta scope-nya untuk inspeksi dan pengujian.
func (s *Server) Routes() []RouteInfo {
	out := make([]RouteInfo, len(s.routes))
	copy(out, s.routes)
	return out
}

// handle mendaftarkan handler pada mux sekaligus mencatat scope endpoint (penegakan aktif mulai Batch 11).
func (s *Server) handle(pattern string, scope auth.Scope, h http.HandlerFunc) {
	s.routes = append(s.routes, RouteInfo{Pattern: pattern, Scope: scope})
	s.mux.HandleFunc(pattern, h)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz memverifikasi kesiapan database dan level governor tanpa autentikasi serta tanpa membocorkan detail error.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.gov != nil && s.gov.Level() == governor.Red {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "not_ready", 0)
		return
	}
	if s.appDB == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "not_ready", 0)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()

	var ok int
	if err := s.appDB.QueryRowContext(ctx, "SELECT 1").Scan(&ok); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "not_ready", 0)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimSpace(r.Header.Get("X-API-Key"))
		meta, ok := s.auth.Verify(raw)
		if !ok {
			writeError(w, http.StatusUnauthorized, "unauthorized", "X-API-Key tidak valid atau sudah dicabut", 0)
			return
		}
		ctx := context.WithValue(r.Context(), keyMetaCtx, meta)
		next(w, r.WithContext(ctx))
	}
}

func (s *Server) withInstanceAuth(next func(http.ResponseWriter, *http.Request, *wa.Instance)) http.HandlerFunc {
	return s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		meta := r.Context().Value(keyMetaCtx).(*auth.KeyMeta)
		if !meta.AllowedInstance(id) {
			writeError(w, http.StatusForbidden, "forbidden_instance", "API key tidak memiliki akses ke instance ini", 0)
			return
		}
		inst, ok := s.wa.Get(id)
		if !ok {
			writeError(w, http.StatusNotFound, "instance_not_found", "instance tidak ditemukan", 0)
			return
		}
		next(w, r, inst)
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic pada handler HTTP publik", "err", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal_error", "terjadi kesalahan internal", 0)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// LimitListener membatasi jumlah koneksi TCP konkuren untuk menjaga konsumsi RAM server HTTP.
func LimitListener(ln net.Listener, max int) net.Listener {
	if max <= 0 {
		return ln
	}
	return &limitedListener{
		Listener: ln,
		sem:      make(chan struct{}, max),
	}
}

type limitedListener struct {
	net.Listener
	sem chan struct{}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	l.sem <- struct{}{}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitedConn{Conn: c, release: func() { <-l.sem }}, nil
}

type limitedConn struct {
	net.Conn
	once    bool
	release func()
}

func (c *limitedConn) Close() error {
	if !c.once {
		c.once = true
		c.release()
	}
	return c.Conn.Close()
}

