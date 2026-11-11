package httpapi

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type ctxKey int

const keyMetaCtx ctxKey = 1

// Server menyatukan router HTTP publik dengan auth manager dan instance manager.
type Server struct {
	auth *auth.Manager
	wa   *wa.Manager
}

// NewHandler membangun http.Handler untuk seluruh endpoint publik (/healthz dan /v1/*).
func NewHandler(a *auth.Manager, w *wa.Manager) http.Handler {
	s := &Server{auth: a, wa: w}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /v1/instances", s.withAuth(s.handleListInstances))
	mux.HandleFunc("GET /v1/instances/{id}", s.withInstanceAuth(s.handleGetInstance))
	mux.HandleFunc("POST /v1/instances/{id}/pair", s.withInstanceAuth(s.handleStartPair))
	mux.HandleFunc("GET /v1/instances/{id}/pair", s.withInstanceAuth(s.handleGetPair))
	mux.HandleFunc("DELETE /v1/instances/{id}/pair", s.withInstanceAuth(s.handleCancelPair))
	mux.HandleFunc("GET /v1/instances/{id}/qr.png", s.withInstanceAuth(s.handleQRPNG))
	mux.HandleFunc("POST /v1/instances/{id}/messages/text", s.withInstanceAuth(s.handleSendText))
	mux.HandleFunc("POST /v1/instances/{id}/messages/media", s.withInstanceAuth(s.handleSendMedia))
	mux.HandleFunc("POST /v1/instances/{id}/messages/media-url", s.withInstanceAuth(s.handleSendMediaURL))
	mux.HandleFunc("POST /v1/instances/{id}/contacts/check", s.withInstanceAuth(s.handleCheckNumbers))
	mux.HandleFunc("POST /v1/instances/{id}/media/download", s.withInstanceAuth(s.handleDownloadMedia))

	return recoverMiddleware(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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

