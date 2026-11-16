package httpapi

import (
	"context"
	"net/http"

	"github.com/fikfikk-org/waapi/internal/guardian"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type healthTransport interface {
	Health(ctx context.Context, instanceID string) (guardian.HealthReport, error)
}

// WithHealthDriver menyambungkan penyedia laporan kesehatan Guardian ke endpoint /v2/instances/{id}/health.
func WithHealthDriver(drv healthTransport) ServerOption {
	return func(s *Server) {
		if drv != nil {
			s.healthDriver = drv
		}
	}
}

// handleV2GetHealth mengembalikan skor kesehatan akun Guardian, daftar faktor, counter 24 jam, batas kuota, dan status ban.
func (s *Server) handleV2GetHealth(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.healthDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan kesehatan akun belum aktif", 0)
		return
	}
	rep, err := s.healthDriver.Health(r.Context(), r.PathValue("id"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}
