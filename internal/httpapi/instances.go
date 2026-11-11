package httpapi

import (
	"net/http"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// handleListInstances mengembalikan daftar instance sesuai scope API key.
func (s *Server) handleListInstances(w http.ResponseWriter, r *http.Request) {
	meta := r.Context().Value(keyMetaCtx).(*auth.KeyMeta)
	all, err := s.wa.ListStatuses(r.Context())
	if err != nil {
		writeMappedError(w, err)
		return
	}
	filtered := make([]wa.InstanceStatus, 0, len(all))
	for _, it := range all {
		if meta.AllowedInstance(it.ID) {
			filtered = append(filtered, it)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"instances": filtered})
}

// handleGetInstance mengembalikan status detail satu instance.
func (s *Server) handleGetInstance(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	id := r.PathValue("id")
	st, err := s.wa.Status(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

