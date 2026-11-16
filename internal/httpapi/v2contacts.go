package httpapi

import (
	"net/http"

	"github.com/fikfikk-org/waapi/internal/wa"
)

// handleV2GetContactProfile mengambil profil kontak, pemetaan PN/LID, dan URL foto profil.
func (s *Server) handleV2GetContactProfile(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.contactDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan kontak belum aktif", 0)
		return
	}
	prof, err := s.contactDriver.GetContactProfile(r.Context(), r.PathValue("id"), r.PathValue("jid"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, prof)
}
