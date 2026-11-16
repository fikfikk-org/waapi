package httpapi

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/fikfikk-org/waapi/internal/wa"
)

// handleV2GetSettings mengembalikan objek store.InstanceSettings milik instance langsung di level atas JSON.
func (s *Server) handleV2GetSettings(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	st, err := s.wa.Status(r.Context(), r.PathValue("id"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st.Settings)
}

// handleV2PatchSettings memperbarui sebagian field InstanceSettings secara tervalidasi dan mencatat audit settings.update.
func (s *Server) handleV2PatchSettings(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	id := r.PathValue("id")
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	patchBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "gagal membaca body JSON settings", 0)
		return
	}
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(patchBytes, &rawMap); err != nil || len(rawMap) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload patch settings harus berupa objek JSON non-kosong", 0)
		return
	}

	cur, err := s.wa.Store().GetInstance(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	merged, err := cur.Settings.Merge(patchBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}

	updated, err := s.wa.UpdateSettings(r.Context(), id, merged)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	s.recordHTTPAudit(r, "settings.update", id, string(patchBytes))
	writeJSON(w, http.StatusOK, updated.Settings)
}
