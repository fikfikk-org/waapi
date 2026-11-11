package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/fikfikk-org/waapi/internal/wa"
)

type checkNumbersRequest struct {
	Numbers []string `json:"numbers"`
}

// handleCheckNumbers memeriksa apakah daftar nomor terdaftar di WhatsApp.
func (s *Server) handleCheckNumbers(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	var req checkNumbersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid", 0)
		return
	}
	if len(req.Numbers) == 0 || len(req.Numbers) > 20 {
		writeError(w, http.StatusBadRequest, "invalid_request", "numbers wajib berisi 1 sampai 20 nomor", 0)
		return
	}

	res, err := inst.CheckNumbers(r.Context(), req.Numbers)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": res})
}

