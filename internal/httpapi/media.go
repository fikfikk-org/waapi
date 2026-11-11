package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/fikfikk-org/waapi/internal/wa"
)

// handleDownloadMedia mengunduh media masuk berdasarkan descriptor dari payload webhook.
func (s *Server) handleDownloadMedia(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	var desc wa.MediaDescriptor
	if err := json.NewDecoder(r.Body).Decode(&desc); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "descriptor media tidak valid", 0)
		return
	}
	if strings.TrimSpace(desc.DirectPath) == "" || strings.TrimSpace(desc.MediaKey) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "direct_path dan media_key wajib diisi", 0)
		return
	}

	mime := desc.Mimetype
	if mime == "" {
		mime = "application/octet-stream"
	}
	w.Header().Set("Content-Type", mime)
	if err := inst.DownloadMediaToWriter(r.Context(), desc, w); err != nil {
		// Jika belum ada byte yang ditulis ke response, kirim JSON error
		writeMappedError(w, err)
	}
}

