package httpapi

import (
	"net/http"

	"github.com/fikfikk-org/waapi/internal/wa"
)

// handleDownloadMedia mengunduh media masuk berdasarkan descriptor dari payload webhook.
func (s *Server) handleDownloadMedia(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	s.serveDescriptorDownload(w, r, inst)
}
