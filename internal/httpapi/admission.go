package httpapi

import (
	"net/http"

	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/wa"
)

const admissionRetryAfterSec = 15

// requireNotRed adalah middleware admission yang menolak permintaan berat dengan HTTP 503 saat level governor Red.
func (s *Server) requireNotRed(next func(http.ResponseWriter, *http.Request, *wa.Instance)) func(http.ResponseWriter, *http.Request, *wa.Instance) {
	return func(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
		if s.rejectIfRed(w) {
			return
		}
		next(w, r, inst)
	}
}

func (s *Server) rejectIfRed(w http.ResponseWriter) bool {
	if s != nil && s.gov != nil && s.gov.Level() == governor.Red {
		writeError(w, http.StatusServiceUnavailable, "memory_pressure", "daemon berada dalam tekanan memori level Red", admissionRetryAfterSec)
		return true
	}
	return false
}
