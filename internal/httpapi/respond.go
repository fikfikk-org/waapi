// Package httpapi menyediakan endpoint REST API publik berautentikasi X-API-Key.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// ErrorBody membungkus format error standar sesuai spesifikasi API.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail memuat kode mesin, pesan manusia, dan opsional retry_after.
type ErrorDetail struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	RetryAfter int    `json:"retry_after,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string, retryAfter int) {
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	}
	writeJSON(w, status, ErrorBody{
		Error: ErrorDetail{
			Code:       code,
			Message:    msg,
			RetryAfter: retryAfter,
		},
	})
}

// writeMappedError memetakan sentinel error domain ke kode status HTTP yang sesuai.
func writeMappedError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "instance_not_found", "instance tidak ditemukan", 0)
	case errors.Is(err, wa.ErrNotConnected):
		writeError(w, http.StatusServiceUnavailable, "instance_not_connected", "instance sedang tidak tersambung", 0)
	case errors.Is(err, wa.ErrQueueFull):
		writeError(w, http.StatusTooManyRequests, "queue_full", "antrian kirim penuh, coba lagi", 3)
	case errors.Is(err, wa.ErrMediaTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "media_too_large", err.Error(), 0)
	case errors.Is(err, wa.ErrInvalidRecipient):
		writeError(w, http.StatusBadRequest, "invalid_number", err.Error(), 0)
	case errors.Is(err, wa.ErrInvalidMediaType):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
	default:
		writeError(w, http.StatusBadGateway, "send_failed", err.Error(), 0)
	}
}

