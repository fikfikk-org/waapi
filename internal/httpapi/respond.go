// Package httpapi menyediakan endpoint REST API publik berautentikasi X-API-Key.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/netguard"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// ErrorBody membungkus format error standar sesuai spesifikasi API.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail memuat kode mesin, pesan manusia, retry_after opsional, request_id, dan rincian terstruktur.
type ErrorDetail struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	RetryAfter int            `json:"retry_after,omitempty"`
	RequestID  string         `json:"request_id,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string, retryAfter int) {
	writeErrorDetails(w, status, code, msg, retryAfter, nil)
}

func writeErrorDetails(w http.ResponseWriter, status int, code, msg string, retryAfter int, details map[string]any) {
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	}
	reqID := w.Header().Get("X-Request-Id")
	writeJSON(w, status, ErrorBody{
		Error: ErrorDetail{
			Code:       code,
			Message:    msg,
			RetryAfter: retryAfter,
			RequestID:  reqID,
			Details:    details,
		},
	})
}

type httpStructuredError interface {
	error
	HTTPStatus() int
	ErrorCode() string
	ErrorRetryAfter() int
	ErrorDetails() map[string]any
}

// writeMappedError memetakan sentinel error domain ke kode status HTTP yang sesuai.
func writeMappedError(w http.ResponseWriter, err error) {
	var se httpStructuredError
	if errors.As(err, &se) {
		writeErrorDetails(w, se.HTTPStatus(), se.ErrorCode(), se.Error(), se.ErrorRetryAfter(), se.ErrorDetails())
		return
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "instance_not_found", "instance tidak ditemukan", 0)
	case errors.Is(err, journal.ErrNotFound):
		writeError(w, http.StatusNotFound, "event_not_found", "event tidak ditemukan", 0)
	case errors.Is(err, media.ErrUploadNotFound):
		writeError(w, http.StatusNotFound, "upload_not_found", "upload_id tidak ditemukan", 0)
	case errors.Is(err, media.ErrUploadExpired):
		writeError(w, http.StatusGone, "upload_expired", "upload media sudah kedaluwarsa", 0)
	case errors.Is(err, media.ErrMediaGone):
		writeError(w, http.StatusGone, "media_gone", "media tidak lagi tersedia pada server maupun pengirim", 0)
	case errors.Is(err, media.ErrPTTRequiresOggOpus):
		writeError(w, http.StatusUnsupportedMediaType, "ptt_requires_ogg_opus", err.Error(), 0)
	case errors.Is(err, media.ErrStickerRequiresWebP):
		writeError(w, http.StatusUnsupportedMediaType, "sticker_requires_webp", err.Error(), 0)
	case errors.Is(err, media.ErrUnsupportedMedia):
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media", err.Error(), 0)
	case errors.Is(err, outbox.ErrNotFound), errors.Is(err, wa.ErrPollNotFound):
		writeError(w, http.StatusNotFound, "message_not_found", "pesan tidak ditemukan", 0)
	case errors.Is(err, outbox.ErrNotPaired):
		writeError(w, http.StatusConflict, "instance_not_paired", "instance belum di-pairing", 0)
	case errors.Is(err, outbox.ErrNotCancellable):
		writeError(w, http.StatusConflict, "message_not_cancellable", "pesan tidak dapat dibatalkan pada status ini", 0)
	case errors.Is(err, outbox.ErrIdempotencyMismatch):
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", err.Error(), 0)
	case errors.Is(err, outbox.ErrInvalidSpec):
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
	case errors.Is(err, outbox.ErrQueueFull):
		writeError(w, http.StatusTooManyRequests, "queue_full", "antrian outbox instance penuh, coba lagi", 3)
	case errors.Is(err, outbox.ErrMediaExpired):
		writeError(w, http.StatusGone, "media_gone", "media sudah kedaluwarsa", 0)
	case errors.Is(err, outbox.ErrWaitTimeout):
		writeError(w, http.StatusGatewayTimeout, "send_timeout", "batas waktu tunggu pengiriman pesan habis", 0)
	case errors.Is(err, wa.ErrNotConnected):
		writeError(w, http.StatusServiceUnavailable, "instance_not_connected", "instance sedang tidak tersambung", 0)
	case errors.Is(err, wa.ErrQueueFull):
		writeError(w, http.StatusTooManyRequests, "queue_full", "antrian kirim penuh, coba lagi", 3)
	case errors.Is(err, wa.ErrMediaTooLarge), errors.Is(err, media.ErrMediaTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "media_too_large", err.Error(), 0)
	case errors.Is(err, wa.ErrInvalidRecipient), errors.Is(err, jid.ErrInvalidRecipient):
		writeError(w, http.StatusBadRequest, "invalid_number", err.Error(), 0)
	case errors.Is(err, wa.ErrInvalidMediaType), errors.Is(err, media.ErrInvalidKind), errors.Is(err, wa.ErrPresenceSubLimit),
		errors.Is(err, netguard.ErrBlockedIP), errors.Is(err, netguard.ErrNonHTTPS), errors.Is(err, netguard.ErrTooManyRedirects):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
	default:
		slog.Error("kesalahan upstream API", "request_id", w.Header().Get("X-Request-Id"), "err", err)
		writeError(w, http.StatusBadGateway, "upstream_error", "terjadi kesalahan saat memproses permintaan ke layanan upstream", 0)
	}
}

