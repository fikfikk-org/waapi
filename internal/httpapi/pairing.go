package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/fikfikk-org/waapi/internal/wa"
)

type startPairRequest struct {
	Phone string `json:"phone"`
}

// handleStartPair memulai sesi pairing dan mengembalikan QR/kode pertama.
func (s *Server) handleStartPair(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	var req startPairRequest
	r.Body = http.MaxBytesReader(w, r.Body, 1<<10)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "body harus JSON valid maksimal 1KB", 0)
		return
	}

	sess, err := inst.StartPairing(strings.TrimSpace(req.Phone))
	if err != nil {
		writePairingError(w, err)
		return
	}

	snap, err := sess.WaitReady(r.Context())
	if err != nil {
		if errors.Is(err, wa.ErrPairStartTimeout) {
			writeError(w, http.StatusGatewayTimeout, "pairing_start_timeout", err.Error(), 0)
			return
		}
		// Klien memutus koneksi sebelum QR muncul; sesi tetap hidup sampai TTL
		return
	}
	if snap.State == wa.PairError {
		msg := "pairing gagal"
		if snap.Error != nil {
			msg = *snap.Error
		}
		writeError(w, http.StatusBadGateway, "pairing_failed", msg, 0)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// handleGetPair mengembalikan snapshot sesi pairing terkini untuk polling dashboard.
func (s *Server) handleGetPair(w http.ResponseWriter, _ *http.Request, inst *wa.Instance) {
	snap, err := inst.PairingSnapshot()
	if err != nil {
		writePairingError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// handleCancelPair membatalkan sesi pairing aktif pada sebuah instance.
func (s *Server) handleCancelPair(w http.ResponseWriter, _ *http.Request, inst *wa.Instance) {
	snap, err := inst.CancelPairing()
	if err != nil {
		writePairingError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// handleQRPNG mengirim gambar PNG QR aktif dengan cache-control no-store.
func (s *Server) handleQRPNG(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	scale := 6
	if v := r.URL.Query().Get("scale"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 2 || n > 16 {
			writeError(w, http.StatusBadRequest, "invalid_request", "scale harus 2-16", 0)
			return
		}
		scale = n
	}

	png, expiresIn, err := inst.PairingQRPNG(scale)
	if err != nil {
		writePairingError(w, err)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	w.Header().Set("X-QR-Expires-In", strconv.Itoa(expiresIn))
	w.Header().Set("Content-Length", strconv.Itoa(len(png)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(png)
}

// writePairingError memetakan sentinel error pairing ke kode HTTP yang sesuai.
func writePairingError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, wa.ErrInvalidPhone):
		writeError(w, http.StatusBadRequest, "invalid_phone", err.Error(), 0)
	case errors.Is(err, wa.ErrAlreadyPaired):
		writeError(w, http.StatusConflict, "already_paired", err.Error(), 0)
	case errors.Is(err, wa.ErrPairingInProgress):
		writeError(w, http.StatusConflict, "pairing_in_progress", err.Error(), 0)
	case errors.Is(err, wa.ErrPairingBusy):
		writeError(w, http.StatusConflict, "pairing_busy", err.Error(), 10)
	case errors.Is(err, wa.ErrNoPairingSession):
		writeError(w, http.StatusNotFound, "no_pairing_session", err.Error(), 0)
	case errors.Is(err, wa.ErrNoActivePairing):
		writeError(w, http.StatusNotFound, "no_active_pairing", err.Error(), 0)
	case errors.Is(err, wa.ErrQRNotAvailable):
		writeError(w, http.StatusNotFound, "qr_not_available", err.Error(), 0)
	default:
		writeError(w, http.StatusBadGateway, "pairing_failed", err.Error(), 0)
	}
}
