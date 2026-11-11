package control

import (
	"bufio"
	"encoding/json"
	"net/http"

	"github.com/fikfikk-org/waapi/internal/wa"
)

type pairReq struct {
	Phone string `json:"phone"`
}

// handlePairInstance mengalirkan event pairing (QR code / 8-digit code / connected) sebagai NDJSON.
func (s *Server) handlePairInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	inst, ok := s.wa.Get(id)
	if !ok {
		writeErr(w, http.StatusNotFound, ErrInstanceNotFound)
		return
	}

	var req pairReq
	_ = json.NewDecoder(r.Body).Decode(&req)

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, ErrStreamingUnsupported)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	enc := json.NewEncoder(w)
	emit := func(ev wa.PairEvent) {
		_ = enc.Encode(ev)
		flusher.Flush()
	}

	_ = inst.Pair(r.Context(), req.Phone, emit)
}

// ReadPairStream membaca aliran NDJSON dari response pairing dan memanggil onEvent untuk setiap baris.
func ReadPairStream(r *http.Response, onEvent func(wa.PairEvent) error) error {
	sc := bufio.NewScanner(r.Body)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev wa.PairEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return err
		}
		if err := onEvent(ev); err != nil {
			return err
		}
	}
	return sc.Err()
}

