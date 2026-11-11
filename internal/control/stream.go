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

// handlePairInstance mengalirkan snapshot pairing sebagai NDJSON ke CLI.
func (s *Server) handlePairInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	inst, ok := s.wa.Get(id)
	if !ok {
		writeErr(w, http.StatusNotFound, ErrInstanceNotFound)
		return
	}

	var req pairReq
	_ = json.NewDecoder(r.Body).Decode(&req)

	sess, err := inst.StartPairing(req.Phone)
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, ErrStreamingUnsupported)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	events, unsubscribe := sess.Subscribe()
	defer unsubscribe()

	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			// Operator menekan Ctrl+C: CLI adalah pemilik sesi, batalkan pairing
			sess.Cancel()
			return
		case snap, ok := <-events:
			if !ok {
				return
			}
			_ = enc.Encode(snap)
			flusher.Flush()
		}
	}
}

// ReadPairStream membaca aliran NDJSON dari response pairing dan memanggil onEvent untuk setiap baris.
func ReadPairStream(r *http.Response, onEvent func(wa.PairSnapshot) error) error {
	sc := bufio.NewScanner(r.Body)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var snap wa.PairSnapshot
		if err := json.Unmarshal(line, &snap); err != nil {
			return err
		}
		if err := onEvent(snap); err != nil {
			return err
		}
	}
	return sc.Err()
}
