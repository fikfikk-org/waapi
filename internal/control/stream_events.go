package control

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

const (
	controlSSEHeartbeat = 25 * time.Second
	controlSSEWriteWait = 30 * time.Second
	controlSSEBatchMax  = 100
)

func (s *Server) handleStreamEvents(w http.ResponseWriter, r *http.Request) {
	if s.journal == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("journal masuk belum aktif"))
		return
	}
	id := r.PathValue("id")
	if s.store != nil {
		if _, err := s.store.GetInstance(r.Context(), id); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeErr(w, http.StatusNotFound, err)
				return
			}
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
	}

	qp := r.URL.Query()
	var types []string
	if rawTypes := strings.TrimSpace(qp.Get("types")); rawTypes != "" {
		for _, p := range strings.Split(rawTypes, ",") {
			if t := strings.TrimSpace(p); t != "" {
				types = append(types, t)
			}
		}
	}

	var afterSeq int64
	rawStart := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if rawStart == "" {
		rawStart = strings.TrimSpace(qp.Get("after"))
	}
	if rawStart != "" {
		v, err := strconv.ParseInt(rawStart, 10, 64)
		if err != nil || v < 0 {
			writeErr(w, http.StatusBadRequest, errors.New("parameter after/Last-Event-ID harus bilangan bulat >= 0"))
			return
		}
		afterSeq = v
	} else {
		head, err := s.journal.HeadSeq(r.Context(), id)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		afterSeq = head
	}

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")

	_ = rc.SetWriteDeadline(time.Now().Add(controlSSEWriteWait))
	w.WriteHeader(http.StatusOK)
	err := rc.Flush()
	_ = rc.SetWriteDeadline(time.Time{})
	if err != nil {
		return
	}

	subCh, unsub := s.journal.Subscribe(id)
	defer unsub()

	cursor := afterSeq
	for {
		if r.Context().Err() != nil {
			return
		}

		res, err := s.journal.Read(r.Context(), journal.Query{
			InstanceID: id,
			AfterSeq:   cursor,
			Types:      types,
			Limit:      controlSSEBatchMax,
		})
		if err != nil {
			return
		}

		for _, ev := range res.Events {
			raw, err := ev.MarshalEnvelope()
			if err != nil {
				return
			}
			frame := fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, ev.Type, raw)
			if err := writeControlSSE(rc, w, []byte(frame)); err != nil {
				return
			}
			cursor = ev.Seq
		}
		if res.ScannedTo > cursor {
			cursor = res.ScannedTo
		}
		if res.HasMore {
			continue
		}

		timer := time.NewTimer(controlSSEHeartbeat)
		select {
		case <-r.Context().Done():
			timer.Stop()
			return
		case <-subCh:
			timer.Stop()
		case <-timer.C:
			if err := writeControlSSE(rc, w, []byte(": ping\n\n")); err != nil {
				return
			}
		}
	}
}

func writeControlSSE(rc *http.ResponseController, w http.ResponseWriter, data []byte) error {
	_ = rc.SetWriteDeadline(time.Now().Add(controlSSEWriteWait))
	_, err := w.Write(data)
	if err == nil {
		err = rc.Flush()
	}
	_ = rc.SetWriteDeadline(time.Time{})
	return err
}
