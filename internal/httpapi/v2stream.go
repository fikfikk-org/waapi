package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/wa"
)

const (
	sseHeartbeatInterval = 25 * time.Second
	sseWriteTimeout      = 30 * time.Second
	sseBatchLimit        = 100
)

type sseJournalSource interface {
	HeadSeq(ctx context.Context, instanceID string) (int64, error)
	Read(ctx context.Context, q journal.Query) (journal.ReadResult, error)
	Subscribe(instanceID string) (<-chan struct{}, func())
}

func (s *Server) handleEventsStream(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.journal == nil {
		writeError(w, http.StatusServiceUnavailable, "journal_unavailable", "journal masuk belum aktif", 0)
		return
	}

	id := r.PathValue("id")
	afterSeq, types, err := parseSSEStartAndFilter(r, s.journal, id)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), 0)
		return
	}

	select {
	case s.sseSem <- struct{}{}:
		defer func() { <-s.sseSem }()
	default:
		writeError(w, http.StatusTooManyRequests, "sse_limit", "batas koneksi SSE konkuren tercapai", 10)
		return
	}

	serveSSEStream(r.Context(), w, s.journal, id, afterSeq, types, sseHeartbeatInterval, sseWriteTimeout)
}

func parseSSEStartAndFilter(r *http.Request, src sseJournalSource, instanceID string) (int64, []string, error) {
	qp := r.URL.Query()
	var types []string
	if rawTypes := strings.TrimSpace(qp.Get("types")); rawTypes != "" {
		for _, part := range strings.Split(rawTypes, ",") {
			if t := strings.TrimSpace(part); t != "" {
				types = append(types, t)
			}
		}
	}

	// Prioritas posisi awal: header Last-Event-ID -> query ?after= -> head journal saat ini.
	rawStart := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if rawStart == "" {
		rawStart = strings.TrimSpace(qp.Get("after"))
	}
	if rawStart != "" {
		seq, err := strconv.ParseInt(rawStart, 10, 64)
		if err != nil || seq < 0 {
			return 0, nil, errInvalidParam("posisi awal SSE harus bilangan bulat >= 0")
		}
		return seq, types, nil
	}

	head, err := src.HeadSeq(r.Context(), instanceID)
	if err != nil {
		return 0, nil, err
	}
	return head, types, nil
}

func serveSSEStream(
	ctx context.Context,
	w http.ResponseWriter,
	src sseJournalSource,
	instanceID string,
	afterSeq int64,
	types []string,
	heartbeat time.Duration,
	writeTimeout time.Duration,
) {
	rc := http.NewResponseController(w)
	// Lepaskan WriteTimeout global http.Server agar aliran panjang tetap hidup.
	_ = rc.SetWriteDeadline(time.Time{})

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")

	if err := writeSSEHeaders(rc, w, writeTimeout); err != nil {
		return
	}

	subCh, unsub := src.Subscribe(instanceID)
	defer unsub()

	cursor := afterSeq
	for {
		if ctx.Err() != nil {
			return
		}

		res, err := src.Read(ctx, journal.Query{
			InstanceID: instanceID,
			AfterSeq:   cursor,
			Types:      types,
			Limit:      sseBatchLimit,
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
			if err := writeSSEChunk(rc, w, []byte(frame), writeTimeout); err != nil {
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

		timer := time.NewTimer(heartbeat)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-subCh:
			timer.Stop()
		case <-timer.C:
			if err := writeSSEChunk(rc, w, []byte(": ping\n\n"), writeTimeout); err != nil {
				return
			}
		}
	}
}

func writeSSEHeaders(rc *http.ResponseController, w http.ResponseWriter, timeout time.Duration) error {
	if timeout > 0 {
		_ = rc.SetWriteDeadline(time.Now().Add(timeout))
	}
	w.WriteHeader(http.StatusOK)
	err := rc.Flush()
	_ = rc.SetWriteDeadline(time.Time{})
	return err
}

func writeSSEChunk(rc *http.ResponseController, w http.ResponseWriter, data []byte, timeout time.Duration) error {
	if timeout > 0 {
		_ = rc.SetWriteDeadline(time.Now().Add(timeout))
	}
	_, err := w.Write(data)
	if err == nil {
		err = rc.Flush()
	}
	_ = rc.SetWriteDeadline(time.Time{})
	return err
}
