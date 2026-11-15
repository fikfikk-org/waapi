package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

type controlEventsResponse struct {
	Events    []json.RawMessage `json:"events"`
	NextAfter int64             `json:"next_after"`
	HasMore   bool              `json:"has_more"`
}

func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
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

	q, waitSec, err := parseControlEventsQuery(r, id)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	// Langganan sinyal sebelum Read pertama agar event yang tiba di sela-sela tidak terlewat.
	var (
		ch    <-chan struct{}
		unsub func()
	)
	if waitSec > 0 {
		ch, unsub = s.journal.Subscribe(id)
		defer unsub()
	}

	res, err := s.journal.Read(r.Context(), q)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	if len(res.Events) == 0 && !res.HasMore && waitSec > 0 {
		timer := time.NewTimer(time.Duration(waitSec) * time.Second)
		defer timer.Stop()
		select {
		case <-ch:
			res, err = s.journal.Read(r.Context(), q)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err)
				return
			}
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}

	items := make([]json.RawMessage, 0, len(res.Events))
	for _, ev := range res.Events {
		raw, err := ev.MarshalEnvelope()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		items = append(items, json.RawMessage(raw))
	}

	writeJSON(w, http.StatusOK, controlEventsResponse{
		Events:    items,
		NextAfter: res.ScannedTo,
		HasMore:   res.HasMore,
	})
}

func parseControlEventsQuery(r *http.Request, instanceID string) (journal.Query, int, error) {
	q := journal.Query{
		InstanceID: instanceID,
		Limit:      100,
	}
	qp := r.URL.Query()
	if rawAfter := strings.TrimSpace(qp.Get("after")); rawAfter != "" {
		v, err := strconv.ParseInt(rawAfter, 10, 64)
		if err != nil || v < 0 {
			return q, 0, errors.New("parameter after harus >= 0")
		}
		q.AfterSeq = v
	}
	if rawLimit := strings.TrimSpace(qp.Get("limit")); rawLimit != "" {
		v, err := strconv.Atoi(rawLimit)
		if err != nil || v <= 0 {
			return q, 0, errors.New("parameter limit harus >= 1")
		}
		if v > journal.MaxReadLimit {
			v = journal.MaxReadLimit
		}
		q.Limit = v
	}
	if rawTypes := strings.TrimSpace(qp.Get("types")); rawTypes != "" {
		for _, part := range strings.Split(rawTypes, ",") {
			if t := strings.TrimSpace(part); t != "" {
				q.Types = append(q.Types, t)
			}
		}
	}
	waitSec := 0
	if rawWait := strings.TrimSpace(qp.Get("wait")); rawWait != "" {
		v, err := strconv.Atoi(rawWait)
		if err != nil || v < 0 || v > 30 {
			return q, 0, errors.New("parameter wait harus antara 0 dan 30 detik")
		}
		waitSec = v
	}
	return q, waitSec, nil
}
