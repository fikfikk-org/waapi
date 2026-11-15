package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type eventsPageResponse struct {
	Events    []json.RawMessage `json:"events"`
	NextAfter int64             `json:"next_after"`
	HasMore   bool              `json:"has_more"`
}

// ParseEventsQuery membaca parameter query pagination (after, limit, types) dari HTTP request.
func ParseEventsQuery(r *http.Request, instanceID string) (journal.Query, error) {
	q := journal.Query{
		InstanceID: instanceID,
		Limit:      100,
	}
	qp := r.URL.Query()
	if rawAfter := strings.TrimSpace(qp.Get("after")); rawAfter != "" {
		v, err := strconv.ParseInt(rawAfter, 10, 64)
		if err != nil || v < 0 {
			return q, errInvalidParam("parameter after harus bilangan bulat >= 0")
		}
		q.AfterSeq = v
	}
	if rawLimit := strings.TrimSpace(qp.Get("limit")); rawLimit != "" {
		v, err := strconv.Atoi(rawLimit)
		if err != nil || v <= 0 {
			return q, errInvalidParam("parameter limit harus bilangan bulat >= 1")
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
	return q, nil
}

type paramError string

func errInvalidParam(msg string) error { return paramError(msg) }
func (e paramError) Error() string     { return string(e) }

// BuildEventsPage mengubah Journal.ReadResult menjadi payload JSON halaman pull API v2.
func BuildEventsPage(res journal.ReadResult) (any, error) {
	items := make([]json.RawMessage, 0, len(res.Events))
	for _, ev := range res.Events {
		raw, err := ev.MarshalEnvelope()
		if err != nil {
			return nil, err
		}
		items = append(items, json.RawMessage(raw))
	}
	return eventsPageResponse{
		Events:    items,
		NextAfter: res.ScannedTo,
		HasMore:   res.HasMore,
	}, nil
}

func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.journal == nil {
		writeError(w, http.StatusServiceUnavailable, "journal_unavailable", "journal masuk belum aktif", 0)
		return
	}
	id := r.PathValue("id")
	q, err := ParseEventsQuery(r, id)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), 0)
		return
	}
	res, err := s.journal.Read(r.Context(), q)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	page, err := BuildEventsPage(res)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}
