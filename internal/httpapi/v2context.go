package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/wa"
)

func (s *Server) handleV2ContextChats(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.ctxStore == nil {
		writeMappedError(w, ctxstore.ErrDisabled)
		return
	}
	limit, ok := parseContextLimit(w, r, 50)
	if !ok {
		return
	}
	page, err := s.ctxStore.Chats(r.Context(), r.PathValue("id"), limit, r.URL.Query().Get("before"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleV2ContextMessages(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.ctxStore == nil {
		writeMappedError(w, ctxstore.ErrDisabled)
		return
	}
	chatStr, err := parseContextChatParam(r.PathValue("jid"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	limit, ok := parseContextLimit(w, r, 20)
	if !ok {
		return
	}
	page, err := s.ctxStore.Messages(r.Context(), r.PathValue("id"), chatStr, limit, r.URL.Query().Get("before"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleV2ContextSearch(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.ctxStore == nil {
		writeMappedError(w, ctxstore.ErrDisabled)
		return
	}
	qp := r.URL.Query()
	var chatFilter string
	if rawChat := strings.TrimSpace(qp.Get("chat")); rawChat != "" {
		parsed, err := parseContextChatParam(rawChat)
		if err != nil {
			writeMappedError(w, err)
			return
		}
		chatFilter = parsed
	}
	limit, ok := parseContextLimit(w, r, 20)
	if !ok {
		return
	}
	prefix := false
	if rawPrefix := strings.TrimSpace(qp.Get("prefix")); rawPrefix != "" {
		v, err := strconv.ParseBool(rawPrefix)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "parameter prefix harus bernilai boolean true/false", 0)
			return
		}
		prefix = v
	}

	hits, err := s.ctxStore.Search(r.Context(), r.PathValue("id"), qp.Get("q"), chatFilter, limit, prefix)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, hits)
}

func parseContextLimit(w http.ResponseWriter, r *http.Request, def int) (int, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return def, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "parameter limit harus bilangan bulat >= 1", 0)
		return 0, false
	}
	return n, true
}

func parseContextChatParam(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if strings.HasSuffix(s, "@lid") && len(s) > 4 {
		return s, nil
	}
	parsed, err := jid.ParseRecipient(s)
	if err != nil {
		return "", err
	}
	return parsed.String(), nil
}
