package control

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/hooks"
	"github.com/fikfikk-org/waapi/internal/store"
)

const maxControlHookBodyBytes = 256 << 10

type controlHookView struct {
	ID              string          `json:"id"`
	Name            string          `json:"name,omitempty"`
	Enabled         bool            `json:"enabled"`
	Priority        int             `json:"priority"`
	Trusted         bool            `json:"trusted"`
	On              []string        `json:"on"`
	When            *hooks.Cond     `json:"when,omitempty"`
	Do              []hooks.Action  `json:"do"`
	Throttle        *hooks.Throttle `json:"throttle,omitempty"`
	ActiveHours     string          `json:"active_hours,omitempty"`
	RespectTakeover bool            `json:"respect_takeover"`
	OnFromMe        bool            `json:"on_from_me"`
	Stop            bool            `json:"stop"`
	Version         int             `json:"version"`
	CreatedAt       int64           `json:"created_at"`
	UpdatedAt       int64           `json:"updated_at"`
}

func toControlHookView(rec store.HookRecord) controlHookView {
	rule, _ := hooks.ParseRuleJSON([]byte(rec.Spec))
	return controlHookView{
		ID:              rec.ID,
		Name:            rec.Name,
		Enabled:         rec.Enabled,
		Priority:        rec.Priority,
		Trusted:         rec.Trusted,
		On:              rule.On,
		When:            rule.When,
		Do:              rule.Do,
		Throttle:        rule.Throttle,
		ActiveHours:     rule.ActiveHours,
		RespectTakeover: rule.RespectsTakeover(),
		OnFromMe:        rule.OnFromMe,
		Stop:            rule.Stop,
		Version:         rec.Version,
		CreatedAt:       rec.CreatedAt,
		UpdatedAt:       rec.UpdatedAt,
	}
}

func (s *Server) handleListHooks(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetInstance(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}
	list, err := s.store.ListHooks(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	views := make([]controlHookView, 0, len(list))
	for _, rec := range list {
		views = append(views, toControlHookView(rec))
	}
	writeJSON(w, http.StatusOK, map[string]any{"hooks": views})
}

func (s *Server) handleUpsertHook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetInstance(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxControlHookBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("gagal membaca JSON rule hook"))
		return
	}
	rule, err := hooks.ParseRuleJSON(raw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	// Control plane mengizinkan trusted=true (boleh http ke 127.0.0.1 / localhost).
	comp, err := hooks.CompileWithOptions(rule, hooks.CompileOptions{Trusted: true})
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if _, getErr := s.store.GetHook(r.Context(), id, comp.Rule.ID); errors.Is(getErr, store.ErrHookNotFound) {
		cnt, _ := s.store.CountHooks(r.Context(), id)
		if cnt >= hooks.MaxRulesPerInstance {
			writeErr(w, http.StatusUnprocessableEntity, errors.New("jumlah rule hook per instance mencapai batas maksimum 50"))
			return
		}
	}

	specBytes, err := json.Marshal(comp.Rule)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.store.UpsertHook(r.Context(), store.HookRecord{
		InstanceID: id,
		ID:         comp.Rule.ID,
		Name:       comp.Rule.Name,
		Enabled:    comp.Enabled,
		Priority:   comp.Priority,
		Trusted:    true,
		Spec:       string(specBytes),
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	saved, err := s.store.GetHook(r.Context(), id, comp.Rule.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if s.hooks != nil {
		_ = s.hooks.ReloadInstance(r.Context(), id)
	}
	s.recordAudit(r.Context(), "hook.upsert", id+"/"+saved.ID, "")
	writeJSON(w, http.StatusCreated, toControlHookView(saved))
}

func (s *Server) handleEnableHook(w http.ResponseWriter, r *http.Request) {
	s.setHookEnabled(w, r, true)
}

func (s *Server) handleDisableHook(w http.ResponseWriter, r *http.Request) {
	s.setHookEnabled(w, r, false)
}

func (s *Server) setHookEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	id := r.PathValue("id")
	hookID := r.PathValue("hook_id")
	rec, err := s.store.SetHookEnabled(r.Context(), id, hookID, enabled)
	if errors.Is(err, store.ErrHookNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if s.hooks != nil {
		_ = s.hooks.ReloadInstance(r.Context(), id)
	}
	act := "hook.disable"
	if enabled {
		act = "hook.enable"
	}
	s.recordAudit(r.Context(), act, id+"/"+hookID, "")
	writeJSON(w, http.StatusOK, toControlHookView(rec))
}

func (s *Server) handleDeleteHook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	hookID := r.PathValue("hook_id")
	err := s.store.DeleteHook(r.Context(), id, hookID)
	if errors.Is(err, store.ErrHookNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if s.hooks != nil {
		_ = s.hooks.ReloadInstance(r.Context(), id)
	}
	s.recordAudit(r.Context(), "hook.delete", id+"/"+hookID, "")
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": hookID})
}

func (s *Server) handleListHookLogs(w http.ResponseWriter, r *http.Request) {
	if s.journal == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("journal belum siap"))
		return
	}
	id := r.PathValue("id")
	if _, err := s.store.GetInstance(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}
	qp := r.URL.Query()
	hookFilter := strings.TrimSpace(qp.Get("hook"))
	limit := 50
	if rawLim := strings.TrimSpace(qp.Get("limit")); rawLim != "" {
		n, err := strconv.Atoi(rawLim)
		if err != nil || n <= 0 {
			writeErr(w, http.StatusBadRequest, errors.New("limit harus bilangan bulat >= 1"))
			return
		}
		limit = n
	}
	var since time.Time
	if rawSince := strings.TrimSpace(qp.Get("since")); rawSince != "" {
		if dur, err := time.ParseDuration(rawSince); err == nil && dur > 0 {
			since = time.Now().Add(-dur)
		} else if unixSec, err := strconv.ParseInt(rawSince, 10, 64); err == nil && unixSec > 0 {
			since = time.Unix(unixSec, 0)
		} else {
			writeErr(w, http.StatusBadRequest, errors.New("format parameter since harus durasi (mis. 1h) atau unix timestamp"))
			return
		}
	}

	runs, err := hooks.NewSQLiteRunStore(s.journal.DB()).ListRuns(r.Context(), id, hookFilter, since, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if runs == nil {
		runs = []hooks.RunRecord{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"logs": runs})
}

func (s *Server) handleTestHookDryRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetInstance(r.Context(), id); err != nil {
		writeStatusErr(w, err)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxControlHookBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("gagal membaca payload uji hook"))
		return
	}
	var req struct {
		HookID string          `json:"hook_id"`
		Rule   json.RawMessage `json:"rule"`
		Event  json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(raw, &req); err != nil || len(req.Event) == 0 {
		writeErr(w, http.StatusBadRequest, errors.New("payload JSON wajib menyertakan field event"))
		return
	}

	var comp *hooks.Compiled
	if len(req.Rule) > 0 && string(req.Rule) != "null" {
		rule, err := hooks.ParseRuleJSON(req.Rule)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		comp, err = hooks.CompileWithOptions(rule, hooks.CompileOptions{Trusted: true})
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
	} else if strings.TrimSpace(req.HookID) != "" {
		rec, err := s.store.GetHook(r.Context(), id, req.HookID)
		if errors.Is(err, store.ErrHookNotFound) {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		rule, err := hooks.ParseRuleJSON([]byte(rec.Spec))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		rule.ID = rec.ID
		comp, err = hooks.CompileWithOptions(rule, hooks.CompileOptions{Trusted: rec.Trusted})
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
	} else {
		writeErr(w, http.StatusBadRequest, errors.New("sertakan field rule atau hook_id untuk dry-run"))
		return
	}

	ev, _, err := hooks.EventViewFromEnvelope(req.Event, id, "", time.Now())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	eng := s.hooks
	if eng == nil {
		eng = hooks.NewEngine(hooks.EngineDeps{AppStore: s.store, Journal: s.journal, Governor: s.gov})
	}
	res, err := eng.DryRun(r.Context(), id, comp, ev)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}
