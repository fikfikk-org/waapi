package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/fikfikk-org/waapi/internal/hooks"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

const maxHookRuleBodyBytes = 256 << 10 // 256 KiB per rule / payload dry-run

func (s *Server) hookStore() *store.Store {
	if s.wa != nil && s.wa.Store() != nil {
		return s.wa.Store()
	}
	if s.auth != nil && s.auth.Store() != nil {
		return s.auth.Store()
	}
	if s.appDB != nil {
		return &store.Store{DB: s.appDB}
	}
	return nil
}

type hookDTO struct {
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

func toHookDTO(rec store.HookRecord) hookDTO {
	rule, _ := hooks.ParseRuleJSON([]byte(rec.Spec))
	return hookDTO{
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

func (s *Server) handleV2ListHooks(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	st := s.hookStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "store hooks tidak tersedia", 0)
		return
	}
	list, err := st.ListHooks(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal membaca daftar hooks", 0)
		return
	}
	out := make([]hookDTO, 0, len(list))
	for _, rec := range list {
		out = append(out, toHookDTO(rec))
	}
	writeJSON(w, http.StatusOK, map[string]any{"hooks": out})
}

func (s *Server) handleV2GetHook(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	st := s.hookStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "store hooks tidak tersedia", 0)
		return
	}
	rec, err := st.GetHook(r.Context(), r.PathValue("id"), r.PathValue("hook_id"))
	if errors.Is(err, store.ErrHookNotFound) {
		writeError(w, http.StatusNotFound, "hook_not_found", "rule hook tidak ditemukan", 0)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal membaca rule hook", 0)
		return
	}
	writeJSON(w, http.StatusOK, toHookDTO(rec))
}

func (s *Server) handleV2CreateHook(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	st := s.hookStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "store hooks tidak tersedia", 0)
		return
	}
	instanceID := r.PathValue("id")
	count, err := st.CountHooks(r.Context(), instanceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal menghitung hooks instance", 0)
		return
	}
	if count >= hooks.MaxRulesPerInstance {
		writeError(w, http.StatusUnprocessableEntity, "hooks_limit_reached", "jumlah rule hook per instance mencapai batas maksimum 50", 0)
		return
	}

	raw, err := readLimitedBody(w, r, maxHookRuleBodyBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
		return
	}
	rule, err := hooks.ParseRuleJSON(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}

	// REST API wajib selalu memaksa Trusted: false meskipun body JSON mengirim trusted=true.
	rule.Trusted = false
	comp, err := hooks.CompileWithOptions(rule, hooks.CompileOptions{
		Trusted:   false,
		AIEnabled: s.hooks != nil && s.hooks.AIEnabled(),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}
	canonicalSpec, err := json.Marshal(comp.Rule)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal menyusun spesifikasi hook", 0)
		return
	}

	rec, err := st.CreateHook(r.Context(), store.HookRecord{
		InstanceID: instanceID,
		ID:         comp.Rule.ID,
		Name:       comp.Rule.Name,
		Enabled:    comp.Enabled,
		Priority:   comp.Priority,
		Trusted:    false,
		Spec:       string(canonicalSpec),
	})
	if errors.Is(err, store.ErrHookExists) {
		writeError(w, http.StatusConflict, "hook_exists", "rule hook dengan ID tersebut sudah terdaftar", 0)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal menyimpan rule hook", 0)
		return
	}

	if s.hooks != nil {
		_ = s.hooks.ReloadInstance(r.Context(), instanceID)
	}
	s.recordHTTPAudit(r, "hook.create", instanceID+"/"+rec.ID, "")
	writeJSON(w, http.StatusCreated, toHookDTO(rec))
}

func (s *Server) handleV2UpdateHook(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	st := s.hookStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "store hooks tidak tersedia", 0)
		return
	}
	instanceID := r.PathValue("id")
	hookID := r.PathValue("hook_id")

	raw, err := readLimitedBody(w, r, maxHookRuleBodyBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
		return
	}
	rule, err := hooks.ParseRuleJSON(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}
	if rule.ID == "" {
		rule.ID = hookID
	} else if rule.ID != hookID {
		writeError(w, http.StatusBadRequest, "invalid_spec", "id rule pada body harus sama dengan parameter URL hook_id", 0)
		return
	}

	// Setiap pembaruan lewat HTTP REST wajib menurunkan trusted=0 agar tidak bisa menginjeksi URL lokal.
	rule.Trusted = false
	comp, err := hooks.CompileWithOptions(rule, hooks.CompileOptions{
		Trusted:   false,
		AIEnabled: s.hooks != nil && s.hooks.AIEnabled(),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}
	canonicalSpec, err := json.Marshal(comp.Rule)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal menyusun spesifikasi hook", 0)
		return
	}

	rec, err := st.UpdateHook(r.Context(), store.HookRecord{
		InstanceID: instanceID,
		ID:         hookID,
		Name:       comp.Rule.Name,
		Enabled:    comp.Enabled,
		Priority:   comp.Priority,
		Trusted:    false,
		Spec:       string(canonicalSpec),
	})
	if errors.Is(err, store.ErrHookNotFound) {
		writeError(w, http.StatusNotFound, "hook_not_found", "rule hook tidak ditemukan", 0)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal memperbarui rule hook", 0)
		return
	}

	if s.hooks != nil {
		_ = s.hooks.ReloadInstance(r.Context(), instanceID)
	}
	s.recordHTTPAudit(r, "hook.update", instanceID+"/"+hookID, "")
	writeJSON(w, http.StatusOK, toHookDTO(rec))
}

func (s *Server) handleV2DeleteHook(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	st := s.hookStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "store hooks tidak tersedia", 0)
		return
	}
	instanceID := r.PathValue("id")
	hookID := r.PathValue("hook_id")

	err := st.DeleteHook(r.Context(), instanceID, hookID)
	if errors.Is(err, store.ErrHookNotFound) {
		writeError(w, http.StatusNotFound, "hook_not_found", "rule hook tidak ditemukan", 0)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal menghapus rule hook", 0)
		return
	}

	if s.hooks != nil {
		_ = s.hooks.ReloadInstance(r.Context(), instanceID)
	}
	s.recordHTTPAudit(r, "hook.delete", instanceID+"/"+hookID, "")
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": hookID})
}

func (s *Server) handleV2TestHook(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	st := s.hookStore()
	if st == nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "store hooks tidak tersedia", 0)
		return
	}
	instanceID := r.PathValue("id")
	hookID := r.PathValue("hook_id")

	rec, err := st.GetHook(r.Context(), instanceID, hookID)
	if errors.Is(err, store.ErrHookNotFound) {
		writeError(w, http.StatusNotFound, "hook_not_found", "rule hook tidak ditemukan", 0)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "gagal membaca rule hook", 0)
		return
	}

	raw, err := readLimitedBody(w, r, maxHookRuleBodyBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
		return
	}
	var body struct {
		Event json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || len(body.Event) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "field event JSON wajib diisi untuk simulasi hook", 0)
		return
	}

	rule, err := hooks.ParseRuleJSON([]byte(rec.Spec))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}
	rule.ID = rec.ID
	comp, err := hooks.CompileWithOptions(rule, hooks.CompileOptions{
		Trusted:   rec.Trusted,
		AIEnabled: s.hooks != nil && s.hooks.AIEnabled(),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}

	ev, _, err := hooks.EventViewFromEnvelope(body.Event, instanceID, "", time.Now())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "format envelope event tidak valid", 0)
		return
	}

	eng := s.hooks
	if eng == nil {
		eng = hooks.NewEngine(hooks.EngineDeps{AppStore: st, Governor: s.gov})
	}
	res, err := eng.DryRun(r.Context(), instanceID, comp, ev)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func readLimitedBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errors.New("gagal membaca body request atau ukuran melebihi batas")
	}
	return raw, nil
}
