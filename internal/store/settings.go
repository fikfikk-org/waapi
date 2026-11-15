package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // pastikan database timezone IANA tersedia di distroless/Windows tanpa tzdata OS
	"unicode/utf8"
)

// Duration membungkus time.Duration agar di-marshal dan di-unmarshal sebagai string JSON (mis. "120s").
type Duration time.Duration

// Duration mengembalikan nilai time.Duration standar.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// String memformat durasi sebagai string ringkas Go.
func (d Duration) String() string {
	return time.Duration(d).String()
}

// MarshalJSON menyandikan Duration sebagai string JSON.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON membaca Duration dari string JSON (mis. "24h", "120s").
func (d *Duration) UnmarshalJSON(b []byte) error {
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		return errors.New("durasi tidak boleh null")
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("durasi harus berupa string: %w", err)
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("format durasi tidak valid %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// InstanceSettings menyimpan konfigurasi per instance di kolom instances.settings.
type InstanceSettings struct {
	Timezone            string   `json:"timezone"`
	JournalReceipts     string   `json:"journal_receipts"`
	WebhookLanes        int      `json:"webhook_lanes"`
	WebhookFormat       string   `json:"webhook_format"`
	WebhookMaxAttempts  int      `json:"webhook_max_attempts"`
	ContextStore        bool     `json:"context_store"`
	ContextPerChat      int      `json:"context_per_chat"`
	ContextMaxAge       Duration `json:"context_max_age"`
	GuardianProfile     string   `json:"guardian_profile"`
	GuardianShadowUntil int64    `json:"guardian_shadow_until"`
	WebhookPaused       string   `json:"webhook_paused"`
	QuietHours          string   `json:"quiet_hours"`
	OptoutKeywords      []string `json:"optout_keywords"`
	OptinKeywords       []string `json:"optin_keywords"`
	OptoutConfirmText   string   `json:"optout_confirm_text"`
	AutoRejectCalls     bool     `json:"auto_reject_calls"`
	CallRejectMessage   string   `json:"call_reject_message"`
	RecycleAt           string   `json:"recycle_at"`
	HooksMaxAge         Duration `json:"hooks_max_age"`
	TakeoverPause       Duration `json:"takeover_pause"`
	AIHandoffKeywords   []string `json:"ai_handoff_keywords"`
}

// Defaults mengembalikan konfigurasi bawaan InstanceSettings sesuai spesifikasi v2.
func Defaults() InstanceSettings {
	return InstanceSettings{
		Timezone:            "Asia/Jakarta",
		JournalReceipts:     "dm",
		WebhookLanes:        4,
		WebhookFormat:       "waapi",
		WebhookMaxAttempts:  12,
		ContextStore:        false,
		ContextPerChat:      50,
		ContextMaxAge:       Duration(168 * time.Hour),
		GuardianProfile:     "standard",
		GuardianShadowUntil: 0,
		WebhookPaused:       "",
		QuietHours:          "",
		OptoutKeywords:      []string{"STOP", "BERHENTI", "UNSUBSCRIBE"},
		OptinKeywords:       []string{"START", "MULAI"},
		OptoutConfirmText:   "",
		AutoRejectCalls:     false,
		CallRejectMessage:   "",
		RecycleAt:           "",
		HooksMaxAge:         Duration(120 * time.Second),
		TakeoverPause:       Duration(30 * time.Minute),
		AIHandoffKeywords:   []string{"agen", "manusia", "cs"},
	}
}

// Validate memeriksa seluruh batasan nilai pada InstanceSettings.
func (s InstanceSettings) Validate() error {
	if strings.TrimSpace(s.Timezone) == "" {
		return errors.New("timezone wajib diisi")
	}
	if _, err := time.LoadLocation(s.Timezone); err != nil {
		return fmt.Errorf("timezone tidak valid %q: %w", s.Timezone, err)
	}
	switch s.JournalReceipts {
	case "dm", "all", "none":
	default:
		return fmt.Errorf("journal_receipts harus dm, all, atau none, didapat %q", s.JournalReceipts)
	}
	if s.WebhookLanes < 1 || s.WebhookLanes > 16 {
		return fmt.Errorf("webhook_lanes harus antara 1 sampai 16, didapat %d", s.WebhookLanes)
	}
	switch s.WebhookFormat {
	case "waapi", "standard":
	default:
		return fmt.Errorf("webhook_format harus waapi atau standard, didapat %q", s.WebhookFormat)
	}
	if s.WebhookMaxAttempts < 1 || s.WebhookMaxAttempts > 20 {
		return fmt.Errorf("webhook_max_attempts harus antara 1 sampai 20, didapat %d", s.WebhookMaxAttempts)
	}
	if s.ContextPerChat < 10 || s.ContextPerChat > 500 {
		return fmt.Errorf("context_per_chat harus antara 10 sampai 500, didapat %d", s.ContextPerChat)
	}
	if s.ContextMaxAge <= 0 {
		return errors.New("context_max_age harus lebih besar dari 0")
	}
	switch s.GuardianProfile {
	case "strict", "standard", "relaxed", "off":
	default:
		return fmt.Errorf("guardian_profile harus strict, standard, relaxed, atau off, didapat %q", s.GuardianProfile)
	}
	if s.GuardianShadowUntil < 0 {
		return errors.New("guardian_shadow_until tidak boleh negatif")
	}
	if utf8.RuneCountInString(s.WebhookPaused) > 64 {
		return errors.New("webhook_paused maksimal 64 karakter")
	}
	if s.QuietHours != "" {
		start, end, ok := strings.Cut(s.QuietHours, "-")
		if !ok || !validHHMM(start) || !validHHMM(end) {
			return fmt.Errorf("quiet_hours harus berformat HH:MM-HH:MM, didapat %q", s.QuietHours)
		}
	}
	if utf8.RuneCountInString(s.OptoutConfirmText) > 500 {
		return errors.New("optout_confirm_text maksimal 500 karakter")
	}
	if utf8.RuneCountInString(s.CallRejectMessage) > 500 {
		return errors.New("call_reject_message maksimal 500 karakter")
	}
	if s.RecycleAt != "" && !validHHMM(s.RecycleAt) {
		return fmt.Errorf("recycle_at harus berformat HH:MM, didapat %q", s.RecycleAt)
	}
	if s.HooksMaxAge <= 0 {
		return errors.New("hooks_max_age harus lebih besar dari 0")
	}
	if s.TakeoverPause <= 0 {
		return errors.New("takeover_pause harus lebih besar dari 0")
	}
	return nil
}

type settingsPatch struct {
	Timezone            *string   `json:"timezone"`
	JournalReceipts     *string   `json:"journal_receipts"`
	WebhookLanes        *int      `json:"webhook_lanes"`
	WebhookFormat       *string   `json:"webhook_format"`
	WebhookMaxAttempts  *int      `json:"webhook_max_attempts"`
	ContextStore        *bool     `json:"context_store"`
	ContextPerChat      *int      `json:"context_per_chat"`
	ContextMaxAge       *Duration `json:"context_max_age"`
	GuardianProfile     *string   `json:"guardian_profile"`
	GuardianShadowUntil *int64    `json:"guardian_shadow_until"`
	WebhookPaused       *string   `json:"webhook_paused"`
	QuietHours          *string   `json:"quiet_hours"`
	OptoutKeywords      *[]string `json:"optout_keywords"`
	OptinKeywords       *[]string `json:"optin_keywords"`
	OptoutConfirmText   *string   `json:"optout_confirm_text"`
	AutoRejectCalls     *bool     `json:"auto_reject_calls"`
	CallRejectMessage   *string   `json:"call_reject_message"`
	RecycleAt           *string   `json:"recycle_at"`
	HooksMaxAge         *Duration `json:"hooks_max_age"`
	TakeoverPause       *Duration `json:"takeover_pause"`
	AIHandoffKeywords   *[]string `json:"ai_handoff_keywords"`
}

// Merge menerapkan patch JSON parsial ke salinan InstanceSettings, menolak field asing, lalu memvalidasi hasilnya.
func (s InstanceSettings) Merge(patch json.RawMessage) (InstanceSettings, error) {
	next := s.clone()
	trimmed := bytes.TrimSpace(patch)
	if len(trimmed) == 0 {
		if err := next.Validate(); err != nil {
			return InstanceSettings{}, err
		}
		return next, nil
	}

	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	var p settingsPatch
	if err := dec.Decode(&p); err != nil {
		return InstanceSettings{}, fmt.Errorf("patch settings tidak valid: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return InstanceSettings{}, errors.New("patch settings harus berisi tepat satu objek JSON")
	}

	if p.Timezone != nil {
		next.Timezone = strings.TrimSpace(*p.Timezone)
	}
	if p.JournalReceipts != nil {
		next.JournalReceipts = strings.TrimSpace(*p.JournalReceipts)
	}
	if p.WebhookLanes != nil {
		next.WebhookLanes = *p.WebhookLanes
	}
	if p.WebhookFormat != nil {
		next.WebhookFormat = strings.TrimSpace(*p.WebhookFormat)
	}
	if p.WebhookMaxAttempts != nil {
		next.WebhookMaxAttempts = *p.WebhookMaxAttempts
	}
	if p.ContextStore != nil {
		next.ContextStore = *p.ContextStore
	}
	if p.ContextPerChat != nil {
		next.ContextPerChat = *p.ContextPerChat
	}
	if p.ContextMaxAge != nil {
		next.ContextMaxAge = *p.ContextMaxAge
	}
	if p.GuardianProfile != nil {
		next.GuardianProfile = strings.TrimSpace(*p.GuardianProfile)
	}
	if p.GuardianShadowUntil != nil {
		next.GuardianShadowUntil = *p.GuardianShadowUntil
	}
	if p.WebhookPaused != nil {
		next.WebhookPaused = strings.TrimSpace(*p.WebhookPaused)
	}
	if p.QuietHours != nil {
		next.QuietHours = strings.TrimSpace(*p.QuietHours)
	}
	if p.OptoutKeywords != nil {
		next.OptoutKeywords = cloneStrings(*p.OptoutKeywords)
	}
	if p.OptinKeywords != nil {
		next.OptinKeywords = cloneStrings(*p.OptinKeywords)
	}
	if p.OptoutConfirmText != nil {
		next.OptoutConfirmText = *p.OptoutConfirmText
	}
	if p.AutoRejectCalls != nil {
		next.AutoRejectCalls = *p.AutoRejectCalls
	}
	if p.CallRejectMessage != nil {
		next.CallRejectMessage = *p.CallRejectMessage
	}
	if p.RecycleAt != nil {
		next.RecycleAt = strings.TrimSpace(*p.RecycleAt)
	}
	if p.HooksMaxAge != nil {
		next.HooksMaxAge = *p.HooksMaxAge
	}
	if p.TakeoverPause != nil {
		next.TakeoverPause = *p.TakeoverPause
	}
	if p.AIHandoffKeywords != nil {
		next.AIHandoffKeywords = cloneStrings(*p.AIHandoffKeywords)
	}

	if err := next.Validate(); err != nil {
		return InstanceSettings{}, err
	}
	return next, nil
}

// SettingsFieldKind mengembalikan tipe nilai ("bool", "int", "duration", "string", atau "list") untuk satu nama field settings.
func SettingsFieldKind(name string) (string, bool) {
	switch name {
	case "context_store", "auto_reject_calls":
		return "bool", true
	case "webhook_lanes", "webhook_max_attempts", "context_per_chat", "guardian_shadow_until":
		return "int", true
	case "context_max_age", "hooks_max_age", "takeover_pause":
		return "duration", true
	case "timezone", "journal_receipts", "webhook_format", "guardian_profile",
		"webhook_paused", "quiet_hours", "optout_confirm_text", "call_reject_message", "recycle_at":
		return "string", true
	case "optout_keywords", "optin_keywords", "ai_handoff_keywords":
		return "list", true
	default:
		return "", false
	}
}

func (s InstanceSettings) clone() InstanceSettings {
	out := s
	out.OptoutKeywords = cloneStrings(s.OptoutKeywords)
	out.OptinKeywords = cloneStrings(s.OptinKeywords)
	out.AIHandoffKeywords = cloneStrings(s.AIHandoffKeywords)
	return out
}

func cloneStrings(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func validHHMM(s string) bool {
	if len(s) != 5 || s[2] != ':' {
		return false
	}
	hh, errH := strconv.Atoi(s[:2])
	mm, errM := strconv.Atoi(s[3:])
	return errH == nil && errM == nil && hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59
}
