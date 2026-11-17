// Package hooks menyediakan mesin Reactive Hooks deklaratif yang mengevaluasi rule pada konsumen journal.
package hooks

import "time"

const (
	// ConsumerName adalah nama konsumen cursor pada journal.db untuk mesin hooks.
	ConsumerName = "hooks"

	// Batasan kompilasi dan eksekusi rule sesuai spesifikasi v2 §4.3 Batch 14.
	MaxConditionsPerRule    = 64
	MaxConditionDepth       = 4
	MaxRegexLen             = 512
	MaxActionsPerRule       = 8
	MaxRulesPerInstance     = 50
	DefaultRulePriority     = 100
	DefaultActionTimeoutMs  = 5000
	MaxActionTimeoutMs      = 5000
	MaxHTTPRespBytes        = 64 << 10 // 64 KiB
	MaxHooksSendsPerMin     = 30
	ThrottleLRUCap          = 4096
	TakeoverLRUCap          = 4096
	SuppressLRUCap          = 4096
	GateWaitTimeout         = 10 * time.Second
	DefaultConversationMsgs = 20
	DefaultHookRunsKeep     = 7 * 24 * time.Hour

	MaxAIContextMessages     = 30
	DefaultAIContextMessages = 10
	MaxAITokens              = 1024
	DefaultAIMaxTokens       = 300
	DefaultAITemperature     = 0.3
)

// Rule merepresentasikan spesifikasi JSON satu rule deklaratif reactive hooks.
type Rule struct {
	ID              string    `json:"id"`
	Name            string    `json:"name,omitempty"`
	Enabled         *bool     `json:"enabled,omitempty"`
	Priority        *int      `json:"priority,omitempty"`
	Trusted         bool      `json:"trusted,omitempty"`
	On              []string  `json:"on"`
	When            *Cond     `json:"when,omitempty"`
	Do              []Action  `json:"do"`
	Throttle        *Throttle `json:"throttle,omitempty"`
	ActiveHours     string    `json:"active_hours,omitempty"`
	RespectTakeover *bool     `json:"respect_takeover,omitempty"`
	OnFromMe        bool      `json:"on_from_me,omitempty"`
	Stop            bool      `json:"stop,omitempty"`
}

// IsEnabled mengembalikan status aktif rule dengan nilai bawaan true bila dihilangkan.
func (r Rule) IsEnabled() bool {
	return r.Enabled == nil || *r.Enabled
}

// EffectivePriority mengembalikan prioritas rule dengan nilai bawaan 100 bila dihilangkan.
func (r Rule) EffectivePriority() int {
	if r.Priority == nil {
		return DefaultRulePriority
	}
	return *r.Priority
}

// RespectsTakeover mengembalikan apakah rule menghormati jeda human takeover (default true).
func (r Rule) RespectsTakeover() bool {
	return r.RespectTakeover == nil || *r.RespectTakeover
}

// Cond merepresentasikan node kondisi (kombinator all/any/not atau predikat daun).
type Cond struct {
	All   []Cond `json:"all,omitempty"`
	Any   []Cond `json:"any,omitempty"`
	Not   *Cond  `json:"not,omitempty"`
	Field string `json:"field,omitempty"`
	Op    string `json:"op,omitempty"`
	Value any    `json:"value,omitempty"`
}

// Action merepresentasikan satu aksi deklaratif yang dijalankan saat rule cocok.
type Action struct {
	Action          string   `json:"action"`
	Text            string   `json:"text,omitempty"`
	Quote           bool     `json:"quote,omitempty"`
	Typing          bool     `json:"typing,omitempty"`
	To              string   `json:"to,omitempty"`
	Emoji           string   `json:"emoji,omitempty"`
	URL             string   `json:"url,omitempty"`
	TimeoutMs       int      `json:"timeout_ms,omitempty"`
	Include         string   `json:"include,omitempty"`
	ExpectReply     bool     `json:"expect_reply,omitempty"`
	System          string   `json:"system,omitempty"`
	ContextMessages int      `json:"context_messages,omitempty"`
	MaxTokens       int      `json:"max_tokens,omitempty"`
	Temperature     *float64 `json:"temperature,omitempty"`
	Model           string   `json:"model,omitempty"`
	FallbackText    string   `json:"fallback_text,omitempty"`
}

// EffectiveTemperature mengembalikan nilai temperature ai_reply dengan bawaan 0.3 saat tidak diisi.
func (a Action) EffectiveTemperature() float64 {
	if a.Temperature == nil {
		return DefaultAITemperature
	}
	return *a.Temperature
}

// Throttle mengatur batas laju eksekusi rule per chat (mis. "1/6h").
type Throttle struct {
	PerChat string `json:"per_chat,omitempty"`
}

// RunRecord mencatat status eksekusi satu pasangan (instance, seq, hook) pada hook_runs.
type RunRecord struct {
	InstanceID string `json:"instance_id"`
	Seq        int64  `json:"seq"`
	HookID     string `json:"hook_id"`
	Status     string `json:"status"`
	Detail     string `json:"detail,omitempty"`
	Suppressed bool   `json:"suppressed,omitempty"`
	At         int64  `json:"at"`
}

// DryRunAction merangkum efek aksi hasil simulasi dry-run tanpa melakukan eksekusi nyata.
type DryRunAction struct {
	Action string `json:"action"`
	Text   string `json:"text,omitempty"`
	To     string `json:"to,omitempty"`
	Emoji  string `json:"emoji,omitempty"`
	URL    string `json:"url,omitempty"`
}

// DryRunResult merangkum hasil evaluasi dry-run satu rule terhadap sebuah event.
type DryRunResult struct {
	Matched       bool           `json:"matched"`
	SkippedReason string         `json:"skipped_reason,omitempty"`
	Actions       []DryRunAction `json:"actions"`
}
