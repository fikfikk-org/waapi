package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

// KeyPolicy membatasi perilaku pengiriman dan tool MCP per API key (ditegakkan mulai Batch 11).
type KeyPolicy struct {
	ReplyWindow     store.Duration `json:"reply_window"`
	AllowedChats    []string       `json:"allowed_chats"`
	FirstContact    string         `json:"first_contact"`
	MaxSendsPerHour int            `json:"max_sends_per_hour"`
	Destructive     string         `json:"destructive"`
	Tools           []string       `json:"tools"`
}

// DefaultPolicy mengembalikan policy bawaan untuk API key reguler.
func DefaultPolicy() KeyPolicy {
	return KeyPolicy{
		ReplyWindow:     0,
		AllowedChats:    []string{},
		FirstContact:    "allow",
		MaxSendsPerHour: 0,
		Destructive:     "allow",
		Tools:           []string{},
	}
}

// DefaultMCPPolicy mengembalikan policy bawaan yang aman untuk API key ber-scope mcp.
func DefaultMCPPolicy() KeyPolicy {
	return KeyPolicy{
		ReplyWindow:     store.Duration(24 * time.Hour),
		AllowedChats:    []string{},
		FirstContact:    "ask",
		MaxSendsPerHour: 30,
		Destructive:     "ask",
		Tools:           []string{},
	}
}

// Validate memeriksa validitas enum dan pola glob pada KeyPolicy.
func (p KeyPolicy) Validate() error {
	if p.ReplyWindow < 0 {
		return errors.New("reply_window tidak boleh negatif")
	}
	if p.MaxSendsPerHour < 0 {
		return errors.New("max_sends_per_hour tidak boleh negatif")
	}
	if !validPolicyAction(p.FirstContact) {
		return fmt.Errorf("first_contact harus allow, deny, atau ask, didapat %q", p.FirstContact)
	}
	if !validPolicyAction(p.Destructive) {
		return fmt.Errorf("destructive harus allow, deny, atau ask, didapat %q", p.Destructive)
	}
	for _, pat := range p.AllowedChats {
		if strings.TrimSpace(pat) == "" {
			return errors.New("pola allowed_chats tidak boleh kosong")
		}
		if _, err := path.Match(pat, "628123456789"); err != nil {
			return fmt.Errorf("pola glob allowed_chats tidak valid %q: %w", pat, err)
		}
	}
	return nil
}

func validPolicyAction(v string) bool {
	switch v {
	case "allow", "deny", "ask":
		return true
	default:
		return false
	}
}

// ParseKeyPolicy mem-parse JSON KeyPolicy di atas nilai default reguler dan memvalidasinya.
func ParseKeyPolicy(raw string) (KeyPolicy, error) {
	p := DefaultPolicy()
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" && trimmed != "{}" {
		if err := json.Unmarshal([]byte(trimmed), &p); err != nil {
			return KeyPolicy{}, fmt.Errorf("parse key policy JSON: %w", err)
		}
		if p.FirstContact == "" {
			p.FirstContact = "allow"
		}
		if p.Destructive == "" {
			p.Destructive = "allow"
		}
		if p.AllowedChats == nil {
			p.AllowedChats = []string{}
		}
		if p.Tools == nil {
			p.Tools = []string{}
		}
	}
	if err := p.Validate(); err != nil {
		return KeyPolicy{}, err
	}
	return p, nil
}

func normalizePolicyForCreate(p KeyPolicy, scopes ScopeSet) (KeyPolicy, error) {
	isZero := p.ReplyWindow == 0 && len(p.AllowedChats) == 0 && p.FirstContact == "" &&
		p.MaxSendsPerHour == 0 && p.Destructive == "" && len(p.Tools) == 0
	if isZero {
		if scopes.Has(ScopeMCP) {
			p = DefaultMCPPolicy()
		} else {
			p = DefaultPolicy()
		}
	} else {
		base := DefaultPolicy()
		if scopes.Has(ScopeMCP) {
			base = DefaultMCPPolicy()
		}
		if p.FirstContact == "" {
			p.FirstContact = base.FirstContact
		}
		if p.Destructive == "" {
			p.Destructive = base.Destructive
		}
		if p.AllowedChats == nil {
			p.AllowedChats = []string{}
		}
		if p.Tools == nil {
			p.Tools = []string{}
		}
	}
	if err := p.Validate(); err != nil {
		return KeyPolicy{}, err
	}
	return p, nil
}
