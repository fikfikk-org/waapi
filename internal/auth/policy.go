package auth

import (
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
