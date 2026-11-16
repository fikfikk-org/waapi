// Package policy menegakkan batasan KeyPolicy per API key pada jalur pengiriman outbox, grup destruktif, dan MCP.
package policy

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/outbox"
)

const (
	hourWindowMinutes  = 60
	maxRateBucketKeys  = 4096
)

var (
	// ErrChatNotAllowed dikembalikan ketika target chat tidak cocok dengan daftar glob allowed_chats.
	ErrChatNotAllowed = errors.New("tujuan chat tidak diizinkan oleh policy API key")
	// ErrOutsideReplyWindow dikembalikan ketika kontak tujuan tidak memiliki pesan masuk dalam batas reply_window.
	ErrOutsideReplyWindow = errors.New("pengiriman di luar batas waktu balas (reply_window) API key")
	// ErrFirstContactDenied dikembalikan saat policy menolak mengirim ke kontak yang belum pernah mengirim pesan masuk.
	ErrFirstContactDenied = errors.New("policy key menolak kontak pertama tanpa riwayat pesan masuk")
	// ErrFirstContactNeedsApproval dikembalikan saat policy mewajibkan persetujuan sebelum menghubungi kontak baru.
	ErrFirstContactNeedsApproval = errors.New("kontak pertama memerlukan persetujuan operator sesuai policy key")
	// ErrPolicyRate dikembalikan saat jumlah pengiriman per jam melebihi max_sends_per_hour pada policy key.
	ErrPolicyRate = errors.New("batas kirim per jam policy API key terlampaui")
	// ErrDestructiveDenied dikembalikan saat aksi destruktif (revoke, remove, leave) diblokir oleh policy key.
	ErrDestructiveDenied = errors.New("aksi destruktif dilarang oleh policy API key")
	// ErrDestructiveNeedsApproval dikembalikan saat aksi destruktif memerlukan persetujuan eksplisit sesuai policy key.
	ErrDestructiveNeedsApproval = errors.New("aksi destruktif memerlukan persetujuan operator sesuai policy key")
)

// PolicyError membungkus sentinel error policy dengan kode HTTP terstruktur untuk router HTTP API.
type PolicyError struct {
	Status     int
	Code       string
	RetryAfter int
	Sentinel   error
	Details    map[string]any
}

func (e *PolicyError) Error() string              { return e.Sentinel.Error() }
func (e *PolicyError) Unwrap() error              { return e.Sentinel }
func (e *PolicyError) HTTPStatus() int            { return e.Status }
func (e *PolicyError) ErrorCode() string          { return e.Code }
func (e *PolicyError) ErrorRetryAfter() int       { return e.RetryAfter }
func (e *PolicyError) ErrorDetails() map[string]any { return e.Details }

func newPolicyError(status int, code string, retryAfter int, sentinel error) error {
	return &PolicyError{
		Status:     status,
		Code:       code,
		RetryAfter: retryAfter,
		Sentinel:   sentinel,
	}
}

// KeyPolicyLookup menyediakan akses baca KeyPolicy aktif berdasarkan key ID.
type KeyPolicyLookup interface {
	LookupPolicy(keyID string) (auth.KeyPolicy, bool)
}

// ContactView membaca stempel waktu kontak masuk dari contacts_seen (Batch 9).
type ContactView interface {
	LastIn(ctx context.Context, instanceID, canonicalJID string) (time.Time, bool)
	FirstIn(ctx context.Context, instanceID, canonicalJID string) (time.Time, bool)
}

type minuteBucket struct {
	minEpoch int64
	count    int
}

// Enforcer mengimplementasikan outbox.PolicyChecker serta CheckDestructive untuk mutasi grup.
type Enforcer struct {
	keys     KeyPolicyLookup
	contacts ContactView

	mu      sync.Mutex
	buckets map[string]*[hourWindowMinutes]minuteBucket
}

// New membangun PolicyEnforcer baru di atas KeyPolicyLookup (mis. *auth.Manager) dan ContactView.
func New(keys KeyPolicyLookup, contacts ContactView) *Enforcer {
	return &Enforcer{
		keys:     keys,
		contacts: contacts,
		buckets:  make(map[string]*[hourWindowMinutes]minuteBucket),
	}
}

// CheckSend mengevaluasi seluruh aturan KeyPolicy pada jalur enqueue outbox.
func (e *Enforcer) CheckSend(ctx context.Context, instanceID string, origin outbox.Origin, spec outbox.Spec) error {
	if e == nil || e.keys == nil || strings.EqualFold(origin.Kind, "system") || strings.TrimSpace(origin.KeyID) == "" {
		return nil
	}
	pol, ok := e.keys.LookupPolicy(origin.KeyID)
	if !ok {
		return nil
	}

	canonJID, digits := canonicalTarget(spec.To)

	if len(pol.AllowedChats) > 0 && !matchAnyChatPattern(pol.AllowedChats, canonJID, digits, spec.To) {
		return newPolicyError(http.StatusForbidden, "chat_not_allowed", 0, ErrChatNotAllowed)
	}

	if strings.EqualFold(spec.Type, "revoke") {
		if err := evalDestructivePolicy(pol.Destructive); err != nil {
			return err
		}
	}

	if pol.ReplyWindow > 0 {
		var lastIn time.Time
		var found bool
		if e.contacts != nil && canonJID != "" {
			lastIn, found = e.contacts.LastIn(ctx, instanceID, canonJID)
		}
		if !found || time.Since(lastIn) > time.Duration(pol.ReplyWindow) {
			return newPolicyError(http.StatusForbidden, "outside_reply_window", 0, ErrOutsideReplyWindow)
		}
	}

	if pol.FirstContact == "deny" || pol.FirstContact == "ask" {
		var found bool
		if e.contacts != nil && canonJID != "" {
			_, found = e.contacts.FirstIn(ctx, instanceID, canonJID)
		}
		if !found {
			if pol.FirstContact == "deny" {
				return newPolicyError(http.StatusForbidden, "first_contact_denied", 0, ErrFirstContactDenied)
			}
			return newPolicyError(http.StatusForbidden, "first_contact_requires_approval", 0, ErrFirstContactNeedsApproval)
		}
	}

	if pol.MaxSendsPerHour > 0 {
		if !e.allowHourlySend(origin.KeyID, pol.MaxSendsPerHour, time.Now()) {
			return newPolicyError(http.StatusTooManyRequests, "policy_rate_limited", 60, ErrPolicyRate)
		}
	}

	return nil
}

// CheckDestructive memvalidasi kebijakan destructive untuk operasi berbahaya seperti remove peserta atau leave grup.
func (e *Enforcer) CheckDestructive(origin outbox.Origin, action string) error {
	if e == nil || e.keys == nil || strings.EqualFold(origin.Kind, "system") || strings.TrimSpace(origin.KeyID) == "" {
		return nil
	}
	act := strings.ToLower(strings.TrimSpace(action))
	if act != "revoke" && act != "remove" && act != "leave" {
		return nil
	}
	pol, ok := e.keys.LookupPolicy(origin.KeyID)
	if !ok {
		return nil
	}
	return evalDestructivePolicy(pol.Destructive)
}

func evalDestructivePolicy(mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "deny":
		return newPolicyError(http.StatusForbidden, "destructive_denied", 0, ErrDestructiveDenied)
	case "ask":
		return newPolicyError(http.StatusForbidden, "destructive_requires_approval", 0, ErrDestructiveNeedsApproval)
	default:
		return nil
	}
}

func canonicalTarget(rawTo string) (canonJID, digits string) {
	parsed, err := jid.ParseRecipient(rawTo)
	if err == nil && !parsed.IsEmpty() {
		canonJID = parsed.ToNonAD().String()
		digits = parsed.User
		return canonJID, digits
	}
	trimmed := strings.TrimSpace(rawTo)
	return trimmed, jid.DigitsOnly(trimmed)
}

func matchAnyChatPattern(patterns []string, canonJID, digits, rawTo string) bool {
	candidates := make([]string, 0, 3)
	for _, c := range []string{canonJID, digits, strings.TrimSpace(rawTo)} {
		if c != "" {
			candidates = append(candidates, c)
		}
	}
	for _, pat := range patterns {
		for _, cand := range candidates {
			if ok, err := path.Match(pat, cand); err == nil && ok {
				return true
			}
		}
	}
	return false
}

func (e *Enforcer) allowHourlySend(keyID string, limit int, now time.Time) bool {
	curMin := now.Unix() / 60
	cutoffMin := curMin - hourWindowMinutes

	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.buckets) >= maxRateBucketKeys {
		// Pangkas bucket usang sebelum map tumbuh melebihi batas.
		for k, ring := range e.buckets {
			active := false
			for i := range hourWindowMinutes {
				if ring[i].minEpoch > cutoffMin && ring[i].count > 0 {
					active = true
					break
				}
			}
			if !active {
				delete(e.buckets, k)
			}
		}
		if len(e.buckets) >= maxRateBucketKeys {
			clear(e.buckets)
		}
	}

	ring, ok := e.buckets[keyID]
	if !ok {
		ring = new([hourWindowMinutes]minuteBucket)
		e.buckets[keyID] = ring
	}

	var total int
	for i := range hourWindowMinutes {
		if ring[i].minEpoch > cutoffMin {
			total += ring[i].count
		}
	}
	if total >= limit {
		return false
	}

	slot := int(((curMin % hourWindowMinutes) + hourWindowMinutes) % hourWindowMinutes)
	if ring[slot].minEpoch != curMin {
		ring[slot] = minuteBucket{minEpoch: curMin, count: 1}
	} else {
		ring[slot].count++
	}
	return true
}
