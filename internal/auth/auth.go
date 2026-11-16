// Package auth mengelola pembuatan, pencabutan, scope, policy, audit log, dan verifikasi API key berkecepatan O(1).
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
	"golang.org/x/time/rate"
)

const (
	defaultKeyRatePerSec = 50
)

var (
	// ErrUnauthorized dikembalikan apabila API key kosong, tidak dikenal, atau telah dicabut.
	ErrUnauthorized = errors.New("X-API-Key tidak valid atau sudah dicabut")
	// ErrKeyExpired dikembalikan apabila API key telah melewati batas expires_at.
	ErrKeyExpired = errors.New("API key telah kedaluwarsa")
)

// CreateKeyRequest mengatur opsi pembuatan API key v2.
type CreateKeyRequest struct {
	Name       string
	InstanceID string
	Scopes     ScopeSet
	ExpiresAt  time.Time
	RatePerSec int
	Policy     KeyPolicy
}

// KeyMeta menyimpan metadata API key aktif di memori untuk lookup O(1).
type KeyMeta struct {
	ID         string
	Name       string
	Prefix     string
	InstanceID string // kosong = akses semua instance
	Scopes     ScopeSet
	ExpiresAt  time.Time // zero = tidak kedaluwarsa
	RatePerSec int
	Policy     KeyPolicy
	limiter    *rate.Limiter // default 50 rps, burst 100; bisa diubah per key
}

// Allow memeriksa kuota rate limit token bucket per API key.
func (k *KeyMeta) Allow() bool {
	if k == nil || k.limiter == nil {
		return true
	}
	return k.limiter.Allow()
}

func newKeyLimiter(ratePerSec int) *rate.Limiter {
	rps := ratePerSec
	if rps <= 0 {
		rps = defaultKeyRatePerSec
	}
	burst := rps * 2
	if burst < 1 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(rps), burst)
}

// Manager mengelola cache API key in-memory dan sinkronisasi waktu pakai ke database.
type Manager struct {
	store *store.Store

	mu    sync.RWMutex
	cache map[[32]byte]*KeyMeta
	byID  map[string]*KeyMeta
	dirty map[string]int64
}

// NewManager membuat Manager baru dan memuat seluruh key aktif dari database.
func NewManager(ctx context.Context, st *store.Store) (*Manager, error) {
	m := &Manager{
		store: st,
		cache: make(map[[32]byte]*KeyMeta),
		byID:  make(map[string]*KeyMeta),
		dirty: make(map[string]int64),
	}
	if err := m.Reload(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// Store mengembalikan akses database metadata milik auth Manager.
func (m *Manager) Store() *store.Store {
	if m == nil {
		return nil
	}
	return m.store
}

// Reload menyegarkan peta hash -> KeyMeta dan peta ID -> KeyMeta dari app.db.
func (m *Manager) Reload(ctx context.Context) error {
	keys, err := m.store.ListAPIKeys(ctx)
	if err != nil {
		return err
	}
	next := make(map[[32]byte]*KeyMeta, len(keys))
	nextByID := make(map[string]*KeyMeta, len(keys))
	for _, k := range keys {
		if k.RevokedAt != 0 || len(k.Hash) != 32 {
			continue
		}
		scopes := ScopeAll
		if k.Scopes != "" {
			parsed, err := ParseScopes(k.Scopes)
			if err == nil {
				scopes = parsed
			}
		}
		pol, pErr := ParseKeyPolicy(k.PolicyJSON)
		if pErr != nil {
			pol = DefaultPolicy()
		}
		var exp time.Time
		if k.ExpiresAt > 0 {
			exp = time.Unix(k.ExpiresAt, 0)
		}
		km := &KeyMeta{
			ID:         k.ID,
			Name:       k.Name,
			Prefix:     k.Prefix,
			InstanceID: k.InstanceID,
			Scopes:     scopes,
			ExpiresAt:  exp,
			RatePerSec: k.RatePerSec,
			Policy:     pol,
			limiter:    newKeyLimiter(k.RatePerSec),
		}
		var h [32]byte
		copy(h[:], k.Hash)
		next[h] = km
		nextByID[k.ID] = km
	}
	m.mu.Lock()
	m.cache = next
	m.byID = nextByID
	m.mu.Unlock()
	return nil
}

// Create membuat API key baru berscope '*' (kompatibilitas penuh v1), menyimpannya ke DB, dan memperbarui cache memori.
func (m *Manager) Create(ctx context.Context, name, instanceID string) (raw string, meta store.APIKey, err error) {
	return m.createWithNormalized(ctx, CreateKeyRequest{
		Name:       name,
		InstanceID: instanceID,
		Scopes:     ScopeAll,
		Policy:     DefaultPolicy(),
	})
}

// CreateWithOptions membuat API key baru dengan scope default/terpilih, expiry, rate limit per key, dan KeyPolicy tervalidasi.
func (m *Manager) CreateWithOptions(ctx context.Context, req CreateKeyRequest) (raw string, meta store.APIKey, err error) {
	scopes := req.Scopes
	if scopes == 0 {
		scopes = DefaultScopes()
	}
	pol, err := normalizePolicyForCreate(req.Policy, scopes)
	if err != nil {
		return "", meta, err
	}
	req.Scopes = scopes
	req.Policy = pol
	return m.createWithNormalized(ctx, req)
}

func (m *Manager) createWithNormalized(ctx context.Context, req CreateKeyRequest) (raw string, meta store.APIKey, err error) {
	raw, meta, sum, km, err := buildNewAPIKey(req)
	if err != nil {
		return "", meta, err
	}
	if err = m.store.CreateAPIKey(ctx, meta); err != nil {
		return "", meta, err
	}

	m.mu.Lock()
	m.cache[sum] = km
	m.byID[meta.ID] = km
	m.mu.Unlock()
	return raw, meta, nil
}

// Rotate membuat key baru dengan atribut identik dan menetapkan kedaluwarsa key lama pada now + grace secara atomik.
func (m *Manager) Rotate(ctx context.Context, id string, grace time.Duration) (raw string, meta store.APIKey, err error) {
	oldKey, err := m.store.GetAPIKey(ctx, id)
	if err != nil {
		return "", meta, err
	}
	if oldKey.RevokedAt != 0 {
		return "", meta, store.ErrNotFound
	}
	if grace < 0 {
		grace = 0
	}

	scopes := ScopeAll
	if oldKey.Scopes != "" {
		if parsed, pErr := ParseScopes(oldKey.Scopes); pErr == nil {
			scopes = parsed
		}
	}
	pol, pErr := ParseKeyPolicy(oldKey.PolicyJSON)
	if pErr != nil {
		pol = DefaultPolicy()
	}

	var newExp time.Time
	now := time.Now()
	if oldKey.ExpiresAt > now.Unix() {
		newExp = time.Unix(oldKey.ExpiresAt, 0)
	}

	raw, meta, _, _, err = buildNewAPIKey(CreateKeyRequest{
		Name:       oldKey.Name,
		InstanceID: oldKey.InstanceID,
		Scopes:     scopes,
		ExpiresAt:  newExp,
		RatePerSec: oldKey.RatePerSec,
		Policy:     pol,
	})
	if err != nil {
		return "", meta, err
	}

	oldExpiresAt := now.Add(grace).Unix()
	if err = m.store.RotateAPIKeyTx(ctx, id, oldExpiresAt, meta); err != nil {
		return "", meta, err
	}
	if err = m.Reload(ctx); err != nil {
		return "", meta, err
	}
	return raw, meta, nil
}

func buildNewAPIKey(req CreateKeyRequest) (raw string, meta store.APIKey, sum [32]byte, km *KeyMeta, err error) {
	if req.RatePerSec < 0 {
		return "", meta, sum, nil, errors.New("rate_per_sec tidak boleh negatif")
	}
	var randBytes [32]byte
	if _, err = rand.Read(randBytes[:]); err != nil {
		return "", meta, sum, nil, fmt.Errorf("baca crypto/rand: %w", err)
	}
	var idBytes [3]byte
	if _, err = rand.Read(idBytes[:]); err != nil {
		return "", meta, sum, nil, fmt.Errorf("baca id rand: %w", err)
	}

	raw = "wak_" + base64.RawURLEncoding.EncodeToString(randBytes[:])
	sum = sha256.Sum256([]byte(raw))
	prefix := raw
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}

	polBytes, err := json.Marshal(req.Policy)
	if err != nil {
		return "", meta, sum, nil, fmt.Errorf("marshal policy key: %w", err)
	}
	var expiresUnix int64
	if !req.ExpiresAt.IsZero() {
		expiresUnix = req.ExpiresAt.Unix()
	}

	now := time.Now().Unix()
	meta = store.APIKey{
		ID:         "k_" + hex.EncodeToString(idBytes[:]),
		Name:       req.Name,
		Prefix:     prefix,
		Hash:       sum[:],
		InstanceID: req.InstanceID,
		Scopes:     req.Scopes.String(),
		ExpiresAt:  expiresUnix,
		RatePerSec: req.RatePerSec,
		PolicyJSON: string(polBytes),
		CreatedAt:  now,
	}

	km = &KeyMeta{
		ID:         meta.ID,
		Name:       meta.Name,
		Prefix:     meta.Prefix,
		InstanceID: meta.InstanceID,
		Scopes:     req.Scopes,
		ExpiresAt:  req.ExpiresAt,
		RatePerSec: req.RatePerSec,
		Policy:     req.Policy,
		limiter:    newKeyLimiter(req.RatePerSec),
	}
	return raw, meta, sum, km, nil
}

// Revoke mencabut API key di DB dan langsung menghapusnya dari cache in-memory.
func (m *Manager) Revoke(ctx context.Context, id string) error {
	if err := m.store.RevokeAPIKey(ctx, id); err != nil {
		return err
	}
	return m.Reload(ctx)
}

// Verify memeriksa raw key dan mengembalikan ErrUnauthorized atau ErrKeyExpired jika tidak berlaku.
func (m *Manager) Verify(raw string) (*KeyMeta, error) {
	if m == nil || raw == "" {
		return nil, ErrUnauthorized
	}
	sum := sha256.Sum256([]byte(raw))

	m.mu.RLock()
	meta, ok := m.cache[sum]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrUnauthorized
	}

	now := time.Now()
	if !meta.ExpiresAt.IsZero() && !now.Before(meta.ExpiresAt) {
		return nil, ErrKeyExpired
	}

	// Catat waktu pemakaian secara asinkron di peta dirty untuk di-flush tiap 60 detik.
	m.mu.Lock()
	m.dirty[meta.ID] = now.Unix()
	m.mu.Unlock()
	return meta, nil
}

// LookupPolicy mengembalikan KeyPolicy milik keyID dari cache aktif O(1).
func (m *Manager) LookupPolicy(keyID string) (KeyPolicy, bool) {
	if m == nil || keyID == "" {
		return KeyPolicy{}, false
	}
	m.mu.RLock()
	km, ok := m.byID[keyID]
	m.mu.RUnlock()
	if !ok || km == nil {
		return KeyPolicy{}, false
	}
	return km.Policy, true
}

// AllowedInstance memeriksa apakah key berhak mengakses instanceID tertentu.
func (k *KeyMeta) AllowedInstance(instanceID string) bool {
	return k.InstanceID == "" || k.InstanceID == instanceID
}

// FlushLoop menulis waktu pemakaian key secara berkala agar tidak ada write DB per request.
func (m *Manager) FlushLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = m.FlushOnce(context.Background())
			return
		case <-ticker.C:
			_ = m.FlushOnce(ctx)
		}
	}
}

// FlushOnce memindahkan daftar dirty ke database dalam satu transaksi batch.
func (m *Manager) FlushOnce(ctx context.Context) error {
	m.mu.Lock()
	if len(m.dirty) == 0 {
		m.mu.Unlock()
		return nil
	}
	batch := m.dirty
	m.dirty = make(map[string]int64)
	m.mu.Unlock()

	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return m.store.FlushKeyLastUsed(flushCtx, batch)
}

// GenerateSecret membuat secret acak 32 byte ber-encoding base64 untuk penandatanganan HMAC webhook v1 maupun Standard Webhooks.
func GenerateSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("buat webhook secret: %w", err)
	}
	return "whsec_" + base64.StdEncoding.EncodeToString(b[:]), nil
}
