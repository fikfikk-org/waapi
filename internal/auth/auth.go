// Package auth mengelola pembuatan, pencabutan, dan verifikasi API key berkecepatan O(1).
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

// KeyMeta menyimpan metadata API key aktif di memori untuk lookup O(1).
type KeyMeta struct {
	ID         string
	Name       string
	Prefix     string
	InstanceID string // kosong = akses semua instance
}

// Manager mengelola cache API key in-memory dan sinkronisasi waktu pakai ke database.
type Manager struct {
	store *store.Store

	mu    sync.RWMutex
	cache map[[32]byte]*KeyMeta
	dirty map[string]int64
}

// NewManager membuat Manager baru dan memuat seluruh key aktif dari database.
func NewManager(ctx context.Context, st *store.Store) (*Manager, error) {
	m := &Manager{
		store: st,
		cache: make(map[[32]byte]*KeyMeta),
		dirty: make(map[string]int64),
	}
	if err := m.Reload(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// Reload menyegarkan peta hash -> KeyMeta dari app.db.
func (m *Manager) Reload(ctx context.Context) error {
	keys, err := m.store.ListAPIKeys(ctx)
	if err != nil {
		return err
	}
	next := make(map[[32]byte]*KeyMeta, len(keys))
	for _, k := range keys {
		if k.RevokedAt != 0 || len(k.Hash) != 32 {
			continue
		}
		var h [32]byte
		copy(h[:], k.Hash)
		next[h] = &KeyMeta{
			ID:         k.ID,
			Name:       k.Name,
			Prefix:     k.Prefix,
			InstanceID: k.InstanceID,
		}
	}
	m.mu.Lock()
	m.cache = next
	m.mu.Unlock()
	return nil
}

// Create membuat API key baru, menyimpannya ke DB, dan memperbarui cache memori.
func (m *Manager) Create(ctx context.Context, name, instanceID string) (raw string, meta store.APIKey, err error) {
	var randBytes [32]byte
	if _, err = rand.Read(randBytes[:]); err != nil {
		return "", meta, fmt.Errorf("baca crypto/rand: %w", err)
	}
	var idBytes [3]byte
	if _, err = rand.Read(idBytes[:]); err != nil {
		return "", meta, fmt.Errorf("baca id rand: %w", err)
	}

	raw = "wak_" + base64.RawURLEncoding.EncodeToString(randBytes[:])
	sum := sha256.Sum256([]byte(raw))
	prefix := raw
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}

	meta = store.APIKey{
		ID:         "k_" + hex.EncodeToString(idBytes[:]),
		Name:       name,
		Prefix:     prefix,
		Hash:       sum[:],
		InstanceID: instanceID,
		CreatedAt:  time.Now().Unix(),
	}
	if err = m.store.CreateAPIKey(ctx, meta); err != nil {
		return "", meta, err
	}

	m.mu.Lock()
	m.cache[sum] = &KeyMeta{
		ID:         meta.ID,
		Name:       meta.Name,
		Prefix:     meta.Prefix,
		InstanceID: meta.InstanceID,
	}
	m.mu.Unlock()
	return raw, meta, nil
}

// Revoke mencabut API key di DB dan langsung menghapusnya dari cache in-memory.
func (m *Manager) Revoke(ctx context.Context, id string) error {
	if err := m.store.RevokeAPIKey(ctx, id); err != nil {
		return err
	}
	return m.Reload(ctx)
}

// Verify memeriksa raw key dan mengembalikan metadata jika valid.
func (m *Manager) Verify(raw string) (*KeyMeta, bool) {
	if raw == "" {
		return nil, false
	}
	sum := sha256.Sum256([]byte(raw))

	m.mu.RLock()
	meta, ok := m.cache[sum]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}

	// Catat waktu pemakaian secara asinkron di peta dirty untuk di-flush tiap 60 detik
	now := time.Now().Unix()
	m.mu.Lock()
	m.dirty[meta.ID] = now
	m.mu.Unlock()
	return meta, true
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

// GenerateSecret membuat secret acak hex 32 byte untuk penandatanganan HMAC webhook.
func GenerateSecret() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("buat webhook secret: %w", err)
	}
	return "whsec_" + hex.EncodeToString(b[:]), nil
}

