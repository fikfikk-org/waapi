package wa

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/webhook"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
	"golang.org/x/sync/semaphore"
)

var slugRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)

// Manager mengelola siklus hidup seluruh instance WhatsApp di dalam satu proses daemon.
type Manager struct {
	cfg        *config.Config
	store      *store.Store
	waDB       *sql.DB
	container  *sqlstore.Container
	webhook    *webhook.Dispatcher
	pairingSem *semaphore.Weighted
	mediaSem   *semaphore.Weighted

	mu        sync.RWMutex
	instances map[string]*Instance
}

// NewManager membuka wa.db, menjalankan migrasi sqlstore, dan menyiapkan registry instance.
func NewManager(ctx context.Context, cfg *config.Config, st *store.Store, wh *webhook.Dispatcher) (*Manager, error) {
	ConfigureDeviceProps()

	waDB, err := store.OpenDB(ctx, cfg.WADBPath(), 3)
	if err != nil {
		return nil, fmt.Errorf("buka wa.db: %w", err)
	}
	container := sqlstore.NewWithDB(waDB, "sqlite3", waLog.Noop)
	if err := container.Upgrade(ctx); err != nil {
		_ = waDB.Close()
		return nil, fmt.Errorf("migrasi wa.db: %w", err)
	}

	m := &Manager{
		cfg:        cfg,
		store:      st,
		waDB:       waDB,
		container:  container,
		webhook:    wh,
		pairingSem: semaphore.NewWeighted(1), // maksimal 1 proses pairing dalam satu waktu
		mediaSem:   semaphore.NewWeighted(2), // maksimal 2 stream media aktif bersamaan
		instances:  make(map[string]*Instance),
	}

	rows, err := st.ListInstances(ctx)
	if err != nil {
		_ = waDB.Close()
		return nil, err
	}
	for _, row := range rows {
		m.instances[row.ID] = newInstance(m, row)
	}
	return m, nil
}

// AutoStartAll menyalakan instance dengan auto_start=true secara berurutan (jeda 2 detik) agar tidak memicu spike RAM.
func (m *Manager) AutoStartAll(ctx context.Context) {
	rows, err := m.store.ListInstances(ctx)
	if err != nil {
		slog.Warn("gagal membaca daftar instance untuk auto-start", "err", err)
		return
	}
	first := true
	for _, row := range rows {
		if !row.AutoStart || row.JID == "" || row.Status == store.StatusDisabled || row.Status == store.StatusLoggedOut {
			continue
		}
		if !first {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
		first = false

		inst, ok := m.Get(row.ID)
		if !ok {
			continue
		}
		if err := inst.ConnectExisting(ctx); err != nil {
			slog.Warn("auto-start instance gagal", "instance", row.ID, "err", err)
		} else {
			slog.Info("instance tersambung saat startup", "instance", row.ID, "jid", row.JID)
		}
	}
}

// Close menutup semua koneksi WhatsApp dan database wa.db secara berurutan.
func (m *Manager) Close() error {
	m.mu.Lock()
	list := make([]*Instance, 0, len(m.instances))
	for _, inst := range m.instances {
		list = append(list, inst)
	}
	m.mu.Unlock()

	for _, inst := range list {
		inst.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = m.waDB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return m.waDB.Close()
}

// Create membuat instance baru di app.db dan mendaftarkannya ke memori.
func (m *Manager) Create(ctx context.Context, id string, ratePerMin int, autoStart bool) (*InstanceStatus, error) {
	if !slugRegex.MatchString(id) {
		return nil, errors.New("id instance harus huruf kecil/angka/strip (1-32 karakter)")
	}
	secret, err := auth.GenerateSecret()
	if err != nil {
		return nil, err
	}
	meta := store.Instance{
		ID:            id,
		Status:        store.StatusCreated,
		WebhookSecret: secret,
		WebhookEvents: "message,receipt,connection,pairing",
		RatePerMin:    ratePerMin,
		AutoStart:     autoStart,
	}
	if err := m.store.CreateInstance(ctx, meta); err != nil {
		return nil, err
	}

	inst := newInstance(m, meta)
	m.mu.Lock()
	m.instances[id] = inst
	m.mu.Unlock()

	snap := inst.Snapshot(meta)
	return &snap, nil
}

// Get mengambil runtime instance berdasarkan ID.
func (m *Manager) Get(id string) (*Instance, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inst, ok := m.instances[id]
	return inst, ok
}

// Status mengambil status gabungan (DB + runtime) untuk satu instance.
func (m *Manager) Status(ctx context.Context, id string) (*InstanceStatus, error) {
	meta, err := m.store.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	inst, ok := m.Get(id)
	if !ok {
		inst = newInstance(m, *meta)
		m.mu.Lock()
		m.instances[id] = inst
		m.mu.Unlock()
	}
	snap := inst.Snapshot(*meta)
	return &snap, nil
}

// ListStatuses mengembalikan daftar status seluruh instance.
func (m *Manager) ListStatuses(ctx context.Context) ([]InstanceStatus, error) {
	rows, err := m.store.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]InstanceStatus, 0, len(rows))
	for _, row := range rows {
		inst, ok := m.Get(row.ID)
		if !ok {
			inst = newInstance(m, row)
			m.mu.Lock()
			m.instances[row.ID] = inst
			m.mu.Unlock()
		}
		out = append(out, inst.Snapshot(row))
	}
	return out, nil
}

// Update memperbarui konfigurasi instance di DB dan menyinkronkan rate limiter di memori.
func (m *Manager) Update(ctx context.Context, id string, up store.InstanceUpdate) (*InstanceStatus, error) {
	meta, err := m.store.UpdateInstance(ctx, id, up)
	if err != nil {
		return nil, err
	}
	if inst, ok := m.Get(id); ok && up.RatePerMin != nil {
		inst.SetRatePerMin(*up.RatePerMin)
	}
	return m.Status(ctx, meta.ID)
}

// Restart memutus dan menyambungkan ulang koneksi instance.
func (m *Manager) Restart(ctx context.Context, id string) (*InstanceStatus, error) {
	meta, err := m.store.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	inst, ok := m.Get(id)
	if !ok {
		return nil, store.ErrNotFound
	}
	inst.Disconnect()
	auto := true
	_, _ = m.store.UpdateInstance(ctx, id, store.InstanceUpdate{AutoStart: &auto})
	if meta.JID != "" {
		inst.setStatus(store.StatusDisconnected, meta.JID)
		if err := inst.ConnectExisting(ctx); err != nil {
			return nil, err
		}
	}
	return m.Status(ctx, id)
}

// Stop menghentikan koneksi instance dan menonaktifkan auto_start.
func (m *Manager) Stop(ctx context.Context, id string) (*InstanceStatus, error) {
	inst, ok := m.Get(id)
	if !ok {
		return nil, store.ErrNotFound
	}
	inst.Disconnect()
	inst.setStatus(store.StatusDisabled, "")
	auto := false
	if _, err := m.store.UpdateInstance(ctx, id, store.InstanceUpdate{AutoStart: &auto}); err != nil {
		return nil, err
	}
	if err := m.store.UpdateInstanceStatus(ctx, id, store.StatusDisabled, ""); err != nil {
		return nil, err
	}
	return m.Status(ctx, id)
}

// Logout mengeluarkan perangkat dari WhatsApp tanpa menghapus baris instance di app.db.
func (m *Manager) Logout(ctx context.Context, id string) (*InstanceStatus, error) {
	inst, ok := m.Get(id)
	if !ok {
		return nil, store.ErrNotFound
	}
	if err := inst.Logout(ctx); err != nil {
		return nil, err
	}
	return m.Status(ctx, id)
}

// Delete melakukan logout (jika masih tertaut) lalu menghapus instance dari memori dan DB.
func (m *Manager) Delete(ctx context.Context, id string) error {
	inst, ok := m.Get(id)
	if ok {
		_, _ = inst.CancelPairing()
		_ = inst.Logout(ctx)
		inst.Close()
	}
	m.mu.Lock()
	delete(m.instances, id)
	m.mu.Unlock()
	return m.store.DeleteInstance(ctx, id)
}

// CountByStatus menghitung jumlah instance per status untuk laporan doctor.
func (m *Manager) CountByStatus(ctx context.Context) map[string]int {
	counts := map[string]int{
		store.StatusConnected:    0,
		store.StatusDisconnected: 0,
		store.StatusLoggedOut:    0,
		store.StatusCreated:      0,
		store.StatusDisabled:     0,
	}
	list, err := m.ListStatuses(ctx)
	if err != nil {
		return counts
	}
	for _, it := range list {
		counts[it.Status]++
	}
	return counts
}

