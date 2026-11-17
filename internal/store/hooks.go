package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrHookNotFound dikembalikan saat rule hook tidak ditemukan pada instance.
	ErrHookNotFound = errors.New("rule hook tidak ditemukan")
	// ErrHookExists dikembalikan saat mencoba membuat hook dengan ID yang sudah terdaftar.
	ErrHookExists = errors.New("rule hook sudah terdaftar")
)

// HookRecord merepresentasikan baris tersimpan pada tabel hooks di app.db.
type HookRecord struct {
	InstanceID string `json:"instance_id"`
	ID         string `json:"id"`
	Name       string `json:"name"`
	Enabled    bool   `json:"enabled"`
	Priority   int    `json:"priority"`
	Trusted    bool   `json:"trusted"`
	Spec       string `json:"spec"`
	Version    int    `json:"version"`
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
}

func scanHookRecord(scan func(...any) error) (HookRecord, error) {
	var (
		rec     HookRecord
		enabled int
		trusted int
	)
	err := scan(
		&rec.InstanceID, &rec.ID, &rec.Name,
		&enabled, &rec.Priority, &trusted,
		&rec.Spec, &rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if err != nil {
		return HookRecord{}, err
	}
	rec.Enabled = enabled == 1
	rec.Trusted = trusted == 1
	return rec, nil
}

// ListHooks mengembalikan seluruh rule hook milik satu instance diurutkan ascending menurut priority lalu id.
func (s *Store) ListHooks(ctx context.Context, instanceID string) ([]HookRecord, error) {
	if s == nil || s.DB == nil {
		return nil, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT instance_id, id, name, enabled, priority, trusted, spec, version, created_at, updated_at
		FROM hooks
		WHERE instance_id = ?
		ORDER BY priority ASC, id ASC`,
		instanceID,
	)
	if err != nil {
		return nil, fmt.Errorf("daftar hooks instance %s: %w", instanceID, err)
	}
	defer rows.Close()

	var out []HookRecord
	for rows.Next() {
		rec, err := scanHookRecord(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan hook %s: %w", instanceID, err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// ListHookInstances mengembalikan ID seluruh instance yang memiliki minimal satu rule hook aktif.
func (s *Store) ListHookInstances(ctx context.Context) ([]string, error) {
	if s == nil || s.DB == nil {
		return nil, nil
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT DISTINCT instance_id FROM hooks WHERE enabled = 1 ORDER BY instance_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("daftar instance ber-hook: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan instance hook: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// CountHooks mengembalikan total rule hook yang tersimpan untuk sebuah instance.
func (s *Store) CountHooks(ctx context.Context, instanceID string) (int, error) {
	if s == nil || s.DB == nil {
		return 0, nil
	}
	var count int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM hooks WHERE instance_id = ?`, instanceID).Scan(&count); err != nil {
		return 0, fmt.Errorf("hitung hook %s: %w", instanceID, err)
	}
	return count, nil
}

// GetHook mengambil satu rule hook berdasarkan pasangan (instance_id, hook_id).
func (s *Store) GetHook(ctx context.Context, instanceID, hookID string) (HookRecord, error) {
	if s == nil || s.DB == nil {
		return HookRecord{}, ErrHookNotFound
	}
	row := s.DB.QueryRowContext(ctx, `
		SELECT instance_id, id, name, enabled, priority, trusted, spec, version, created_at, updated_at
		FROM hooks
		WHERE instance_id = ? AND id = ?`,
		instanceID, hookID,
	)
	rec, err := scanHookRecord(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return HookRecord{}, ErrHookNotFound
	}
	if err != nil {
		return HookRecord{}, fmt.Errorf("ambil hook %s/%s: %w", instanceID, hookID, err)
	}
	return rec, nil
}

// CreateHook menyisipkan rule hook baru dan mengembalikan ErrHookExists jika ID sudah dipakai.
func (s *Store) CreateHook(ctx context.Context, rec HookRecord) (HookRecord, error) {
	if s == nil || s.DB == nil {
		return HookRecord{}, errors.New("store belum diinisialisasi")
	}
	now := time.Now().UnixMilli()
	if rec.CreatedAt == 0 {
		rec.CreatedAt = now
	}
	rec.UpdatedAt = now
	if rec.Version <= 0 {
		rec.Version = 1
	}

	res, err := s.DB.ExecContext(ctx, `
		INSERT INTO hooks (instance_id, id, name, enabled, priority, trusted, spec, version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id, id) DO NOTHING`,
		rec.InstanceID, rec.ID, rec.Name, boolInt(rec.Enabled), rec.Priority, boolInt(rec.Trusted),
		rec.Spec, rec.Version, rec.CreatedAt, rec.UpdatedAt,
	)
	if err != nil {
		return HookRecord{}, fmt.Errorf("simpan hook %s/%s: %w", rec.InstanceID, rec.ID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return HookRecord{}, ErrHookExists
	}
	return rec, nil
}

// UpdateHook memperbarui rule hook yang sudah ada beserta kenaikan versi dan status trusted terbaru.
func (s *Store) UpdateHook(ctx context.Context, rec HookRecord) (HookRecord, error) {
	if s == nil || s.DB == nil {
		return HookRecord{}, errors.New("store belum diinisialisasi")
	}
	now := time.Now().UnixMilli()
	res, err := s.DB.ExecContext(ctx, `
		UPDATE hooks
		SET name = ?, enabled = ?, priority = ?, trusted = ?, spec = ?, version = version + 1, updated_at = ?
		WHERE instance_id = ? AND id = ?`,
		rec.Name, boolInt(rec.Enabled), rec.Priority, boolInt(rec.Trusted), rec.Spec, now,
		rec.InstanceID, rec.ID,
	)
	if err != nil {
		return HookRecord{}, fmt.Errorf("perbarui hook %s/%s: %w", rec.InstanceID, rec.ID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return HookRecord{}, ErrHookNotFound
	}
	return s.GetHook(ctx, rec.InstanceID, rec.ID)
}

// UpsertHook menyimpan atau mengganti rule hook pada tabel hooks (dipakai control plane & test).
func (s *Store) UpsertHook(ctx context.Context, rec HookRecord) error {
	if s == nil || s.DB == nil {
		return errors.New("store belum diinisialisasi")
	}
	now := time.Now().UnixMilli()
	if rec.CreatedAt == 0 {
		rec.CreatedAt = now
	}
	if rec.UpdatedAt == 0 {
		rec.UpdatedAt = now
	}
	if rec.Version <= 0 {
		rec.Version = 1
	}
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO hooks (instance_id, id, name, enabled, priority, trusted, spec, version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id, id) DO UPDATE SET
			name = excluded.name,
			enabled = excluded.enabled,
			priority = excluded.priority,
			trusted = excluded.trusted,
			spec = excluded.spec,
			version = hooks.version + 1,
			updated_at = excluded.updated_at`,
		rec.InstanceID, rec.ID, rec.Name, boolInt(rec.Enabled), rec.Priority, boolInt(rec.Trusted),
		rec.Spec, rec.Version, rec.CreatedAt, rec.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert hook %s/%s: %w", rec.InstanceID, rec.ID, err)
	}
	return nil
}

// SetHookEnabled mengaktifkan atau menonaktifkan sebuah rule hook beserta pembaruan spec bila perlu.
func (s *Store) SetHookEnabled(ctx context.Context, instanceID, hookID string, enabled bool) (HookRecord, error) {
	if s == nil || s.DB == nil {
		return HookRecord{}, errors.New("store belum diinisialisasi")
	}
	res, err := s.DB.ExecContext(ctx, `
		UPDATE hooks SET enabled = ?, version = version + 1, updated_at = ?
		WHERE instance_id = ? AND id = ?`,
		boolInt(enabled), time.Now().UnixMilli(), instanceID, hookID,
	)
	if err != nil {
		return HookRecord{}, fmt.Errorf("ubah status hook %s/%s: %w", instanceID, hookID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return HookRecord{}, ErrHookNotFound
	}
	return s.GetHook(ctx, instanceID, hookID)
}

// DeleteHook menghapus rule hook berdasarkan (instance_id, hook_id).
func (s *Store) DeleteHook(ctx context.Context, instanceID, hookID string) error {
	if s == nil || s.DB == nil {
		return errors.New("store belum diinisialisasi")
	}
	res, err := s.DB.ExecContext(ctx, `DELETE FROM hooks WHERE instance_id = ? AND id = ?`, instanceID, hookID)
	if err != nil {
		return fmt.Errorf("hapus hook %s/%s: %w", instanceID, hookID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrHookNotFound
	}
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
