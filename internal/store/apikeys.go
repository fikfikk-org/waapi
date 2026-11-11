package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// APIKey merepresentasikan metadata kunci akses API (tanpa plaintext).
type APIKey struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Prefix     string `json:"prefix"`
	Hash       []byte `json:"-"`
	InstanceID string `json:"instance_id,omitempty"`
	CreatedAt  int64  `json:"created_at"`
	LastUsedAt int64  `json:"last_used_at,omitempty"`
	RevokedAt  int64  `json:"revoked_at,omitempty"`
}

// CreateAPIKey menyimpan hash API key baru ke database.
func (s *Store) CreateAPIKey(ctx context.Context, k APIKey) error {
	now := time.Now().Unix()
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO api_keys (id, name, prefix, hash, instance_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		k.ID, k.Name, k.Prefix, k.Hash, nullString(k.InstanceID), now,
	)
	if err != nil {
		return fmt.Errorf("simpan api key %s: %w", k.ID, err)
	}
	return nil
}

// ListAPIKeys mengembalikan semua API key termasuk yang sudah dicabut.
func (s *Store) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, name, prefix, hash, instance_id, created_at, last_used_at, revoked_at
		FROM api_keys ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("ambil daftar api key: %w", err)
	}
	defer rows.Close()

	var out []APIKey
	for rows.Next() {
		var (
			k                APIKey
			instID           sql.NullString
			lastUsed, revoke sql.NullInt64
		)
		if err := rows.Scan(&k.ID, &k.Name, &k.Prefix, &k.Hash, &instID, &k.CreatedAt, &lastUsed, &revoke); err != nil {
			return nil, fmt.Errorf("scan api key: %w", err)
		}
		k.InstanceID = instID.String
		k.LastUsedAt = lastUsed.Int64
		k.RevokedAt = revoke.Int64
		out = append(out, k)
	}
	return out, rows.Err()
}

// RevokeAPIKey mencabut API key berdasarkan ID.
func (s *Store) RevokeAPIKey(ctx context.Context, id string) error {
	res, err := s.DB.ExecContext(ctx,
		`UPDATE api_keys SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL`,
		time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("revoke api key %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// FlushKeyLastUsed menyimpan waktu pemakaian terakhir sejumlah API key dalam satu transaksi.
func (s *Store) FlushKeyLastUsed(ctx context.Context, used map[string]int64) error {
	if len(used) == 0 {
		return nil
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mulai tx flush last_used_at: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, `UPDATE api_keys SET last_used_at = ? WHERE id = ?`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare flush last_used_at: %w", err)
	}
	defer stmt.Close()

	for id, ts := range used {
		if _, err := stmt.ExecContext(ctx, ts, id); err != nil && !errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			return fmt.Errorf("update last_used_at %s: %w", id, err)
		}
	}
	return tx.Commit()
}
