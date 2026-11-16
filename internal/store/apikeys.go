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
	Scopes     string `json:"scopes"`
	ExpiresAt  int64  `json:"expires_at,omitempty"`
	RatePerSec int    `json:"rate_per_sec,omitempty"`
	PolicyJSON string `json:"policy,omitempty"`
	CreatedAt  int64  `json:"created_at"`
	LastUsedAt int64  `json:"last_used_at,omitempty"`
	RevokedAt  int64  `json:"revoked_at,omitempty"`
}

// CreateAPIKey menyimpan hash API key baru beserta atribut v2 ke database.
func (s *Store) CreateAPIKey(ctx context.Context, k APIKey) error {
	now := k.CreatedAt
	if now <= 0 {
		now = time.Now().Unix()
	}
	scopes := k.Scopes
	if scopes == "" {
		scopes = "*"
	}
	polJSON := k.PolicyJSON
	if polJSON == "" {
		polJSON = "{}"
	}
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO api_keys (
			id, name, prefix, hash, instance_id,
			scopes, expires_at, rate_per_sec, policy, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		k.ID, k.Name, k.Prefix, k.Hash, nullString(k.InstanceID),
		scopes, nullPositiveInt64(k.ExpiresAt), nullPositiveInt(k.RatePerSec), polJSON, now,
	)
	if err != nil {
		return fmt.Errorf("simpan api key %s: %w", k.ID, err)
	}
	return nil
}

// GetAPIKey mengambil metadata satu API key berdasarkan ID.
func (s *Store) GetAPIKey(ctx context.Context, id string) (*APIKey, error) {
	row := s.DB.QueryRowContext(ctx, `
		SELECT id, name, prefix, hash, instance_id,
			COALESCE(scopes, '*'), expires_at, rate_per_sec, COALESCE(policy, '{}'),
			created_at, last_used_at, revoked_at
		FROM api_keys WHERE id = ?`, id)
	k, err := scanAPIKey(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ambil api key %s: %w", id, err)
	}
	return &k, nil
}

// ListAPIKeys mengembalikan semua API key termasuk yang sudah dicabut.
func (s *Store) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, name, prefix, hash, instance_id,
			COALESCE(scopes, '*'), expires_at, rate_per_sec, COALESCE(policy, '{}'),
			created_at, last_used_at, revoked_at
		FROM api_keys ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("ambil daftar api key: %w", err)
	}
	defer rows.Close()

	var out []APIKey
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, fmt.Errorf("scan api key: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func scanAPIKey(scanner interface{ Scan(...any) error }) (APIKey, error) {
	var (
		k                      APIKey
		instID                 sql.NullString
		expiresAt, ratePerSec  sql.NullInt64
		lastUsed, revoke       sql.NullInt64
	)
	err := scanner.Scan(
		&k.ID, &k.Name, &k.Prefix, &k.Hash, &instID,
		&k.Scopes, &expiresAt, &ratePerSec, &k.PolicyJSON,
		&k.CreatedAt, &lastUsed, &revoke,
	)
	if err != nil {
		return APIKey{}, err
	}
	k.InstanceID = instID.String
	k.ExpiresAt = expiresAt.Int64
	k.RatePerSec = int(ratePerSec.Int64)
	k.LastUsedAt = lastUsed.Int64
	k.RevokedAt = revoke.Int64
	return k, nil
}

// RotateAPIKeyTx menyetel masa tenggang expires_at pada key lama dan menyimpan key baru secara atomik.
func (s *Store) RotateAPIKeyTx(ctx context.Context, oldID string, oldExpiresAt int64, newKey APIKey) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mulai transaksi rotasi api key: %w", err)
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE api_keys SET expires_at = ? WHERE id = ? AND revoked_at IS NULL`,
		oldExpiresAt, oldID,
	)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("perbarui kedaluwarsa key lama %s: %w", oldID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		_ = tx.Rollback()
		return ErrNotFound
	}

	now := newKey.CreatedAt
	if now <= 0 {
		now = time.Now().Unix()
	}
	scopes := newKey.Scopes
	if scopes == "" {
		scopes = "*"
	}
	polJSON := newKey.PolicyJSON
	if polJSON == "" {
		polJSON = "{}"
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO api_keys (
			id, name, prefix, hash, instance_id,
			scopes, expires_at, rate_per_sec, policy, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		newKey.ID, newKey.Name, newKey.Prefix, newKey.Hash, nullString(newKey.InstanceID),
		scopes, nullPositiveInt64(newKey.ExpiresAt), nullPositiveInt(newKey.RatePerSec), polJSON, now,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("simpan key hasil rotasi %s: %w", newKey.ID, err)
	}
	return tx.Commit()
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

func nullPositiveInt(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}

func nullPositiveInt64(v int64) any {
	if v <= 0 {
		return nil
	}
	return v
}
