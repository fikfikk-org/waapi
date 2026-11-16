package auth

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strings"
	"time"
)

const (
	defaultAuditLimit = 100
	maxAuditLimit     = 500
	// AuditRetention adalah masa simpan log audit administratif sesuai kontrak Batch 11.
	AuditRetention = 90 * 24 * time.Hour
)

// AuditEntry merepresentasikan satu baris rekaman audit aksi administratif di app.db.
type AuditEntry struct {
	ID     int64  `json:"id"`
	At     int64  `json:"at"`
	Actor  string `json:"actor"`
	Action string `json:"action"`
	Target string `json:"target"`
	Detail string `json:"detail"`
}

// AuditQuery memfilter hasil pembacaan log audit.
type AuditQuery struct {
	Since         time.Time
	ActionPattern string // pola glob action, mis. "key.*" atau "settings.update"
	Limit         int
}

// AuditLog mencatat dan membaca rekaman aksi administratif pada tabel audit_log.
type AuditLog struct {
	db *sql.DB
}

// NewAuditLog membuat AuditLog baru di atas koneksi app.db yang telah dimigrasi.
func NewAuditLog(db *sql.DB) *AuditLog {
	return &AuditLog{db: db}
}

// Write menulis satu entri audit ke app.db tanpa menyimpan rahasia mentah.
func (a *AuditLog) Write(ctx context.Context, actor, action, target, detail string) error {
	if a == nil || a.db == nil {
		return nil
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "system"
	}
	action = strings.TrimSpace(action)
	if action == "" {
		return nil
	}
	_, err := a.db.ExecContext(ctx,
		`INSERT INTO audit_log (at, actor, action, target, detail) VALUES (?, ?, ?, ?, ?)`,
		time.Now().Unix(), actor, action, target, detail,
	)
	if err != nil {
		return fmt.Errorf("tulis audit_log: %w", err)
	}
	return nil
}

// List membaca entri audit terbaru sesuai filter Since, ActionPattern, dan Limit.
func (a *AuditLog) List(ctx context.Context, q AuditQuery) ([]AuditEntry, error) {
	if a == nil || a.db == nil {
		return nil, nil
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultAuditLimit
	}
	if limit > maxAuditLimit {
		limit = maxAuditLimit
	}

	var sinceUnix int64
	if !q.Since.IsZero() {
		sinceUnix = q.Since.Unix()
	}

	pattern := strings.TrimSpace(q.ActionPattern)
	if pattern != "" {
		if _, err := path.Match(pattern, "key.create"); err != nil {
			return nil, fmt.Errorf("pola filter action tidak valid %q: %w", pattern, err)
		}
	}

	// Tarik batas terikat dari DB lalu saring dengan path.Match agar pola glob * dan ? akurat.
	fetchCap := limit
	if pattern != "" {
		fetchCap = maxAuditLimit * 4
	}
	rows, err := a.db.QueryContext(ctx, `
		SELECT id, at, actor, action, target, detail
		FROM audit_log
		WHERE at >= ?
		ORDER BY id DESC
		LIMIT ?`,
		sinceUnix, fetchCap,
	)
	if err != nil {
		return nil, fmt.Errorf("baca audit_log: %w", err)
	}
	defer rows.Close()

	out := make([]AuditEntry, 0, min(limit, 32))
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.At, &e.Actor, &e.Action, &e.Target, &e.Detail); err != nil {
			return nil, fmt.Errorf("scan audit_log: %w", err)
		}
		if pattern != "" {
			matched, _ := path.Match(pattern, e.Action)
			if !matched {
				continue
			}
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Prune menghapus entri audit yang lebih tua dari cutoff dan mengembalikan jumlah baris terhapus.
func (a *AuditLog) Prune(ctx context.Context, cutoff time.Time) (int64, error) {
	if a == nil || a.db == nil {
		return 0, nil
	}
	res, err := a.db.ExecContext(ctx, `DELETE FROM audit_log WHERE at < ?`, cutoff.Unix())
	if err != nil {
		return 0, fmt.Errorf("pangkas audit_log: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
