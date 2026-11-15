package webhook

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultDeadListLimit = 100
	maxDeadListLimit     = 500
	maxDeadErrorLen      = 512
)

// DeadEntry merepresentasikan satu baris event gagal di tabel webhook_dead pada journal.db.
type DeadEntry struct {
	InstanceID string `json:"instance_id"`
	Seq        int64  `json:"seq"`
	Attempts   int    `json:"attempts"`
	LastStatus int    `json:"last_status"`
	LastError  string `json:"last_error"`
	DeadAt     int64  `json:"dead_at"`
	EventType  string `json:"event_type,omitempty"`
	Chat       string `json:"chat,omitempty"`
}

// ReplayResult merangkum hasil pemrosesan ulang entri DLQ oleh Service.Replay.
type ReplayResult struct {
	Replayed int     `json:"replayed"`
	Failed   int     `json:"failed"`
	Seqs     []int64 `json:"seqs,omitempty"`
}

// DeadStore mengelola penyimpanan dead-letter queue webhook di journal.db.
type DeadStore struct {
	db *sql.DB
}

// NewDeadStore membuat pengelola DLQ berbasis handle SQLite journal.db.
func NewDeadStore(db *sql.DB) *DeadStore {
	if db == nil {
		return nil
	}
	return &DeadStore{db: db}
}

// Record menyimpan atau memperbarui kegagalan permanen sebuah event ke tabel webhook_dead.
func (d *DeadStore) Record(ctx context.Context, entry DeadEntry) error {
	if d == nil || d.db == nil {
		return errors.New("dead store belum diinisialisasi")
	}
	if entry.DeadAt <= 0 {
		entry.DeadAt = time.Now().UnixMilli()
	}
	errMsg := strings.TrimSpace(entry.LastError)
	if len(errMsg) > maxDeadErrorLen {
		errMsg = errMsg[:maxDeadErrorLen]
	}

	_, err := d.db.ExecContext(ctx, `
		INSERT INTO webhook_dead (instance_id, seq, attempts, last_status, last_error, dead_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id, seq) DO UPDATE SET
			attempts = excluded.attempts,
			last_status = excluded.last_status,
			last_error = excluded.last_error,
			dead_at = excluded.dead_at`,
		entry.InstanceID, entry.Seq, entry.Attempts, entry.LastStatus, errMsg, entry.DeadAt,
	)
	if err != nil {
		return fmt.Errorf("simpan entri webhook_dead %s/%d: %w", entry.InstanceID, entry.Seq, err)
	}
	return nil
}

// List mengambil daftar entri DLQ milik satu instance beserta jenis event dan chat asalnya.
func (d *DeadStore) List(ctx context.Context, instanceID string, limit int) ([]DeadEntry, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("dead store belum diinisialisasi")
	}
	if limit <= 0 {
		limit = defaultDeadListLimit
	}
	if limit > maxDeadListLimit {
		limit = maxDeadListLimit
	}

	rows, err := d.db.QueryContext(ctx, `
		SELECT d.instance_id, d.seq, d.attempts, d.last_status, d.last_error, d.dead_at,
		       COALESCE(e.type, ''), COALESCE(e.chat, '')
		FROM webhook_dead d
		LEFT JOIN events e ON e.instance_id = d.instance_id AND e.seq = d.seq
		WHERE d.instance_id = ?
		ORDER BY d.seq ASC
		LIMIT ?`,
		instanceID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("baca daftar webhook_dead %s: %w", instanceID, err)
	}
	defer rows.Close()

	out := make([]DeadEntry, 0)
	for rows.Next() {
		var it DeadEntry
		if err := rows.Scan(
			&it.InstanceID, &it.Seq, &it.Attempts, &it.LastStatus, &it.LastError, &it.DeadAt,
			&it.EventType, &it.Chat,
		); err != nil {
			return nil, fmt.Errorf("scan webhook_dead: %w", err)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// Count menghitung jumlah total entri DLQ untuk satu instance.
func (d *DeadStore) Count(ctx context.Context, instanceID string) (int, error) {
	if d == nil || d.db == nil {
		return 0, nil
	}
	var n int
	if err := d.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM webhook_dead WHERE instance_id = ?`,
		instanceID,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("hitung webhook_dead %s: %w", instanceID, err)
	}
	return n, nil
}

// Seqs mengambil daftar seq DLQ milik satu instance secara terurut.
func (d *DeadStore) Seqs(ctx context.Context, instanceID string, limit int) ([]int64, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("dead store belum diinisialisasi")
	}
	if limit <= 0 {
		limit = maxDeadListLimit
	}
	rows, err := d.db.QueryContext(ctx,
		`SELECT seq FROM webhook_dead WHERE instance_id = ? ORDER BY seq ASC LIMIT ?`,
		instanceID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("baca seq webhook_dead %s: %w", instanceID, err)
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		out = append(out, seq)
	}
	return out, rows.Err()
}

// Delete menghapus seq tertentu dari webhook_dead setelah berhasil dikirim ulang.
func (d *DeadStore) Delete(ctx context.Context, instanceID string, seq int64) error {
	if d == nil || d.db == nil {
		return errors.New("dead store belum diinisialisasi")
	}
	_, err := d.db.ExecContext(ctx,
		`DELETE FROM webhook_dead WHERE instance_id = ? AND seq = ?`,
		instanceID, seq,
	)
	return err
}

// Purge menghapus seluruh entri DLQ milik satu instance dan mengembalikan jumlah baris yang terhapus.
func (d *DeadStore) Purge(ctx context.Context, instanceID string) (int64, error) {
	if d == nil || d.db == nil {
		return 0, errors.New("dead store belum diinisialisasi")
	}
	res, err := d.db.ExecContext(ctx, `DELETE FROM webhook_dead WHERE instance_id = ?`, instanceID)
	if err != nil {
		return 0, fmt.Errorf("hapus semua webhook_dead %s: %w", instanceID, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
