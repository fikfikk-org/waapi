package journal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Cursor membaca posisi seq terakhir yang sudah diproses untuk kombinasi consumer, instance_id, dan lane.
func (j *Journal) Cursor(ctx context.Context, consumer, instanceID string, lane int) (int64, error) {
	if j == nil || j.db == nil {
		return 0, errors.New("journal belum diinisialisasi")
	}
	var seq int64
	err := j.db.QueryRowContext(ctx,
		`SELECT seq FROM cursors WHERE consumer = ? AND instance_id = ? AND lane = ?`,
		consumer, instanceID, lane,
	).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("baca cursor %s/%s/%d: %w", consumer, instanceID, lane, err)
	}
	return seq, nil
}

// CommitCursor menyimpan posisi seq konsumen secara monoton naik dan menandainya aktif.
func (j *Journal) CommitCursor(ctx context.Context, consumer, instanceID string, lane int, seq int64) error {
	if j == nil || j.db == nil {
		return errors.New("journal belum diinisialisasi")
	}
	now := time.Now().UnixMilli()
	_, err := j.db.ExecContext(ctx, `
		INSERT INTO cursors (consumer, instance_id, lane, seq, enabled, updated_at)
		VALUES (?, ?, ?, ?, 1, ?)
		ON CONFLICT(consumer, instance_id, lane) DO UPDATE SET
			seq = MAX(cursors.seq, excluded.seq),
			enabled = 1,
			updated_at = excluded.updated_at`,
		consumer, instanceID, lane, seq, now,
	)
	if err != nil {
		return fmt.Errorf("commit cursor %s/%s/%d: %w", consumer, instanceID, lane, err)
	}
	return nil
}

// SetConsumerEnabled mengaktifkan atau menonaktifkan seluruh lane milik satu konsumen pada sebuah instance.
func (j *Journal) SetConsumerEnabled(ctx context.Context, consumer, instanceID string, enabled bool) error {
	if j == nil || j.db == nil {
		return errors.New("journal belum diinisialisasi")
	}
	en := 0
	if enabled {
		en = 1
	}
	_, err := j.db.ExecContext(ctx,
		`UPDATE cursors SET enabled = ?, updated_at = ? WHERE consumer = ? AND instance_id = ?`,
		en, time.Now().UnixMilli(), consumer, instanceID,
	)
	if err != nil {
		return fmt.Errorf("ubah status konsumen %s/%s: %w", consumer, instanceID, err)
	}
	return nil
}

// MinCursor mengembalikan seq minimum dari seluruh konsumen aktif (enabled=1) pada suatu instance.
func (j *Journal) MinCursor(ctx context.Context, instanceID string) (int64, error) {
	if j == nil || j.db == nil {
		return 0, errors.New("journal belum diinisialisasi")
	}
	var minSeq sql.NullInt64
	if err := j.db.QueryRowContext(ctx,
		`SELECT MIN(seq) FROM cursors WHERE instance_id = ? AND enabled = 1`,
		instanceID,
	).Scan(&minSeq); err != nil {
		return 0, fmt.Errorf("baca MinCursor %s: %w", instanceID, err)
	}
	if !minSeq.Valid {
		return 0, nil
	}
	return minSeq.Int64, nil
}
