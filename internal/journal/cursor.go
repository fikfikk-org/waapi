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

// HeadSeq mengembalikan seq tertinggi pada journal milik suatu instance (0 jika belum ada event).
func (j *Journal) HeadSeq(ctx context.Context, instanceID string) (int64, error) {
	if j == nil || j.db == nil {
		return 0, errors.New("journal belum diinisialisasi")
	}
	var head int64
	if err := j.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM events WHERE instance_id = ?`,
		instanceID,
	).Scan(&head); err != nil {
		return 0, fmt.Errorf("baca head journal %s: %w", instanceID, err)
	}
	return head, nil
}

// ConsumerCursorMin membaca nilai seq minimum di seluruh lane milik satu konsumen serta jumlah lane tercatat.
func (j *Journal) ConsumerCursorMin(ctx context.Context, consumer, instanceID string) (seq int64, lanes int, err error) {
	if j == nil || j.db == nil {
		return 0, 0, errors.New("journal belum diinisialisasi")
	}
	var minSeq sql.NullInt64
	if err := j.db.QueryRowContext(ctx,
		`SELECT MIN(seq), COUNT(*) FROM cursors WHERE consumer = ? AND instance_id = ?`,
		consumer, instanceID,
	).Scan(&minSeq, &lanes); err != nil {
		return 0, 0, fmt.Errorf("baca min cursor %s/%s: %w", consumer, instanceID, err)
	}
	if minSeq.Valid {
		seq = minSeq.Int64
	}
	return seq, lanes, nil
}

// ResetConsumerCursors mengatur ulang seluruh lane konsumen ke posisi seq tertentu secara atomik dan menghapus lane usang.
func (j *Journal) ResetConsumerCursors(ctx context.Context, consumer, instanceID string, lanes int, seq int64, enabled bool) error {
	if j == nil || j.db == nil {
		return errors.New("journal belum diinisialisasi")
	}
	if lanes <= 0 {
		lanes = 1
	}
	en := 0
	if enabled {
		en = 1
	}
	now := time.Now().UnixMilli()

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mulai transaksi reset cursor %s/%s: %w", consumer, instanceID, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM cursors WHERE consumer = ? AND instance_id = ?`,
		consumer, instanceID,
	); err != nil {
		return fmt.Errorf("hapus cursor lama %s/%s: %w", consumer, instanceID, err)
	}
	for lane := 0; lane < lanes; lane++ {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO cursors (consumer, instance_id, lane, seq, enabled, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)`,
			consumer, instanceID, lane, seq, en, now,
		); err != nil {
			return fmt.Errorf("sisip cursor %s/%s/%d: %w", consumer, instanceID, lane, err)
		}
	}
	return tx.Commit()
}

// EnsureConsumerCursors memastikan seluruh lane [0..lanes) memiliki baris aktif di tabel cursors.
func (j *Journal) EnsureConsumerCursors(ctx context.Context, consumer, instanceID string, lanes int) error {
	if j == nil || j.db == nil {
		return errors.New("journal belum diinisialisasi")
	}
	if lanes <= 0 {
		lanes = 1
	}
	now := time.Now().UnixMilli()

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mulai transaksi ensure cursor %s/%s: %w", consumer, instanceID, err)
	}
	defer func() { _ = tx.Rollback() }()

	for lane := 0; lane < lanes; lane++ {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO cursors (consumer, instance_id, lane, seq, enabled, updated_at)
			VALUES (?, ?, ?, 0, 1, ?)`,
			consumer, instanceID, lane, now,
		); err != nil {
			return fmt.Errorf("ensure cursor %s/%s/%d: %w", consumer, instanceID, lane, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE cursors SET enabled = 1, updated_at = ?
		WHERE consumer = ? AND instance_id = ? AND lane < ?`,
		now, consumer, instanceID, lanes,
	); err != nil {
		return fmt.Errorf("aktifkan cursor %s/%s: %w", consumer, instanceID, err)
	}
	return tx.Commit()
}

// OldestUndeliveredAge menghitung umur event tertua yang seq-nya berada di atas minCursor.
func (j *Journal) OldestUndeliveredAge(ctx context.Context, instanceID string, minCursor int64) (time.Duration, error) {
	if j == nil || j.db == nil {
		return 0, errors.New("journal belum diinisialisasi")
	}
	var oldestMs sql.NullInt64
	if err := j.db.QueryRowContext(ctx,
		`SELECT MIN(received_at) FROM events WHERE instance_id = ? AND seq > ?`,
		instanceID, minCursor,
	).Scan(&oldestMs); err != nil {
		return 0, fmt.Errorf("baca umur event tertua %s: %w", instanceID, err)
	}
	if !oldestMs.Valid {
		return 0, nil
	}
	age := time.Since(time.UnixMilli(oldestMs.Int64))
	if age < 0 {
		return 0, nil
	}
	return age, nil
}
