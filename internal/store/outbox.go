package store

import (
	"context"
	"fmt"
	"time"
)

// OutboxItem merepresentasikan satu event webhook yang tertunda di disk.
type OutboxItem struct {
	ID          int64
	InstanceID  string
	Event       string
	Payload     []byte
	Attempts    int
	NextRetryAt int64
	CreatedAt   int64
}

// EnqueueOutbox menyimpan event webhook ke disk saat pengiriman gagal atau antrian memori penuh.
func (s *Store) EnqueueOutbox(ctx context.Context, instanceID, event string, payload []byte, attempts int, nextRetryAt int64) error {
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO webhook_outbox (instance_id, event, payload, attempts, next_retry_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		instanceID, event, payload, attempts, nextRetryAt, time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("enqueue outbox: %w", err)
	}
	return nil
}

// ClaimDueOutbox mengambil maksimal limit item yang sudah jatuh tempo untuk dikirim ulang.
func (s *Store) ClaimDueOutbox(ctx context.Context, now int64, limit int) ([]OutboxItem, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, instance_id, event, payload, attempts, next_retry_at, created_at
		FROM webhook_outbox
		WHERE next_retry_at <= ?
		ORDER BY next_retry_at ASC
		LIMIT ?`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("ambil outbox jatuh tempo: %w", err)
	}
	defer rows.Close()

	var out []OutboxItem
	for rows.Next() {
		var it OutboxItem
		if err := rows.Scan(&it.ID, &it.InstanceID, &it.Event, &it.Payload, &it.Attempts, &it.NextRetryAt, &it.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// DeleteOutbox menghapus item outbox setelah berhasil dikirim atau melewati batas retry.
func (s *Store) DeleteOutbox(ctx context.Context, id int64) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM webhook_outbox WHERE id = ?`, id)
	return err
}

// RescheduleOutbox menaikkan jumlah percobaan dan menjadwalkan waktu retry berikutnya.
func (s *Store) RescheduleOutbox(ctx context.Context, id int64, attempts int, nextRetryAt int64) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE webhook_outbox SET attempts = ?, next_retry_at = ? WHERE id = ?`,
		attempts, nextRetryAt, id)
	return err
}

// PruneOutbox menghapus baris yang terlalu tua atau sudah melewati batas maksimum percobaan.
func (s *Store) PruneOutbox(ctx context.Context, maxAttempts int, olderThan int64) error {
	_, err := s.DB.ExecContext(ctx,
		`DELETE FROM webhook_outbox WHERE attempts >= ? OR created_at < ?`,
		maxAttempts, olderThan)
	return err
}

// CountOutbox menghitung jumlah event yang masih mengantri di disk.
func (s *Store) CountOutbox(ctx context.Context) (int, error) {
	var n int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM webhook_outbox`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
