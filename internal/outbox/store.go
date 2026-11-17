package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
)

const selectOutboxCols = `
	id, instance_id, idem_key, req_hash, wa_id, to_jid, kind, spec,
	priority, origin, key_id, status, attempts, not_before, lease_until,
	expires_at, error_code, error_msg, sent_at, delivered_at, read_at,
	rcpt_delivered, rcpt_read, created_at, updated_at`

// SQLiteStore mengimplementasikan persistensi outbox pada tabel outbox di journal.db.
type SQLiteStore struct {
	db       *sql.DB
	onTxEvt  func(ctx context.Context, tx *sql.Tx, ev *journal.Event) error
}

// NewSQLiteStore membuat SQLiteStore baru di atas handle journal.db yang sudah dimigrasi.
func NewSQLiteStore(db *sql.DB) *SQLiteStore {
	return &SQLiteStore{db: db}
}

func (s *SQLiteStore) setEventTxHook(fn func(ctx context.Context, tx *sql.Tx, ev *journal.Event) error) {
	if s != nil {
		s.onTxEvt = fn
	}
}

// GetByID mengambil satu job outbox berdasarkan instance_id dan ID job.
func (s *SQLiteStore) GetByID(ctx context.Context, instanceID, id string) (*Job, error) {
	if s == nil || s.db == nil {
		return nil, ErrNotFound
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+selectOutboxCols+` FROM outbox WHERE instance_id = ? AND id = ?`, instanceID, id)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ambil job outbox by id: %w", err)
	}
	return j, nil
}

// GetByIdemKey mengambil job berdasarkan pasangan unik (instance_id, idem_key).
func (s *SQLiteStore) GetByIdemKey(ctx context.Context, instanceID, idemKey string) (*Job, error) {
	if s == nil || s.db == nil || idemKey == "" {
		return nil, ErrNotFound
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+selectOutboxCols+` FROM outbox WHERE instance_id = ? AND idem_key = ?`, instanceID, idemKey)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ambil job outbox by idem_key: %w", err)
	}
	return j, nil
}

// CountQueued mengembalikan jumlah job berstatus queued pada instance tertentu.
func (s *SQLiteStore) CountQueued(ctx context.Context, instanceID string) (int, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE instance_id = ? AND status = 'queued'`, instanceID).Scan(&count); err != nil {
		return 0, fmt.Errorf("hitung antrian outbox: %w", err)
	}
	return count, nil
}

// HasSending mengembalikan true apabila instance memiliki job yang sedang dalam status pengiriman (sending).
func (s *SQLiteStore) HasSending(ctx context.Context, instanceID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	var exists int
	err := s.db.QueryRowContext(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM outbox WHERE instance_id = ? AND status = 'sending')`,
		instanceID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("cek status sending outbox: %w", err)
	}
	return exists == 1, nil
}

// HasWAID memeriksa apakah wa_id tertentu sudah tercatat milik instance di outbox (untuk deteksi human takeover).
func (s *SQLiteStore) HasWAID(ctx context.Context, instanceID, waID string) (bool, error) {
	if s == nil || s.db == nil || instanceID == "" || waID == "" {
		return false, nil
	}
	var exists int
	err := s.db.QueryRowContext(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM outbox WHERE instance_id = ? AND wa_id = ?)`,
		instanceID, waID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("cek wa_id outbox: %w", err)
	}
	return exists == 1, nil
}

// Insert menyisipkan baris job baru dan mengembalikan ErrIdempotencyConflict jika melanggar indeks idem_key.
func (s *SQLiteStore) Insert(ctx context.Context, job *Job) error {
	if s == nil || s.db == nil {
		return errors.New("store outbox belum diinisialisasi")
	}
	specJSON, err := json.Marshal(job.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec outbox: %w", err)
	}
	var idem any
	if job.IdemKey != "" {
		idem = job.IdemKey
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO outbox (
			id, instance_id, idem_key, req_hash, wa_id, to_jid, kind, spec,
			priority, origin, key_id, status, attempts, not_before, lease_until,
			expires_at, error_code, error_msg, sent_at, delivered_at, read_at,
			rcpt_delivered, rcpt_read, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, NULL, 0, 0, ?, ?)
		ON CONFLICT(instance_id, idem_key) WHERE idem_key IS NOT NULL DO NOTHING`,
		job.ID, job.InstanceID, idem, job.ReqHash, job.WAID, job.ToJID, job.Kind, specJSON,
		job.Priority, job.Origin, job.KeyID, string(job.Status), job.Attempts, job.NotBefore,
		job.ExpiresAt, job.ErrorCode, job.ErrorMsg, job.CreatedAt, job.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrIdempotencyConflict
	}
	return nil
}

// Cancel membatalkan job berstatus queued menjadi cancelled, atau menolak bila status lain / tidak ditemukan.
func (s *SQLiteStore) Cancel(ctx context.Context, instanceID, id string, now time.Time) (*Job, error) {
	if s == nil || s.db == nil {
		return nil, ErrNotFound
	}
	nowMs := now.UnixMilli()
	row := s.db.QueryRowContext(ctx, `
		UPDATE outbox
		SET status = 'cancelled', lease_until = NULL, updated_at = ?
		WHERE instance_id = ? AND id = ? AND status = 'queued'
		RETURNING `+selectOutboxCols,
		nowMs, instanceID, id,
	)
	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("cancel job outbox: %w", err)
	}
	var st string
	err2 := s.db.QueryRowContext(ctx, `SELECT status FROM outbox WHERE instance_id = ? AND id = ?`, instanceID, id).Scan(&st)
	if errors.Is(err2, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err2 != nil {
		return nil, fmt.Errorf("cek status cancel job: %w", err2)
	}
	return nil, ErrNotCancellable
}

// ClaimNext mengambil tepat satu job siap kirim berprioritas tertinggi dan menyetel lease sending.
func (s *SQLiteStore) ClaimNext(ctx context.Context, instanceID string, now, leaseUntil time.Time) (*Job, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	nowMs := now.UnixMilli()
	leaseMs := leaseUntil.UnixMilli()
	row := s.db.QueryRowContext(ctx, `
		UPDATE outbox
		SET status = 'sending', lease_until = ?, updated_at = ?
		WHERE id = (
			SELECT id FROM outbox
			WHERE instance_id = ? AND status = 'queued' AND not_before <= ?
			ORDER BY priority DESC, not_before ASC, id ASC
			LIMIT 1
		) AND status = 'queued'
		RETURNING `+selectOutboxCols,
		leaseMs, nowMs, instanceID, nowMs,
	)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim job outbox: %w", err)
	}
	return job, nil
}

// NextDue mengembalikan waktu not_before paling awal dari antrian queued sebuah instance.
func (s *SQLiteStore) NextDue(ctx context.Context, instanceID string) (time.Time, bool, error) {
	if s == nil || s.db == nil {
		return time.Time{}, false, nil
	}
	var minMs sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(not_before) FROM outbox WHERE instance_id = ? AND status = 'queued'`, instanceID).Scan(&minMs); err != nil {
		return time.Time{}, false, fmt.Errorf("baca next due outbox: %w", err)
	}
	if !minMs.Valid {
		return time.Time{}, false, nil
	}
	return time.UnixMilli(minMs.Int64), true, nil
}

// BeginAttempt menaikkan jumlah percobaan sebelum eksekusi ke WhatsApp dilakukan.
func (s *SQLiteStore) BeginAttempt(ctx context.Context, id string, leaseUntil, now time.Time) (int, error) {
	var attempts int
	err := s.db.QueryRowContext(ctx, `
		UPDATE outbox
		SET attempts = attempts + 1, lease_until = ?, updated_at = ?
		WHERE id = ?
		RETURNING attempts`,
		leaseUntil.UnixMilli(), now.UnixMilli(), id,
	).Scan(&attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("begin attempt outbox: %w", err)
	}
	return attempts, nil
}

// Release mengembalikan job dari status sending menjadi queued dengan jadwal not_before baru yang dibatasi expires_at+1.
func (s *SQLiteStore) Release(ctx context.Context, id string, notBefore, now time.Time, errCode, errMsg string) (*Job, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE outbox
		SET status = 'queued',
		    lease_until = NULL,
		    not_before = MIN(?, expires_at + 1),
		    error_code = ?,
		    error_msg = ?,
		    updated_at = ?
		WHERE id = ?
		RETURNING `+selectOutboxCols,
		notBefore.UnixMilli(), errCode, errMsg, now.UnixMilli(), id,
	)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("release job outbox: %w", err)
	}
	return job, nil
}

// MarkSent menandai job telah terkirim dan menyisipkan event outbox atomik ke journal bila tersedia.
func (s *SQLiteStore) MarkSent(ctx context.Context, id string, sentAt, now time.Time) (*Job, error) {
	return s.updateTerminalTx(ctx, `
		UPDATE outbox
		SET status = 'sent', lease_until = NULL, sent_at = ?, error_code = '', error_msg = '', updated_at = ?
		WHERE id = ?
		RETURNING `+selectOutboxCols,
		[]any{sentAt.UnixMilli(), now.UnixMilli(), id},
		sentAt.Unix(), now.UnixMilli(),
	)
}

// MarkFailed menandai job gagal permanen dan mencatat event outbox ke journal dalam transaksi yang sama.
func (s *SQLiteStore) MarkFailed(ctx context.Context, id string, now time.Time, errCode, errMsg string) (*Job, error) {
	return s.updateTerminalTx(ctx, `
		UPDATE outbox
		SET status = 'failed', lease_until = NULL, error_code = ?, error_msg = ?, updated_at = ?
		WHERE id = ?
		RETURNING `+selectOutboxCols,
		[]any{errCode, errMsg, now.UnixMilli(), id},
		now.Unix(), now.UnixMilli(),
	)
}

// MarkExpired menandai job kedaluwarsa karena melewati expires_at dan mencatat event outbox dalam transaksi yang sama.
func (s *SQLiteStore) MarkExpired(ctx context.Context, id string, now time.Time) (*Job, error) {
	return s.updateTerminalTx(ctx, `
		UPDATE outbox
		SET status = 'expired', lease_until = NULL, updated_at = ?
		WHERE id = ?
		RETURNING `+selectOutboxCols,
		[]any{now.UnixMilli(), id},
		now.Unix(), now.UnixMilli(),
	)
}

func (s *SQLiteStore) updateTerminalTx(ctx context.Context, query string, args []any, eventTS, receivedMs int64) (*Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("mulai transaksi final outbox: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	row := tx.QueryRowContext(ctx, query, args...)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("update status final outbox: %w", err)
	}

	if s.onTxEvt != nil {
		ev, err := buildOutboxJournalEvent(job, job.Status, eventTS, receivedMs)
		if err != nil {
			return nil, err
		}
		if err := s.onTxEvt(ctx, tx, ev); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit status final outbox: %w", err)
	}
	committed = true
	return job, nil
}

// RecoverExpiredLeases mengembalikan job berstatus sending dengan lease kedaluwarsa menjadi queued tanpa mengubah wa_id.
func (s *SQLiteStore) RecoverExpiredLeases(ctx context.Context, now time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	nowMs := now.UnixMilli()
	res, err := s.db.ExecContext(ctx, `
		UPDATE outbox
		SET status = 'queued', lease_until = NULL, updated_at = ?
		WHERE status = 'sending' AND lease_until IS NOT NULL AND lease_until < ?`,
		nowMs, nowMs,
	)
	if err != nil {
		return 0, fmt.Errorf("pulihkan lease outbox kedaluwarsa: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// PruneFinal menghapus job berstatus final yang lebih tua dari cutoff per chunk sesuai limit.
func (s *SQLiteStore) PruneFinal(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = 1000
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM outbox
		WHERE id IN (
			SELECT id FROM outbox
			WHERE status IN ('sent','delivered','read','played','failed','cancelled','expired')
				AND updated_at < ?
			ORDER BY updated_at ASC
			LIMIT ?
		)`,
		cutoff.UnixMilli(), limit,
	)
	if err != nil {
		return 0, fmt.Errorf("prune job outbox final: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// MarkReceiptTx memajukan status receipt searah di dalam transaksi journal dan mencatat event outbox untuk DM.
func (s *SQLiteStore) MarkReceiptTx(ctx context.Context, tx *sql.Tx, instanceID string, waIDs []string, rtype string, group bool, now time.Time) (int, []string, error) {
	if tx == nil || instanceID == "" || len(waIDs) == 0 {
		return 0, nil, nil
	}
	kind := normalizeReceiptKind(rtype)
	if kind == "" {
		return 0, nil, nil
	}

	nowMs := now.UnixMilli()
	nowSec := now.Unix()
	newlyDelivered := 0
	var updatedIDs []string

	for _, waID := range waIDs {
		waID = strings.TrimSpace(waID)
		if waID == "" {
			continue
		}

		var (
			id        string
			toJID     string
			msgKind   string
			curStatus string
			attempts  int
		)
		err := tx.QueryRowContext(ctx, `
			SELECT id, to_jid, kind, status, attempts
			FROM outbox
			WHERE instance_id = ? AND wa_id = ?`,
			instanceID, waID,
		).Scan(&id, &toJID, &msgKind, &curStatus, &attempts)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return newlyDelivered, updatedIDs, fmt.Errorf("cari pesan receipt %s: %w", waID, err)
		}

		cur := Status(curStatus)
		var nextStatus Status
		switch kind {
		case "delivered":
			if cur == StatusSent {
				nextStatus = StatusDelivered
				newlyDelivered++
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE outbox
				SET status = CASE WHEN status = 'sent' THEN 'delivered' ELSE status END,
				    delivered_at = COALESCE(delivered_at, ?),
				    rcpt_delivered = rcpt_delivered + 1,
				    updated_at = ?
				WHERE id = ?`,
				nowMs, nowMs, id,
			); err != nil {
				return newlyDelivered, updatedIDs, fmt.Errorf("update receipt delivered %s: %w", id, err)
			}
		case "read":
			if cur == StatusSent || cur == StatusDelivered {
				nextStatus = StatusRead
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE outbox
				SET status = CASE WHEN status IN ('sent','delivered') THEN 'read' ELSE status END,
				    delivered_at = COALESCE(delivered_at, ?),
				    read_at = COALESCE(read_at, ?),
				    rcpt_read = rcpt_read + 1,
				    updated_at = ?
				WHERE id = ?`,
				nowMs, nowMs, nowMs, id,
			); err != nil {
				return newlyDelivered, updatedIDs, fmt.Errorf("update receipt read %s: %w", id, err)
			}
		case "played":
			if cur == StatusSent || cur == StatusDelivered || cur == StatusRead {
				nextStatus = StatusPlayed
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE outbox
				SET status = CASE WHEN status IN ('sent','delivered','read') THEN 'played' ELSE status END,
				    delivered_at = COALESCE(delivered_at, ?),
				    read_at = COALESCE(read_at, ?),
				    updated_at = ?
				WHERE id = ?`,
				nowMs, nowMs, nowMs, id,
			); err != nil {
				return newlyDelivered, updatedIDs, fmt.Errorf("update receipt played %s: %w", id, err)
			}
		}

		updatedIDs = append(updatedIDs, id)

		// Event outbox hanya dicatat untuk receipt DM (delivered/read) yang benar-benar menaikkan status.
		if !group && (nextStatus == StatusDelivered || nextStatus == StatusRead) {
			jJob := &Job{
				ID:         id,
				InstanceID: instanceID,
				WAID:       waID,
				ToJID:      toJID,
				Kind:       msgKind,
				Attempts:   attempts,
			}
			ev, err := buildOutboxJournalEvent(jJob, nextStatus, nowSec, nowMs)
			if err != nil {
				return newlyDelivered, updatedIDs, err
			}
			if _, err := journal.InsertTx(ctx, tx, ev); err != nil {
				return newlyDelivered, updatedIDs, fmt.Errorf("catat event outbox DM %s: %w", nextStatus, err)
			}
		}
	}

	return newlyDelivered, updatedIDs, nil
}

func normalizeReceiptKind(rtype string) string {
	switch strings.ToLower(strings.TrimSpace(rtype)) {
	case "", "delivered":
		return "delivered"
	case "read":
		return "read"
	case "played":
		return "played"
	default:
		return ""
	}
}

func buildOutboxJournalEvent(job *Job, st Status, tsSec, receivedMs int64) (*journal.Event, error) {
	payloadStruct := OutboxEvent{
		ID:          job.ID,
		WAMessageID: job.WAID,
		To:          job.ToJID,
		Type:        job.Kind,
		Status:      st,
		Attempts:    job.Attempts,
		Timestamp:   tsSec,
	}
	if st == StatusFailed {
		if job.ErrorMsg != "" {
			payloadStruct.Error = job.ErrorMsg
		} else {
			payloadStruct.Error = job.ErrorCode
		}
	}
	payload, err := json.Marshal(payloadStruct)
	if err != nil {
		return nil, fmt.Errorf("marshal event outbox: %w", err)
	}
	return &journal.Event{
		InstanceID: job.InstanceID,
		Type:       "outbox",
		Chat:       job.ToJID,
		WAID:       job.WAID,
		DedupeKey:  fmt.Sprintf("outbox:%s:%s", job.ID, st),
		TS:         tsSec,
		ReceivedAt: receivedMs,
		Part:       journal.PartForChat(job.ToJID),
		Payload:    payload,
	}, nil
}

func scanJob(scanner interface{ Scan(...any) error }) (*Job, error) {
	var (
		j           Job
		idem        sql.NullString
		specBytes   []byte
		statusStr   string
		leaseUntil  sql.NullInt64
		sentAt      sql.NullInt64
		deliveredAt sql.NullInt64
		readAt      sql.NullInt64
	)
	err := scanner.Scan(
		&j.ID, &j.InstanceID, &idem, &j.ReqHash, &j.WAID, &j.ToJID, &j.Kind, &specBytes,
		&j.Priority, &j.Origin, &j.KeyID, &statusStr, &j.Attempts, &j.NotBefore, &leaseUntil,
		&j.ExpiresAt, &j.ErrorCode, &j.ErrorMsg, &sentAt, &deliveredAt, &readAt,
		&j.RcptDelivered, &j.RcptRead, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if idem.Valid {
		j.IdemKey = idem.String
	}
	j.Status = Status(statusStr)
	if leaseUntil.Valid {
		j.LeaseUntil = leaseUntil.Int64
	}
	if sentAt.Valid {
		j.SentAt = sentAt.Int64
	}
	if deliveredAt.Valid {
		j.DeliveredAt = deliveredAt.Int64
	}
	if readAt.Valid {
		j.ReadAt = readAt.Int64
	}
	if len(specBytes) > 0 {
		if err := json.Unmarshal(specBytes, &j.Spec); err != nil {
			return nil, fmt.Errorf("unmarshal spec outbox: %w", err)
		}
	}
	return &j, nil
}
