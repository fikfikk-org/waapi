package guardian

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ContactRow merepresentasikan satu baris pada tabel contacts_seen di journal.db.
type ContactRow struct {
	InstanceID string
	JID        string
	FirstIn    int64
	LastIn     int64
	FirstOut   int64
	LastOut    int64
	OptedOut   bool
	OptChanged int64
}

// HourlyRow merepresentasikan satu bucket per jam di guardian_hourly.
type HourlyRow struct {
	InstanceID       string
	Hour             int64
	Sent             int
	FirstContactSent int
	Delivered        int
	Replied          int
	GroupsCreated    int
	RateOverlimit    int
}

// GuardianStore membungkus operasi SQLite journal.db untuk state kesehatan akun dan consent ledger.
type GuardianStore struct {
	db *sql.DB
}

// NewStore membuat GuardianStore pada koneksi *sql.DB milik journal.db.
func NewStore(db *sql.DB) *GuardianStore {
	return &GuardianStore{db: db}
}

// GetContact membaca baris contacts_seen untuk pasangan (instance_id, jid).
func (s *GuardianStore) GetContact(ctx context.Context, instanceID, jid string) (ContactRow, bool, error) {
	if s == nil || s.db == nil || instanceID == "" || jid == "" {
		return ContactRow{}, false, nil
	}
	var (
		row        ContactRow
		firstIn    sql.NullInt64
		lastIn     sql.NullInt64
		firstOut   sql.NullInt64
		lastOut    sql.NullInt64
		optedOut   int
		optChanged sql.NullInt64
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT first_in, last_in, first_out, last_out, opted_out, opt_changed
		FROM contacts_seen
		WHERE instance_id = ? AND jid = ?`,
		instanceID, jid,
	).Scan(&firstIn, &lastIn, &firstOut, &lastOut, &optedOut, &optChanged)
	if errors.Is(err, sql.ErrNoRows) {
		return ContactRow{}, false, nil
	}
	if err != nil {
		return ContactRow{}, false, fmt.Errorf("baca contacts_seen: %w", err)
	}
	row.InstanceID = instanceID
	row.JID = jid
	row.FirstIn = firstIn.Int64
	row.LastIn = lastIn.Int64
	row.FirstOut = firstOut.Int64
	row.LastOut = lastOut.Int64
	row.OptedOut = optedOut != 0
	row.OptChanged = optChanged.Int64
	return row, true, nil
}

// UpsertInboundTx memperbarui jejak inbound (serta opted_out jika ada kata kunci consent) di dalam transaksi SQLite aktif.
// Mengembalikan replied=true apabila sebelumnya last_out > last_in pada kontak tersebut.
func (s *GuardianStore) UpsertInboundTx(ctx context.Context, tx *sql.Tx, instanceID, jid string, nowSec int64, setOptedOut *bool) (replied bool, err error) {
	if tx == nil || instanceID == "" || jid == "" {
		return false, nil
	}

	var prevLastIn, prevLastOut sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		SELECT last_in, last_out
		FROM contacts_seen
		WHERE instance_id = ? AND jid = ?`,
		instanceID, jid,
	).Scan(&prevLastIn, &prevLastOut)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("cek riwayat kontak inbound: %w", err)
	}
	if err == nil && prevLastOut.Valid && prevLastOut.Int64 > prevLastIn.Int64 {
		replied = true
	}

	var optedVal any
	var optChangedVal any
	if setOptedOut != nil {
		if *setOptedOut {
			optedVal = 1
		} else {
			optedVal = 0
		}
		optChangedVal = nowSec
	}

	if setOptedOut != nil {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO contacts_seen (instance_id, jid, first_in, last_in, opted_out, opt_changed)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(instance_id, jid) DO UPDATE SET
				first_in = COALESCE(contacts_seen.first_in, excluded.first_in),
				last_in = CASE WHEN contacts_seen.last_in IS NULL OR excluded.last_in > contacts_seen.last_in THEN excluded.last_in ELSE contacts_seen.last_in END,
				opted_out = excluded.opted_out,
				opt_changed = excluded.opt_changed`,
			instanceID, jid, nowSec, nowSec, optedVal, optChangedVal,
		)
	} else {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO contacts_seen (instance_id, jid, first_in, last_in)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(instance_id, jid) DO UPDATE SET
				first_in = COALESCE(contacts_seen.first_in, excluded.first_in),
				last_in = CASE WHEN contacts_seen.last_in IS NULL OR excluded.last_in > contacts_seen.last_in THEN excluded.last_in ELSE contacts_seen.last_in END`,
			instanceID, jid, nowSec, nowSec,
		)
	}
	if err != nil {
		return false, fmt.Errorf("upsert contacts_seen inbound: %w", err)
	}
	return replied, nil
}

// RecordOutbound mencatat jejak pengiriman pesan keluar pertama dan terakhir ke kontak di contacts_seen.
func (s *GuardianStore) RecordOutbound(ctx context.Context, instanceID, jid string, nowSec int64) error {
	if s == nil || s.db == nil || instanceID == "" || jid == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO contacts_seen (instance_id, jid, first_out, last_out)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(instance_id, jid) DO UPDATE SET
			first_out = COALESCE(contacts_seen.first_out, excluded.first_out),
			last_out = CASE WHEN contacts_seen.last_out IS NULL OR excluded.last_out > contacts_seen.last_out THEN excluded.last_out ELSE contacts_seen.last_out END`,
		instanceID, jid, nowSec, nowSec,
	)
	if err != nil {
		return fmt.Errorf("record outbound contact: %w", err)
	}
	return nil
}

// UpsertContactDirect menyiapkan baris contacts_seen secara langsung untuk kebutuhan pengujian dan sinkronisasi cepat.
func (s *GuardianStore) UpsertContactDirect(ctx context.Context, instanceID, jid string, lastIn, lastOut int64, optedOut int) error {
	var inVal, outVal any
	if lastIn > 0 {
		inVal = lastIn
	}
	if lastOut > 0 {
		outVal = lastOut
	}
	var optChanged any
	if optedOut != 0 {
		optChanged = time.Now().Unix()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO contacts_seen (instance_id, jid, first_in, last_in, first_out, last_out, opted_out, opt_changed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id, jid) DO UPDATE SET
			first_in = COALESCE(excluded.first_in, contacts_seen.first_in),
			last_in = COALESCE(excluded.last_in, contacts_seen.last_in),
			first_out = COALESCE(excluded.first_out, contacts_seen.first_out),
			last_out = COALESCE(excluded.last_out, contacts_seen.last_out),
			opted_out = excluded.opted_out,
			opt_changed = COALESCE(excluded.opt_changed, contacts_seen.opt_changed)`,
		instanceID, jid, inVal, inVal, outVal, outVal, optedOut, optChanged,
	)
	return err
}

// CountRecipients24h menghitung jumlah kontak unik yang dikirimi pesan sejak sinceSec.
func (s *GuardianStore) CountRecipients24h(ctx context.Context, instanceID string, sinceSec int64) (int, error) {
	if s == nil || s.db == nil || instanceID == "" {
		return 0, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM contacts_seen
		WHERE instance_id = ? AND last_out IS NOT NULL AND last_out >= ?`,
		instanceID, sinceSec,
	).Scan(&n)
	return n, err
}

// FanoutStatus memeriksa jumlah penerima konten identik di hari tersebut dan apakah penerima sudah tercatat sebelumnya.
func (s *GuardianStore) FanoutStatus(ctx context.Context, instanceID, day string, contentHash []byte, rcpt string) (count int, contains bool, err error) {
	if s == nil || s.db == nil || instanceID == "" || len(contentHash) == 0 {
		return 0, false, nil
	}
	if err = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM guardian_fanout
		WHERE instance_id = ? AND day = ? AND content_hash = ?`,
		instanceID, day, contentHash,
	).Scan(&count); err != nil {
		return 0, false, err
	}
	if rcpt != "" {
		var found int
		err = s.db.QueryRowContext(ctx, `
			SELECT 1 FROM guardian_fanout
			WHERE instance_id = ? AND day = ? AND content_hash = ? AND rcpt = ?`,
			instanceID, day, contentHash, rcpt,
		).Scan(&found)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, false, err
		}
		contains = found == 1
	}
	return count, contains, nil
}

// MaxFanoutDay mengembalikan jumlah penerima terbanyak untuk satu hash konten pada hari tertentu.
func (s *GuardianStore) MaxFanoutDay(ctx context.Context, instanceID, day string) (int, error) {
	if s == nil || s.db == nil || instanceID == "" {
		return 0, nil
	}
	var maxN sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT MAX(cnt) FROM (
			SELECT COUNT(*) AS cnt
			FROM guardian_fanout
			WHERE instance_id = ? AND day = ?
			GROUP BY content_hash
		)`,
		instanceID, day,
	).Scan(&maxN)
	return int(maxN.Int64), err
}

// RecordFanout mencatat penerima untuk konten identik pada hari tertentu secara idempoten.
func (s *GuardianStore) RecordFanout(ctx context.Context, instanceID, day string, contentHash []byte, rcpt string) error {
	if s == nil || s.db == nil || instanceID == "" || len(contentHash) == 0 || rcpt == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO guardian_fanout (instance_id, day, content_hash, rcpt)
		VALUES (?, ?, ?, ?)`,
		instanceID, day, contentHash, rcpt,
	)
	return err
}

// FlushHourlyDeltas menyimpan inkremen bucket per jam ke tabel guardian_hourly dalam satu transaksi.
func (s *GuardianStore) FlushHourlyDeltas(ctx context.Context, deltas []HourlyRow) error {
	if s == nil || s.db == nil || len(deltas) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO guardian_hourly (instance_id, hour, sent, first_contact_sent, delivered, replied, groups_created, rate_overlimit)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id, hour) DO UPDATE SET
			sent = guardian_hourly.sent + excluded.sent,
			first_contact_sent = guardian_hourly.first_contact_sent + excluded.first_contact_sent,
			delivered = guardian_hourly.delivered + excluded.delivered,
			replied = guardian_hourly.replied + excluded.replied,
			groups_created = guardian_hourly.groups_created + excluded.groups_created,
			rate_overlimit = guardian_hourly.rate_overlimit + excluded.rate_overlimit`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for _, d := range deltas {
		if _, err := stmt.ExecContext(ctx,
			d.InstanceID, d.Hour, d.Sent, d.FirstContactSent,
			d.Delivered, d.Replied, d.GroupsCreated, d.RateOverlimit,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LoadRecentHourly memuat bucket per jam sejak sinceHour saat Guardian startup.
func (s *GuardianStore) LoadRecentHourly(ctx context.Context, sinceHour int64) ([]HourlyRow, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT instance_id, hour, sent, first_contact_sent, delivered, replied, groups_created, rate_overlimit
		FROM guardian_hourly
		WHERE hour >= ?
		ORDER BY hour ASC`,
		sinceHour,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []HourlyRow
	for rows.Next() {
		var r HourlyRow
		if err := rows.Scan(&r.InstanceID, &r.Hour, &r.Sent, &r.FirstContactSent, &r.Delivered, &r.Replied, &r.GroupsCreated, &r.RateOverlimit); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InsertHealthLog mencatat peristiwa kesehatan penting ke health_log.
func (s *GuardianStore) InsertHealthLog(ctx context.Context, instanceID string, atSec int64, kind string, code int, detail string) error {
	if s == nil || s.db == nil || instanceID == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO health_log (instance_id, at, kind, code, detail)
		VALUES (?, ?, ?, ?, ?)`,
		instanceID, atSec, kind, code, detail,
	)
	return err
}

// LatestTempBan mengambil catatan temporary ban terakhir untuk sebuah instance dari health_log.
func (s *GuardianStore) LatestTempBan(ctx context.Context, instanceID string) (code int, at int64, err error) {
	if s == nil || s.db == nil || instanceID == "" {
		return 0, 0, nil
	}
	err = s.db.QueryRowContext(ctx, `
		SELECT code, at FROM health_log
		WHERE instance_id = ? AND kind = 'temp_ban'
		ORDER BY at DESC, id DESC LIMIT 1`,
		instanceID,
	).Scan(&code, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	return code, at, err
}

// IsShadowBootstrapped memeriksa apakah instance sudah pernah melewati tahap inisialisasi shadow mode.
func (s *GuardianStore) IsShadowBootstrapped(ctx context.Context, instanceID string) (bool, error) {
	if s == nil || s.db == nil || instanceID == "" {
		return false, nil
	}
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM guardian_shadow_meta WHERE instance_id = ?`, instanceID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && one == 1, err
}

// MarkShadowBootstrapped mencatat bahwa status shadow awal instance sudah diputuskan.
func (s *GuardianStore) MarkShadowBootstrapped(ctx context.Context, instanceID string, atSec int64) error {
	if s == nil || s.db == nil || instanceID == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO guardian_shadow_meta (instance_id, booted_at)
		VALUES (?, ?)`,
		instanceID, atSec,
	)
	return err
}

// PruneOld menghapus bucket per jam > 8 hari dan baris fan-out > 2 hari (UTC aman untuk semua zona waktu).
func (s *GuardianStore) PruneOld(ctx context.Context, now time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	cutoffHour := now.Add(-8*24*time.Hour).Unix() / 3600
	if _, err := s.db.ExecContext(ctx, `DELETE FROM guardian_hourly WHERE hour < ?`, cutoffHour); err != nil {
		return fmt.Errorf("hapus guardian_hourly lama: %w", err)
	}
	cutoffDay := now.UTC().Add(-48 * time.Hour).Format("2006-01-02")
	if _, err := s.db.ExecContext(ctx, `DELETE FROM guardian_fanout WHERE day < ?`, cutoffDay); err != nil {
		return fmt.Errorf("hapus guardian_fanout lama: %w", err)
	}
	return nil
}
