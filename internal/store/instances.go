package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// DefaultWebhookEvents adalah daftar event bawaan saat instance baru dibuat.
const DefaultWebhookEvents = "message,receipt,connection,pairing,outbox,poll_vote,health"

// Status instance yang diizinkan di database.
const (
	StatusCreated      = "created"
	StatusPairing      = "pairing"
	StatusConnected    = "connected"
	StatusDisconnected = "disconnected"
	StatusLoggedOut    = "logged_out"
	StatusDisabled     = "disabled"
)

// Instance merepresentasikan satu nomor/sesi WhatsApp yang dikelola daemon.
type Instance struct {
	ID            string           `json:"id"`
	JID           string           `json:"jid,omitempty"`
	Status        string           `json:"status"`
	WebhookURL    string           `json:"webhook_url,omitempty"`
	WebhookSecret string           `json:"-"`
	WebhookEvents string           `json:"webhook_events"`
	RatePerMin    int              `json:"rate_per_min"`
	AutoStart     bool             `json:"auto_start"`
	Settings      InstanceSettings `json:"settings"`
	PairedAt      int64            `json:"paired_at,omitempty"`
	CreatedAt     int64            `json:"created_at"`
	UpdatedAt     int64            `json:"updated_at"`
}

// InstanceUpdate membawa perubahan parsial untuk operasi edit instance via CLI atau control API.
type InstanceUpdate struct {
	WebhookURL    *string
	WebhookSecret *string
	WebhookEvents *string
	WebhookFrom   string
	RatePerMin    *int
	AutoStart     *bool
	Settings      *InstanceSettings
}

// CreateInstance menyimpan instance baru ke app.db.
func (s *Store) CreateInstance(ctx context.Context, in Instance) error {
	now := time.Now().Unix()
	if in.Status == "" {
		in.Status = StatusCreated
	}
	if in.WebhookEvents == "" {
		in.WebhookEvents = DefaultWebhookEvents
	}
	if in.RatePerMin <= 0 {
		in.RatePerMin = 20
	}
	auto := 1
	if !in.AutoStart {
		auto = 0
	}

	settingsJSON := "{}"
	if !reflect.ValueOf(in.Settings).IsZero() {
		if err := in.Settings.Validate(); err != nil {
			return fmt.Errorf("validasi settings instance %s: %w", in.ID, err)
		}
		raw, err := json.Marshal(in.Settings)
		if err != nil {
			return fmt.Errorf("marshal settings instance %s: %w", in.ID, err)
		}
		settingsJSON = string(raw)
	}

	var pairedAt int64
	if in.PairedAt > 0 {
		pairedAt = in.PairedAt
	} else if in.JID != "" {
		pairedAt = now
	}

	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO instances (id, jid, status, webhook_url, webhook_secret, webhook_events, rate_per_min, auto_start, settings, paired_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		in.ID, nullString(in.JID), in.Status, nullString(in.WebhookURL), in.WebhookSecret,
		in.WebhookEvents, in.RatePerMin, auto, settingsJSON, nullInt64(pairedAt), now, now,
	)
	if err != nil {
		return fmt.Errorf("simpan instance %s: %w", in.ID, err)
	}
	return nil
}

// GetInstance mengambil satu instance berdasarkan ID.
func (s *Store) GetInstance(ctx context.Context, id string) (*Instance, error) {
	row := s.DB.QueryRowContext(ctx, `
		SELECT id, jid, status, webhook_url, webhook_secret, webhook_events, rate_per_min, auto_start, settings, paired_at, created_at, updated_at
		FROM instances WHERE id = ?`, id)
	return scanInstance(row)
}

// ListInstances mengembalikan seluruh instance terurut berdasarkan waktu pembuatan.
func (s *Store) ListInstances(ctx context.Context) ([]Instance, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, jid, status, webhook_url, webhook_secret, webhook_events, rate_per_min, auto_start, settings, paired_at, created_at, updated_at
		FROM instances ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("ambil daftar instance: %w", err)
	}
	defer rows.Close()

	var out []Instance
	for rows.Next() {
		inst, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *inst)
	}
	return out, rows.Err()
}

// UpdateInstanceStatus memperbarui status dan opsional JID suatu instance serta mereset paired_at bila JID berubah.
func (s *Store) UpdateInstanceStatus(ctx context.Context, id, status, jid string) error {
	now := time.Now().Unix()
	var (
		res sql.Result
		err error
	)
	if jid != "" {
		res, err = s.DB.ExecContext(ctx, `
			UPDATE instances
			SET status = ?,
			    paired_at = CASE WHEN jid IS NULL OR jid != ? OR paired_at IS NULL THEN ? ELSE paired_at END,
			    jid = ?,
			    updated_at = ?
			WHERE id = ?`,
			status, jid, now, jid, now, id)
	} else {
		res, err = s.DB.ExecContext(ctx,
			`UPDATE instances SET status = ?, updated_at = ? WHERE id = ?`,
			status, now, id)
	}
	if err != nil {
		return fmt.Errorf("ubah status instance %s: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ClearInstanceJID mengosongkan JID dan paired_at saat perangkat logout dan menetapkan status baru.
func (s *Store) ClearInstanceJID(ctx context.Context, id, status string) error {
	res, err := s.DB.ExecContext(ctx,
		`UPDATE instances SET jid = NULL, paired_at = NULL, status = ?, updated_at = ? WHERE id = ?`,
		status, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("reset jid instance %s: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateInstance menerapkan perubahan parsial pada konfigurasi instance.
func (s *Store) UpdateInstance(ctx context.Context, id string, up InstanceUpdate) (*Instance, error) {
	var sets []string
	var args []any

	if up.WebhookURL != nil {
		sets = append(sets, "webhook_url = ?")
		args = append(args, nullString(*up.WebhookURL))
	}
	if up.WebhookSecret != nil {
		sets = append(sets, "webhook_secret = ?")
		args = append(args, *up.WebhookSecret)
	}
	if up.WebhookEvents != nil {
		sets = append(sets, "webhook_events = ?")
		args = append(args, *up.WebhookEvents)
	}
	if up.RatePerMin != nil {
		sets = append(sets, "rate_per_min = ?")
		args = append(args, *up.RatePerMin)
	}
	if up.AutoStart != nil {
		auto := 0
		if *up.AutoStart {
			auto = 1
		}
		sets = append(sets, "auto_start = ?")
		args = append(args, auto)
	}
	if up.Settings != nil {
		if err := up.Settings.Validate(); err != nil {
			return nil, fmt.Errorf("validasi settings instance %s: %w", id, err)
		}
		raw, err := json.Marshal(*up.Settings)
		if err != nil {
			return nil, fmt.Errorf("marshal settings instance %s: %w", id, err)
		}
		sets = append(sets, "settings = ?")
		args = append(args, string(raw))
	}
	if len(sets) == 0 {
		return s.GetInstance(ctx, id)
	}
	sets = append(sets, "updated_at = ?")
	args = append(args, time.Now().Unix(), id)

	q := fmt.Sprintf("UPDATE instances SET %s WHERE id = ?", strings.Join(sets, ", "))
	res, err := s.DB.ExecContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("update instance %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, ErrNotFound
	}
	return s.GetInstance(ctx, id)
}

// DeleteInstance menghapus instance beserta api_keys dan outbox terkait (via ON DELETE CASCADE).
func (s *Store) DeleteInstance(ctx context.Context, id string) error {
	res, err := s.DB.ExecContext(ctx, `DELETE FROM instances WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("hapus instance %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanInstance(r rowScanner) (*Instance, error) {
	var (
		inst                 Instance
		jid, whURL, settings sql.NullString
		pairedAt             sql.NullInt64
		auto                 int
	)
	err := r.Scan(
		&inst.ID, &jid, &inst.Status, &whURL, &inst.WebhookSecret,
		&inst.WebhookEvents, &inst.RatePerMin, &auto, &settings, &pairedAt, &inst.CreatedAt, &inst.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan instance: %w", err)
	}
	inst.JID = jid.String
	inst.WebhookURL = whURL.String
	inst.AutoStart = auto == 1
	inst.PairedAt = pairedAt.Int64

	inst.Settings = Defaults()
	rawSettings := strings.TrimSpace(settings.String)
	if rawSettings != "" && rawSettings != "{}" {
		if err := json.Unmarshal([]byte(rawSettings), &inst.Settings); err != nil {
			return nil, fmt.Errorf("parse settings instance %s: %w", inst.ID, err)
		}
	}
	return &inst, nil
}
