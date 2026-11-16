package media

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fikfikk-org/waapi/internal/idgen"
	"go.mau.fi/whatsmeow"
)

const uploadTTL = 24 * time.Hour

var (
	// ErrUploadNotFound dikembalikan jika upload_id tidak terdaftar pada instance terkait.
	ErrUploadNotFound = errors.New("upload media tidak ditemukan")
	// ErrUploadExpired dikembalikan jika descriptor upload sudah melewati masa berlaku 24 jam.
	ErrUploadExpired = errors.New("upload media sudah kedaluwarsa")
)

type uploadDescriptorBlob struct {
	Resp     whatsmeow.UploadResponse `json:"resp"`
	Width    uint32                   `json:"width,omitempty"`
	Height   uint32                   `json:"height,omitempty"`
	Seconds  uint32                   `json:"seconds,omitempty"`
	PTT      bool                     `json:"ptt,omitempty"`
	Filename string                   `json:"filename,omitempty"`
}

// UploadRecord merepresentasikan satu descriptor upload media yang disimpan di journal.db.
type UploadRecord struct {
	ID         string
	InstanceID string
	Kind       Kind
	Mimetype   string
	Size       int64
	Resp       whatsmeow.UploadResponse
	Width      uint32
	Height     uint32
	Seconds    uint32
	PTT        bool
	Filename   string
	CreatedAt  int64 // unix milidetik
	ExpiresAt  int64 // unix milidetik
}

// UploadStore mengelola persistensi descriptor media hasil upload di tabel uploads (journal.db).
type UploadStore struct {
	db *sql.DB
}

// NewUploadStore membuat UploadStore yang terikat ke handle journal.db.
func NewUploadStore(db *sql.DB) *UploadStore {
	if db == nil {
		return nil
	}
	return &UploadStore{db: db}
}

// Put menyimpan descriptor media ke tabel uploads dengan ID awalan upl_ dan masa berlaku bawaan 24 jam.
func (s *UploadStore) Put(ctx context.Context, rec UploadRecord) (UploadRecord, error) {
	if s == nil || s.db == nil {
		return UploadRecord{}, errors.New("upload store belum siap")
	}
	now := time.Now()
	if rec.CreatedAt <= 0 {
		rec.CreatedAt = now.UnixMilli()
	}
	if rec.ExpiresAt <= 0 {
		rec.ExpiresAt = rec.CreatedAt + uploadTTL.Milliseconds()
	}
	if rec.ID == "" {
		rec.ID = idgen.NewAt("upl_", time.UnixMilli(rec.CreatedAt))
	}

	blob, err := json.Marshal(uploadDescriptorBlob{
		Resp:     rec.Resp,
		Width:    rec.Width,
		Height:   rec.Height,
		Seconds:  rec.Seconds,
		PTT:      rec.PTT,
		Filename: rec.Filename,
	})
	if err != nil {
		return UploadRecord{}, fmt.Errorf("marshal descriptor upload: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO uploads (id, instance_id, kind, mimetype, size, descriptor, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.ID, rec.InstanceID, string(rec.Kind), rec.Mimetype, rec.Size, blob, rec.CreatedAt, rec.ExpiresAt,
	)
	if err != nil {
		return UploadRecord{}, fmt.Errorf("simpan descriptor upload: %w", err)
	}
	return rec, nil
}

// Get memuat descriptor upload milik instanceID dan memeriksa apakah masa berlakunya masih aktif pada waktu now.
func (s *UploadStore) Get(ctx context.Context, instanceID, uploadID string, now time.Time) (*UploadRecord, error) {
	if s == nil || s.db == nil {
		return nil, ErrUploadNotFound
	}
	if now.IsZero() {
		now = time.Now()
	}

	var (
		rec  UploadRecord
		kind string
		blob []byte
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT id, instance_id, kind, mimetype, size, descriptor, created_at, expires_at
		FROM uploads
		WHERE id = ? AND instance_id = ?`,
		uploadID, instanceID,
	).Scan(&rec.ID, &rec.InstanceID, &kind, &rec.Mimetype, &rec.Size, &blob, &rec.CreatedAt, &rec.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("baca descriptor upload: %w", err)
	}

	if now.UnixMilli() > rec.ExpiresAt {
		return nil, ErrUploadExpired
	}

	var decoded uploadDescriptorBlob
	if err := json.Unmarshal(blob, &decoded); err != nil {
		return nil, fmt.Errorf("decode descriptor upload: %w", err)
	}
	rec.Kind = Kind(kind)
	rec.Resp = decoded.Resp
	rec.Width = decoded.Width
	rec.Height = decoded.Height
	rec.Seconds = decoded.Seconds
	rec.PTT = decoded.PTT
	rec.Filename = decoded.Filename
	return &rec, nil
}

// Prune menghapus descriptor upload yang sudah melewati waktu expires_at pada journal.db.
func (s *UploadStore) Prune(ctx context.Context, now time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM uploads WHERE expires_at <= ?`, now.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("pangkas uploads kedaluwarsa: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
