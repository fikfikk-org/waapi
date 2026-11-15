// Package outbox menyediakan antrian pesan keluar tahan-restart (durable outbox) beserta scheduler, rate limiter, dan pelacakan status pengiriman.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
)

// Status merepresentasikan siklus hidup sebuah job pesan keluar di outbox.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusSending   Status = "sending"
	StatusSent      Status = "sent"
	StatusDelivered Status = "delivered"
	StatusRead      Status = "read"
	StatusPlayed    Status = "played"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
	StatusExpired   Status = "expired"
)

// Konstanta prioritas pengiriman pesan outbox.
const (
	PriorityNormal = 0
	PriorityUrgent = 10
)

// IsTerminal mengembalikan true jika status sudah final dan dapat dibersihkan oleh pruner setelah masa retensi.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSent, StatusDelivered, StatusRead, StatusPlayed, StatusFailed, StatusCancelled, StatusExpired:
		return true
	default:
		return false
	}
}

// IsFailureTerminal mengembalikan true jika job berhenti pada status gagal/batal/kedaluwarsa sebelum terkirim.
func (s Status) IsFailureTerminal() bool {
	switch s {
	case StatusFailed, StatusCancelled, StatusExpired:
		return true
	default:
		return false
	}
}

// StatusRank mengembalikan urutan progres pengiriman (sent < delivered < read < played), atau 0 bila belum terkirim.
func (s Status) StatusRank() int {
	switch s {
	case StatusSent:
		return 1
	case StatusDelivered:
		return 2
	case StatusRead:
		return 3
	case StatusPlayed:
		return 4
	default:
		return 0
	}
}

// Error sentinel paket outbox.
var (
	ErrNotFound            = errors.New("pesan outbox tidak ditemukan")
	ErrNotPaired           = errors.New("instance belum di-pairing")
	ErrInvalidSpec         = errors.New("spesifikasi pesan tidak valid")
	ErrIdempotencyMismatch = errors.New("idempotency key sudah digunakan dengan payload berbeda")
	ErrIdempotencyConflict = errors.New("konflik penyisipan idempotency key")
	ErrQueueFull           = errors.New("antrian outbox instance penuh")
	ErrNotCancellable      = errors.New("pesan tidak dapat dibatalkan pada status ini")
	ErrWaitTimeout         = errors.New("batas waktu tunggu pengiriman pesan habis")
	ErrMediaExpired        = errors.New("media sudah kedaluwarsa")
	ErrSendFailed          = errors.New("pengiriman pesan gagal")
)

// Spec menyimpan spesifikasi pesan keluar lengkap lintas tipe v2 (B5 hanya mengaktifkan tipe text).
type Spec struct {
	Type     string    `json:"type"`
	To       string    `json:"to"`
	Text     string    `json:"text,omitempty"`
	ReplyTo  *ReplyRef `json:"reply_to,omitempty"`
	Mentions []string  `json:"mentions,omitempty"`
	Typing   bool      `json:"typing,omitempty"`
	Media    *MediaRef `json:"media,omitempty"`    // B6
	Location *Location `json:"location,omitempty"` // B7
	Contacts []Contact `json:"contacts,omitempty"` // B7
	Poll     *PollSpec `json:"poll,omitempty"`     // B7
	Reaction *Reaction `json:"reaction,omitempty"` // B7
	Edit     *Edit     `json:"edit,omitempty"`     // B7
	Revoke   *Revoke   `json:"revoke,omitempty"`   // B7
}

// ReplyRef mendeskripsikan pesan rujukan yang dibalas.
type ReplyRef struct {
	ID     string `json:"id"`
	Sender string `json:"sender,omitempty"`
}

// MediaRef mendeskripsikan referensi media untuk Batch 6.
type MediaRef struct {
	UploadID  string `json:"upload_id,omitempty"`
	URL       string `json:"url,omitempty"`
	Caption   string `json:"caption,omitempty"`
	Filename  string `json:"filename,omitempty"`
	Thumbnail string `json:"thumbnail,omitempty"`
	PTT       bool   `json:"ptt,omitempty"`
	GIF       bool   `json:"gif,omitempty"`
	Seconds   uint32 `json:"seconds,omitempty"`
}

// Location mendeskripsikan pesan lokasi untuk Batch 7.
type Location struct {
	Lat     float64 `json:"lat"`
	Lng     float64 `json:"lng"`
	Name    string  `json:"name,omitempty"`
	Address string  `json:"address,omitempty"`
}

// Contact mendeskripsikan kartu kontak untuk Batch 7.
type Contact struct {
	Name   string   `json:"name"`
	Phones []string `json:"phones,omitempty"`
	VCard  string   `json:"vcard,omitempty"`
}

// PollSpec mendeskripsikan jajak pendapat untuk Batch 7.
type PollSpec struct {
	Name       string   `json:"name"`
	Options    []string `json:"options"`
	Selectable int      `json:"selectable,omitempty"`
}

// Reaction mendeskripsikan reaksi pesan untuk Batch 7.
type Reaction struct {
	MessageID string `json:"message_id"`
	Sender    string `json:"sender,omitempty"`
	Emoji     string `json:"emoji"`
}

// Edit mendeskripsikan penyuntingan pesan untuk Batch 7.
type Edit struct {
	MessageID string `json:"message_id"`
	Text      string `json:"text"`
}

// Revoke mendeskripsikan penghapusan pesan untuk Batch 7.
type Revoke struct {
	MessageID string `json:"message_id"`
	Sender    string `json:"sender,omitempty"`
}

// Origin menyimpan sumber dan metadata otorisasi permintaan kirim pesan.
type Origin struct {
	Kind   string
	KeyID  string
	Policy *auth.KeyPolicy
}

// EnqueueRequest berisi parameter input untuk memasukkan pesan ke outbox.
type EnqueueRequest struct {
	InstanceID string
	IdemKey    string
	Spec       Spec
	SendAt     time.Time     // zero = segera
	Priority   int           // PriorityNormal (0) atau PriorityUrgent (10)
	TTL        time.Duration // zero = WAAPI_OUTBOX_TTL
	Origin     Origin
}

// Job merepresentasikan satu baris pesan keluar di tabel outbox.
type Job struct {
	ID            string
	InstanceID    string
	IdemKey       string
	ReqHash       []byte
	WAID          string
	ToJID         string
	Kind          string
	Spec          Spec
	Priority      int
	Origin        string
	KeyID         string
	Status        Status
	Attempts      int
	NotBefore     int64 // unix milidetik
	LeaseUntil    int64 // unix milidetik, 0 jika NULL
	ExpiresAt     int64 // unix milidetik
	ErrorCode     string
	ErrorMsg      string
	SentAt        int64 // unix milidetik, 0 jika NULL
	DeliveredAt   int64 // unix milidetik, 0 jika NULL
	ReadAt        int64 // unix milidetik, 0 jika NULL
	RcptDelivered int
	RcptRead      int
	CreatedAt     int64 // unix milidetik
	UpdatedAt     int64 // unix milidetik
}

// ExecResult menyimpan metadata balasan WhatsApp setelah pesan berhasil dikirim.
type ExecResult struct {
	ServerTS time.Time
}

// SendError membungkus error pengiriman dari Executor agar outbox dapat mengklasifikasikannya tanpa mengimpor whatsmeow.
type SendError struct {
	Transient bool          // true = boleh dicoba ulang
	Code      string        // error_code outbox, mis. "not_connected", "invalid_recipient"
	MinDelay  time.Duration // jeda minimal sebelum percobaan berikutnya (mis. 60s untuk rate_overlimit)
	Err       error
}

// Error mengembalikan deskripsi kesalahan kirim beserta kode klasifikasinya.
func (e *SendError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil && e.Code != "" {
		return fmt.Sprintf("%s: %v", e.Code, e.Err)
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Code
}

// Unwrap mengembalikan error asli yang dibungkus oleh SendError.
func (e *SendError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Decision menyatakan keputusan Governor ketika scheduler siap mengeksekusi sebuah job.
type Decision struct {
	Allow     bool
	Delay     time.Duration
	Code      string // kosong, "rate", "quiet_hours", "guardian_*"
	Permanent bool   // true = gagalkan job dengan Code
	Reason    string
}

// Outcome melaporkan hasil satu percobaan eksekusi kepada Governor.
type Outcome struct {
	Success     bool
	Code        string
	Transient   bool
	RateLimited bool
	Attempts    int
	Err         error
}

// Executor menghubungkan outbox dengan lapisan transport WhatsApp.
type Executor interface {
	Ready(instanceID string) bool
	NewMessageID(instanceID string) (string, error)
	Execute(ctx context.Context, job *Job) (ExecResult, error)
}

// Governor mengendalikan izin laju kirim, penundaan, dan umpan balik hasil pengiriman.
type Governor interface {
	Precheck(ctx context.Context, job *Job) error
	Decide(ctx context.Context, job *Job, now time.Time) Decision
	Record(ctx context.Context, job *Job, out Outcome)
}

// PolicyChecker memvalidasi kebijakan pengiriman API key (diimplementasikan penuh pada B11).
type PolicyChecker interface {
	CheckSend(ctx context.Context, instanceID string, origin Origin, spec Spec) error
}

// OutboxEvent merepresentasikan data event "outbox" yang ditulis ke journal.
type OutboxEvent struct {
	ID          string `json:"id"`
	WAMessageID string `json:"wa_message_id"`
	To          string `json:"to"`
	Type        string `json:"type"`
	Status      Status `json:"status"`
	Attempts    int    `json:"attempts"`
	Timestamp   int64  `json:"timestamp"`
	Error       string `json:"error,omitempty"`
}

// EventRecorder mencatat event outbox ketika journal atau fake sink dipakai di pengujian.
type EventRecorder interface {
	RecordOutboxEvent(ctx context.Context, ev OutboxEvent) error
}

// Store mendefinisikan operasi persistensi antrian outbox yang diimplementasikan SQLiteStore maupun fake store untuk pengujian.
type Store interface {
	GetByID(ctx context.Context, instanceID, id string) (*Job, error)
	GetByIdemKey(ctx context.Context, instanceID, idemKey string) (*Job, error)
	CountQueued(ctx context.Context, instanceID string) (int, error)
	Insert(ctx context.Context, job *Job) error
	Cancel(ctx context.Context, instanceID, id string, now time.Time) (*Job, error)
	ClaimNext(ctx context.Context, instanceID string, now, leaseUntil time.Time) (*Job, error)
	NextDue(ctx context.Context, instanceID string) (time.Time, bool, error)
	BeginAttempt(ctx context.Context, id string, leaseUntil, now time.Time) (int, error)
	Release(ctx context.Context, id string, notBefore, now time.Time, errCode, errMsg string) (*Job, error)
	MarkSent(ctx context.Context, id string, sentAt, now time.Time) (*Job, error)
	MarkFailed(ctx context.Context, id string, now time.Time, errCode, errMsg string) (*Job, error)
	MarkExpired(ctx context.Context, id string, now time.Time) (*Job, error)
	RecoverExpiredLeases(ctx context.Context, now time.Time) (int64, error)
	PruneFinal(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

// ReceiptStore adalah antarmuka opsional bagi Store yang mendukung pembaruan status receipt di dalam transaksi journal.
type ReceiptStore interface {
	MarkReceiptTx(ctx context.Context, tx *sql.Tx, instanceID string, waIDs []string, rtype string, group bool, now time.Time) (int, []string, error)
}
