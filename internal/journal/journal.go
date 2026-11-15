// Package journal menyediakan penyimpanan event masuk tahan-crash (effectively-once) di SQLite terpisah.
package journal

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

const (
	// FlagOffline menandai event diterima saat fase offline sync awal WhatsApp.
	FlagOffline uint32 = 1 << 0
	// FlagTruncated menandai isi teks atau vcard dipotong agar memenuhi batas ukuran payload.
	FlagTruncated uint32 = 1 << 1

	// MaxPayloadBytes membatasi ukuran JSON data per baris event maksimal 64 KiB.
	MaxPayloadBytes = 64 << 10
	// MaxReadLimit membatasi jumlah event per panggilan Read.
	MaxReadLimit = 500
	// numPartitions menetapkan jumlah partisi hash chat untuk penjadwalan lane paralel.
	numPartitions = 64
)

// ErrNotFound dikembalikan ketika event journal yang dicari tidak ditemukan.
var ErrNotFound = errors.New("event journal tidak ditemukan")

//go:embed migrations/*.sql
var migrationFS embed.FS

// Event merepresentasikan satu baris event masuk di journal.db.
type Event struct {
	Seq        int64
	InstanceID string
	Type       string
	Chat       string
	Sender     string
	WAID       string
	DedupeKey  string // kosong = tanpa dedupe
	TS         int64  // unix detik menurut WhatsApp
	ReceivedAt int64  // unix milidetik saat diterima daemon
	Part       uint8  // fnv32a(Chat) % 64
	Flags      uint32 // bit0 offline, bit1 truncated
	Payload    []byte // JSON data, maksimal 64 KiB
}

// AppendResult merangkum nomor urut dan status duplikasi setelah pemanggilan Append.
type AppendResult struct {
	Seq       int64
	Duplicate bool
}

// TxHook dijalankan di dalam transaksi Append sebelum commit dan sebelum WhatsApp menerima ack.
type TxHook func(ctx context.Context, tx *sql.Tx, seq int64) error

// Query menentukan filter dan batasan pembacaan event dari journal.db.
type Query struct {
	InstanceID string
	AfterSeq   int64
	Types      []string // kosong = semua
	Lane       int      // dipakai bila Lanes > 0
	Lanes      int      // 0 = tanpa filter lane
	Limit      int      // maksimal 500
	MaxBytes   int      // berhenti membaca bila total payload melewati batas
}

// ReadResult mengembalikan irisan event serta seq tertinggi yang sudah diperiksa oleh query.
type ReadResult struct {
	Events    []Event
	ScannedTo int64 // seq tertinggi yang sudah diperiksa (cocok filter atau tidak); konsumen commit nilai ini
	HasMore   bool
}

// Journal mengelola koneksi SQLite journal.db beserta subscriber notifikasi per instance.
type Journal struct {
	db         *sql.DB
	insertStmt *sql.Stmt

	subMu     sync.Mutex
	subs      map[string]map[uint64]chan struct{}
	nextSubID uint64
}

const insertEventSQL = `
INSERT INTO events (instance_id, type, chat, sender, wa_id, dedupe_key, ts, received_at, part, flags, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(instance_id, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
RETURNING seq`

// Open membuka journal.db, menjalankan migrasi, dan menyiapkan prepared statement insert.
func Open(ctx context.Context, path string, opts ...store.DBOptions) (*Journal, error) {
	opt := store.DBOptions{
		MaxConns:    3,
		CacheKiB:    1024,
		IdleTimeout: 60 * time.Second,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	}
	if len(opts) > 0 {
		o := opts[0]
		if o.MaxConns > 0 {
			opt.MaxConns = o.MaxConns
		}
		if o.CacheKiB > 0 {
			opt.CacheKiB = o.CacheKiB
		}
		if o.IdleTimeout > 0 {
			opt.IdleTimeout = o.IdleTimeout
		}
		if o.TxLock != "" {
			opt.TxLock = o.TxLock
		}
		if o.Sync != "" {
			opt.Sync = o.Sync
		}
	}

	db, err := store.OpenDB(ctx, path, opt)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx, db, migrationFS, "migrations"); err != nil {
		_ = db.Close()
		return nil, err
	}

	stmt, err := db.PrepareContext(ctx, insertEventSQL)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("prepare insert journal: %w", err)
	}

	return &Journal{
		db:         db,
		insertStmt: stmt,
		subs:       make(map[string]map[uint64]chan struct{}),
	}, nil
}

// DB mengembalikan handle *sql.DB milik journal.db untuk kebutuhan diagnostik.
func (j *Journal) DB() *sql.DB {
	if j == nil {
		return nil
	}
	return j.db
}

// Close menutup prepared statement, melakukan checkpoint WAL, lalu menutup database.
func (j *Journal) Close() error {
	if j == nil || j.db == nil {
		return nil
	}
	if j.insertStmt != nil {
		_ = j.insertStmt.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = j.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return j.db.Close()
}

// PartForChat menghitung nomor partisi stabil [0..63] memakai FNV-1a 32-bit tanpa alokasi heap.
func PartForChat(chat string) uint8 {
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	h := offset32
	for i := 0; i < len(chat); i++ {
		h ^= uint32(chat[i])
		h *= prime32
	}
	return uint8(h % numPartitions)
}

// Append menyimpan event secara atomik bersama hooks dalam satu transaksi immediate sebelum ack.
func (j *Journal) Append(ctx context.Context, ev *Event, hooks ...TxHook) (AppendResult, error) {
	if j == nil || j.db == nil {
		return AppendResult{}, errors.New("journal belum diinisialisasi")
	}
	if ev == nil || ev.InstanceID == "" || ev.Type == "" {
		return AppendResult{}, errors.New("event journal tidak valid: instance_id dan type wajib diisi")
	}

	if ev.ReceivedAt == 0 {
		ev.ReceivedAt = time.Now().UnixMilli()
	}
	if ev.TS == 0 {
		ev.TS = ev.ReceivedAt / 1000
	}
	if ev.Part == 0 && ev.Chat != "" {
		ev.Part = PartForChat(ev.Chat)
	}
	payload := ev.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return AppendResult{}, fmt.Errorf("mulai transaksi append journal: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var seq int64
	stmt := tx.StmtContext(ctx, j.insertStmt)
	err = stmt.QueryRowContext(
		ctx,
		ev.InstanceID,
		ev.Type,
		ev.Chat,
		ev.Sender,
		nullStr(ev.WAID),
		nullStr(ev.DedupeKey),
		ev.TS,
		ev.ReceivedAt,
		ev.Part,
		ev.Flags,
		payload,
	).Scan(&seq)

	if errors.Is(err, sql.ErrNoRows) {
		// Bentrok natural key -> ambil seq lama dalam transaksi ini tanpa mengeksekusi TxHook
		if selErr := tx.QueryRowContext(
			ctx,
			`SELECT seq FROM events WHERE instance_id = ? AND dedupe_key = ?`,
			ev.InstanceID, ev.DedupeKey,
		).Scan(&seq); selErr != nil {
			return AppendResult{}, fmt.Errorf("ambil seq duplikat journal: %w", selErr)
		}
		if err := tx.Commit(); err != nil {
			return AppendResult{}, fmt.Errorf("commit transaksi duplikat journal: %w", err)
		}
		committed = true
		ev.Seq = seq
		return AppendResult{Seq: seq, Duplicate: true}, nil
	}
	if err != nil {
		return AppendResult{}, fmt.Errorf("insert event journal: %w", err)
	}

	for _, hook := range hooks {
		if hook == nil {
			continue
		}
		if err := hook(ctx, tx, seq); err != nil {
			return AppendResult{}, fmt.Errorf("jalankan hook journal: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return AppendResult{}, fmt.Errorf("commit transaksi append journal: %w", err)
	}
	committed = true
	ev.Seq = seq

	j.notify(ev.InstanceID)
	return AppendResult{Seq: seq, Duplicate: false}, nil
}

// RunTx menjalankan kumpulan TxHook dalam satu transaksi immediate tanpa menyisipkan baris ke tabel events.
func (j *Journal) RunTx(ctx context.Context, hooks ...TxHook) error {
	if j == nil || j.db == nil {
		return errors.New("journal belum diinisialisasi")
	}
	hasHook := false
	for _, h := range hooks {
		if h != nil {
			hasHook = true
			break
		}
	}
	if !hasHook {
		return nil
	}

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mulai transaksi RunTx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for _, hook := range hooks {
		if hook == nil {
			continue
		}
		if err := hook(ctx, tx, 0); err != nil {
			return fmt.Errorf("jalankan hook RunTx: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit RunTx: %w", err)
	}
	committed = true
	return nil
}

// Read mengambil event milik satu instance terurut berdasarkan seq dengan dukungan filter tipe, lane, limit, dan batas byte payload.
func (j *Journal) Read(ctx context.Context, q Query) (ReadResult, error) {
	if j == nil || j.db == nil {
		return ReadResult{}, errors.New("journal belum diinisialisasi")
	}
	if q.InstanceID == "" {
		return ReadResult{}, errors.New("instance_id wajib diisi pada Query Read")
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > MaxReadLimit {
		limit = MaxReadLimit
	}
	if q.Lanes > 0 && (q.Lane < 0 || q.Lane >= q.Lanes) {
		return ReadResult{}, fmt.Errorf("lane %d di luar rentang [0, %d)", q.Lane, q.Lanes)
	}

	// Baca head lebih dulu agar ScannedTo tetap konsisten walaupun filter tipe/lane menghasilkan 0 baris
	var head int64
	if err := j.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM events WHERE instance_id = ?`, q.InstanceID).Scan(&head); err != nil {
		return ReadResult{}, fmt.Errorf("baca head journal: %w", err)
	}
	if head <= q.AfterSeq {
		return ReadResult{ScannedTo: q.AfterSeq, HasMore: false}, nil
	}

	var sb strings.Builder
	sb.WriteString(`SELECT seq, instance_id, type, chat, sender, COALESCE(wa_id, ''), COALESCE(dedupe_key, ''), ts, received_at, part, flags, payload FROM events WHERE instance_id = ? AND seq > ? AND seq <= ?`)
	args := make([]any, 0, 6+len(q.Types))
	args = append(args, q.InstanceID, q.AfterSeq, head)

	if len(q.Types) > 0 {
		sb.WriteString(` AND type IN (`)
		for idx, t := range q.Types {
			if idx > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('?')
			args = append(args, t)
		}
		sb.WriteByte(')')
	}
	if q.Lanes > 0 {
		sb.WriteString(` AND (part % ?) = ?`)
		args = append(args, q.Lanes, q.Lane)
	}
	sb.WriteString(` ORDER BY seq ASC LIMIT ?`)
	args = append(args, limit+1)

	rows, err := j.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return ReadResult{}, fmt.Errorf("query read journal: %w", err)
	}
	defer rows.Close()

	out := make([]Event, 0, min(limit, 32))
	totalBytes := 0
	truncatedEarly := false

	for rows.Next() {
		var ev Event
		if err := rows.Scan(
			&ev.Seq,
			&ev.InstanceID,
			&ev.Type,
			&ev.Chat,
			&ev.Sender,
			&ev.WAID,
			&ev.DedupeKey,
			&ev.TS,
			&ev.ReceivedAt,
			&ev.Part,
			&ev.Flags,
			&ev.Payload,
		); err != nil {
			return ReadResult{}, fmt.Errorf("scan baris journal: %w", err)
		}

		// Baris ke-(limit+1) menandakan masih ada event berikutnya sebelum head
		if len(out) >= limit {
			truncatedEarly = true
			break
		}
		if q.MaxBytes > 0 && len(out) > 0 && totalBytes+len(ev.Payload) > q.MaxBytes {
			truncatedEarly = true
			break
		}

		totalBytes += len(ev.Payload)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return ReadResult{}, fmt.Errorf("iterasi baris journal: %w", err)
	}

	scannedTo := head
	hasMore := false
	if truncatedEarly && len(out) > 0 {
		scannedTo = out[len(out)-1].Seq
		hasMore = true
	}
	return ReadResult{
		Events:    out,
		ScannedTo: scannedTo,
		HasMore:   hasMore,
	}, nil
}

// Get mengambil satu event berdasarkan instance_id dan seq.
func (j *Journal) Get(ctx context.Context, instanceID string, seq int64) (*Event, error) {
	if j == nil || j.db == nil {
		return nil, errors.New("journal belum diinisialisasi")
	}
	row := j.db.QueryRowContext(ctx, `
		SELECT seq, instance_id, type, chat, sender, COALESCE(wa_id, ''), COALESCE(dedupe_key, ''), ts, received_at, part, flags, payload
		FROM events WHERE instance_id = ? AND seq = ?`,
		instanceID, seq,
	)
	return scanSingleEvent(row)
}

// FindByWAID mencari event terbaru berdasarkan pasangan instance_id, chat, dan wa_id.
func (j *Journal) FindByWAID(ctx context.Context, instanceID, chat, waID string) (*Event, error) {
	if j == nil || j.db == nil {
		return nil, errors.New("journal belum diinisialisasi")
	}
	var row *sql.Row
	if chat != "" {
		row = j.db.QueryRowContext(ctx, `
			SELECT seq, instance_id, type, chat, sender, COALESCE(wa_id, ''), COALESCE(dedupe_key, ''), ts, received_at, part, flags, payload
			FROM events WHERE instance_id = ? AND chat = ? AND wa_id = ? ORDER BY seq DESC LIMIT 1`,
			instanceID, chat, waID,
		)
	} else {
		row = j.db.QueryRowContext(ctx, `
			SELECT seq, instance_id, type, chat, sender, COALESCE(wa_id, ''), COALESCE(dedupe_key, ''), ts, received_at, part, flags, payload
			FROM events WHERE instance_id = ? AND wa_id = ? ORDER BY seq DESC LIMIT 1`,
			instanceID, waID,
		)
	}
	return scanSingleEvent(row)
}

func scanSingleEvent(row *sql.Row) (*Event, error) {
	var ev Event
	err := row.Scan(
		&ev.Seq,
		&ev.InstanceID,
		&ev.Type,
		&ev.Chat,
		&ev.Sender,
		&ev.WAID,
		&ev.DedupeKey,
		&ev.TS,
		&ev.ReceivedAt,
		&ev.Part,
		&ev.Flags,
		&ev.Payload,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ambil event journal: %w", err)
	}
	return &ev, nil
}

// Subscribe mengembalikan channel notifikasi berkapasitas 1 untuk sebuah instance serta fungsi berhenti langganan.
func (j *Journal) Subscribe(instanceID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	if j == nil {
		return ch, func() {}
	}

	j.subMu.Lock()
	j.nextSubID++
	subID := j.nextSubID
	perInst := j.subs[instanceID]
	if perInst == nil {
		perInst = make(map[uint64]chan struct{})
		j.subs[instanceID] = perInst
	}
	perInst[subID] = ch
	j.subMu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			j.subMu.Lock()
			if m := j.subs[instanceID]; m != nil {
				delete(m, subID)
				if len(m) == 0 {
					delete(j.subs, instanceID)
				}
			}
			j.subMu.Unlock()
		})
	}
}

func (j *Journal) notify(instanceID string) {
	j.subMu.Lock()
	perInst := j.subs[instanceID]
	for _, ch := range perInst {
		// Non-blocking: sinyal digabung agar append tidak pernah terhambat konsumen lambat
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	j.subMu.Unlock()
}

// MarshalEnvelope menyusun JSON envelope v2 sesuai §4.1.2 dengan menyambung Payload apa adanya tanpa decode ulang.
func (e Event) MarshalEnvelope() ([]byte, error) {
	payload := bytes.TrimSpace(e.Payload)
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	buf := make([]byte, 0, 96+len(payload))
	buf = append(buf, `{"v":2,"id":"`...)
	buf = strconv.AppendInt(buf, e.Seq, 10)
	buf = append(buf, `","event":`...)
	buf = strconv.AppendQuote(buf, e.Type)
	buf = append(buf, `,"instance":`...)
	buf = strconv.AppendQuote(buf, e.InstanceID)
	buf = append(buf, `,"timestamp":`...)
	buf = strconv.AppendInt(buf, e.TS, 10)
	buf = append(buf, `,"data":`...)
	buf = append(buf, payload...)
	buf = append(buf, '}')
	return buf, nil
}

func nullStr(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
