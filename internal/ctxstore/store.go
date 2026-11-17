// Package ctxstore menyediakan penyimpanan riwayat percakapan opt-in terbatas per chat dengan pencarian FTS5.
package ctxstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

const (
	maxMessageRunes      = 4096
	maxPreviewRunes      = 120
	maxDirtyChats        = 10000
	pruneChunkSize       = 1000
	defaultChatsLimit    = 50
	maxChatsLimit        = 100
	defaultMessagesLimit = 20
	maxMessagesLimit     = 100
	defaultSearchLimit   = 20
	maxSearchLimit       = 100
)

var (
	// ErrDisabled dikembalikan saat endpoint context store dipanggil pada instance yang belum mengaktifkannya.
	ErrDisabled = errors.New("context store tidak aktif untuk instance ini")
	// ErrInvalidQuery dikembalikan saat parameter kueri pencarian atau paginasi tidak valid.
	ErrInvalidQuery = errors.New("parameter kueri context store tidak valid")
)

// Settings memasok konfigurasi context_store per instance.
type Settings interface {
	ContextConfig(ctx context.Context, instanceID string) (enabled bool, perChat int, maxAge time.Duration, err error)
}

type storeSettingsAdapter struct {
	st *store.Store
}

// NewStoreSettings membungkus *store.Store sebagai implementasi Settings context store.
func NewStoreSettings(st *store.Store) Settings {
	if st == nil {
		return nil
	}
	return storeSettingsAdapter{st: st}
}

func (a storeSettingsAdapter) ContextConfig(ctx context.Context, instanceID string) (bool, int, time.Duration, error) {
	inst, err := a.st.GetInstance(ctx, instanceID)
	if err != nil {
		return false, 0, 0, err
	}
	return inst.Settings.ContextStore, inst.Settings.ContextPerChat, inst.Settings.ContextMaxAge.Duration(), nil
}

// OutboundMessage mendeskripsikan pesan keluar sukses dikirim yang dicatat ke context store.
type OutboundMessage struct {
	InstanceID string
	Chat       string
	WAID       string
	Sender     string
	TS         int64
	Type       string
	Text       string
	QuotedID   string
	MediaKind  string
	Filename   string
	Origin     string
	TargetID   string
}

// ChatItem merepresentasikan ringkasan satu percakapan di respons GET /v2/instances/{id}/chats.
type ChatItem struct {
	Chat        string `json:"chat"`
	ChatType    string `json:"chat_type"`
	Name        string `json:"name"`
	LastTS      int64  `json:"last_ts"`
	LastPreview string `json:"last_preview"`
	Unread      int    `json:"unread"`
}

// ChatsPage membungkus hasil paginasi daftar chat.
type ChatsPage struct {
	Chats      []ChatItem `json:"chats"`
	NextBefore *string    `json:"next_before"`
}

// MessageItem merepresentasikan satu pesan pada respons GET /v2/instances/{id}/chats/{jid}/messages.
type MessageItem struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	FromMe    bool   `json:"from_me"`
	Timestamp int64  `json:"timestamp"`
	Type      string `json:"type"`
	Text      string `json:"text"`
	QuotedID  string `json:"quoted_id"`
}

// MessagesPage membungkus hasil paginasi pesan chat yang terurut naik.
type MessagesPage struct {
	Messages   []MessageItem `json:"messages"`
	NextBefore *string       `json:"next_before"`
}

// SearchHit merepresentasikan satu temuan pencarian FTS5 BM25.
type SearchHit struct {
	Chat      string  `json:"chat"`
	ID        string  `json:"id"`
	From      string  `json:"from"`
	FromMe    bool    `json:"from_me"`
	Timestamp int64   `json:"timestamp"`
	Type      string  `json:"type"`
	Snippet   string  `json:"snippet"`
	Rank      float64 `json:"rank"`
}

// SearchPage membungkus hasil pencarian FTS5.
type SearchPage struct {
	Hits []SearchHit `json:"hits"`
}

type messageMeta struct {
	QuotedID  string `json:"quoted_id,omitempty"`
	MediaKind string `json:"media_kind,omitempty"`
	Filename  string `json:"filename,omitempty"`
	Origin    string `json:"origin,omitempty"`
}

type inboundPayloadSubset struct {
	ID       string `json:"id"`
	From     string `json:"from"`
	Chat     string `json:"chat"`
	ChatType string `json:"chat_type"`
	IsGroup  bool   `json:"is_group"`
	IsFromMe bool   `json:"is_from_me"`
	PushName string `json:"push_name"`
	Type     string `json:"type"`
	Text     string `json:"text"`
	Media    *struct {
		MediaType string `json:"media_type"`
		Filename  string `json:"filename"`
	} `json:"media"`
	Quoted *struct {
		ID string `json:"id"`
	} `json:"quoted"`
	Edit *struct {
		TargetID string `json:"target_id"`
		Text     string `json:"text"`
	} `json:"edit"`
	Revoke *struct {
		TargetID string `json:"target_id"`
	} `json:"revoke"`
}

type recordEntry struct {
	instanceID string
	chat       string
	waID       string
	sender     string
	fromMe     bool
	ts         int64
	msgType    string
	text       string
	chatName   string
	meta       messageMeta
	targetID   string
}

type dirtyKey struct {
	instanceID string
	chat       string
}

// Store mengelola tabel messages, chats, dan indeks FTS5 di journal.db secara opt-in per instance.
type Store struct {
	db       *sql.DB
	settings Settings

	mu    sync.Mutex
	dirty map[dirtyKey]struct{} // dialokasikan malas saat pesan aktif pertama kali dicatat
}

// New membuat Store tanpa mengalokasikan state tambahan atau menjalankan goroutine saat context_store nonaktif.
func New(db *sql.DB, settings Settings) *Store {
	return &Store{
		db:       db,
		settings: settings,
	}
}

func (s *Store) configFor(ctx context.Context, instanceID string) (enabled bool, perChat int, maxAge time.Duration, err error) {
	if s == nil || s.db == nil || s.settings == nil || instanceID == "" {
		return false, 0, 0, nil
	}
	return s.settings.ContextConfig(ctx, instanceID)
}

func (s *Store) requireEnabled(ctx context.Context, instanceID string) error {
	enabled, _, _, err := s.configFor(ctx, instanceID)
	if err != nil {
		return err
	}
	if !enabled {
		return ErrDisabled
	}
	return nil
}

// RecordHook mengembalikan TxHook untuk menulis pesan masuk (atau edit/revoke) atomik bersama event journal.
func (s *Store) RecordHook(ev *journal.Event) journal.TxHook {
	if s == nil || s.db == nil || ev == nil || ev.Type != "message" || ev.InstanceID == "" || ev.Chat == "" {
		return nil
	}
	enabled, _, _, err := s.configFor(context.Background(), ev.InstanceID)
	if err != nil || !enabled {
		return nil
	}
	if ev.Chat == "status@broadcast" {
		return nil
	}

	var p inboundPayloadSubset
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return nil
	}
	if p.ChatType == "status" || p.Type == "" || p.Type == "reaction" {
		return nil
	}

	sender := ev.Sender
	if sender == "" {
		sender = p.From
	}
	chatName := ""
	if !p.IsGroup && !p.IsFromMe {
		chatName = strings.TrimSpace(p.PushName)
	}

	entry := recordEntry{
		instanceID: ev.InstanceID,
		chat:       ev.Chat,
		waID:       firstNonEmpty(ev.WAID, p.ID),
		sender:     sender,
		fromMe:     p.IsFromMe,
		ts:         ev.TS,
		msgType:    p.Type,
		text:       p.Text,
		chatName:   chatName,
	}
	if p.Quoted != nil {
		entry.meta.QuotedID = p.Quoted.ID
	}
	if p.Media != nil {
		entry.meta.MediaKind = p.Media.MediaType
		entry.meta.Filename = p.Media.Filename
	}
	if p.Type == "edit" && p.Edit != nil {
		entry.targetID = p.Edit.TargetID
		if p.Edit.Text != "" {
			entry.text = p.Edit.Text
		}
	}
	if p.Type == "revoke" && p.Revoke != nil {
		entry.targetID = p.Revoke.TargetID
	}

	return func(txCtx context.Context, tx *sql.Tx, _ int64) error {
		return s.applyEntryTx(txCtx, tx, entry, true)
	}
}

// RecordOutbound mencatat pesan keluar yang berstatus sent ke context store bila aktif.
func (s *Store) RecordOutbound(ctx context.Context, msg OutboundMessage) error {
	if s == nil || s.db == nil || msg.InstanceID == "" || msg.Chat == "" || msg.Chat == "status@broadcast" {
		return nil
	}
	enabled, _, _, err := s.configFor(ctx, msg.InstanceID)
	if err != nil || !enabled {
		return nil
	}
	if msg.Type == "" || msg.Type == "reaction" {
		return nil
	}

	entry := recordEntry{
		instanceID: msg.InstanceID,
		chat:       msg.Chat,
		waID:       msg.WAID,
		sender:     msg.Sender,
		fromMe:     true,
		ts:         msg.TS,
		msgType:    msg.Type,
		text:       msg.Text,
		meta: messageMeta{
			QuotedID:  msg.QuotedID,
			MediaKind: msg.MediaKind,
			Filename:  msg.Filename,
			Origin:    msg.Origin,
		},
		targetID: msg.TargetID,
	}

	return s.withImmediateTx(ctx, "catat outbound context store", func(tx *sql.Tx) error {
		return s.applyEntryTx(ctx, tx, entry, false)
	})
}

func (s *Store) applyEntryTx(ctx context.Context, tx *sql.Tx, entry recordEntry, viaJournalHook bool) error {
	switch entry.msgType {
	case "reaction":
		return nil
	case "edit":
		return s.applyEditTx(ctx, tx, entry.instanceID, entry.chat, entry.targetID, entry.text)
	case "revoke":
		return s.applyRevokeTx(ctx, tx, entry.instanceID, entry.chat, entry.targetID)
	}
	if entry.waID == "" {
		return nil
	}

	text := truncateRunes(entry.text, maxMessageRunes)
	metaBlob, err := encodeMeta(entry.meta)
	if err != nil {
		return err
	}
	fromMeInt := 0
	if entry.fromMe {
		fromMeInt = 1
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO messages (instance_id, chat, wa_id, sender, from_me, ts, type, text, meta)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id, chat, wa_id) DO NOTHING`,
		entry.instanceID, entry.chat, entry.waID, entry.sender, fromMeInt, entry.ts, entry.msgType, text, metaBlob,
	)
	if err != nil {
		return fmt.Errorf("simpan pesan context store: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return nil
	}

	preview := formatPreview(text)
	if entry.fromMe {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO chats (instance_id, chat, name, last_ts, last_preview, unread)
			VALUES (?, ?, ?, ?, ?, 0)
			ON CONFLICT(instance_id, chat) DO UPDATE SET
				name = CASE WHEN excluded.name != '' THEN excluded.name ELSE chats.name END,
				last_ts = CASE WHEN excluded.last_ts >= chats.last_ts THEN excluded.last_ts ELSE chats.last_ts END,
				last_preview = CASE WHEN excluded.last_ts >= chats.last_ts THEN excluded.last_preview ELSE chats.last_preview END,
				unread = 0`,
			entry.instanceID, entry.chat, entry.chatName, entry.ts, preview,
		)
	} else {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO chats (instance_id, chat, name, last_ts, last_preview, unread)
			VALUES (?, ?, ?, ?, ?, 1)
			ON CONFLICT(instance_id, chat) DO UPDATE SET
				name = CASE WHEN excluded.name != '' THEN excluded.name ELSE chats.name END,
				last_ts = CASE WHEN excluded.last_ts >= chats.last_ts THEN excluded.last_ts ELSE chats.last_ts END,
				last_preview = CASE WHEN excluded.last_ts >= chats.last_ts THEN excluded.last_preview ELSE chats.last_preview END,
				unread = chats.unread + 1`,
			entry.instanceID, entry.chat, entry.chatName, entry.ts, preview,
		)
	}
	if err != nil {
		return fmt.Errorf("perbarui ringkasan chat context store: %w", err)
	}

	if viaJournalHook {
		journal.OnCommit(ctx, func() {
			s.markDirty(entry.instanceID, entry.chat)
		})
	} else {
		// RecordOutbound memanggil markDirty sesudah commit via caller bila sukses, atau boleh langsung catat pasca-insert.
		// Karena tx ini di-commit segera sebelum ada prune paralel, pencatatan tetap aman dalam set dirty.
		s.markDirty(entry.instanceID, entry.chat)
	}
	return nil
}

// ApplyEdit memperbarui teks pesan target pada context store beserta last_preview chat dan indeks FTS5.
func (s *Store) ApplyEdit(ctx context.Context, instanceID, chat, targetWAID, newText string) error {
	if s == nil || s.db == nil || instanceID == "" || chat == "" || targetWAID == "" {
		return nil
	}
	return s.withImmediateTx(ctx, "edit pesan context store", func(tx *sql.Tx) error {
		return s.applyEditTx(ctx, tx, instanceID, chat, targetWAID, newText)
	})
}

func (s *Store) applyEditTx(ctx context.Context, tx *sql.Tx, instanceID, chat, targetWAID, newText string) error {
	if targetWAID == "" {
		return nil
	}
	trimmed := truncateRunes(newText, maxMessageRunes)
	var editedID int64
	err := tx.QueryRowContext(ctx, `
		UPDATE messages
		SET text = ?
		WHERE instance_id = ? AND chat = ? AND wa_id = ?
		RETURNING id`,
		trimmed, instanceID, chat, targetWAID,
	).Scan(&editedID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("perbarui teks pesan edit context store: %w", err)
	}

	var latestID int64
	if err := tx.QueryRowContext(ctx, `
		SELECT id FROM messages
		WHERE instance_id = ? AND chat = ?
		ORDER BY ts DESC, id DESC LIMIT 1`,
		instanceID, chat,
	).Scan(&latestID); err == nil && latestID == editedID {
		if _, err := tx.ExecContext(ctx, `
			UPDATE chats SET last_preview = ? WHERE instance_id = ? AND chat = ?`,
			formatPreview(trimmed), instanceID, chat,
		); err != nil {
			return fmt.Errorf("perbarui preview edit chat: %w", err)
		}
	}
	return nil
}

// ApplyRevoke menghapus pesan target dari context store beserta FTS5 dan menyinkronkan ringkasan chat.
func (s *Store) ApplyRevoke(ctx context.Context, instanceID, chat, targetWAID string) error {
	if s == nil || s.db == nil || instanceID == "" || chat == "" || targetWAID == "" {
		return nil
	}
	return s.withImmediateTx(ctx, "hapus pesan revoke context store", func(tx *sql.Tx) error {
		return s.applyRevokeTx(ctx, tx, instanceID, chat, targetWAID)
	})
}

func (s *Store) applyRevokeTx(ctx context.Context, tx *sql.Tx, instanceID, chat, targetWAID string) error {
	if targetWAID == "" {
		return nil
	}
	res, err := tx.ExecContext(ctx, `
		DELETE FROM messages
		WHERE instance_id = ? AND chat = ? AND wa_id = ?`,
		instanceID, chat, targetWAID,
	)
	if err != nil {
		return fmt.Errorf("hapus pesan revoke context store: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil
	}

	var latestTS int64
	var latestText string
	err = tx.QueryRowContext(ctx, `
		SELECT ts, text FROM messages
		WHERE instance_id = ? AND chat = ?
		ORDER BY ts DESC, id DESC LIMIT 1`,
		instanceID, chat,
	).Scan(&latestTS, &latestText)
	if errors.Is(err, sql.ErrNoRows) {
		if _, delErr := tx.ExecContext(ctx, `DELETE FROM chats WHERE instance_id = ? AND chat = ?`, instanceID, chat); delErr != nil {
			return fmt.Errorf("hapus chat kosong pasca-revoke: %w", delErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("ambil pesan terbaru pasca-revoke: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE chats SET last_ts = ?, last_preview = ? WHERE instance_id = ? AND chat = ?`,
		latestTS, formatPreview(latestText), instanceID, chat,
	); err != nil {
		return fmt.Errorf("sinkronkan preview pasca-revoke: %w", err)
	}
	return nil
}

// Chats mengembalikan daftar chat terurut dari pesan terakhir dengan paginasi cursor before=<ts>|<ts>:<chat>.
func (s *Store) Chats(ctx context.Context, instanceID string, limit int, before string) (ChatsPage, error) {
	if err := s.requireEnabled(ctx, instanceID); err != nil {
		return ChatsPage{}, err
	}
	limit = clampLimit(limit, defaultChatsLimit, maxChatsLimit)

	hasCursor, beforeTS, beforeChat, hasTie, err := parseChatsBefore(before)
	if err != nil {
		return ChatsPage{}, err
	}

	var (
		query string
		args  []any
	)
	if !hasCursor {
		query = `
			SELECT chat, name, last_ts, last_preview, unread
			FROM chats
			WHERE instance_id = ?
			ORDER BY last_ts DESC, chat DESC
			LIMIT ?`
		args = []any{instanceID, limit + 1}
	} else if !hasTie {
		query = `
			SELECT chat, name, last_ts, last_preview, unread
			FROM chats
			WHERE instance_id = ? AND last_ts < ?
			ORDER BY last_ts DESC, chat DESC
			LIMIT ?`
		args = []any{instanceID, beforeTS, limit + 1}
	} else {
		query = `
			SELECT chat, name, last_ts, last_preview, unread
			FROM chats
			WHERE instance_id = ? AND (last_ts < ? OR (last_ts = ? AND chat < ?))
			ORDER BY last_ts DESC, chat DESC
			LIMIT ?`
		args = []any{instanceID, beforeTS, beforeTS, beforeChat, limit + 1}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ChatsPage{}, fmt.Errorf("kueri daftar chat context store: %w", err)
	}
	defer rows.Close()

	items := make([]ChatItem, 0, min(limit, defaultChatsLimit))
	hasMore := false
	for rows.Next() {
		var c ChatItem
		if err := rows.Scan(&c.Chat, &c.Name, &c.LastTS, &c.LastPreview, &c.Unread); err != nil {
			return ChatsPage{}, fmt.Errorf("scan chat context store: %w", err)
		}
		if len(items) == limit {
			hasMore = true
			break
		}
		c.ChatType = detectChatTypeString(c.Chat)
		items = append(items, c)
	}
	if err := rows.Err(); err != nil {
		return ChatsPage{}, fmt.Errorf("iterasi chat context store: %w", err)
	}

	var nextBefore *string
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		tok := fmt.Sprintf("%d:%s", last.LastTS, last.Chat)
		nextBefore = &tok
	}
	return ChatsPage{Chats: items, NextBefore: nextBefore}, nil
}

// Messages mengembalikan pesan chat dalam urutan kronologis naik per halaman serta cursor next_before komposit.
func (s *Store) Messages(ctx context.Context, instanceID, chat string, limit int, before string) (MessagesPage, error) {
	if err := s.requireEnabled(ctx, instanceID); err != nil {
		return MessagesPage{}, err
	}
	if strings.TrimSpace(chat) == "" {
		return MessagesPage{}, fmt.Errorf("%w: chat wajib diisi", ErrInvalidQuery)
	}
	limit = clampLimit(limit, defaultMessagesLimit, maxMessagesLimit)

	hasCursor, beforeTS, beforeID, hasTie, err := parseMessagesBefore(before)
	if err != nil {
		return MessagesPage{}, err
	}

	var (
		query string
		args  []any
	)
	if !hasCursor {
		query = `
			SELECT id, wa_id, sender, from_me, ts, type, text, meta
			FROM messages
			WHERE instance_id = ? AND chat = ?
			ORDER BY ts DESC, id DESC
			LIMIT ?`
		args = []any{instanceID, chat, limit + 1}
	} else if !hasTie {
		query = `
			SELECT id, wa_id, sender, from_me, ts, type, text, meta
			FROM messages
			WHERE instance_id = ? AND chat = ? AND ts < ?
			ORDER BY ts DESC, id DESC
			LIMIT ?`
		args = []any{instanceID, chat, beforeTS, limit + 1}
	} else {
		query = `
			SELECT id, wa_id, sender, from_me, ts, type, text, meta
			FROM messages
			WHERE instance_id = ? AND chat = ? AND (ts < ? OR (ts = ? AND id < ?))
			ORDER BY ts DESC, id DESC
			LIMIT ?`
		args = []any{instanceID, chat, beforeTS, beforeTS, beforeID, limit + 1}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return MessagesPage{}, fmt.Errorf("kueri pesan context store: %w", err)
	}
	defer rows.Close()

	type scannedMsg struct {
		rowID int64
		item  MessageItem
	}
	desc := make([]scannedMsg, 0, min(limit, defaultMessagesLimit))
	hasMore := false
	for rows.Next() {
		var (
			rowID    int64
			waID     string
			sender   string
			fromMe   int
			ts       int64
			msgType  string
			text     string
			metaBlob []byte
		)
		if err := rows.Scan(&rowID, &waID, &sender, &fromMe, &ts, &msgType, &text, &metaBlob); err != nil {
			return MessagesPage{}, fmt.Errorf("scan pesan context store: %w", err)
		}
		if len(desc) == limit {
			hasMore = true
			break
		}
		var m messageMeta
		if len(metaBlob) > 0 {
			_ = json.Unmarshal(metaBlob, &m)
		}
		desc = append(desc, scannedMsg{
			rowID: rowID,
			item: MessageItem{
				ID:        waID,
				From:      sender,
				FromMe:    fromMe == 1,
				Timestamp: ts,
				Type:      msgType,
				Text:      text,
				QuotedID:  m.QuotedID,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return MessagesPage{}, fmt.Errorf("iterasi pesan context store: %w", err)
	}

	var nextBefore *string
	if hasMore && len(desc) > 0 {
		oldest := desc[len(desc)-1]
		tok := fmt.Sprintf("%d:%d", oldest.item.Timestamp, oldest.rowID)
		nextBefore = &tok
	}

	// Balik urutan menjadi kronologis menaik dalam satu halaman.
	items := make([]MessageItem, len(desc))
	for i := range desc {
		items[len(desc)-1-i] = desc[i].item
	}
	return MessagesPage{Messages: items, NextBefore: nextBefore}, nil
}

// Search mencari pesan teks melalui indeks FTS5 dengan sanitasi sintaks dan ranking BM25.
func (s *Store) Search(ctx context.Context, instanceID, query, chat string, limit int, prefix bool) (SearchPage, error) {
	if err := s.requireEnabled(ctx, instanceID); err != nil {
		return SearchPage{}, err
	}
	limit = clampLimit(limit, defaultSearchLimit, maxSearchLimit)

	ftsQuery, err := sanitizeFTSQuery(query, prefix)
	if err != nil {
		return SearchPage{}, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT m.chat, m.wa_id, m.sender, m.from_me, m.ts, m.type,
		       snippet(messages_fts, 0, '[', ']', '…', 12) AS snip, bm25(messages_fts) AS rank
		FROM messages_fts
		JOIN messages m ON m.id = messages_fts.rowid
		WHERE messages_fts MATCH ?1 AND m.instance_id = ?2 AND (?3 = '' OR m.chat = ?3)
		ORDER BY rank, m.ts DESC, m.id DESC
		LIMIT ?4`,
		ftsQuery, instanceID, chat, limit,
	)
	if err != nil {
		return SearchPage{}, fmt.Errorf("kueri pencarian FTS5: %w", err)
	}
	defer rows.Close()

	hits := make([]SearchHit, 0, min(limit, defaultSearchLimit))
	for rows.Next() {
		var (
			hit    SearchHit
			fromMe int
		)
		if err := rows.Scan(&hit.Chat, &hit.ID, &hit.From, &fromMe, &hit.Timestamp, &hit.Type, &hit.Snippet, &hit.Rank); err != nil {
			return SearchPage{}, fmt.Errorf("scan hasil pencarian FTS5: %w", err)
		}
		hit.FromMe = fromMe == 1
		hits = append(hits, hit)
	}
	if err := rows.Err(); err != nil {
		return SearchPage{}, fmt.Errorf("iterasi hasil pencarian FTS5: %w", err)
	}
	return SearchPage{Hits: hits}, nil
}

// LookupQuoted membaca ringkasan pengirim dan teks pesan rujukan dari context store saat fitur aktif.
func (s *Store) LookupQuoted(ctx context.Context, instanceID, chat, waID string) (sender, text string, fromMe bool, ok bool) {
	if s == nil || s.db == nil || instanceID == "" || chat == "" || waID == "" {
		return "", "", false, false
	}
	enabled, _, _, err := s.configFor(ctx, instanceID)
	if err != nil || !enabled {
		return "", "", false, false
	}

	var fromMeInt int
	err = s.db.QueryRowContext(ctx, `
		SELECT sender, from_me, text
		FROM messages
		WHERE instance_id = ? AND chat = ? AND wa_id = ?
		LIMIT 1`,
		instanceID, chat, waID,
	).Scan(&sender, &fromMeInt, &text)
	if err != nil {
		return "", "", false, false
	}
	return sender, text, fromMeInt == 1, true
}

// MarkChatRead mereset counter unread ke 0 untuk satu chat bila context_store aktif (no-op jika mati).
func (s *Store) MarkChatRead(ctx context.Context, instanceID, chat string) error {
	if s == nil || s.db == nil || instanceID == "" || chat == "" {
		return nil
	}
	enabled, _, _, err := s.configFor(ctx, instanceID)
	if err != nil || !enabled {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE chats SET unread = 0 WHERE instance_id = ? AND chat = ?`, instanceID, chat); err != nil {
		return fmt.Errorf("reset unread context store: %w", err)
	}
	return nil
}

// Purge menghapus seluruh data context store milik satu instance atau spesifik satu chat.
func (s *Store) Purge(ctx context.Context, instanceID, chat string) error {
	if s == nil || s.db == nil || instanceID == "" {
		return nil
	}
	err := s.withImmediateTx(ctx, "purge context store", func(tx *sql.Tx) error {
		if chat != "" {
			if _, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE instance_id = ? AND chat = ?`, instanceID, chat); err != nil {
				return fmt.Errorf("hapus pesan chat %s: %w", chat, err)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM chats WHERE instance_id = ? AND chat = ?`, instanceID, chat); err != nil {
				return fmt.Errorf("hapus ringkasan chat %s: %w", chat, err)
			}
			return nil
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE instance_id = ?`, instanceID); err != nil {
			return fmt.Errorf("hapus semua pesan instance %s: %w", instanceID, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM chats WHERE instance_id = ?`, instanceID); err != nil {
			return fmt.Errorf("hapus semua chat instance %s: %w", instanceID, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.dirty != nil {
		if chat != "" {
			delete(s.dirty, dirtyKey{instanceID: instanceID, chat: chat})
		} else {
			for k := range s.dirty {
				if k.instanceID == instanceID {
					delete(s.dirty, k)
				}
			}
		}
	}
	s.mu.Unlock()
	return nil
}

// Prune memotong chat tersentuh hingga context_per_chat serta menghapus pesan melewati context_max_age per chunk 1000.
func (s *Store) Prune(ctx context.Context, now time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if now.IsZero() {
		now = time.Now()
	}

	var totalDeleted int64

	// Langkah 1: potong jumlah pesan per chat pada chat yang tersentuh sejak prune terakhir.
	touched := s.drainDirty()
	for _, key := range touched {
		if ctx.Err() != nil {
			return totalDeleted, ctx.Err()
		}
		enabled, perChat, _, err := s.configFor(ctx, key.instanceID)
		if err != nil || !enabled || perChat <= 0 {
			continue
		}
		res, err := s.db.ExecContext(ctx, `
			DELETE FROM messages
			WHERE id IN (
				SELECT id FROM messages
				WHERE instance_id = ? AND chat = ?
				ORDER BY ts DESC, id DESC
				LIMIT -1 OFFSET ?
			)`,
			key.instanceID, key.chat, perChat,
		)
		if err != nil {
			return totalDeleted, fmt.Errorf("prune cap chat %s/%s: %w", key.instanceID, key.chat, err)
		}
		n, _ := res.RowsAffected()
		totalDeleted += n
	}

	// Langkah 2: hapus pesan yang melewati context_max_age per chunk 1000 baris.
	instances, err := s.listInstances(ctx)
	if err != nil {
		return totalDeleted, err
	}
	for _, inst := range instances {
		if ctx.Err() != nil {
			return totalDeleted, ctx.Err()
		}
		_, _, maxAge, cfgErr := s.configFor(ctx, inst)
		if cfgErr != nil || maxAge <= 0 {
			maxAge = store.Defaults().ContextMaxAge.Duration()
		}
		cutoff := now.Add(-maxAge).Unix()

		var instDeleted int64
		for {
			if ctx.Err() != nil {
				return totalDeleted, ctx.Err()
			}
			res, err := s.db.ExecContext(ctx, `
				DELETE FROM messages
				WHERE id IN (
					SELECT id FROM messages
					WHERE instance_id = ? AND ts < ?
					ORDER BY ts ASC, id ASC
					LIMIT ?
				)`,
				inst, cutoff, pruneChunkSize,
			)
			if err != nil {
				return totalDeleted, fmt.Errorf("prune max_age instance %s: %w", inst, err)
			}
			n, _ := res.RowsAffected()
			totalDeleted += n
			instDeleted += n
			if n < pruneChunkSize {
				break
			}
		}
		if instDeleted > 0 {
			if _, err := s.db.ExecContext(ctx, `
				DELETE FROM chats
				WHERE instance_id = ?
				  AND NOT EXISTS (
					SELECT 1 FROM messages m
					WHERE m.instance_id = chats.instance_id AND m.chat = chats.chat
				  )`,
				inst,
			); err != nil {
				return totalDeleted, fmt.Errorf("bersihkan chat yatim instance %s: %w", inst, err)
			}
		}
	}

	return totalDeleted, nil
}

func (s *Store) listInstances(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT instance_id FROM messages`)
	if err != nil {
		return nil, fmt.Errorf("daftar instance context store: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan instance context store: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) markDirty(instanceID, chat string) {
	s.mu.Lock()
	if s.dirty == nil {
		s.dirty = make(map[dirtyKey]struct{})
	}
	if len(s.dirty) < maxDirtyChats {
		s.dirty[dirtyKey{instanceID: instanceID, chat: chat}] = struct{}{}
	}
	s.mu.Unlock()
}

func (s *Store) drainDirty() []dirtyKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.dirty) == 0 {
		return nil
	}
	out := make([]dirtyKey, 0, len(s.dirty))
	for k := range s.dirty {
		out = append(out, k)
	}
	s.dirty = nil
	return out
}

func (s *Store) withImmediateTx(ctx context.Context, op string, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mulai transaksi %s: %w", op, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaksi %s: %w", op, err)
	}
	committed = true
	return nil
}

// sanitizeFTSQuery membungkus tiap token ke dalam tanda kutip ganda agar sintaks FTS5 pengguna tidak dievaluasi.
func sanitizeFTSQuery(raw string, prefix bool) (string, error) {
	tokens := strings.Fields(strings.TrimSpace(raw))
	if len(tokens) == 0 {
		return "", fmt.Errorf("%w: parameter q wajib diisi", ErrInvalidQuery)
	}
	quoted := make([]string, 0, len(tokens))
	for i, tok := range tokens {
		escaped := strings.ReplaceAll(tok, `"`, `""`)
		if prefix && i == len(tokens)-1 {
			quoted = append(quoted, `"`+escaped+`"*`)
		} else {
			quoted = append(quoted, `"`+escaped+`"`)
		}
	}
	return strings.Join(quoted, " "), nil
}

func parseChatsBefore(raw string) (hasCursor bool, ts int64, chat string, hasTie bool, err error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return false, 0, "", false, nil
	}
	if left, right, ok := strings.Cut(s, ":"); ok {
		parsedTS, pErr := strconv.ParseInt(left, 10, 64)
		if pErr != nil || parsedTS < 0 || right == "" {
			return false, 0, "", false, fmt.Errorf("%w: format before tidak valid", ErrInvalidQuery)
		}
		return true, parsedTS, right, true, nil
	}
	parsedTS, pErr := strconv.ParseInt(s, 10, 64)
	if pErr != nil || parsedTS < 0 {
		return false, 0, "", false, fmt.Errorf("%w: parameter before harus berupa Unix detik atau cursor <ts>:<chat>", ErrInvalidQuery)
	}
	return true, parsedTS, "", false, nil
}

func parseMessagesBefore(raw string) (hasCursor bool, ts, id int64, hasTie bool, err error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return false, 0, 0, false, nil
	}
	if left, right, ok := strings.Cut(s, ":"); ok {
		parsedTS, errTS := strconv.ParseInt(left, 10, 64)
		parsedID, errID := strconv.ParseInt(right, 10, 64)
		if errTS != nil || errID != nil || parsedTS < 0 || parsedID <= 0 {
			return false, 0, 0, false, fmt.Errorf("%w: format cursor before tidak valid", ErrInvalidQuery)
		}
		return true, parsedTS, parsedID, true, nil
	}
	parsedTS, errTS := strconv.ParseInt(s, 10, 64)
	if errTS != nil || parsedTS < 0 {
		return false, 0, 0, false, fmt.Errorf("%w: parameter before harus berupa Unix detik atau cursor <ts>:<id>", ErrInvalidQuery)
	}
	return true, parsedTS, 0, false, nil
}

func truncateRunes(s string, max int) string {
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	var count int
	for idx := range s {
		if count == max {
			return s[:idx]
		}
		count++
	}
	return s
}

func formatPreview(text string) string {
	cleaned := strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(strings.TrimSpace(text))
	return truncateRunes(cleaned, maxPreviewRunes)
}

func encodeMeta(m messageMeta) ([]byte, error) {
	if m.QuotedID == "" && m.MediaKind == "" && m.Filename == "" && m.Origin == "" {
		return nil, nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode meta context store: %w", err)
	}
	return raw, nil
}

func clampLimit(val, defVal, maxVal int) int {
	if val <= 0 {
		return defVal
	}
	if val > maxVal {
		return maxVal
	}
	return val
}

func detectChatTypeString(chat string) string {
	switch {
	case chat == "status@broadcast":
		return "status"
	case strings.HasSuffix(chat, "@g.us"):
		return "group"
	case strings.HasSuffix(chat, "@newsletter"):
		return "newsletter"
	case strings.HasSuffix(chat, "@broadcast"):
		return "broadcast"
	default:
		return "dm"
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
