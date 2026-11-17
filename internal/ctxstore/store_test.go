package ctxstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/journal"
)

type stubSettings struct {
	mu      sync.RWMutex
	configs map[string]stubConfig
}

type stubConfig struct {
	enabled bool
	perChat int
	maxAge  time.Duration
}

func newStubSettings(inst string, enabled bool, perChat int, maxAge time.Duration) *stubSettings {
	return &stubSettings{
		configs: map[string]stubConfig{
			inst: {enabled: enabled, perChat: perChat, maxAge: maxAge},
		},
	}
}

func (s *stubSettings) set(inst string, enabled bool, perChat int, maxAge time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs[inst] = stubConfig{enabled: enabled, perChat: perChat, maxAge: maxAge}
}

func (s *stubSettings) ContextConfig(_ context.Context, instanceID string) (bool, int, time.Duration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.configs[instanceID]
	if !ok {
		return false, 50, 168 * time.Hour, nil
	}
	return c.enabled, c.perChat, c.maxAge, nil
}

func openTestJournalAndStore(t *testing.T, settings ctxstore.Settings) (*journal.Journal, *ctxstore.Store) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "journal.db")
	j, err := journal.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j, ctxstore.New(j.DB(), settings)
}

func makeMsgEvent(t *testing.T, inst, chat, sender, waID string, ts int64, fromMe bool, pushName, msgType, text string) *journal.Event {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"id":         waID,
		"from":       sender,
		"chat":       chat,
		"chat_type":  chatTypeFromJID(chat),
		"is_group":   strings.HasSuffix(chat, "@g.us"),
		"is_from_me": fromMe,
		"push_name":  pushName,
		"type":       msgType,
		"text":       text,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &journal.Event{
		InstanceID: inst,
		Type:       "message",
		Chat:       chat,
		Sender:     sender,
		WAID:       waID,
		DedupeKey:  "m:" + chat + ":" + sender + ":" + waID,
		TS:         ts,
		ReceivedAt: ts * 1000,
		Payload:    payload,
	}
}

func chatTypeFromJID(chat string) string {
	switch {
	case chat == "status@broadcast":
		return "status"
	case strings.HasSuffix(chat, "@g.us"):
		return "group"
	default:
		return "dm"
	}
}

// TestRecord_AtomikDenganJournal memastikan kegagalan hook transaksi menggulung balik event journal sekaligus baris pesan context store.
func TestRecord_AtomikDenganJournal(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 50, 168*time.Hour)
	j, cs := openTestJournalAndStore(t, settings)

	chat := "628111111111@s.whatsapp.net"
	evFail := makeMsgEvent(t, "sales", chat, chat, "WA-FAIL-1", 1700000100, false, "Budi", "text", "pesan gagal")
	errBoom := errors.New("simulasi rollback transaksi")

	_, err := j.Append(ctx, evFail, cs.RecordHook(evFail), func(context.Context, *sql.Tx, int64) error {
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("diharapkan error %v, didapat %v", errBoom, err)
	}

	var evCount, msgCount, chatCount int
	_ = j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&evCount)
	_ = j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&msgCount)
	_ = j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM chats`).Scan(&chatCount)
	if evCount != 0 || msgCount != 0 || chatCount != 0 {
		t.Fatalf("rollback gagal: events=%d messages=%d chats=%d", evCount, msgCount, chatCount)
	}

	// Lewati status@broadcast dan reaction sesuai aturan Batch 12
	evStatus := makeMsgEvent(t, "sales", "status@broadcast", "6281@s.whatsapp.net", "WA-STAT-1", 1700000200, false, "Ani", "text", "story status")
	if hook := cs.RecordHook(evStatus); hook != nil {
		if _, err := j.Append(ctx, evStatus, hook); err != nil {
			t.Fatal(err)
		}
	}
	evReact := makeMsgEvent(t, "sales", chat, chat, "WA-REACT-1", 1700000201, false, "Budi", "reaction", "👍")
	if hook := cs.RecordHook(evReact); hook != nil {
		if _, err := j.Append(ctx, evReact, hook); err != nil {
			t.Fatal(err)
		}
	}

	// Simpan pesan panjang (>4096 karakter) untuk memastikan pemotongan text 4096 dan preview 120 karakter
	longText := strings.Repeat("a", 5000)
	evOK := makeMsgEvent(t, "sales", chat, chat, "WA-OK-1", 1700000300, false, "Budi", "text", longText)
	if _, err := j.Append(ctx, evOK, cs.RecordHook(evOK)); err != nil {
		t.Fatalf("append pesan valid gagal: %v", err)
	}

	page, err := cs.Messages(ctx, "sales", chat, 20, "")
	if err != nil {
		t.Fatalf("Messages gagal: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != "WA-OK-1" {
		t.Fatalf("pesan yang tersimpan tidak sesuai: %+v", page.Messages)
	}
	if len(page.Messages[0].Text) != 4096 {
		t.Fatalf("panjang text harus dipotong ke 4096 rune, didapat %d", len(page.Messages[0].Text))
	}
	chatsPage, err := cs.Chats(ctx, "sales", 20, "")
	if err != nil {
		t.Fatalf("Chats gagal: %v", err)
	}
	if len(chatsPage.Chats) != 1 || len(chatsPage.Chats[0].LastPreview) != 120 {
		t.Fatalf("last_preview harus 120 karakter: %+v", chatsPage.Chats)
	}
}

// TestMessages_Paginasi memverifikasi paginasi before=<ts>:<id> dan fallback before=<ts> serta urutan naik per halaman.
func TestMessages_Paginasi(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 100, 168*time.Hour)
	j, cs := openTestJournalAndStore(t, settings)

	chat := "628122233344@s.whatsapp.net"
	baseTS := int64(1700001000)
	// Masukkan 5 pesan dengan 3 pesan berada di detik yang sama untuk menguji cursor komposit anti-skip
	inputs := []struct {
		id string
		ts int64
	}{
		{"m1", baseTS + 1},
		{"m2", baseTS + 2},
		{"m3", baseTS + 2},
		{"m4", baseTS + 2},
		{"m5", baseTS + 3},
	}
	for _, in := range inputs {
		ev := makeMsgEvent(t, "sales", chat, chat, in.id, in.ts, false, "Budi", "text", "isi "+in.id)
		if _, err := j.Append(ctx, ev, cs.RecordHook(ev)); err != nil {
			t.Fatal(err)
		}
	}

	p1, err := cs.Messages(ctx, "sales", chat, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Messages) != 2 || p1.Messages[0].ID != "m4" || p1.Messages[1].ID != "m5" {
		t.Fatalf("halaman 1 diharapkan [m4 m5] urut naik, didapat %+v", p1.Messages)
	}
	if p1.NextBefore == nil || !strings.Contains(*p1.NextBefore, ":") {
		t.Fatalf("next_before harus cursor string komposit <ts>:<id>, didapat %v", p1.NextBefore)
	}

	p2, err := cs.Messages(ctx, "sales", chat, 2, *p1.NextBefore)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Messages) != 2 || p2.Messages[0].ID != "m2" || p2.Messages[1].ID != "m3" {
		t.Fatalf("halaman 2 diharapkan [m2 m3], didapat %+v", p2.Messages)
	}
	if p2.NextBefore == nil {
		t.Fatal("halaman 2 seharusnya masih memiliki next_before")
	}

	p3, err := cs.Messages(ctx, "sales", chat, 2, *p2.NextBefore)
	if err != nil {
		t.Fatal(err)
	}
	if len(p3.Messages) != 1 || p3.Messages[0].ID != "m1" || p3.NextBefore != nil {
		t.Fatalf("halaman 3 diharapkan [m1] dengan next_before nil, didapat %+v (next=%v)", p3.Messages, p3.NextBefore)
	}

	// Uji fallback kompatibel before=<ts> angka polos
	pCompat, err := cs.Messages(ctx, "sales", chat, 10, fmt.Sprintf("%d", baseTS+2))
	if err != nil {
		t.Fatal(err)
	}
	if len(pCompat.Messages) != 1 || pCompat.Messages[0].ID != "m1" {
		t.Fatalf("fallback before=<ts> diharapkan mengembalikan [m1], didapat %+v", pCompat.Messages)
	}
}

// TestPrune_CapPerChat memastikan chat yang disentuh dipotong ke batas context_per_chat dengan menyisakan N pesan terbaru.
func TestPrune_CapPerChat(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 50, 168*time.Hour)
	j, cs := openTestJournalAndStore(t, settings)

	chat := "628133344455@s.whatsapp.net"
	for i := 1; i <= 60; i++ {
		ev := makeMsgEvent(t, "sales", chat, chat, fmt.Sprintf("WA-%03d", i), int64(1700000000+i), false, "Dina", "text", fmt.Sprintf("pesan nomor %03d", i))
		if _, err := j.Append(ctx, ev, cs.RecordHook(ev)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := cs.Prune(ctx, time.Unix(1700000100, 0)); err != nil {
		t.Fatalf("Prune gagal: %v", err)
	}

	page, err := cs.Messages(ctx, "sales", chat, 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 50 {
		t.Fatalf("jumlah pesan setelah prune cap diharapkan 50, didapat %d", len(page.Messages))
	}
	if page.Messages[0].ID != "WA-011" || page.Messages[49].ID != "WA-060" {
		t.Fatalf("harus menyisakan WA-011..WA-060, didapat pertama=%s terakhir=%s", page.Messages[0].ID, page.Messages[49].ID)
	}

	// Indeks FTS5 pesan terhapus juga harus bersih
	hits, err := cs.Search(ctx, "sales", "001", "", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits.Hits) != 0 {
		t.Fatalf("pesan ter-prune seharusnya hilang dari FTS5, didapat %+v", hits.Hits)
	}
}

// TestPrune_UmurMaksimal memverifikasi penghapusan pesan melewati context_max_age per chunk 1000 memakai waktu palsu.
func TestPrune_UmurMaksimal(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 500, 24*time.Hour)
	_, cs := openTestJournalAndStore(t, settings)

	now := time.Unix(1700200000, 0)
	oldTS := now.Add(-48 * time.Hour).Unix()
	freshTS := now.Add(-1 * time.Hour).Unix()

	// Sisipkan 1200 pesan kedaluwarsa di satu chat + 1 pesan baru di chat lain
	oldChat := "628144455566@s.whatsapp.net"
	for i := 0; i < 1200; i++ {
		if err := cs.RecordOutbound(ctx, ctxstore.OutboundMessage{
			InstanceID: "sales",
			Chat:       oldChat,
			WAID:       fmt.Sprintf("OLD-%04d", i),
			TS:         oldTS + int64(i%100),
			Type:       "text",
			Text:       fmt.Sprintf("arsip lama %d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	freshChat := "628155566677@s.whatsapp.net"
	if err := cs.RecordOutbound(ctx, ctxstore.OutboundMessage{
		InstanceID: "sales",
		Chat:       freshChat,
		WAID:       "NEW-1",
		TS:         freshTS,
		Type:       "text",
		Text:       "pesan segar",
	}); err != nil {
		t.Fatal(err)
	}

	deleted, err := cs.Prune(ctx, now)
	if err != nil {
		t.Fatalf("Prune umur maksimal gagal: %v", err)
	}
	if deleted < 1200 {
		t.Fatalf("diharapkan >= 1200 baris kedaluwarsa terhapus, didapat %d", deleted)
	}

	remOld, err := cs.Messages(ctx, "sales", oldChat, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(remOld.Messages) != 0 {
		t.Fatalf("semua pesan lama seharusnya habis terhapus, sisa %d", len(remOld.Messages))
	}
	remNew, err := cs.Messages(ctx, "sales", freshChat, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(remNew.Messages) != 1 || remNew.Messages[0].ID != "NEW-1" {
		t.Fatalf("pesan segar tidak boleh terhapus: %+v", remNew.Messages)
	}
}

// TestEdit_MemperbaruiFTS memastikan event edit memperbarui text target, last_preview, dan indeks FTS5.
func TestEdit_MemperbaruiFTS(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 50, 168*time.Hour)
	j, cs := openTestJournalAndStore(t, settings)

	chat := "628166677788@s.whatsapp.net"
	ev := makeMsgEvent(t, "sales", chat, chat, "WA-TARGET-1", 1700001000, false, "Rina", "text", "promo lama diskon")
	if _, err := j.Append(ctx, ev, cs.RecordHook(ev)); err != nil {
		t.Fatal(err)
	}

	editPayload, _ := json.Marshal(map[string]any{
		"id":   "WA-EDIT-1",
		"from": chat,
		"chat": chat,
		"type": "edit",
		"text": "harga baru spesial",
		"edit": map[string]any{"target_id": "WA-TARGET-1", "text": "harga baru spesial"},
	})
	evEdit := &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       chat,
		Sender:     chat,
		WAID:       "WA-EDIT-1",
		TS:         1700001010,
		ReceivedAt: 1700001010000,
		Payload:    editPayload,
	}
	if _, err := j.Append(ctx, evEdit, cs.RecordHook(evEdit)); err != nil {
		t.Fatal(err)
	}

	oldHits, err := cs.Search(ctx, "sales", "lama", "", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldHits.Hits) != 0 {
		t.Fatalf("kata lama seharusnya tidak ditemukan lagi di FTS5: %+v", oldHits.Hits)
	}
	newHits, err := cs.Search(ctx, "sales", "spesial", "", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(newHits.Hits) != 1 || newHits.Hits[0].ID != "WA-TARGET-1" {
		t.Fatalf("kata baru harus ditemukan pada target ID: %+v", newHits.Hits)
	}
	chatsPage, err := cs.Chats(ctx, "sales", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(chatsPage.Chats) != 1 || chatsPage.Chats[0].LastPreview != "harga baru spesial" {
		t.Fatalf("last_preview chat harus ikut diperbarui: %+v", chatsPage.Chats)
	}
}

// TestRevoke_Menghapus memastikan revoke menghapus baris pesan beserta entri FTS5 dan menyinkronkan tabel chats.
func TestRevoke_Menghapus(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 50, 168*time.Hour)
	j, cs := openTestJournalAndStore(t, settings)

	chat := "628177788899@s.whatsapp.net"
	ev1 := makeMsgEvent(t, "sales", chat, chat, "WA-R-1", 1700002000, false, "Eko", "text", "pesan pertama tetap ada")
	ev2 := makeMsgEvent(t, "sales", chat, chat, "WA-R-2", 1700002010, false, "Eko", "text", "rahasia ditarik kembali")
	for _, ev := range []*journal.Event{ev1, ev2} {
		if _, err := j.Append(ctx, ev, cs.RecordHook(ev)); err != nil {
			t.Fatal(err)
		}
	}

	revPayload, _ := json.Marshal(map[string]any{
		"id":     "WA-REV-1",
		"from":   chat,
		"chat":   chat,
		"type":   "revoke",
		"revoke": map[string]any{"target_id": "WA-R-2"},
	})
	evRev := &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       chat,
		Sender:     chat,
		WAID:       "WA-REV-1",
		TS:         1700002020,
		ReceivedAt: 1700002020000,
		Payload:    revPayload,
	}
	if _, err := j.Append(ctx, evRev, cs.RecordHook(evRev)); err != nil {
		t.Fatal(err)
	}

	hits, err := cs.Search(ctx, "sales", "ditarik", "", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits.Hits) != 0 {
		t.Fatalf("pesan yang di-revoke wajib terhapus dari FTS5: %+v", hits.Hits)
	}
	msgs, err := cs.Messages(ctx, "sales", chat, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs.Messages) != 1 || msgs.Messages[0].ID != "WA-R-1" {
		t.Fatalf("hanya pesan WA-R-1 yang boleh tersisa: %+v", msgs.Messages)
	}
	chatsPage, err := cs.Chats(ctx, "sales", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(chatsPage.Chats) != 1 || chatsPage.Chats[0].LastPreview != "pesan pertama tetap ada" {
		t.Fatalf("chats harus mundur ke pesan terakhir tersisa: %+v", chatsPage.Chats)
	}

	// Revoke pesan terakhir -> baris chat ikut dihapus
	if err := cs.ApplyRevoke(ctx, "sales", chat, "WA-R-1"); err != nil {
		t.Fatal(err)
	}
	chatsAfter, err := cs.Chats(ctx, "sales", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(chatsAfter.Chats) != 0 {
		t.Fatalf("chat kosong setelah semua pesan di-revoke harus dihapus, didapat %+v", chatsAfter.Chats)
	}
}

// TestSearch_SanitasiDanRanking menguji sanitasi operator FTS5, ranking BM25, filter chat, dan mode prefix.
func TestSearch_SanitasiDanRanking(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 50, 168*time.Hour)
	j, cs := openTestJournalAndStore(t, settings)

	chatA := "628100000001@s.whatsapp.net"
	chatB := "628100000002@s.whatsapp.net"
	msgs := []struct {
		chat, id, text string
		ts             int64
	}{
		{chatA, "DOC-1", "pembayaran invoice satu kali", 1700003000},
		{chatA, "DOC-2", "invoice pembayaran invoice terkonfirmasi lunas", 1700003010},
		{chatB, "DOC-3", "konfirmasi invoice pengiriman berbeda", 1700003020},
	}
	for _, m := range msgs {
		ev := makeMsgEvent(t, "sales", m.chat, m.chat, m.id, m.ts, false, "User", "text", m.text)
		if _, err := j.Append(ctx, ev, cs.RecordHook(ev)); err != nil {
			t.Fatal(err)
		}
	}

	// Kueri operator FTS5 berbahaya tidak boleh menyebabkan error SQLite syntax
	for _, tricky := range []string{`a" OR "b`, `NEAR(invoice, lunas)`, `*`, `from:admin`, `"`} {
		if _, err := cs.Search(ctx, "sales", tricky, "", 10, false); err != nil {
			t.Fatalf("kueri FTS5 berbahaya %q gagal disanitasi: %v", tricky, err)
		}
	}

	// Dokumen dengan dua kemunculan "invoice" harus berperingkat lebih tinggi (lebih kecil di BM25 SQLite / urutan pertama)
	res, err := cs.Search(ctx, "sales", "invoice", chatA, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) != 2 || res.Hits[0].ID != "DOC-2" {
		t.Fatalf("DOC-2 (dua kemunculan invoice) harus peringkat pertama, didapat %+v", res.Hits)
	}

	// Prefix search harus mencocokkan awalan "terkonfirm" -> DOC-2
	prefRes, err := cs.Search(ctx, "sales", "terkonfirm", "", 10, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(prefRes.Hits) != 1 || prefRes.Hits[0].ID != "DOC-2" {
		t.Fatalf("prefix=true harus menemukan DOC-2, didapat %+v", prefRes.Hits)
	}
}

// TestUnread_Counter memverifikasi counter unread bertambah pada pesan masuk dan kembali 0 saat pesan keluar atau MarkChatRead.
func TestUnread_Counter(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 50, 168*time.Hour)
	j, cs := openTestJournalAndStore(t, settings)

	chat := "628188899900@s.whatsapp.net"
	ev1 := makeMsgEvent(t, "sales", chat, chat, "U-1", 1700004001, false, "Tono", "text", "halo")
	ev2 := makeMsgEvent(t, "sales", chat, chat, "U-2", 1700004002, false, "Tono", "text", "cek")
	for _, ev := range []*journal.Event{ev1, ev2} {
		if _, err := j.Append(ctx, ev, cs.RecordHook(ev)); err != nil {
			t.Fatal(err)
		}
	}
	chats, _ := cs.Chats(ctx, "sales", 10, "")
	if len(chats.Chats) != 1 || chats.Chats[0].Unread != 2 {
		t.Fatalf("unread diharapkan 2, didapat %+v", chats.Chats)
	}

	// Pesan keluar mereset unread ke 0
	if err := cs.RecordOutbound(ctx, ctxstore.OutboundMessage{
		InstanceID: "sales",
		Chat:       chat,
		WAID:       "U-OUT-1",
		TS:         1700004003,
		Type:       "text",
		Text:       "balasan",
		Origin:     "api",
	}); err != nil {
		t.Fatal(err)
	}
	chats, _ = cs.Chats(ctx, "sales", 10, "")
	if chats.Chats[0].Unread != 0 {
		t.Fatalf("unread harus 0 setelah outbound, didapat %d", chats.Chats[0].Unread)
	}

	// Pesan masuk baru -> unread=1, lalu MarkChatRead -> 0
	ev3 := makeMsgEvent(t, "sales", chat, chat, "U-3", 1700004004, false, "Tono", "text", "makasih")
	if _, err := j.Append(ctx, ev3, cs.RecordHook(ev3)); err != nil {
		t.Fatal(err)
	}
	if err := cs.MarkChatRead(ctx, "sales", chat); err != nil {
		t.Fatal(err)
	}
	chats, _ = cs.Chats(ctx, "sales", 10, "")
	if chats.Chats[0].Unread != 0 {
		t.Fatalf("unread harus 0 setelah MarkChatRead, didapat %d", chats.Chats[0].Unread)
	}
}

// TestNonaktif_409 memverifikasi ErrDisabled saat context_store mati serta tidak ada penulisan pesan saat nonaktif.
func TestNonaktif_409(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", false, 50, 168*time.Hour)
	j, cs := openTestJournalAndStore(t, settings)

	chat := "628199900011@s.whatsapp.net"
	ev := makeMsgEvent(t, "sales", chat, chat, "OFF-1", 1700005000, false, "Off", "text", "jangan disimpan")
	if hook := cs.RecordHook(ev); hook != nil {
		t.Fatalf("RecordHook seharusnya nil saat context_store nonaktif")
	}
	if _, err := cs.Chats(ctx, "sales", 10, ""); !errors.Is(err, ctxstore.ErrDisabled) {
		t.Fatalf("Chats saat mati diharapkan ErrDisabled, didapat %v", err)
	}
	if _, err := cs.Messages(ctx, "sales", chat, 10, ""); !errors.Is(err, ctxstore.ErrDisabled) {
		t.Fatalf("Messages saat mati diharapkan ErrDisabled, didapat %v", err)
	}
	if _, err := cs.Search(ctx, "sales", "halo", "", 10, false); !errors.Is(err, ctxstore.ErrDisabled) {
		t.Fatalf("Search saat mati diharapkan ErrDisabled, didapat %v", err)
	}
	if err := cs.MarkChatRead(ctx, "sales", chat); err != nil {
		t.Fatalf("MarkChatRead saat mati harus no-op, didapat %v", err)
	}
	_ = j
}

// TestPurge_HapusPerInstanceDanChat memastikan Purge menghapus satu chat saja atau seluruh data satu instance tanpa mengganggu instance lain.
func TestPurge_HapusPerInstanceDanChat(t *testing.T) {
	ctx := context.Background()
	settings := newStubSettings("sales", true, 50, 168*time.Hour)
	settings.set("support", true, 50, 168*time.Hour)
	_, cs := openTestJournalAndStore(t, settings)

	c1 := "628111000001@s.whatsapp.net"
	c2 := "628111000002@s.whatsapp.net"
	_ = cs.RecordOutbound(ctx, ctxstore.OutboundMessage{InstanceID: "sales", Chat: c1, WAID: "S1", TS: 1700000100, Type: "text", Text: "halo sales c1"})
	_ = cs.RecordOutbound(ctx, ctxstore.OutboundMessage{InstanceID: "sales", Chat: c2, WAID: "S2", TS: 1700000101, Type: "text", Text: "halo sales c2"})
	_ = cs.RecordOutbound(ctx, ctxstore.OutboundMessage{InstanceID: "support", Chat: c1, WAID: "P1", TS: 1700000102, Type: "text", Text: "halo support c1"})

	// Purge satu chat c1 di instance sales
	if err := cs.Purge(ctx, "sales", c1); err != nil {
		t.Fatalf("Purge chat c1 gagal: %v", err)
	}
	sc, _ := cs.Chats(ctx, "sales", 10, "")
	if len(sc.Chats) != 1 || sc.Chats[0].Chat != c2 {
		t.Fatalf("hanya chat c2 yang boleh tersisa di sales: %+v", sc.Chats)
	}

	// Purge seluruh instance sales -> support tetap utuh
	if err := cs.Purge(ctx, "sales", ""); err != nil {
		t.Fatalf("Purge instance sales gagal: %v", err)
	}
	scAll, _ := cs.Chats(ctx, "sales", 10, "")
	if len(scAll.Chats) != 0 {
		t.Fatalf("semua chat sales seharusnya kosong: %+v", scAll.Chats)
	}
	sup, _ := cs.Chats(ctx, "support", 10, "")
	if len(sup.Chats) != 1 || sup.Chats[0].Chat != c1 {
		t.Fatalf("data instance support tidak boleh ikut terhapus: %+v", sup.Chats)
	}
}

// BenchmarkSearch_10k mengukur latensi pencarian FTS5 dan pertumbuhan journal.db per pesan pada 10.000 pesan.
func BenchmarkSearch_10k(b *testing.B) {
	ctx := context.Background()
	dbPath := filepath.Join(b.TempDir(), "journal.db")
	j, err := journal.Open(ctx, dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()

	settings := newStubSettings("sales", true, 20000, 168*time.Hour)
	cs := ctxstore.New(j.DB(), settings)

	beforeBytes, err := sqliteDBDiskBytes(ctx, j.DB(), dbPath)
	if err != nil {
		b.Fatal(err)
	}

	const total = 10000
	baseTS := int64(1700000000)
	for i := 0; i < total; i++ {
		chat := fmt.Sprintf("6281%08d@s.whatsapp.net", i%50)
		text := fmt.Sprintf("pesan diskusi produk seri %d kategori umum", i)
		if i%25 == 0 {
			text = fmt.Sprintf("konfirmasi tagihan faktur nomor %d sudah dibayar", i)
		}
		if err := cs.RecordOutbound(ctx, ctxstore.OutboundMessage{
			InstanceID: "sales",
			Chat:       chat,
			WAID:       fmt.Sprintf("BM-%05d", i),
			TS:         baseTS + int64(i),
			Type:       "text",
			Text:       text,
		}); err != nil {
			b.Fatal(err)
		}
	}
	afterBytes, err := sqliteDBDiskBytes(ctx, j.DB(), dbPath)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := cs.Search(ctx, "sales", "tagihan dibayar", "", 20, false)
		if err != nil || len(res.Hits) == 0 {
			b.Fatalf("Search 10k gagal: len=%d err=%v", len(res.Hits), err)
		}
	}
	b.StopTimer()
	growthPerMsg := float64(afterBytes-beforeBytes) / float64(total)
	b.ReportMetric(growthPerMsg, "bytes/msg")
	b.Logf("pertumbuhan journal.db untuk %d pesan: before=%d after=%d delta=%d (%.1f bytes/pesan)", total, beforeBytes, afterBytes, afterBytes-beforeBytes, growthPerMsg)
}

func sqliteDBDiskBytes(ctx context.Context, db *sql.DB, dbPath string) (int64, error) {
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return 0, err
	}
	var total int64
	for _, p := range []string{dbPath, dbPath + "-wal"} {
		if info, err := os.Stat(p); err == nil {
			total += info.Size()
		}
	}
	var pages, pageSize int64
	if err := db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err == nil {
		if err := db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err == nil && pages*pageSize > total {
			total = pages * pageSize
		}
	}
	return total, nil
}
