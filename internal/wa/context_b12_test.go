package wa

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func openWAContextEnv(t *testing.T, enabled bool) (*store.Store, *journal.Journal, *ctxstore.Store, *Manager, *Instance) {
	t.Helper()
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 2, CacheKiB: 512, TxLock: "immediate"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })

	cs := ctxstore.New(j.DB(), ctxstore.NewStoreSettings(st))
	mgr, err := NewManager(ctx, cfg, st, nil, WithJournal(j), WithContextStore(cs))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	if _, err := mgr.Create(ctx, "sales", 20, false); err != nil {
		t.Fatal(err)
	}
	if enabled {
		row, _ := st.GetInstance(ctx, "sales")
		s := row.Settings
		s.ContextStore = true
		if _, err := mgr.UpdateSettings(ctx, "sales", s); err != nil {
			t.Fatal(err)
		}
	}
	inst, _ := mgr.Get("sales")
	return st, j, cs, mgr, inst
}

// TestInstance_RecordHookDenganJournal memastikan handleEventV2 hanya menulis ke context store ketika context_store aktif.
func TestInstance_RecordHookDenganJournal(t *testing.T) {
	ctx := context.Background()

	// 1. Nonaktif -> hanya masuk journal, tidak ada baris di context store
	_, jOff, csOff, _, instOff := openWAContextEnv(t, false)
	if !instOff.handleEventV2(sampleTextMsgEvent("CTX-OFF-1")) {
		t.Fatal("handleEventV2 seharusnya sukses saat context_store mati")
	}
	resOff, _ := jOff.Read(ctx, journal.Query{InstanceID: "sales", Limit: 10})
	if len(resOff.Events) != 1 {
		t.Fatalf("event journal diharapkan 1, didapat %d", len(resOff.Events))
	}
	var countOff int
	_ = jOff.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&countOff)
	if countOff != 0 {
		t.Fatalf("tidak boleh ada pesan di context store saat nonaktif, didapat %d", countOff)
	}
	_ = csOff

	// 2. Aktif -> masuk journal sekaligus context store secara atomik
	_, _, csOn, _, instOn := openWAContextEnv(t, true)
	if !instOn.handleEventV2(sampleTextMsgEvent("CTX-ON-1")) {
		t.Fatal("handleEventV2 gagal saat context_store aktif")
	}
	// Kirim juga status@broadcast -> harus diabaikan context store meski tetap masuk journal
	statusEvt := &events.Message{
		Info: types.MessageInfo{
			ID:        "CTX-STAT-1",
			Timestamp: time.Unix(1794360200, 0),
			MessageSource: types.MessageSource{
				Chat:   types.StatusBroadcastJID,
				Sender: types.NewJID("628111", types.DefaultUserServer),
			},
		},
		Message: &waE2E.Message{Conversation: proto.String("status story")},
	}
	_ = instOn.handleEventV2(statusEvt)

	page, err := csOn.Messages(ctx, "sales", "6281234567890@s.whatsapp.net", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != "CTX-ON-1" {
		t.Fatalf("pesan context store tidak sesuai: %+v", page.Messages)
	}
	chats, _ := csOn.Chats(ctx, "sales", 10, "")
	if len(chats.Chats) != 1 {
		t.Fatalf("hanya 1 chat DM yang boleh tercatat (status@broadcast diabaikan): %+v", chats.Chats)
	}
}

// TestCompose_QuotedDariContextStoreLebihDulu memverifikasi Quoted membaca context store sebelum fallback ke journal.
func TestCompose_QuotedDariContextStoreLebihDulu(t *testing.T) {
	ctx := context.Background()
	_, j, cs, mgr, _ := openWAContextEnv(t, true)

	chat := "6281234567890@s.whatsapp.net"
	// Masukkan pesan ke context store dan ke journal dengan teks berbeda agar prioritas context store teruji nyata
	if err := cs.RecordOutbound(ctx, ctxstore.OutboundMessage{
		InstanceID: "sales",
		Chat:       chat,
		WAID:       "Q-CTX-1",
		Sender:     "6281999@s.whatsapp.net",
		TS:         1700000100,
		Type:       "text",
		Text:       "teks dari context store",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       chat,
		Sender:     "6281000@s.whatsapp.net",
		WAID:       "Q-JRN-ONLY",
		TS:         1700000200,
		ReceivedAt: 1700000200000,
		Payload:    []byte(`{"id":"Q-JRN-ONLY","from":"6281000@s.whatsapp.net","chat":"6281234567890@s.whatsapp.net","is_from_me":false,"type":"text","text":"hanya dari journal"}`),
	}); err != nil {
		t.Fatal(err)
	}

	deps := &realComposeDeps{mgr: mgr, instanceID: "sales"}
	q1, ok1 := deps.Quoted(ctx, chat, "Q-CTX-1")
	if !ok1 || q1.Sender != "6281999@s.whatsapp.net" || q1.Message.GetConversation() != "teks dari context store" {
		t.Fatalf("Quoted harus memprioritaskan context store: ok=%v info=%+v", ok1, q1)
	}

	// Fallback ke journal saat ID tidak ada di context store
	q2, ok2 := deps.Quoted(ctx, chat, "Q-JRN-ONLY")
	if !ok2 || q2.Sender != "6281000@s.whatsapp.net" || q2.Message.GetConversation() != "hanya dari journal" {
		t.Fatalf("Quoted harus fallback ke journal saat tidak ada di context store: ok=%v info=%+v", ok2, q2)
	}
}
