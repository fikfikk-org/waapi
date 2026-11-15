package wa

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/webhook"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func openInstanceTestEnv(t *testing.T, wh *webhook.Dispatcher, tc *webhook.TargetCache) (*store.Store, *journal.Journal, *Manager, *Instance) {
	t.Helper()
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs gagal: %v", err)
	}

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: 60 * time.Second,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	var opts []ManagerOption
	opts = append(opts, WithJournal(j))
	if tc != nil {
		opts = append(opts, WithTargetCache(tc))
	}

	mgr, err := NewManager(ctx, cfg, st, wh, opts...)
	if err != nil {
		t.Fatalf("NewManager gagal: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	if _, err := mgr.Create(ctx, "sales", 20, false); err != nil {
		t.Fatalf("mgr.Create gagal: %v", err)
	}
	inst, ok := mgr.Get("sales")
	if !ok {
		t.Fatal("instance sales tidak ditemukan di Manager")
	}
	return st, j, mgr, inst
}

func sampleTextMsgEvent(id string) *events.Message {
	pn := types.NewJID("6281234567890", types.DefaultUserServer)
	return &events.Message{
		Info: types.MessageInfo{
			ID:        id,
			Timestamp: time.Unix(1794360100, 0),
			MessageSource: types.MessageSource{
				Chat:   pn,
				Sender: pn,
			},
		},
		Message: &waE2E.Message{Conversation: proto.String("halo " + id)},
	}
}

// TestHandler_ObserverDalamTransaksi memastikan error pada TxHook InboundObserver membatalkan penyimpanan journal dan mengembalikan false.
func TestHandler_ObserverDalamTransaksi(t *testing.T) {
	_, j, mgr, inst := openInstanceTestEnv(t, nil, nil)

	errObs := errors.New("observer menolak transaksi")
	mgr.AddInboundObserver(func(ctx context.Context, ev *journal.Event, raw any) journal.TxHook {
		return func(ctx context.Context, tx *sql.Tx, seq int64) error {
			return errObs
		}
	})

	ok := inst.handleEventV2(sampleTextMsgEvent("OBS-1"))
	if ok {
		t.Fatal("handleEventV2 harus mengembalikan false ketika TxHook observer mengembalikan error")
	}

	var count int
	if err := j.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("event tidak boleh tersimpan jika TxHook gagal, didapat %d baris", count)
	}
}

// TestHandler_FalseSaatJournalGagal memastikan handler mengembalikan false untuk Message/Receipt/Undecryptable dan true untuk Connected saat journal error.
func TestHandler_FalseSaatJournalGagal(t *testing.T) {
	_, j, _, inst := openInstanceTestEnv(t, nil, nil)

	// Tutup journal DB untuk mensimulasikan kegagalan penulisan journal
	_ = j.Close()

	if inst.handleEventV2(sampleTextMsgEvent("FAIL-1")) {
		t.Fatal("Message harus mengembalikan false saat journal gagal")
	}
	if inst.handleEventV2(&events.Receipt{
		MessageSource: types.MessageSource{
			Chat:   types.NewJID("6281234567890", types.DefaultUserServer),
			Sender: types.NewJID("6281234567890", types.DefaultUserServer),
		},
		MessageIDs: []types.MessageID{"FAIL-R1"},
		Timestamp:  time.Unix(1794360100, 0),
		Type:       types.ReceiptTypeRead,
	}) {
		t.Fatal("Receipt harus mengembalikan false saat journal gagal")
	}
	if inst.handleEventV2(&events.UndecryptableMessage{
		Info: types.MessageInfo{
			ID:        "FAIL-U1",
			Timestamp: time.Unix(1794360100, 0),
			MessageSource: types.MessageSource{
				Chat:   types.NewJID("6281234567890", types.DefaultUserServer),
				Sender: types.NewJID("6281234567890", types.DefaultUserServer),
			},
		},
	}) {
		t.Fatal("UndecryptableMessage harus mengembalikan false saat journal gagal")
	}
	if !inst.handleEventV2(&events.Connected{}) {
		t.Fatal("Connected harus tetap mengembalikan true meski journal gagal")
	}
}

// TestHandler_FlagOfflineSync memastikan OfflineSyncPreview menandai FlagOffline pada pesan berikutnya sampai OfflineSyncCompleted.
func TestHandler_FlagOfflineSync(t *testing.T) {
	_, j, _, inst := openInstanceTestEnv(t, nil, nil)

	if !inst.handleEventV2(&events.OfflineSyncPreview{Total: 1}) {
		t.Fatal("OfflineSyncPreview harus mengembalikan true")
	}
	if !inst.handleEventV2(sampleTextMsgEvent("SYNC-OFFLINE")) {
		t.Fatal("pesan saat offline sync harus mengembalikan true")
	}
	if !inst.handleEventV2(&events.OfflineSyncCompleted{Count: 1}) {
		t.Fatal("OfflineSyncCompleted harus mengembalikan true")
	}
	if !inst.handleEventV2(sampleTextMsgEvent("SYNC-LIVE")) {
		t.Fatal("pesan setelah offline sync selesai harus mengembalikan true")
	}

	res, err := j.Read(context.Background(), journal.Query{InstanceID: "sales", Limit: 10})
	if err != nil {
		t.Fatalf("Read journal gagal: %v", err)
	}
	if len(res.Events) != 2 {
		t.Fatalf("diharapkan 2 event tersimpan, didapat %d", len(res.Events))
	}
	if res.Events[0].Flags&journal.FlagOffline == 0 {
		t.Fatal("pesan pertama selama offline sync harus memiliki FlagOffline")
	}
	if res.Events[1].Flags&journal.FlagOffline != 0 {
		t.Fatal("pesan kedua setelah OfflineSyncCompleted tidak boleh memiliki FlagOffline")
	}
}

// TestHandler_TanpaQueryDBDiHotPath memastikan 100 event masuk tidak memanggil GetInstance maupun melakukan query app.db.
func TestHandler_TanpaQueryDBDiHotPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var getInstanceCalls atomic.Int64
	tc := webhook.NewTargetCache()
	tc.Set("sales", webhook.Target{
		URL:       "http://127.0.0.1:9/unused",
		Secret:    "sec",
		EventsCSV: "message,receipt",
		Format:    "waapi",
	})

	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	wh := webhook.NewDispatcher(st, func(ctx context.Context, instanceID string) (string, string, string, error) {
		getInstanceCalls.Add(1)
		return tc.Resolve(instanceID)
	}, 1)
	wh.SetTargetCache(tc)

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: 60 * time.Second,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	mgr, err := NewManager(ctx, cfg, st, wh, WithJournal(j), WithTargetCache(tc))
	if err != nil {
		t.Fatalf("NewManager gagal: %v", err)
	}
	defer mgr.Close()

	if _, err := mgr.Create(ctx, "sales", 20, false); err != nil {
		t.Fatalf("mgr.Create gagal: %v", err)
	}
	inst, _ := mgr.Get("sales")

	// Tutup app.db sebelum memproses 100 event: setiap query ke app.db akan terbukti langsung jika sampai menyentuh store
	_ = st.Close()

	for k := 0; k < 100; k++ {
		if !inst.handleEventV2(sampleTextMsgEvent(fmt.Sprintf("HOT-%d", k))) {
			t.Fatalf("handleEventV2 ke-%d gagal", k)
		}
	}

	if calls := getInstanceCalls.Load(); calls != 0 {
		t.Fatalf("GetInstance fallback tidak boleh dipanggil di hot path, dipanggil %d kali", calls)
	}

	res, err := j.Read(ctx, journal.Query{InstanceID: "sales", Limit: 200})
	if err != nil || len(res.Events) != 100 {
		t.Fatalf("diharapkan 100 event di journal, didapat %d, err=%v", len(res.Events), err)
	}
}
