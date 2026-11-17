package control

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// TestControl_ContextEndpointsDanAudit memverifikasi /control/instances/{id}/context, /context/purge, sinkronisasi settings, dan pencatatan audit_log.
func TestControl_ContextEndpointsDanAudit(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, cfg.AppDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	j, err := journal.Open(ctx, cfg.JournalDBPath(), store.DBOptions{MaxConns: 2, CacheKiB: 512, TxLock: "immediate"})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	cs := ctxstore.New(j.DB(), ctxstore.NewStoreSettings(st))
	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j), wa.WithContextStore(cs))
	if err != nil {
		t.Fatal(err)
	}
	defer waMgr.Close()
	if _, err := waMgr.Create(ctx, "sales", 60, false); err != nil {
		t.Fatal(err)
	}

	audit := auth.NewAuditLog(st.DB)
	srv := NewServer(cfg, st, authMgr, waMgr, nil, WithJournal(j), WithContextStore(cs), WithAuditLog(audit))
	h := srv.Handler()

	// 1. Aktifkan context store lewat control plane
	recOn := httptest.NewRecorder()
	h.ServeHTTP(recOn, httptest.NewRequest(http.MethodPost, "/control/instances/sales/context", strings.NewReader(`{"enabled":true}`)))
	if recOn.Code != http.StatusOK {
		t.Fatalf("POST /context enable diharapkan 200, didapat %d (%s)", recOn.Code, recOn.Body.String())
	}
	row, _ := st.GetInstance(ctx, "sales")
	if !row.Settings.ContextStore {
		t.Fatal("InstanceSettings.ContextStore seharusnya bernilai true")
	}

	// Masukkan data dummy ke dua chat berbeda
	_ = cs.RecordOutbound(ctx, ctxstore.OutboundMessage{InstanceID: "sales", Chat: "6281110001@s.whatsapp.net", WAID: "M1", TS: 1700000100, Type: "text", Text: "chat satu"})
	_ = cs.RecordOutbound(ctx, ctxstore.OutboundMessage{InstanceID: "sales", Chat: "6282220002@s.whatsapp.net", WAID: "M2", TS: 1700000101, Type: "text", Text: "chat dua"})

	// 2. Purge spesifik satu chat
	recPurgeChat := httptest.NewRecorder()
	h.ServeHTTP(recPurgeChat, httptest.NewRequest(http.MethodPost, "/control/instances/sales/context/purge", strings.NewReader(`{"chat":"6281110001"}`)))
	if recPurgeChat.Code != http.StatusOK {
		t.Fatalf("POST /context/purge diharapkan 200, didapat %d (%s)", recPurgeChat.Code, recPurgeChat.Body.String())
	}
	chatsLeft, _ := cs.Chats(ctx, "sales", 10, "")
	if len(chatsLeft.Chats) != 1 || chatsLeft.Chats[0].Chat != "6282220002@s.whatsapp.net" {
		t.Fatalf("chat 6281110001 seharusnya terhapus, sisa: %+v", chatsLeft.Chats)
	}

	// 3. Matikan dengan purge -> seluruh sisa baris instance terhapus
	recOff := httptest.NewRecorder()
	h.ServeHTTP(recOff, httptest.NewRequest(http.MethodPost, "/control/instances/sales/context", strings.NewReader(`{"enabled":false,"purge":true}`)))
	if recOff.Code != http.StatusOK {
		t.Fatalf("POST /context off --purge diharapkan 200, didapat %d (%s)", recOff.Code, recOff.Body.String())
	}
	var rem int
	_ = j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE instance_id = 'sales'`).Scan(&rem)
	if rem != 0 {
		t.Fatalf("semua pesan sales harus terhapus pasca off --purge, sisa %d", rem)
	}

	entries, err := audit.List(ctx, auth.AuditQuery{ActionPattern: "context.*"})
	if err != nil {
		t.Fatal(err)
	}
	var gotActions []string
	for _, e := range entries {
		gotActions = append(gotActions, e.Action)
	}
	rawActions, _ := json.Marshal(gotActions)
	if len(entries) < 3 {
		t.Fatalf("audit context.* diharapkan >= 3 entri (enable, purge, disable), didapat %s", string(rawActions))
	}
}
