package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

func setupV2ContextServer(t *testing.T, enabled bool) (*Server, *wa.Manager, *journal.Journal, *ctxstore.Store, *store.Store, string) {
	t.Helper()
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 2, TxLock: "immediate"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })

	cs := ctxstore.New(j.DB(), ctxstore.NewStoreSettings(st))
	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j), wa.WithContextStore(cs))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })

	if _, err := waMgr.Create(ctx, "sales", 60, false); err != nil {
		t.Fatal(err)
	}
	if enabled {
		row, _ := st.GetInstance(ctx, "sales")
		s := row.Settings
		s.ContextStore = true
		if _, err := waMgr.UpdateSettings(ctx, "sales", s); err != nil {
			t.Fatal(err)
		}
	}

	key, _, err := authMgr.Create(ctx, "ctx-test", "")
	if err != nil {
		t.Fatal(err)
	}
	drv := newFakeChatDriver()
	srv := NewServer(authMgr, waMgr,
		WithDB(st.DB),
		WithJournal(j),
		WithContextStore(cs),
		withChatDriver(drv),
	)
	return srv, waMgr, j, cs, st, key
}

// TestV2ContextAPI_EndpointsDan409 memverifikasi endpoint /chats, /chats/{jid}/messages, /search, 409 saat nonaktif, dan reset unread via /read.
func TestV2ContextAPI_EndpointsDan409(t *testing.T) {
	ctx := context.Background()

	// 1. Saat mati -> /chats, /messages, dan /search wajib 409 context_store_disabled, sedangkan /read tetap 204
	srvOff, _, _, _, _, keyOff := setupV2ContextServer(t, false)
	for _, path := range []string{
		"/v2/instances/sales/chats",
		"/v2/instances/sales/chats/628111111111/messages",
		"/v2/instances/sales/search?q=halo",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-API-Key", keyOff)
		rec := httptest.NewRecorder()
		srvOff.ServeHTTP(rec, req)
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "context_store_disabled") {
			t.Fatalf("%s saat mati diharapkan 409 context_store_disabled, didapat %d (%s)", path, rec.Code, rec.Body.String())
		}
	}
	rReadOff := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/chats/628111111111/read", strings.NewReader(`{"message_ids":["M1"]}`))
	rReadOff.Header.Set("X-API-Key", keyOff)
	wReadOff := httptest.NewRecorder()
	srvOff.ServeHTTP(wReadOff, rReadOff)
	if wReadOff.Code != http.StatusNoContent {
		t.Fatalf("POST /read saat context_store mati harus tetap 204, didapat %d (%s)", wReadOff.Code, wReadOff.Body.String())
	}

	// 2. Saat aktif -> uji paginasi, search FTS5, dan reset unread saat POST /read
	srvOn, _, jOn, csOn, _, keyOn := setupV2ContextServer(t, true)
	chat := "628123456789@s.whatsapp.net"
	for _, item := range []struct {
		id   string
		ts   int64
		text string
	}{
		{"M1", 1700000100, "halo selamat pagi"},
		{"M2", 1700000101, "konfirmasi pembayaran invoice"},
		{"M3", 1700000101, "invoice sudah diverifikasi tim"},
	} {
		payload, _ := json.Marshal(map[string]any{
			"id": item.id, "from": chat, "chat": chat, "chat_type": "dm",
			"is_from_me": false, "push_name": "Sari", "type": "text", "text": item.text,
		})
		ev := &journal.Event{
			InstanceID: "sales", Type: "message", Chat: chat, Sender: chat, WAID: item.id,
			DedupeKey: "m:" + chat + ":" + item.id, TS: item.ts, ReceivedAt: item.ts * 1000, Payload: payload,
		}
		if _, err := jOn.Append(ctx, ev, csOn.RecordHook(ev)); err != nil {
			t.Fatal(err)
		}
	}

	// Cek GET /chats
	rChats := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/chats?limit=10", nil)
	rChats.Header.Set("X-API-Key", keyOn)
	wChats := httptest.NewRecorder()
	srvOn.ServeHTTP(wChats, rChats)
	if wChats.Code != http.StatusOK {
		t.Fatalf("GET /chats diharapkan 200, didapat %d (%s)", wChats.Code, wChats.Body.String())
	}
	var chatsBody struct {
		Chats      []ctxstore.ChatItem `json:"chats"`
		NextBefore *string             `json:"next_before"`
	}
	if err := json.Unmarshal(wChats.Body.Bytes(), &chatsBody); err != nil {
		t.Fatal(err)
	}
	if len(chatsBody.Chats) != 1 || chatsBody.Chats[0].Unread != 3 || chatsBody.Chats[0].ChatType != "dm" || chatsBody.Chats[0].Name != "Sari" {
		t.Fatalf("GET /chats payload tidak sesuai: %+v", chatsBody)
	}

	// Cek GET /chats/{jid}/messages dengan limit=2 (harus dapat M2, M3 ascending + next_before)
	rMsgs := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/chats/628123456789/messages?limit=2", nil)
	rMsgs.Header.Set("X-API-Key", keyOn)
	wMsgs := httptest.NewRecorder()
	srvOn.ServeHTTP(wMsgs, rMsgs)
	if wMsgs.Code != http.StatusOK {
		t.Fatalf("GET /messages diharapkan 200, didapat %d (%s)", wMsgs.Code, wMsgs.Body.String())
	}
	var msgsBody ctxstore.MessagesPage
	if err := json.Unmarshal(wMsgs.Body.Bytes(), &msgsBody); err != nil {
		t.Fatal(err)
	}
	if len(msgsBody.Messages) != 2 || msgsBody.Messages[0].ID != "M2" || msgsBody.Messages[1].ID != "M3" || msgsBody.NextBefore == nil {
		t.Fatalf("GET /messages halaman 1 tidak sesuai: %+v", msgsBody)
	}

	// Halaman kedua pakai next_before
	rMsgs2 := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/chats/628123456789/messages?limit=2&before="+*msgsBody.NextBefore, nil)
	rMsgs2.Header.Set("X-API-Key", keyOn)
	wMsgs2 := httptest.NewRecorder()
	srvOn.ServeHTTP(wMsgs2, rMsgs2)
	var msgsBody2 ctxstore.MessagesPage
	_ = json.Unmarshal(wMsgs2.Body.Bytes(), &msgsBody2)
	if len(msgsBody2.Messages) != 1 || msgsBody2.Messages[0].ID != "M1" || msgsBody2.NextBefore != nil {
		t.Fatalf("GET /messages halaman 2 tidak sesuai: %+v", msgsBody2)
	}

	// Cek GET /search
	rSearch := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/search?q=diverifi&prefix=true", nil)
	rSearch.Header.Set("X-API-Key", keyOn)
	wSearch := httptest.NewRecorder()
	srvOn.ServeHTTP(wSearch, rSearch)
	if wSearch.Code != http.StatusOK {
		t.Fatalf("GET /search diharapkan 200, didapat %d (%s)", wSearch.Code, wSearch.Body.String())
	}
	var sBody ctxstore.SearchPage
	_ = json.Unmarshal(wSearch.Body.Bytes(), &sBody)
	if len(sBody.Hits) != 1 || sBody.Hits[0].ID != "M3" {
		t.Fatalf("GET /search prefix=true harus mengembalikan M3: %+v", sBody.Hits)
	}

	// POST /read mereset unread ke 0
	rReadOn := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/chats/628123456789/read", strings.NewReader(`{"message_ids":["M3"]}`))
	rReadOn.Header.Set("X-API-Key", keyOn)
	wReadOn := httptest.NewRecorder()
	srvOn.ServeHTTP(wReadOn, rReadOn)
	if wReadOn.Code != http.StatusNoContent {
		t.Fatalf("POST /read diharapkan 204, didapat %d", wReadOn.Code)
	}
	rChatsAfter := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/chats", nil)
	rChatsAfter.Header.Set("X-API-Key", keyOn)
	wChatsAfter := httptest.NewRecorder()
	srvOn.ServeHTTP(wChatsAfter, rChatsAfter)
	_ = json.Unmarshal(wChatsAfter.Body.Bytes(), &chatsBody)
	if chatsBody.Chats[0].Unread != 0 {
		t.Fatalf("unread harus reset ke 0 pasca POST /read, didapat %d", chatsBody.Chats[0].Unread)
	}
}
