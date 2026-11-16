package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"go.mau.fi/whatsmeow/types"
)

type fakeChatDriver struct {
	mu           sync.Mutex
	readCalls    []fakeReadCall
	presCalls    []types.Presence
	chatPres     []fakeChatPresCall
	subCalls     []types.JID
	subs         map[string]struct{}
	maxSubscribe int
}

type fakeReadCall struct {
	chat   types.JID
	sender types.JID
	ids    []types.MessageID
	played bool
}

type fakeChatPresCall struct {
	chat  types.JID
	state types.ChatPresence
	media types.ChatPresenceMedia
}

func newFakeChatDriver() *fakeChatDriver {
	return &fakeChatDriver{
		subs:         make(map[string]struct{}),
		maxSubscribe: 100,
	}
}

func (f *fakeChatDriver) MarkRead(_ context.Context, _ string, chat, sender types.JID, ids []types.MessageID, played bool) error {
	f.mu.Lock()
	f.readCalls = append(f.readCalls, fakeReadCall{chat: chat, sender: sender, ids: ids, played: played})
	f.mu.Unlock()
	return nil
}

func (f *fakeChatDriver) SendPresence(_ context.Context, _ string, state types.Presence) error {
	f.mu.Lock()
	f.presCalls = append(f.presCalls, state)
	f.mu.Unlock()
	return nil
}

func (f *fakeChatDriver) SendChatPresence(_ context.Context, _ string, chat types.JID, state types.ChatPresence, media types.ChatPresenceMedia) error {
	f.mu.Lock()
	f.chatPres = append(f.chatPres, fakeChatPresCall{chat: chat, state: state, media: media})
	f.mu.Unlock()
	return nil
}

func (f *fakeChatDriver) SubscribePresence(_ context.Context, _ string, target types.JID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := target.ToNonAD().String()
	if _, ok := f.subs[key]; !ok {
		if len(f.subs) >= f.maxSubscribe {
			return wa.ErrPresenceSubLimit
		}
		f.subs[key] = struct{}{}
	}
	f.subCalls = append(f.subCalls, target)
	return nil
}

func setupV2ChatServer(t *testing.T, drv *fakeChatDriver) (http.Handler, *journal.Journal, *wa.PollStore, string) {
	t.Helper()
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	authMgr, _ := auth.NewManager(ctx, st)
	waMgr, err := wa.NewManager(ctx, cfg, st, nil)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })
	_, _ = waMgr.Create(ctx, "sales", 60, false)

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 2})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	ps := wa.NewPollStore(j.DB())
	key, _, err := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{
		Name:       "chat-test",
		Scopes:     auth.ScopeAll,
		RatePerSec: 1000,
	})
	if err != nil {
		t.Fatalf("CreateWithOptions gagal: %v", err)
	}
	h := NewHandler(authMgr, waMgr,
		WithDB(st.DB),
		WithJournal(j),
		WithPolls(ps),
		withChatDriver(drv),
	)
	return h, j, ps, key
}

// TestPollTallyAPI memverifikasi endpoint GET /polls/{wa_id} menghitung tally suara, memfilter unvote dari total_voters, dan mewajibkan ?chat=.
func TestPollTallyAPI(t *testing.T) {
	ctx := context.Background()
	h, j, ps, key := setupV2ChatServer(t, newFakeChatDriver())

	const chat = "120363012345@g.us"
	const pollID = "POLL-API-1"
	if err := ps.SavePoll(ctx, wa.Poll{
		InstanceID: "sales",
		Chat:       chat,
		WAID:       pollID,
		Creator:    "628000000001@s.whatsapp.net",
		Name:       "Pilih lokasi?",
		Options:    []string{"Jakarta", "Bandung"},
		Selectable: 1,
		CreatedAt:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("SavePoll gagal: %v", err)
	}

	hJkt := sha256.Sum256([]byte("Jakarta"))
	hBdg := sha256.Sum256([]byte("Bandung"))

	// Pemilih 1 -> Jakarta, Pemilih 2 -> Bandung, Pemilih 3 -> sempat memilih lalu batal ([]).
	_, _ = ps.RecordVoteAndAppend(ctx, j, &journal.Event{
		InstanceID: "sales", Type: "poll_vote", Chat: chat, Sender: "628111111111@s.whatsapp.net", WAID: "V1", DedupeKey: "pv:1", TS: 100, ReceivedAt: 100000,
	}, pollID, "628111111111@s.whatsapp.net", [][]byte{hJkt[:]}, nil)
	_, _ = ps.RecordVoteAndAppend(ctx, j, &journal.Event{
		InstanceID: "sales", Type: "poll_vote", Chat: chat, Sender: "628222222222@s.whatsapp.net", WAID: "V2", DedupeKey: "pv:2", TS: 101, ReceivedAt: 101000,
	}, pollID, "628222222222@s.whatsapp.net", [][]byte{hBdg[:]}, nil)
	_, _ = ps.RecordVoteAndAppend(ctx, j, &journal.Event{
		InstanceID: "sales", Type: "poll_vote", Chat: chat, Sender: "628333333333@s.whatsapp.net", WAID: "V3", DedupeKey: "pv:3", TS: 102, ReceivedAt: 102000,
	}, pollID, "628333333333@s.whatsapp.net", [][]byte{hJkt[:]}, nil)
	_, _ = ps.RecordVoteAndAppend(ctx, j, &journal.Event{
		InstanceID: "sales", Type: "poll_vote", Chat: chat, Sender: "628333333333@s.whatsapp.net", WAID: "V4", DedupeKey: "pv:4", TS: 103, ReceivedAt: 103000,
	}, pollID, "628333333333@s.whatsapp.net", nil, nil)

	// Tanpa query ?chat= -> 400 invalid_request.
	rNoChat := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/polls/"+pollID, nil)
	rNoChat.Header.Set("X-API-Key", key)
	wNoChat := httptest.NewRecorder()
	h.ServeHTTP(wNoChat, rNoChat)
	if wNoChat.Code != http.StatusBadRequest {
		t.Fatalf("tanpa ?chat= harus 400, didapat %d %s", wNoChat.Code, wNoChat.Body.String())
	}

	// Polling tidak ditemukan -> 404 message_not_found.
	r404 := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/polls/POLL-NONE?chat="+chat, nil)
	r404.Header.Set("X-API-Key", key)
	w404 := httptest.NewRecorder()
	h.ServeHTTP(w404, r404)
	if w404.Code != http.StatusNotFound || !strings.Contains(w404.Body.String(), "message_not_found") {
		t.Fatalf("polling hilang harus 404 message_not_found, didapat %d %s", w404.Code, w404.Body.String())
	}

	// Tally valid.
	rOK := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/polls/"+pollID+"?chat="+chat, nil)
	rOK.Header.Set("X-API-Key", key)
	wOK := httptest.NewRecorder()
	h.ServeHTTP(wOK, rOK)
	if wOK.Code != http.StatusOK {
		t.Fatalf("GET tally gagal: %d %s", wOK.Code, wOK.Body.String())
	}
	var res struct {
		PollID      string `json:"poll_id"`
		Chat        string `json:"chat"`
		Name        string `json:"name"`
		Selectable  int    `json:"selectable"`
		TotalVoters int    `json:"total_voters"`
		Options     []struct {
			Name   string   `json:"name"`
			Votes  int      `json:"votes"`
			Voters []string `json:"voters"`
		} `json:"options"`
	}
	if err := json.Unmarshal(wOK.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode tally gagal: %v", err)
	}
	if res.PollID != pollID || res.TotalVoters != 2 || len(res.Options) != 2 ||
		res.Options[0].Votes != 1 || res.Options[1].Votes != 1 {
		t.Fatalf("hasil tally tidak sesuai (unvote tidak boleh dihitung): %+v", res)
	}
}

// TestMarkReadAPI memastikan endpoint /chats/{jid}/read meneruskan argumen ke client dengan batas maksimal 50 ID pesan.
func TestMarkReadAPI(t *testing.T) {
	drv := newFakeChatDriver()
	h, _, _, key := setupV2ChatServer(t, drv)

	// > 50 ID harus ditolak 400.
	ids := make([]string, 51)
	for i := range ids {
		ids[i] = fmt.Sprintf("M%d", i)
	}
	b51, _ := json.Marshal(map[string]any{"message_ids": ids})
	rBad := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/chats/628123456789/read", strings.NewReader(string(b51)))
	rBad.Header.Set("X-API-Key", key)
	wBad := httptest.NewRecorder()
	h.ServeHTTP(wBad, rBad)
	if wBad.Code != http.StatusBadRequest {
		t.Fatalf("> 50 message_ids harus ditolak 400, didapat %d %s", wBad.Code, wBad.Body.String())
	}

	// Valid dengan played=true pada chat grup + sender -> 204.
	rOK := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/chats/120363012345@g.us/read", strings.NewReader(`{"message_ids":["M1","M2"],"sender":"628111111111@s.whatsapp.net","played":true}`))
	rOK.Header.Set("X-API-Key", key)
	wOK := httptest.NewRecorder()
	h.ServeHTTP(wOK, rOK)
	if wOK.Code != http.StatusNoContent {
		t.Fatalf("POST /read diharapkan 204, didapat %d %s", wOK.Code, wOK.Body.String())
	}
	if len(drv.readCalls) != 1 || len(drv.readCalls[0].ids) != 2 || !drv.readCalls[0].played || drv.readCalls[0].sender.User != "628111111111" {
		t.Fatalf("argumen MarkRead ke driver tidak sesuai: %+v", drv.readCalls)
	}
}

// TestPresenceSubscribeBatas memverifikasi endpoint presence mengembalikan 204 dan batas subscribe 100 JID menolak JID ke-101 dengan 400 invalid_request.
func TestPresenceSubscribeBatas(t *testing.T) {
	drv := newFakeChatDriver()
	h, _, _, key := setupV2ChatServer(t, drv)

	// Uji POST /presence -> 204
	rPres := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/presence", strings.NewReader(`{"state":"available"}`))
	rPres.Header.Set("X-API-Key", key)
	wPres := httptest.NewRecorder()
	h.ServeHTTP(wPres, rPres)
	if wPres.Code != http.StatusNoContent {
		t.Fatalf("POST /presence diharapkan 204, didapat %d %s", wPres.Code, wPres.Body.String())
	}

	// Uji POST /chats/{jid}/presence recording -> 204
	rChatPres := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/chats/628123456789/presence", strings.NewReader(`{"state":"recording"}`))
	rChatPres.Header.Set("X-API-Key", key)
	wChatPres := httptest.NewRecorder()
	h.ServeHTTP(wChatPres, rChatPres)
	if wChatPres.Code != http.StatusNoContent {
		t.Fatalf("POST /chats/{jid}/presence diharapkan 204, didapat %d %s", wChatPres.Code, wChatPres.Body.String())
	}
	if len(drv.chatPres) != 1 || drv.chatPres[0].state != types.ChatPresenceComposing || drv.chatPres[0].media != types.ChatPresenceMediaAudio {
		t.Fatalf("mapping recording tidak sesuai: %+v", drv.chatPres)
	}

	// Subscribe 100 JID berbeda -> semuanya 204.
	for i := 0; i < 100; i++ {
		target := fmt.Sprintf("6289000%05d@s.whatsapp.net", i)
		rSub := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/presence/subscribe", strings.NewReader(`{"jid":"`+target+`"}`))
		rSub.Header.Set("X-API-Key", key)
		wSub := httptest.NewRecorder()
		h.ServeHTTP(wSub, rSub)
		if wSub.Code != http.StatusNoContent {
			t.Fatalf("subscribe JID ke-%d diharapkan 204, didapat %d %s", i+1, wSub.Code, wSub.Body.String())
		}
	}

	// JID ke-101 -> 400 invalid_request.
	rOver := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/presence/subscribe", strings.NewReader(`{"jid":"628999999999@s.whatsapp.net"}`))
	rOver.Header.Set("X-API-Key", key)
	wOver := httptest.NewRecorder()
	h.ServeHTTP(wOver, rOver)
	if wOver.Code != http.StatusBadRequest || !strings.Contains(wOver.Body.String(), "invalid_request") {
		t.Fatalf("JID ke-101 wajib ditolak 400 invalid_request, didapat %d %s", wOver.Code, wOver.Body.String())
	}
}

// TestV2Messages_RichTypes memverifikasi validasi dan enqueue POST /v2/messages untuk seluruh tipe pesan Batch 7.
func TestV2Messages_RichTypes(t *testing.T) {
	exec := &apiFakeExecutor{
		ready:  map[string]bool{"sales": true},
		paired: map[string]bool{"sales": true},
	}
	h, outSvc, key := setupV2MessagesServer(t, exec)
	_ = outSvc

	// Teks melebihi 60.000 byte -> 400 invalid_spec.
	overBody, _ := json.Marshal(map[string]any{
		"to":   "628123456789",
		"type": "text",
		"text": strings.Repeat("a", 60001),
	})
	rOver := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages", strings.NewReader(string(overBody)))
	rOver.Header.Set("X-API-Key", key)
	wOver := httptest.NewRecorder()
	h.ServeHTTP(wOver, rOver)
	if wOver.Code != http.StatusBadRequest || !strings.Contains(wOver.Body.String(), "invalid_spec") {
		t.Fatalf("teks > 60000 byte wajib ditolak 400 invalid_spec, didapat %d %s", wOver.Code, wOver.Body.String())
	}

	payloads := []string{
		`{"to":"628123456789","type":"location","location":{"lat":-6.2,"lng":106.8,"name":"Monas"}}`,
		`{"to":"628123456789","type":"contact","contacts":[{"name":"Budi","phones":["628111111111"]}]}`,
		`{"to":"120363012345@g.us","type":"poll","poll":{"name":"Pilih hari?","options":["Senin","Selasa"],"selectable":1}}`,
		`{"to":"628123456789","type":"reaction","reaction":{"message_id":"MSG1","emoji":"👍"}}`,
		`{"to":"628123456789","type":"edit","edit":{"message_id":"MSG1","text":"Revisi"}}`,
		`{"to":"628123456789","type":"revoke","revoke":{"message_id":"MSG1"}}`,
	}
	for _, body := range payloads {
		r := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages", strings.NewReader(body))
		r.Header.Set("X-API-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusAccepted {
			t.Fatalf("enqueue tipe kaya gagal untuk %s: %d %s", body, w.Code, w.Body.String())
		}
	}
}
