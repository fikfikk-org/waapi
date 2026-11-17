package hooks

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
	"go.mau.fi/whatsmeow/types"
)

// --- Fakes & helpers pengujian in-memory (aman untuk testing/synctest tanpa SQLite) ---

type memRuleStore struct {
	mu    sync.RWMutex
	rules map[string][]Rule
}

func newMemRuleStore() *memRuleStore {
	return &memRuleStore{rules: make(map[string][]Rule)}
}

func (m *memRuleStore) Set(instanceID string, rules ...Rule) {
	m.mu.Lock()
	cp := make([]Rule, len(rules))
	copy(cp, rules)
	m.rules[instanceID] = cp
	m.mu.Unlock()
}

func (m *memRuleStore) ListHooks(_ context.Context, instanceID string) ([]Rule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	src := m.rules[instanceID]
	out := make([]Rule, len(src))
	copy(out, src)
	return out, nil
}

func (m *memRuleStore) ListHookInstances(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.rules))
	for k, v := range m.rules {
		if len(v) > 0 {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out, nil
}

type memRunStore struct {
	mu   sync.Mutex
	runs map[string]RunRecord // key: instance|seq|hook
	list []RunRecord
}

func newMemRunStore() *memRunStore {
	return &memRunStore{runs: make(map[string]RunRecord)}
}

func runKey(instanceID string, seq int64, hookID string) string {
	return instanceID + "|" + string(rune(seq)) + "|" + hookID + "|" + jsonNumber(seq)
}

func jsonNumber(seq int64) string {
	b, _ := json.Marshal(seq)
	return string(b)
}

func (m *memRunStore) GetRun(_ context.Context, instanceID string, seq int64, hookID string) (RunRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.runs[runKey(instanceID, seq, hookID)]
	return rec, ok, nil
}

func (m *memRunStore) UpsertRun(_ context.Context, rec RunRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := runKey(rec.InstanceID, rec.Seq, rec.HookID)
	m.runs[k] = rec
	replaced := false
	for i := range m.list {
		if m.list[i].InstanceID == rec.InstanceID && m.list[i].Seq == rec.Seq && m.list[i].HookID == rec.HookID {
			m.list[i] = rec
			replaced = true
			break
		}
	}
	if !replaced {
		m.list = append(m.list, rec)
	}
	return nil
}

func (m *memRunStore) IsSeqSuppressed(_ context.Context, instanceID string, seq int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rec := range m.list {
		if rec.InstanceID == instanceID && rec.Seq == seq && rec.Suppressed {
			return true, nil
		}
	}
	return false, nil
}

func (m *memRunStore) All() []RunRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RunRecord, len(m.list))
	copy(out, m.list)
	return out
}

type memJournal struct {
	mu      sync.Mutex
	nextSeq int64
	events  map[string][]journal.Event
	cursors map[string]int64
	enabled map[string]bool
	subs    map[string]map[uint64]chan struct{}
	subID   uint64
}

func newMemJournal() *memJournal {
	return &memJournal{
		events:  make(map[string][]journal.Event),
		cursors: make(map[string]int64),
		enabled: make(map[string]bool),
		subs:    make(map[string]map[uint64]chan struct{}),
	}
}

func (m *memJournal) append(ev journal.Event) int64 {
	m.mu.Lock()
	m.nextSeq++
	ev.Seq = m.nextSeq
	if ev.ReceivedAt == 0 {
		ev.ReceivedAt = time.Now().UnixMilli()
	}
	m.events[ev.InstanceID] = append(m.events[ev.InstanceID], ev)
	subs := make([]chan struct{}, 0, len(m.subs[ev.InstanceID]))
	for _, ch := range m.subs[ev.InstanceID] {
		subs = append(subs, ch)
	}
	m.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return ev.Seq
}

func (m *memJournal) Read(_ context.Context, q journal.Query) (journal.ReadResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var res journal.ReadResult
	res.ScannedTo = q.AfterSeq
	limit := q.Limit
	if limit <= 0 {
		limit = 32
	}
	typeSet := map[string]bool{}
	for _, tp := range q.Types {
		typeSet[tp] = true
	}
	all := m.events[q.InstanceID]
	for _, ev := range all {
		if ev.Seq <= q.AfterSeq {
			continue
		}
		res.ScannedTo = ev.Seq
		if len(typeSet) == 0 || typeSet[ev.Type] {
			res.Events = append(res.Events, ev)
			if len(res.Events) >= limit {
				break
			}
		}
	}
	if len(all) > 0 && res.ScannedTo < all[len(all)-1].Seq {
		res.HasMore = true
	}
	return res, nil
}

func (m *memJournal) Subscribe(instanceID string) (<-chan struct{}, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subID++
	id := m.subID
	ch := make(chan struct{}, 1)
	if m.subs[instanceID] == nil {
		m.subs[instanceID] = make(map[uint64]chan struct{})
	}
	m.subs[instanceID][id] = ch
	return ch, func() {
		m.mu.Lock()
		delete(m.subs[instanceID], id)
		m.mu.Unlock()
	}
}

func (m *memJournal) Cursor(_ context.Context, consumer, instanceID string, lane int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursors[consumer+"|"+instanceID+"|"+jsonNumber(int64(lane))], nil
}

func (m *memJournal) CommitCursor(_ context.Context, consumer, instanceID string, lane int, seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := consumer + "|" + instanceID + "|" + jsonNumber(int64(lane))
	if seq > m.cursors[k] {
		m.cursors[k] = seq
	}
	m.enabled[consumer+"|"+instanceID] = true
	return nil
}

func (m *memJournal) SetConsumerEnabled(_ context.Context, consumer, instanceID string, en bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled[consumer+"|"+instanceID] = en
	return nil
}

type fakeOutboxSender struct {
	mu    sync.Mutex
	reqs  []outbox.EnqueueRequest
	idem  map[string]*outbox.Job
	waIDs map[string]bool
}

func newFakeOutbox() *fakeOutboxSender {
	return &fakeOutboxSender{
		idem:  make(map[string]*outbox.Job),
		waIDs: make(map[string]bool),
	}
}

func (f *fakeOutboxSender) Enqueue(_ context.Context, req outbox.EnqueueRequest) (*outbox.Job, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.IdemKey != "" {
		if j, ok := f.idem[req.InstanceID+"|"+req.IdemKey]; ok {
			return j, false, nil
		}
	}
	j := &outbox.Job{
		ID:         "msg_" + jsonNumber(int64(len(f.reqs)+1)),
		InstanceID: req.InstanceID,
		IdemKey:    req.IdemKey,
		WAID:       "3EB0HOOK" + jsonNumber(int64(len(f.reqs)+1)),
		ToJID:      req.Spec.To,
		Kind:       req.Spec.Type,
		Origin:     req.Origin.Kind,
		KeyID:      req.Origin.KeyID,
		Status:     outbox.StatusQueued,
		Spec:       req.Spec,
	}
	f.reqs = append(f.reqs, req)
	if req.IdemKey != "" {
		f.idem[req.InstanceID+"|"+req.IdemKey] = j
	}
	f.waIDs[req.InstanceID+"|"+j.WAID] = true
	return j, true, nil
}

func (f *fakeOutboxSender) HasWAID(_ context.Context, instanceID, waID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.waIDs[instanceID+"|"+waID], nil
}

func (f *fakeOutboxSender) Requests() []outbox.EnqueueRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]outbox.EnqueueRequest, len(f.reqs))
	copy(out, f.reqs)
	return out
}

type fakeMarkReader struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeMarkReader) MarkRead(_ context.Context, instanceID string, chat, _ types.JID, ids []types.MessageID, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, instanceID+":"+chat.String()+":"+string(ids[0]))
	return nil
}

type staticSettingsReader struct {
	settings store.InstanceSettings
	selfJID  string
}

func (s staticSettingsReader) InstanceMeta(_ context.Context, _ string) (store.InstanceSettings, string, error) {
	return s.settings, s.selfJID, nil
}

type stubGovernor struct {
	lvl atomic.Int32
}

func (g *stubGovernor) Level() governor.Level {
	return governor.Level(g.lvl.Load())
}

func boolPtr(b bool) *bool { return &b }
func intPtr(n int) *int    { return &n }

func mustMsgEvent(instID string, seq int64, chat, sender, senderPN, pushName, text string, fromMe, offline bool, receivedAt time.Time) journal.Event {
	var flags uint32
	if offline {
		flags |= journal.FlagOffline
	}
	payload, _ := json.Marshal(wa.MessageV2{
		ID:        "WAMID_" + jsonNumber(seq),
		From:      sender,
		SenderPN:  senderPN,
		Chat:      chat,
		ChatType:  "dm",
		IsFromMe:  fromMe,
		PushName:  pushName,
		Type:      "text",
		Timestamp: receivedAt.Unix(),
		Text:      text,
		Flags:     wa.MsgFlags{Offline: offline},
	})
	return journal.Event{
		Seq:        seq,
		InstanceID: instID,
		Type:       "message",
		Chat:       chat,
		Sender:     sender,
		WAID:       "WAMID_" + jsonNumber(seq),
		TS:         receivedAt.Unix(),
		ReceivedAt: receivedAt.UnixMilli(),
		Flags:      flags,
		Payload:    payload,
	}
}

// TestCompile_MenolakInvalid memverifikasi validasi kompilator mengembalikan error dengan path field presisi.
func TestCompile_MenolakInvalid(t *testing.T) {
	deepCond := Cond{Field: "text", Op: "eq", Value: "x"}
	// Kedalaman 5 -> melebihi batas maksimum 4
	tooDeep := Cond{Not: &Cond{Not: &Cond{Not: &Cond{Not: &deepCond}}}}

	// 65 kondisi -> melebihi batas 64
	tooMany := make([]Cond, 65)
	for i := range tooMany {
		tooMany[i] = Cond{Field: "text", Op: "eq", Value: "a"}
	}

	// 9 aksi -> melebihi batas 8
	tooManyActions := make([]Action, 9)
	for i := range tooManyActions {
		tooManyActions[i] = Action{Action: "mark_read"}
	}

	cases := []struct {
		name     string
		rule     Rule
		trusted  bool
		wantPath string
	}{
		{
			name:     "id invalid",
			rule:     Rule{ID: "Bad ID!", On: []string{"message"}, Do: []Action{{Action: "mark_read"}}},
			wantPath: "id",
		},
		{
			name:     "field tak dikenal",
			rule:     Rule{ID: "r1", On: []string{"message"}, When: &Cond{All: []Cond{{Field: "chat_type", Op: "eq", Value: "dm"}, {Field: "unknown_field", Op: "eq", Value: "x"}}}, Do: []Action{{Action: "mark_read"}}},
			wantPath: "when.all[1].field",
		},
		{
			name:     "op tak dikenal",
			rule:     Rule{ID: "r1", On: []string{"message"}, When: &Cond{All: []Cond{{Field: "text", Op: "eval_js", Value: "x"}}}, Do: []Action{{Action: "mark_read"}}},
			wantPath: "when.all[0].op",
		},
		{
			name:     "regex rusak dengan path",
			rule:     Rule{ID: "r1", On: []string{"message"}, When: &Cond{All: []Cond{{Field: "chat_type", Op: "eq", Value: "dm"}, {Field: "chat_type", Op: "eq", Value: "dm"}, {Field: "text", Op: "matches", Value: "(?P<unclosed"}}}, Do: []Action{{Action: "mark_read"}}},
			wantPath: "when.all[2].value",
		},
		{
			name:     "regex terlalu panjang",
			rule:     Rule{ID: "r1", On: []string{"message"}, When: &Cond{Field: "text", Op: "matches", Value: strings.Repeat("a", 513)}, Do: []Action{{Action: "mark_read"}}},
			wantPath: "when.value",
		},
		{
			name:     "kedalaman melebihi 4",
			rule:     Rule{ID: "r1", On: []string{"message"}, When: &tooDeep, Do: []Action{{Action: "mark_read"}}},
			wantPath: "when",
		},
		{
			name:     "kondisi melebihi 64",
			rule:     Rule{ID: "r1", On: []string{"message"}, When: &Cond{All: tooMany}, Do: []Action{{Action: "mark_read"}}},
			wantPath: "when",
		},
		{
			name:     "aksi melebihi 8",
			rule:     Rule{ID: "r1", On: []string{"message"}, Do: tooManyActions},
			wantPath: "do",
		},
		{
			name:     "non-trusted menolak http localhost",
			rule:     Rule{ID: "r1", On: []string{"message"}, Do: []Action{{Action: "http", URL: "http://127.0.0.1:9000/hook"}}},
			trusted:  false,
			wantPath: "do[0].url",
		},
		{
			name:     "trusted menolak IP LAN privat non-loopback",
			rule:     Rule{ID: "r1", On: []string{"message"}, Do: []Action{{Action: "http", URL: "http://192.168.1.10:8080/hook"}}},
			trusted:  true,
			wantPath: "do[0].url",
		},
		{
			name:     "timeout http melebihi 5000ms",
			rule:     Rule{ID: "r1", On: []string{"message"}, Do: []Action{{Action: "http", URL: "https://example.com/hook", TimeoutMs: 6000}}},
			wantPath: "do[0].timeout_ms",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CompileWithOptions(tc.rule, CompileOptions{Trusted: tc.trusted})
			if err == nil {
				t.Fatalf("diharapkan error untuk %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantPath) {
				t.Fatalf("error %q harus menyebut path %q", err.Error(), tc.wantPath)
			}
		})
	}

	// Rule trusted dengan http://localhost & http://127.0.0.1 wajib diterima
	for _, u := range []string{"http://localhost:8088/cb", "http://127.0.0.1:8088/cb"} {
		if _, err := CompileWithOptions(Rule{
			ID: "ok-local",
			On: []string{"message"},
			Do: []Action{{Action: "http", URL: u, TimeoutMs: 2500}},
		}, CompileOptions{Trusted: true}); err != nil {
			t.Fatalf("rule trusted %s harus lolos kompilasi: %v", u, err)
		}
	}
}

// TestEval_OperatorTabel menguji seluruh 14 operator kondisi dan kombinator all/any/not beserta seluruh field EventView.
func TestEval_OperatorTabel(t *testing.T) {
	now := time.Unix(1795000100, 0)
	payload, _ := json.Marshal(wa.MessageV2{
		ID:        "M1",
		From:      "628123456789@s.whatsapp.net",
		SenderPN:  "628123456789@s.whatsapp.net",
		Chat:      "120363011@g.us",
		ChatType:  "group",
		IsGroup:   true,
		IsFromMe:  false,
		PushName:  "Sinta",
		Type:      "audio",
		Timestamp: now.Add(-15 * time.Second).Unix(),
		Text:      "Halo Admin WAAPI, Jam Buka berapa?",
		Mentions:  []string{"628000000000@s.whatsapp.net"},
		Quoted:    &wa.QuotedPayload{ID: "Q1", Participant: "628000000000@s.whatsapp.net", Type: "text"},
		Media:     &wa.MediaDescriptor{MediaType: "audio", FileLength: 4096, PTT: true},
		Poll:      &wa.PollInfo{Name: "Pilih Jadwal"},
	})
	ev := journal.Event{
		Seq:        10,
		InstanceID: "sales",
		Type:       "message",
		Chat:       "120363011@g.us",
		Sender:     "628123456789@s.whatsapp.net",
		WAID:       "M1",
		TS:         now.Add(-15 * time.Second).Unix(),
		ReceivedAt: now.Add(-15 * time.Second).UnixMilli(),
		Payload:    payload,
	}
	view, err := EventViewFromJournal(ev, "628000000000@s.whatsapp.net", now)
	if err != nil {
		t.Fatalf("EventViewFromJournal gagal: %v", err)
	}

	checks := []struct {
		name string
		cond Cond
		want bool
	}{
		{"eq match", Cond{Field: "chat_type", Op: "eq", Value: "group"}, true},
		{"ne match", Cond{Field: "from_me", Op: "ne", Value: true}, true},
		{"in match", Cond{Field: "media_kind", Op: "in", Value: []any{"ptt", "audio"}}, true},
		{"nin match", Cond{Field: "msg_type", Op: "nin", Value: []any{"video", "sticker"}}, true},
		{"contains", Cond{Field: "text", Op: "contains", Value: "Admin WAAPI"}, true},
		{"icontains", Cond{Field: "text", Op: "icontains", Value: "jam buka"}, true},
		{"prefix", Cond{Field: "text", Op: "prefix", Value: "Halo"}, true},
		{"suffix", Cond{Field: "text", Op: "suffix", Value: "berapa?"}, true},
		{"matches", Cond{Field: "text", Op: "matches", Value: `(?i)jam\s+buka`}, true},
		{"exists true", Cond{Field: "poll_name", Op: "exists", Value: true}, true},
		{"exists false", Cond{Field: "group_action", Op: "exists", Value: false}, true},
		{"gt", Cond{Field: "media_size", Op: "gt", Value: 4000}, true},
		{"gte", Cond{Field: "age_sec", Op: "gte", Value: 15}, true},
		{"lt", Cond{Field: "age_sec", Op: "lt", Value: 20}, true},
		{"lte", Cond{Field: "media_size", Op: "lte", Value: 4096}, true},
		{"mentions_me", Cond{Field: "mentions_me", Op: "eq", Value: true}, true},
		{"quoted_from_me", Cond{Field: "quoted_from_me", Op: "eq", Value: true}, true},
		{"is_reply", Cond{Field: "is_reply", Op: "eq", Value: true}, true},
		{"offline", Cond{Field: "offline", Op: "eq", Value: false}, true},
		{"all+any+not", Cond{
			All: []Cond{
				{Any: []Cond{{Field: "push_name", Op: "eq", Value: "Budi"}, {Field: "push_name", Op: "eq", Value: "Sinta"}}},
				{Not: &Cond{Field: "sender", Op: "eq", Value: "blocked"}},
			},
		}, true},
		{"not gagal", Cond{Not: &Cond{Field: "is_reply", Op: "eq", Value: true}}, false},
	}

	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			comp, err := Compile(Rule{
				ID:   "eval",
				On:   []string{"message"},
				When: &tc.cond,
				Do:   []Action{{Action: "mark_read"}},
			})
			if err != nil {
				t.Fatalf("Compile gagal: %v", err)
			}
			if got := comp.Match(view); got != tc.want {
				t.Fatalf("operator %s diharapkan %v, didapat %v", tc.name, tc.want, got)
			}
		})
	}
}

// TestTemplate_Placeholder memastikan placeholder dikenal diganti di timezone instance dan placeholder asing tetap utuh.
func TestTemplate_Placeholder(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 17, 2, 30, 0, 0, time.UTC) // 09:30 WIB
	view := EventView{
		Chat:     "628123@s.whatsapp.net",
		SenderPN: "628123",
		PushName: "Budi",
		Text:     "Harga berapa?",
	}
	out := RenderTemplate("Halo {{push_name}} ({{sender_pn}}) di {{chat}}: {{text}} @ {{date}} {{time}} | {{unknown}} {{.Env}}", view, loc, now)
	want := "Halo Budi (628123) di 628123@s.whatsapp.net: Harga berapa? @ 2026-11-17 09:30 | {{unknown}} {{.Env}}"
	if out != want {
		t.Fatalf("hasil template:\n got: %q\nwant: %q", out, want)
	}
}

// TestGuard_Stale memverifikasi event > hooks_max_age dan event offline dicatat sebagai skipped_stale.
func TestGuard_Stale(t *testing.T) {
	now := time.Unix(1795001000, 0)
	rules := newMemRuleStore()
	rules.Set("sales", Rule{
		ID: "faq",
		On: []string{"message"},
		Do: []Action{{Action: "reply", Text: "ok"}},
	})
	runs := newMemRunStore()
	mj := newMemJournal()
	ob := newFakeOutbox()

	eng := NewEngine(EngineDeps{
		Rules:    rules,
		Runs:     runs,
		Jr:       mj,
		Outbox:   ob,
		Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
		Now:      func() time.Time { return now },
	})
	if err := eng.ReloadInstance(context.Background(), "sales"); err != nil {
		t.Fatal(err)
	}

	evOld := mustMsgEvent("sales", 1, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "A", "lama", false, false, now.Add(-5*time.Minute))
	evOff := mustMsgEvent("sales", 2, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "A", "offline", false, true, now)
	evFresh := mustMsgEvent("sales", 3, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "A", "segar", false, false, now.Add(-10*time.Second))

	for _, ev := range []journal.Event{evOld, evOff, evFresh} {
		if err := eng.ProcessOne(context.Background(), "sales", ev); err != nil {
			t.Fatal(err)
		}
	}

	if got := len(ob.Requests()); got != 1 {
		t.Fatalf("hanya 1 pesan segar boleh dikirim, didapat %d", got)
	}
	r1, ok1, _ := runs.GetRun(context.Background(), "sales", 1, "faq")
	r2, ok2, _ := runs.GetRun(context.Background(), "sales", 2, "faq")
	r3, ok3, _ := runs.GetRun(context.Background(), "sales", 3, "faq")
	if !ok1 || r1.Status != "skipped_stale" || !ok2 || r2.Status != "skipped_stale" || !ok3 || r3.Status != "done" {
		t.Fatalf("status guard stale tidak sesuai: r1=%+v r2=%+v r3=%+v", r1, r2, r3)
	}
}

// TestGuard_HumanTakeover memastikan pesan manual from_me non-outbox menjeda otomasi chat selama 30 menit lalu pulih di synctest.
func TestGuard_HumanTakeover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rules := newMemRuleStore()
		rules.Set("sales",
			Rule{ID: "auto-faq", Priority: intPtr(10), On: []string{"message"}, Do: []Action{{Action: "reply", Text: "bot"}}, RespectTakeover: boolPtr(true)},
			Rule{ID: "always-tag", Priority: intPtr(20), On: []string{"message"}, Do: []Action{{Action: "mark_read"}}, RespectTakeover: boolPtr(false)},
		)
		runs := newMemRunStore()
		mj := newMemJournal()
		ob := newFakeOutbox()
		mr := &fakeMarkReader{}

		eng := NewEngine(EngineDeps{
			Rules:    rules,
			Runs:     runs,
			Jr:       mj,
			Outbox:   ob,
			WA:       mr,
			Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
		})
		if err := eng.ReloadInstance(context.Background(), "sales"); err != nil {
			t.Fatal(err)
		}

		chat := "628111@s.whatsapp.net"
		// 1. Pesan from_me manual dari HP (wa_id tidak ada di outbox) -> memicu takeover 30 menit
		hpMsg := mustMsgEvent("sales", 1, chat, "628000@s.whatsapp.net", "628000@s.whatsapp.net", "", "Saya agen manual", true, false, time.Now())
		if err := eng.ProcessOne(context.Background(), "sales", hpMsg); err != nil {
			t.Fatal(err)
		}

		// 2. Pesan masuk dari pelanggan 5 menit kemudian -> auto-faq skipped_takeover, always-tag tetap jalan
		time.Sleep(5 * time.Minute)
		cust1 := mustMsgEvent("sales", 2, chat, chat, chat, "Budi", "Halo?", false, false, time.Now())
		if err := eng.ProcessOne(context.Background(), "sales", cust1); err != nil {
			t.Fatal(err)
		}
		rAuto, ok, _ := runs.GetRun(context.Background(), "sales", 2, "auto-faq")
		if !ok || rAuto.Status != "skipped_takeover" {
			t.Fatalf("diharapkan skipped_takeover selama jeda 30m, didapat %+v", rAuto)
		}
		if len(ob.Requests()) != 0 {
			t.Fatalf("balasan otomatis dilarang keluar selama takeover")
		}
		if len(mr.calls) != 1 {
			t.Fatalf("rule respect_takeover=false harus tetap berjalan")
		}

		// 3. Setelah lewat 30 menit -> takeover kedaluwarsa, auto-faq aktif kembali
		time.Sleep(26 * time.Minute)
		cust2 := mustMsgEvent("sales", 3, chat, chat, chat, "Budi", "Masih ada?", false, false, time.Now())
		if err := eng.ProcessOne(context.Background(), "sales", cust2); err != nil {
			t.Fatal(err)
		}
		if len(ob.Requests()) != 1 {
			t.Fatalf("setelah 30 menit, auto-faq harus aktif lagi (got %d)", len(ob.Requests()))
		}
	})
}

// TestGuard_ThrottlePerChat memastikan throttle N/durasi memblokir balasan kedua dalam jendela lalu mengizinkan setelah lewat.
func TestGuard_ThrottlePerChat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rules := newMemRuleStore()
		rules.Set("sales", Rule{
			ID:       "faq-throttle",
			On:       []string{"message"},
			When:     &Cond{Field: "text", Op: "icontains", Value: "jam buka"},
			Do:       []Action{{Action: "reply", Text: "08-17"}},
			Throttle: &Throttle{PerChat: "1/6h"},
		})
		runs := newMemRunStore()
		ob := newFakeOutbox()

		eng := NewEngine(EngineDeps{
			Rules:    rules,
			Runs:     runs,
			Outbox:   ob,
			Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
		})
		if err := eng.ReloadInstance(context.Background(), "sales"); err != nil {
			t.Fatal(err)
		}

		chat := "628111@s.whatsapp.net"
		_ = eng.ProcessOne(context.Background(), "sales", mustMsgEvent("sales", 1, chat, chat, chat, "B", "jam buka?", false, false, time.Now()))
		time.Sleep(1 * time.Hour)
		_ = eng.ProcessOne(context.Background(), "sales", mustMsgEvent("sales", 2, chat, chat, chat, "B", "jam buka lagi?", false, false, time.Now()))
		r2, _, _ := runs.GetRun(context.Background(), "sales", 2, "faq-throttle")
		if r2.Status != "throttled" || len(ob.Requests()) != 1 {
			t.Fatalf("balasan kedua <6h wajib throttled, r2=%+v reqs=%d", r2, len(ob.Requests()))
		}

		time.Sleep(6*time.Hour + time.Minute)
		_ = eng.ProcessOne(context.Background(), "sales", mustMsgEvent("sales", 3, chat, chat, chat, "B", "jam buka besok?", false, false, time.Now()))
		if len(ob.Requests()) != 2 {
			t.Fatalf("setelah >6h, balasan ketiga harus terkirim, reqs=%d", len(ob.Requests()))
		}
	})
}

// TestGuard_SkippedPressure memastikan governor Red melewati aksi berat sebagai skipped_pressure tetapi tetap menjalankan aksi ringan.
func TestGuard_SkippedPressure(t *testing.T) {
	now := time.Unix(1795002000, 0)
	rules := newMemRuleStore()
	rules.Set("sales",
		Rule{ID: "heavy", Priority: intPtr(10), On: []string{"message"}, Do: []Action{{Action: "reply", Text: "hi"}}},
		Rule{ID: "light", Priority: intPtr(20), On: []string{"message"}, Do: []Action{{Action: "suppress_webhook"}, {Action: "mark_read"}}},
	)
	runs := newMemRunStore()
	ob := newFakeOutbox()
	mr := &fakeMarkReader{}
	gov := &stubGovernor{}
	gov.lvl.Store(int32(governor.Red))

	eng := NewEngine(EngineDeps{
		Rules:    rules,
		Runs:     runs,
		Outbox:   ob,
		WA:       mr,
		Governor: gov,
		Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
		Now:      func() time.Time { return now },
	})
	_ = eng.ReloadInstance(context.Background(), "sales")
	_ = eng.ProcessOne(context.Background(), "sales", mustMsgEvent("sales", 1, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "A", "hai", false, false, now))

	rHeavy, _, _ := runs.GetRun(context.Background(), "sales", 1, "heavy")
	rLight, _, _ := runs.GetRun(context.Background(), "sales", 1, "light")
	if rHeavy.Status != "skipped_pressure" || len(ob.Requests()) != 0 {
		t.Fatalf("aksi berat saat Red harus skipped_pressure: %+v reqs=%d", rHeavy, len(ob.Requests()))
	}
	if rLight.Status != "done" || !rLight.Suppressed || len(mr.calls) != 1 {
		t.Fatalf("aksi ringan suppress_webhook/mark_read saat Red harus tetap berjalan: %+v calls=%d", rLight, len(mr.calls))
	}
}

// TestEngine_ReplayIdempoten memastikan crash setelah matched sebelum done tetap hanya menghasilkan 1 job outbox saat di-replay.
func TestEngine_ReplayIdempoten(t *testing.T) {
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
	j, err := journal.Open(ctx, cfg.JournalDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	_ = st.CreateInstance(ctx, store.Instance{ID: "sales", RatePerMin: 60})
	_ = st.UpdateInstanceStatus(ctx, "sales", store.StatusConnected, "628000@s.whatsapp.net")

	spec, _ := json.Marshal(Rule{ID: "auto-hi", On: []string{"message"}, Do: []Action{{Action: "reply", Text: "Halo {{push_name}}"}}})
	if err := st.UpsertHook(ctx, store.HookRecord{
		InstanceID: "sales", ID: "auto-hi", Name: "Auto", Enabled: true, Priority: 100, Spec: string(spec),
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	ev := mustMsgEvent("sales", 0, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "Budi", "hai", false, false, now)
	res, err := j.Append(ctx, &ev)
	if err != nil {
		t.Fatal(err)
	}
	ev.Seq = res.Seq

	ob := newFakeOutbox()
	runs := NewSQLiteRunStore(j.DB())
	// Simulasikan state pasca crash: row matched tercatat, job outbox #0 sudah sempat di-enqueue sebelum done tertulis
	_ = runs.UpsertRun(ctx, RunRecord{InstanceID: "sales", Seq: ev.Seq, HookID: "auto-hi", Status: "matched", At: now.UnixMilli()})
	_, _, _ = ob.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Spec:       outbox.Spec{Type: "text", To: "628111@s.whatsapp.net", Text: "Halo Budi"},
		IdemKey:    "hook:auto-hi:" + jsonNumber(ev.Seq) + ":0",
		Origin:     outbox.Origin{Kind: "hook", KeyID: "auto-hi"},
	})

	eng := NewEngine(EngineDeps{AppStore: st, Journal: j, Outbox: ob})
	_ = eng.ReloadInstance(ctx, "sales")
	if err := eng.ProcessOne(ctx, "sales", ev); err != nil {
		t.Fatal(err)
	}

	if got := len(ob.Requests()); got != 1 {
		t.Fatalf("idempotency key hook:<id>:<seq>:<n> harus mencegah pengiriman ganda, didapat %d job", got)
	}
	rec, ok, _ := runs.GetRun(ctx, "sales", ev.Seq, "auto-hi")
	if !ok || rec.Status != "done" {
		t.Fatalf("status pasca replay harus menjadi done, didapat %+v", rec)
	}
}

// TestAction_HTTPBalasanMenjadiReply memverifikasi aksi http bertanda tangan memproses JSON balasan reply/react/suppress dan conversation.
func TestAction_HTTPBalasanMenjadiReply(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, _ := store.Open(ctx, cfg.AppDBPath())
	defer st.Close()
	j, _ := journal.Open(ctx, cfg.JournalDBPath())
	defer j.Close()

	_ = st.CreateInstance(ctx, store.Instance{
		ID:            "sales",
		WebhookSecret: "whsec_secret_hook",
		RatePerMin:    60,
	})
	settings := store.Defaults()
	settings.ContextStore = true
	_, _ = st.UpdateInstance(ctx, "sales", store.InstanceUpdate{Settings: &settings})
	cs := ctxstore.New(j.DB(), ctxstore.NewStoreSettings(st))
	_ = cs.RecordOutbound(ctx, ctxstore.OutboundMessage{
		InstanceID: "sales", Chat: "628111@s.whatsapp.net", WAID: "PREV1", Sender: "628000@s.whatsapp.net",
		TS: time.Now().Unix() - 5, Type: "text", Text: "riwayat sebelumnya",
	})

	var gotHeaderHookRun, gotHeaderSig, gotHeaderEvent string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaderHookRun = r.Header.Get("X-WA-Hook-Run")
		gotHeaderSig = r.Header.Get("X-WA-Signature")
		gotHeaderEvent = r.Header.Get("X-WA-Event")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"reply":"ok","react":"👍","suppress_webhook":true}`))
	}))
	defer srv.Close()

	ob := newFakeOutbox()
	tc := webhook.NewTargetCache()
	_ = tc.Load(ctx, st)

	rules := newMemRuleStore()
	rules.Set("sales", Rule{
		ID:      "http-ai",
		Trusted: true,
		On:      []string{"message"},
		Do: []Action{{
			Action:      "http",
			URL:         srv.URL,
			TimeoutMs:   2000,
			Include:     "conversation",
			ExpectReply: true,
		}},
	})

	eng := NewEngine(EngineDeps{
		Rules:             rules,
		Runs:              NewSQLiteRunStore(j.DB()),
		Outbox:            ob,
		CtxStore:          cs,
		Targets:           tc,
		Settings:          staticSettingsReader{settings: settings, selfJID: "628000@s.whatsapp.net"},
		HTTPClientFactory: func(_ bool, _ time.Duration) *http.Client { return srv.Client() },
	})
	_ = eng.ReloadInstance(ctx, "sales")

	ev := mustMsgEvent("sales", 42, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "Andi", "halo", false, false, time.Now())
	if err := eng.ProcessOne(ctx, "sales", ev); err != nil {
		t.Fatal(err)
	}

	if gotHeaderHookRun != "42:http-ai" || gotHeaderSig == "" || gotHeaderEvent != "message" {
		t.Fatalf("header HTTP hook tidak lengkap: run=%q sig=%q ev=%q", gotHeaderHookRun, gotHeaderSig, gotHeaderEvent)
	}
	if conv, ok := gotBody["conversation"].([]any); !ok || len(conv) == 0 {
		t.Fatalf("payload conversation harus terisi dari context store: %+v", gotBody)
	}
	reqs := ob.Requests()
	if len(reqs) != 2 || reqs[0].Spec.Text != "ok" || reqs[0].Origin.Kind != "hook" || reqs[1].Spec.Type != "reaction" || reqs[1].Spec.Reaction.Emoji != "👍" {
		t.Fatalf("balasan HTTP seharusnya di-enqueue menjadi reply 'ok' dan react '👍': %+v", reqs)
	}
	if !eng.Gate().IsSuppressed(ctx, "sales", 42) {
		t.Fatalf("suppress_webhook:true dari respons HTTP harus dicatat di Gate")
	}
}

// TestAction_HTTPTimeout memverifikasi timeout 5 detik menghasilkan error pada rule tsb namun tetap mengevaluasi rule berikutnya.
func TestAction_HTTPTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		srvCtx, cancelSrv := context.WithCancel(ctx)
		defer cancelSrv()
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-srvCtx.Done():
			case <-time.After(10 * time.Second):
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer func() {
			cancelSrv()
			srv.CloseClientConnections()
			srv.Close()
		}()

		rules := newMemRuleStore()
		rules.Set("sales",
			Rule{
				ID:       "slow-http",
				Priority: intPtr(10),
				Trusted:  true,
				On:       []string{"message"},
				Stop:     true, // meskipun stop=true, error tidak boleh menghentikan rule berikutnya
				Do:       []Action{{Action: "http", URL: "http://127.0.0.1/hook", TimeoutMs: 5000, ExpectReply: true}},
			},
			Rule{
				ID:       "fallback",
				Priority: intPtr(20),
				On:       []string{"message"},
				Do:       []Action{{Action: "reply", Text: "cadangan"}},
			},
		)
		runs := newMemRunStore()
		ob := newFakeOutbox()

		eng := NewEngine(EngineDeps{
			Rules:             rules,
			Runs:              runs,
			Outbox:            ob,
			Settings:          staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
			HTTPClientFactory: func(_ bool, to time.Duration) *http.Client { c := *srv.Client(); c.Timeout = to; return &c },
		})
		_ = eng.ReloadInstance(ctx, "sales")

		ev := mustMsgEvent("sales", 1, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "B", "test", false, false, time.Now())
		if err := eng.ProcessOne(ctx, "sales", ev); err != nil {
			t.Fatal(err)
		}

		r1, _, _ := runs.GetRun(ctx, "sales", 1, "slow-http")
		r2, _, _ := runs.GetRun(ctx, "sales", 1, "fallback")
		if r1.Status != "error" {
			t.Fatalf("rule HTTP timeout harus berstatus error, didapat %+v", r1)
		}
		if r2.Status != "done" || len(ob.Requests()) != 1 || ob.Requests()[0].Spec.Text != "cadangan" {
			t.Fatalf("rule fallback setelah HTTP timeout harus dievaluasi: r2=%+v reqs=%+v", r2, ob.Requests())
		}
	})
}

// TestGate_SuppressWebhook memverifikasi suppression event oleh hooks serta fail-open setelah 10 detik saat hooks lambat.
func TestGate_SuppressWebhook(t *testing.T) {
	// Subtest 1: Integrasi Gate nyata dengan SQLite + webhook.Service
	t.Run("integrasi_suppress_dan_lolos", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tmp := t.TempDir()
		cfg := &config.Config{DataDir: tmp}
		cfg.Normalize()
		_ = cfg.EnsureDirs()

		st, _ := store.Open(ctx, cfg.AppDBPath())
		defer st.Close()
		j, _ := journal.Open(ctx, cfg.JournalDBPath())
		defer j.Close()

		var whMu sync.Mutex
		var deliveredSeqs []int64
		whSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var env struct {
				ID string `json:"id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&env)
			whMu.Lock()
			deliveredSeqs = append(deliveredSeqs, func() int64 {
				var n int64
				_ = json.Unmarshal([]byte(env.ID), &n)
				return n
			}())
			whMu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		defer whSrv.Close()

		_ = st.CreateInstance(ctx, store.Instance{
			ID: "sales", WebhookURL: whSrv.URL, WebhookSecret: "s", WebhookEvents: "message", RatePerMin: 60,
		})
		spec, _ := json.Marshal(Rule{
			ID:   "sup-otp",
			On:   []string{"message"},
			When: &Cond{Field: "text", Op: "contains", Value: "OTP"},
			Do:   []Action{{Action: "suppress_webhook"}},
		})
		_ = st.UpsertHook(ctx, store.HookRecord{InstanceID: "sales", ID: "sup-otp", Enabled: true, Priority: 10, Spec: string(spec)})

		tc := webhook.NewTargetCache()
		_ = tc.Load(ctx, st)

		eng := NewEngine(EngineDeps{AppStore: st, Journal: j})
		eng.Start(ctx)
		defer eng.Stop()

		whSvc := webhook.NewService(j, tc, webhook.NewDeadStore(j.DB()), whSrv.Client(), eng.Gate(), webhook.Options{
			Store: st, CommitEvery: 1, CommitInterval: 100 * time.Millisecond,
		})
		whSvc.Start(ctx, []string{"sales"})
		defer whSvc.Stop()

		now := time.Now()
		ev1 := mustMsgEvent("sales", 0, "6281@s.whatsapp.net", "6281@s.whatsapp.net", "6281@s.whatsapp.net", "A", "kode OTP 123", false, false, now)
		ev2 := mustMsgEvent("sales", 0, "6281@s.whatsapp.net", "6281@s.whatsapp.net", "6281@s.whatsapp.net", "A", "halo biasa", false, false, now)
		r1, _ := j.Append(ctx, &ev1)
		r2, _ := j.Append(ctx, &ev2)
		j.Notify("sales")

		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			whMu.Lock()
			n := len(deliveredSeqs)
			whMu.Unlock()
			if n >= 1 {
				c, _ := j.Cursor(ctx, "webhook", "sales", 0)
				if c >= r2.Seq {
					break
				}
			}
			time.Sleep(25 * time.Millisecond)
		}

		whMu.Lock()
		got := append([]int64(nil), deliveredSeqs...)
		whMu.Unlock()
		if len(got) != 1 || got[0] != r2.Seq || slices.Contains(got, r1.Seq) {
			t.Fatalf("event OTP (%d) wajib di-suppress dan event biasa (%d) terkirim; got=%v", r1.Seq, r2.Seq, got)
		}
	})

	// Subtest 2: Fail-open 10 detik di dalam synctest saat engine tidak memajukan cursor
	t.Run("fail_open_10s_synctest", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			rules := newMemRuleStore()
			rules.Set("sales", Rule{ID: "sup", On: []string{"message"}, Do: []Action{{Action: "suppress_webhook"}}})
			eng := NewEngine(EngineDeps{Rules: rules})
			_ = eng.ReloadInstance(context.Background(), "sales")

			var deliver atomic.Bool
			var done atomic.Bool
			go func() {
				d, err := eng.Gate().Wait(context.Background(), "sales", 99)
				if err == nil {
					deliver.Store(d)
				}
				done.Store(true)
			}()

			time.Sleep(9 * time.Second)
			synctest.Wait()
			if done.Load() {
				t.Fatalf("gate tidak boleh fail-open sebelum 10 detik")
			}

			time.Sleep(2 * time.Second)
			synctest.Wait()
			if !done.Load() || !deliver.Load() {
				t.Fatalf("gate wajib fail-open (deliver=true) setelah 10 detik")
			}
		})
	})
}

// TestDryRun_TanpaEfekSamping memastikan dry-run mengembalikan ringkasan evaluasi tanpa enqueue outbox maupun menulis hook_runs.
func TestDryRun_TanpaEfekSamping(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1795003000, 0)
	rules := newMemRuleStore()
	comp, err := CompileWithOptions(Rule{
		ID:       "dry-faq",
		On:       []string{"message"},
		When:     &Cond{Field: "text", Op: "contains", Value: "harga"},
		Do:       []Action{{Action: "reply", Text: "Halo {{push_name}}, cek {{text}}"}, {Action: "mark_read"}},
		Throttle: &Throttle{PerChat: "1/6h"},
	}, CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rules.Set("sales", comp.Rule)
	runs := newMemRunStore()
	ob := newFakeOutbox()
	mr := &fakeMarkReader{}

	eng := NewEngine(EngineDeps{
		Rules:    rules,
		Runs:     runs,
		Outbox:   ob,
		WA:       mr,
		Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
		Now:      func() time.Time { return now },
	})

	ev := mustMsgEvent("sales", 1, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "Sari", "harga paket?", false, false, now)
	res, err := eng.DryRun(ctx, "sales", comp, ev)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Matched || res.SkippedReason != "" || len(res.Actions) != 2 || res.Actions[0].Text != "Halo Sari, cek harga paket?" {
		t.Fatalf("hasil dry-run tidak sesuai: %+v", res)
	}
	if len(ob.Requests()) != 0 || len(runs.All()) != 0 || len(mr.calls) != 0 {
		t.Fatalf("dry-run dilarang menimbulkan efek samping: outbox=%d runs=%d mark=%d", len(ob.Requests()), len(runs.All()), len(mr.calls))
	}
}

// TestPriorityDanStop memastikan rule dievaluasi berdasarkan priority naik dan stop=true menghentikan rule berikutnya.
func TestPriorityDanStop(t *testing.T) {
	now := time.Unix(1795004000, 0)
	rules := newMemRuleStore()
	rules.Set("sales",
		Rule{ID: "low", Priority: intPtr(20), On: []string{"message"}, Do: []Action{{Action: "reply", Text: "20"}}},
		Rule{ID: "first", Priority: intPtr(5), On: []string{"message"}, Stop: false, Do: []Action{{Action: "reply", Text: "5"}}},
		Rule{ID: "high", Priority: intPtr(10), On: []string{"message"}, Stop: true, Do: []Action{{Action: "reply", Text: "10"}}},
	)
	runs := newMemRunStore()
	ob := newFakeOutbox()
	eng := NewEngine(EngineDeps{
		Rules:    rules,
		Runs:     runs,
		Outbox:   ob,
		Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
		Now:      func() time.Time { return now },
	})
	_ = eng.ReloadInstance(context.Background(), "sales")
	_ = eng.ProcessOne(context.Background(), "sales", mustMsgEvent("sales", 1, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "A", "hai", false, false, now))

	reqs := ob.Requests()
	if len(reqs) != 2 || reqs[0].Spec.Text != "5" || reqs[1].Spec.Text != "10" {
		t.Fatalf("urutan evaluasi priority/stop tidak sesuai: %+v", reqs)
	}
	if _, exists, _ := runs.GetRun(context.Background(), "sales", 1, "low"); exists {
		t.Fatalf("rule prioritas 20 tidak boleh dievaluasi setelah priority 10 stop=true")
	}
}

// BenchmarkHooksEval mengukur alokasi dan kecepatan evaluasi 50 rule terkompilasi pada hot path.
func BenchmarkHooksEval(b *testing.B) {
	compiled := make([]*Compiled, 50)
	for i := range compiled {
		r, err := Compile(Rule{
			ID: "r-" + jsonNumber(int64(i)),
			On: []string{"message"},
			When: &Cond{All: []Cond{
				{Field: "chat_type", Op: "eq", Value: "dm"},
				{Field: "text", Op: "icontains", Value: "jam buka"},
			}},
			Do: []Action{{Action: "reply", Text: "08:00-17:00"}},
		})
		if err != nil {
			b.Fatal(err)
		}
		compiled[i] = r
	}
	view := EventView{
		Type:     "message",
		ChatType: "dm",
		Text:     "Permisi mau tanya info promo",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		matched := 0
		for _, c := range compiled {
			if c.Match(view) {
				matched++
			}
		}
		if matched != 0 {
			b.Fatal("tidak seharusnya cocok")
		}
	}
}

// FuzzCompileRule memastikan input JSON acak ke parser + kompilator rule hooks tidak pernah panic.
func FuzzCompileRule(f *testing.F) {
	f.Add([]byte(`{"id":"faq","on":["message"],"when":{"all":[{"field":"text","op":"matches","value":"(?i)hai"}]},"do":[{"action":"reply","text":"halo"}]}`))
	f.Add([]byte(`{"id":"bad","on":["message"],"do":[{"action":"http","url":"http://127.0.0.1"}]}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := ParseRuleJSON(data)
		if err != nil {
			return
		}
		_, _ = Compile(r)
		_, _ = CompileWithOptions(r, CompileOptions{Trusted: true})
	})
}
