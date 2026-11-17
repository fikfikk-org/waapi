package hooks

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/ai"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/guardian"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow/types"
)

func strPtrFloat(f float64) *float64 { return &f }

// TestAI_NonaktifTanpaAlokasi memastikan Base URL kosong menghasilkan Client nil dan aksi ai_reply ditolak saat kompilasi rule.
func TestAI_NonaktifTanpaAlokasi(t *testing.T) {
	if cli := ai.NewClient(ai.Config{BaseURL: ""}); cli != nil {
		t.Fatalf("WAAPI_AI_BASE_URL kosong wajib mengembalikan *ai.Client nil tanpa alokasi")
	}

	rule := Rule{
		ID: "cs-ai",
		On: []string{"message"},
		Do: []Action{{Action: "ai_reply", System: "Bantu jawab", ContextMessages: 5, MaxTokens: 200}},
	}
	if _, err := Compile(rule); err == nil || !strings.Contains(err.Error(), "do[0].action") {
		t.Fatalf("aksi ai_reply tanpa AI aktif wajib ditolak pada do[0].action, didapat: %v", err)
	}

	// Validasi batas context_messages <= 30 dan max_tokens <= 1024 saat AIEnabled=true
	badCtx := rule
	badCtx.Do = []Action{{Action: "ai_reply", ContextMessages: 31}}
	if _, err := CompileWithOptions(badCtx, CompileOptions{AIEnabled: true}); err == nil || !strings.Contains(err.Error(), "do[0].context_messages") {
		t.Fatalf("context_messages > 30 harus ditolak: %v", err)
	}
	badTok := rule
	badTok.Do = []Action{{Action: "ai_reply", MaxTokens: 2048}}
	if _, err := CompileWithOptions(badTok, CompileOptions{AIEnabled: true}); err == nil || !strings.Contains(err.Error(), "do[0].max_tokens") {
		t.Fatalf("max_tokens > 1024 harus ditolak: %v", err)
	}
}

// TestAI_FallbackSaatTimeout memastikan timeout pemanggilan endpoint AI segera mengirim fallback_text via outbox tanpa retry hooks.
func TestAI_FallbackSaatTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		srvCtx, cancelSrv := context.WithCancel(ctx)
		defer cancelSrv()

		var reqCount atomic.Int32
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount.Add(1)
			select {
			case <-r.Context().Done():
			case <-srvCtx.Done():
			case <-time.After(30 * time.Second):
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer func() {
			cancelSrv()
			srv.CloseClientConnections()
			srv.Close()
		}()

		aiCli := ai.NewClient(ai.Config{
			BaseURL:        srv.URL,
			APIKey:         "k",
			Model:          "m",
			Timeout:        2 * time.Second,
			MaxConcurrency: 2,
			HTTPClient:     srv.Client(),
		})
		rules := newMemRuleStore()
		rules.Set("sales", Rule{
			ID: "cs-bot",
			On: []string{"message"},
			Do: []Action{{
				Action:       "ai_reply",
				System:       "Anda CS",
				FallbackText: "Mohon tunggu sebentar, petugas kami segera membalas.",
			}},
		})
		runs := newMemRunStore()
		ob := newFakeOutbox()

		eng := NewEngine(EngineDeps{
			Rules:    rules,
			Runs:     runs,
			Outbox:   ob,
			AI:       aiCli,
			Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
		})
		_ = eng.ReloadInstance(ctx, "sales")

		ev := mustMsgEvent("sales", 7, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "Adi", "Ada stok?", false, false, time.Now())
		if err := eng.ProcessOne(ctx, "sales", ev); err != nil {
			t.Fatal(err)
		}

		if got := reqCount.Load(); got != 1 {
			t.Fatalf("hooks tidak boleh melakukan retry AI (harus 1 request), didapat %d", got)
		}
		reqs := ob.Requests()
		if len(reqs) != 1 || reqs[0].Spec.Text != "Mohon tunggu sebentar, petugas kami segera membalas." || reqs[0].Origin.Kind != "ai" || reqs[0].IdemKey != "ai:cs-bot:7" {
			t.Fatalf("fallback outbox tidak sesuai: %+v", reqs)
		}
	})
}

// TestAI_HandoffKeyword memastikan kata kunci handoff menjeda AI selama takeover_pause namun tetap mengeksekusi aksi eskalasi berikutnya.
func TestAI_HandoffKeyword(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		var aiCalls atomic.Int32
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			aiCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"Dari AI"}}]}`))
		}))
		defer func() {
			srv.CloseClientConnections()
			srv.Close()
		}()

		aiCli := ai.NewClient(ai.Config{BaseURL: srv.URL, Model: "m", HTTPClient: srv.Client()})
		rules := newMemRuleStore()
		rules.Set("sales", Rule{
			ID: "cs-handoff",
			On: []string{"message"},
			Do: []Action{
				{Action: "ai_reply", System: "CS", FallbackText: "fallback"},
				{Action: "send", To: "1203630999@g.us", Text: "Eskalasi {{chat}}: {{text}}"},
			},
		})
		runs := newMemRunStore()
		ob := newFakeOutbox()

		eng := NewEngine(EngineDeps{
			Rules:    rules,
			Runs:     runs,
			Outbox:   ob,
			AI:       aiCli,
			Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
		})
		_ = eng.ReloadInstance(ctx, "sales")

		chat := "628111@s.whatsapp.net"
		// 1. Substring "acs" bukan batas kata -> AI tetap menjawab
		_ = eng.ProcessOne(ctx, "sales", mustMsgEvent("sales", 1, chat, chat, chat, "B", "tanya acs", false, false, time.Now()))
		if aiCalls.Load() != 1 {
			t.Fatalf("substring non-kata tidak boleh memicu handoff")
		}

		// 2. Pesan mengandung batas kata "CS" -> AI dilewati, handoff memicu send ke grup admin & mengaktifkan takeover
		_ = eng.ProcessOne(ctx, "sales", mustMsgEvent("sales", 2, chat, chat, chat, "B", "mau bicara dengan CS dong", false, false, time.Now()))
		if aiCalls.Load() != 1 {
			t.Fatalf("handoff keyword dilarang memanggil AI")
		}
		r2, ok2, _ := runs.GetRun(ctx, "sales", 2, "cs-handoff")
		if !ok2 || r2.Status != "done" || r2.Detail != "handoff" {
			t.Fatalf("handoff dengan aksi lanjutan send harus berstatus done/handoff, didapat %+v", r2)
		}
		reqs := ob.Requests()
		if len(reqs) != 3 || reqs[2].Spec.To != "1203630999@g.us" || reqs[2].IdemKey != "hook:cs-handoff:2:1" {
			t.Fatalf("aksi send eskalasi admin harus tetap berjalan tanpa ai_reply ke-2: %+v", reqs)
		}

		// Verifikasi varian rule hanya berisi ai_reply mencatat skipped_takeover / handoff.
		rules.Set("solo", Rule{
			ID: "ai-solo",
			On: []string{"message"},
			Do: []Action{{Action: "ai_reply", System: "CS", FallbackText: "fallback"}},
		})
		_ = eng.ReloadInstance(ctx, "solo")
		_ = eng.ProcessOne(ctx, "solo", mustMsgEvent("solo", 1, chat, chat, chat, "B", "minta CS", false, false, time.Now()))
		rSolo, okSolo, _ := runs.GetRun(ctx, "solo", 1, "ai-solo")
		if !okSolo || rSolo.Status != "skipped_takeover" || rSolo.Detail != "handoff" || aiCalls.Load() != 1 {
			t.Fatalf("handoff tanpa aksi lain harus tercatat skipped_takeover/handoff tanpa AI call: %+v calls=%d", rSolo, aiCalls.Load())
		}

		// 3. Pesan berikutnya dalam 10 menit dijeda takeover -> skipped_takeover
		time.Sleep(10 * time.Minute)
		_ = eng.ProcessOne(ctx, "sales", mustMsgEvent("sales", 3, chat, chat, chat, "B", "halo?", false, false, time.Now()))
		r3, _, _ := runs.GetRun(ctx, "sales", 3, "cs-handoff")
		if r3.Status != "skipped_takeover" || aiCalls.Load() != 1 {
			t.Fatalf("chat pasca-handoff wajib skipped_takeover selama 30m: %+v aiCalls=%d", r3, aiCalls.Load())
		}

		// 4. Setelah melewati 30 menit -> AI aktif kembali
		time.Sleep(21 * time.Minute)
		_ = eng.ProcessOne(ctx, "sales", mustMsgEvent("sales", 4, chat, chat, chat, "B", "halo lagi?", false, false, time.Now()))
		if aiCalls.Load() != 2 {
			t.Fatalf("setelah takeover_pause berakhir AI harus kembali aktif, calls=%d", aiCalls.Load())
		}
	})
}

type staticHealthExec struct{}

func (staticHealthExec) Ready(string) bool { return true }
func (staticHealthExec) NewMessageID(string) (string, error) {
	return "3EB0AI_TEST", nil
}
func (staticHealthExec) Execute(context.Context, *outbox.Job) (outbox.ExecResult, error) {
	return outbox.ExecResult{ServerTS: time.Now()}, nil
}

type stubGuardianAdapter struct {
	st *store.Store
}

func (a stubGuardianAdapter) InstanceMeta(ctx context.Context, id string) (store.Instance, bool) {
	inst, err := a.st.GetInstance(ctx, id)
	if err != nil || inst == nil {
		return store.Instance{}, false
	}
	return *inst, true
}
func (a stubGuardianAdapter) ListInstances(ctx context.Context) ([]store.Instance, error) {
	return a.st.ListInstances(ctx)
}
func (a stubGuardianAdapter) SetShadowUntil(ctx context.Context, id string, until int64) error {
	inst, err := a.st.GetInstance(ctx, id)
	if err != nil {
		return err
	}
	s := inst.Settings
	s.GuardianShadowUntil = until
	_, err = a.st.UpdateInstance(ctx, id, store.InstanceUpdate{Settings: &s})
	return err
}
func (a stubGuardianAdapter) IsSavedContact(context.Context, string, types.JID) bool {
	return false
}
func (a stubGuardianAdapter) ResolvePN(context.Context, string, types.JID) (types.JID, bool) {
	return types.EmptyJID, false
}
func (a stubGuardianAdapter) EnqueueSystemText(context.Context, string, string, string, string) error {
	return nil
}
func (a stubGuardianAdapter) EmitHealth(context.Context, string, guardian.HealthEventPayload) error {
	return nil
}

// TestAI_DitahanGuardian memastikan kiriman ai_reply lewat outbox tetap ditolak saat akun dalam state restricted untuk first-contact.
func TestAI_DitahanGuardian(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, _ := store.Open(ctx, cfg.AppDBPath())
	defer st.Close()
	j, _ := journal.Open(ctx, cfg.JournalDBPath())
	defer j.Close()

	settings := store.Defaults()
	settings.GuardianShadowUntil = 0
	_ = st.CreateInstance(ctx, store.Instance{ID: "sales", RatePerMin: 60, Settings: settings})
	_, _ = st.UpdateInstance(ctx, "sales", store.InstanceUpdate{Settings: &settings})
	_ = st.UpdateInstanceStatus(ctx, "sales", store.StatusConnected, "628000@s.whatsapp.net")

	// Turunkan skor Guardian ke < 40 (restricted) lewat 60 pesan first-contact undelivered di jam lampau.
	pastHour := time.Now().Add(-2*time.Hour).Unix() / 3600
	_, err := j.DB().ExecContext(ctx, `
		INSERT INTO guardian_hourly (instance_id, hour, sent, first_contact_sent, delivered, replied, groups_created, rate_overlimit)
		VALUES ('sales', ?, 60, 60, 0, 0, 0, 0)`, pastHour)
	if err != nil {
		t.Fatal(err)
	}

	guard, err := guardian.New(j.DB(), guardian.Options{
		Settings: stubGuardianAdapter{st: st},
		Contacts: stubGuardianAdapter{st: st},
		LIDs:     stubGuardianAdapter{st: st},
		Enqueuer: stubGuardianAdapter{st: st},
		Emitter:  stubGuardianAdapter{st: st},
	})
	if err != nil {
		t.Fatal(err)
	}

	rep, err := guard.Health(ctx, "sales")
	if err != nil || rep.State != guardian.StateRestricted {
		t.Fatalf("diharapkan state restricted untuk skor rendah, didapat %+v (err=%v)", rep, err)
	}

	ob := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, staticHealthExec{}, guard, nil, outbox.Options{})
	defer ob.Stop()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"Balasan AI"}}]}`))
	}))
	defer srv.Close()

	aiCli := ai.NewClient(ai.Config{BaseURL: srv.URL, Model: "m"})
	spec, _ := json.Marshal(Rule{
		ID: "ai-g", On: []string{"message"}, Do: []Action{{Action: "ai_reply", System: "CS"}},
	})
	_ = st.UpsertHook(ctx, store.HookRecord{InstanceID: "sales", ID: "ai-g", Enabled: true, Priority: 10, Spec: string(spec)})

	eng := NewEngine(EngineDeps{AppStore: st, Journal: j, Outbox: ob, AI: aiCli})
	_ = eng.ReloadInstance(ctx, "sales")

	// Penerima baru tanpa last_in 24 jam harus ditolak Guardian restricted (0 job outbox masuk)
	ev := mustMsgEvent("sales", 1, "628999111222@s.whatsapp.net", "628999111222@s.whatsapp.net", "628999111222@s.whatsapp.net", "Baru", "hai", false, false, time.Now())
	_ = eng.ProcessOne(ctx, "sales", ev)

	queued, _ := ob.CountQueued(ctx, "sales")
	if queued != 0 {
		t.Fatalf("Guardian restricted wajib menolak enqueue outbox dari ai_reply, queued=%d", queued)
	}
	runs := NewSQLiteRunStore(j.DB())
	r, _, _ := runs.GetRun(ctx, "sales", 1, "ai-g")
	if r.Status != "error" || !strings.Contains(strings.ToLower(r.Detail), "restricted") {
		t.Fatalf("eksekusi diblokir Guardian harus tercatat error restricted: %+v", r)
	}
}

// TestAI_RedPressureDilewati memastikan governor Red melewati panggilan HTTP AI sebagai skipped_pressure dan tetap mengirim fallback_text jika ada.
func TestAI_RedPressureDilewati(t *testing.T) {
	ctx := context.Background()
	var aiCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		aiCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"tidak terpanggil"}}]}`))
	}))
	defer srv.Close()

	aiCli := ai.NewClient(ai.Config{BaseURL: srv.URL, Model: "m"})
	rules := newMemRuleStore()
	rules.Set("sales",
		Rule{ID: "ai-nofb", Priority: intPtr(10), On: []string{"message"}, Do: []Action{{Action: "ai_reply", System: "CS"}}},
		Rule{ID: "ai-fb", Priority: intPtr(20), On: []string{"message"}, Do: []Action{{Action: "ai_reply", System: "CS", FallbackText: "Server sibuk, tunggu ya."}}},
	)
	runs := newMemRunStore()
	ob := newFakeOutbox()
	gov := &stubGovernor{}
	gov.lvl.Store(int32(governor.Red))

	eng := NewEngine(EngineDeps{
		Rules:    rules,
		Runs:     runs,
		Outbox:   ob,
		Governor: gov,
		AI:       aiCli,
		Settings: staticSettingsReader{settings: store.Defaults(), selfJID: "628000@s.whatsapp.net"},
	})
	_ = eng.ReloadInstance(ctx, "sales")

	ev := mustMsgEvent("sales", 10, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "Adi", "hai", false, false, time.Now())
	if err := eng.ProcessOne(ctx, "sales", ev); err != nil {
		t.Fatal(err)
	}

	if aiCalls.Load() != 0 {
		t.Fatalf("pada level governor Red endpoint AI dilarang dipanggil")
	}
	rNo, _, _ := runs.GetRun(ctx, "sales", 10, "ai-nofb")
	rFb, _, _ := runs.GetRun(ctx, "sales", 10, "ai-fb")
	if rNo.Status != "skipped_pressure" || rFb.Status != "skipped_pressure" {
		t.Fatalf("status hook_runs saat Red harus skipped_pressure: rNo=%+v rFb=%+v", rNo, rFb)
	}
	reqs := ob.Requests()
	if len(reqs) != 1 || reqs[0].Spec.Text != "Server sibuk, tunggu ya." || reqs[0].Origin.Kind != "ai" || reqs[0].IdemKey != "ai:ai-fb:10" {
		t.Fatalf("fallback_text saat Red wajib tetap di-enqueue: %+v", reqs)
	}
}

// TestAI_DedupePesanPemicuDariContextStore memastikan pesan pemicu yang sudah tercatat di ctxstore tidak dikirim dua kali dalam prompt LLM.
func TestAI_DedupePesanPemicuDariContextStore(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, _ := store.Open(ctx, cfg.AppDBPath())
	defer st.Close()
	j, _ := journal.Open(ctx, cfg.JournalDBPath())
	defer j.Close()

	_ = st.CreateInstance(ctx, store.Instance{ID: "sales", RatePerMin: 60})
	settings := store.Defaults()
	settings.ContextStore = true
	_, _ = st.UpdateInstance(ctx, "sales", store.InstanceUpdate{Settings: &settings})
	cs := ctxstore.New(j.DB(), ctxstore.NewStoreSettings(st))

	// Simulasikan RecordHook sudah menulis pesan pemicu ke ctxstore sebelum hooks dievaluasi.
	ev := mustMsgEvent("sales", 1, "628111@s.whatsapp.net", "628111@s.whatsapp.net", "628111@s.whatsapp.net", "Rina", "Harga kopi?", false, false, time.Now())
	_, _ = j.Append(ctx, &ev, cs.RecordHook(&ev))

	var sentMsgs []ai.ChatMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []ai.ChatMessage `json:"messages"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		sentMsgs = body.Messages
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"Rp 25.000"}}]}`))
	}))
	defer srv.Close()

	aiCli := ai.NewClient(ai.Config{BaseURL: srv.URL, Model: "m"})
	spec, _ := json.Marshal(Rule{
		ID: "ai-dup", On: []string{"message"}, Do: []Action{{Action: "ai_reply", System: "CS", ContextMessages: 5, Temperature: strPtrFloat(0.2)}},
	})
	_ = st.UpsertHook(ctx, store.HookRecord{InstanceID: "sales", ID: "ai-dup", Enabled: true, Priority: 10, Spec: string(spec)})

	ob := newFakeOutbox()
	eng := NewEngine(EngineDeps{AppStore: st, Journal: j, Outbox: ob, CtxStore: cs, AI: aiCli})
	_ = eng.ReloadInstance(ctx, "sales")
	_ = eng.ProcessOne(ctx, "sales", ev)

	userCount := 0
	for _, m := range sentMsgs {
		if m.Role == "user" && strings.Contains(m.Content, "Harga kopi?") {
			userCount++
		}
	}
	if userCount != 1 {
		t.Fatalf("pesan pemicu tidak boleh terduplikasi dari ctxstore (expected 1, got %d): %+v", userCount, sentMsgs)
	}
}
