package guardian

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow/types"
)

type fakeEnqueuerItem struct {
	instanceID string
	to         string
	text       string
	idemKey    string
}

type fakeOutboxEnqueuer struct {
	mu    sync.Mutex
	items []fakeEnqueuerItem
}

func (f *fakeOutboxEnqueuer) EnqueueSystemText(_ context.Context, instanceID, to, text, idemKey string) error {
	f.mu.Lock()
	f.items = append(f.items, fakeEnqueuerItem{instanceID: instanceID, to: to, text: text, idemKey: idemKey})
	f.mu.Unlock()
	return nil
}

type fakeMetaSource struct {
	mu        sync.Mutex
	instances map[string]store.Instance
}

func (f *fakeMetaSource) InstanceMeta(_ context.Context, instanceID string) (store.Instance, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.instances[instanceID]
	return it, ok
}

func (f *fakeMetaSource) ListInstances(_ context.Context) ([]store.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Instance, 0, len(f.instances))
	for _, it := range f.instances {
		out = append(out, it)
	}
	return out, nil
}

func (f *fakeMetaSource) SetShadowUntil(_ context.Context, instanceID string, until int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	it, ok := f.instances[instanceID]
	if !ok {
		return store.ErrNotFound
	}
	it.Settings.GuardianShadowUntil = until
	f.instances[instanceID] = it
	return nil
}

type fakeContactBook struct {
	mu    sync.Mutex
	saved map[string]bool
	hits  int
}

func (f *fakeContactBook) IsSavedContact(_ context.Context, instanceID string, target types.JID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
	return f.saved[instanceID+"|"+target.ToNonAD().String()]
}

type fakeLIDLookup map[string]types.JID

func (f fakeLIDLookup) ResolvePN(_ context.Context, _ string, j types.JID) (types.JID, bool) {
	p, ok := f[j.ToNonAD().String()]
	return p, ok
}

type fakeHealthEmitter struct {
	mu     sync.Mutex
	events []HealthEventPayload
}

func (f *fakeHealthEmitter) EmitHealth(_ context.Context, _ string, ev HealthEventPayload) error {
	f.mu.Lock()
	f.events = append(f.events, ev)
	f.mu.Unlock()
	return nil
}

func newTestGuardian(t *testing.T, opts ...func(*Options, *fakeMetaSource)) (*Guardian, *journal.Journal, *fakeMetaSource) {
	t.Helper()
	ctx := context.Background()
	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	meta := &fakeMetaSource{
		instances: map[string]store.Instance{
			"sales": {
				ID:         "sales",
				JID:        "628111111111@s.whatsapp.net",
				RatePerMin: 60,
				PairedAt:   time.Now().Add(-30 * 24 * time.Hour).Unix(),
				Settings:   store.Defaults(),
			},
		},
	}
	opt := Options{
		Settings:      meta,
		DisableJitter: true,
	}
	for _, fn := range opts {
		if fn != nil {
			fn(&opt, meta)
		}
	}
	g, err := New(j.DB(), opt)
	if err != nil {
		t.Fatalf("guardian.New gagal: %v", err)
	}
	return g, j, meta
}

// TestDecide_UrutanAturan memverifikasi evaluasi berurutan aturan 1..10 tabel Decide serta preseden aturan sebelumnya.
func TestDecide_UrutanAturan(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	rcptOptOut := "628100000001@s.whatsapp.net"
	rcptCold := "628100000002@s.whatsapp.net"
	rcptWarm := "628100000003@s.whatsapp.net"

	// 1. Aturan 1: opted_out menang atas semua (termasuk saat ban aktif & quiet hours).
	g1, _, m1 := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
		it := m.instances["sales"]
		it.Settings.QuietHours = "00:00-23:59"
		m.instances["sales"] = it
	})
	_ = g1.st.UpsertContactDirect(ctx, "sales", rcptOptOut, 0, 0, 1)
	_ = g1.ObserveTempBan(ctx, "sales", 101, 30*time.Minute)
	if dec := g1.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptOptOut, Origin: "api"}, now); dec.Code != "recipient_opted_out" || !dec.Permanent {
		t.Fatalf("aturan 1 opted_out harus menang atas ban/quiet_hours, didapat %+v", dec)
	}
	_ = m1

	// 2. Aturan 2: ban aktif menang atas restricted.
	if dec := g1.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptCold, Origin: "api"}, now); dec.Code != "guardian_suspended" || dec.Permanent {
		t.Fatalf("aturan 2 ban aktif harus mengembalikan guardian_suspended tunda, didapat %+v", dec)
	}

	// 3. Aturan 3: state restricted & tanpa last_in 24 jam -> permanent guardian_restricted.
	g3, _, _ := newTestGuardian(t)
	g3.forcePenaltyForTest("sales", now, 65, 25, 0) // skor rendah (< 40) -> restricted
	if dec := g3.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptCold, Origin: "api"}, now); dec.Code != "guardian_restricted" || !dec.Permanent {
		t.Fatalf("aturan 3 restricted harus menolak permanen, didapat %+v", dec)
	}
	// Penerima dengan last_in <= 24 jam tetap lolos dari aturan 3.
	_ = g3.st.UpsertContactDirect(ctx, "sales", rcptWarm, now.Add(-1*time.Hour).Unix(), now.Add(-1*time.Hour).Unix(), 0)
	if dec := g3.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptWarm, Kind: "text", Spec: outbox.Spec{Type: "text", Text: "Balasan"}}, now); !dec.Allow {
		t.Fatalf("kontak dengan last_in dalam 24h harus diizinkan saat restricted, didapat %+v", dec)
	}

	// 4. Aturan 4: fan-out konten identik mencapai kuota (strict=10).
	g4, _, _ := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
		it := m.instances["sales"]
		it.Settings.GuardianProfile = ProfileStrict
		m.instances["sales"] = it
	})
	// Jadikan rcptWarm penerima non first-contact namun fan-out penuh untuk teks sama.
	specSame := outbox.Spec{Type: "text", Text: "Promo spesial"}
	day := time.Now().Format("2006-01-02")
	for k := 0; k < 10; k++ {
		_ = g4.st.RecordFanout(ctx, "sales", day, ContentHash("promo spesial"), fmt.Sprintf("6289000%02d@s.whatsapp.net", k))
	}
	_ = g4.st.UpsertContactDirect(ctx, "sales", rcptWarm, now.Unix(), now.Unix(), 0)
	if dec := g4.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptWarm, Kind: "text", Spec: specSame}, now); dec.Code != "guardian_content_fanout" || !dec.Permanent {
		t.Fatalf("aturan 4 fan-out harus menolak permanen, didapat %+v", dec)
	}

	// 5. Aturan 5: first-contact dengan skor < minimal profil (cautious score=45 < min standard=50).
	g5, _, _ := newTestGuardian(t)
	g5.forcePenaltyForTest("sales", now, 55, 0, 0) // 100 - 55 = 45 (cautious, < min standard 50)
	if dec := g5.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptCold, Kind: "text", Spec: outbox.Spec{Type: "text", Text: "Unik 1"}}, now); dec.Code != "guardian_low_score" || !dec.Permanent {
		t.Fatalf("aturan 5 skor < min profil pada first-contact harus guardian_low_score, didapat %+v", dec)
	}

	// 6. Aturan 6: first-contact dan kuota hari ini habis (healthy standard cap=50, saat day >=14).
	g6, _, _ := newTestGuardian(t)
	g6.seedHourlyForTest("sales", now, 50, 50, 50, 20)
	if dec := g6.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptCold, Kind: "text", Spec: outbox.Spec{Type: "text", Text: "Unik 2"}}, now); dec.Code != "guardian_first_contact_cap" || dec.Permanent {
		t.Fatalf("aturan 6 kuota first-contact habis harus tunda guardian_first_contact_cap, didapat %+v", dec)
	}

	// 7. Aturan 7: kuota harian total habis (penerima sudah dikenal agar melewati aturan 6).
	g7, _, _ := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
		it := m.instances["sales"]
		it.Settings.GuardianProfile = ProfileStrict // daily cap 200
		m.instances["sales"] = it
	})
	_ = g7.st.UpsertContactDirect(ctx, "sales", rcptWarm, now.Add(-30*time.Minute).Unix(), now.Add(-30*time.Minute).Unix(), 0)
	g7.seedHourlyForTest("sales", now, 200, 0, 200, 50)
	if dec := g7.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptWarm, Kind: "text", Spec: outbox.Spec{Type: "text", Text: "Unik 3"}}, now); dec.Code != "guardian_daily_cap" || dec.Permanent {
		t.Fatalf("aturan 7 kuota harian habis harus tunda guardian_daily_cap, didapat %+v", dec)
	}

	// 8. Aturan 8: quiet hours aktif pada penerima lama.
	g8, _, _ := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
		it := m.instances["sales"]
		it.Settings.QuietHours = "00:00-23:59"
		m.instances["sales"] = it
	})
	_ = g8.st.UpsertContactDirect(ctx, "sales", rcptWarm, now.Add(-2*time.Hour).Unix(), now.Add(-2*time.Hour).Unix(), 0)
	if dec := g8.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptWarm, Kind: "text", Priority: outbox.PriorityNormal}, now); dec.Code != "quiet_hours" || dec.Permanent {
		t.Fatalf("aturan 8 quiet_hours harus tunda, didapat %+v", dec)
	}

	// 9. Aturan 9: token bucket habis setelah burst 3 terpenuhi.
	g9, _, _ := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
		it := m.instances["sales"]
		it.RatePerMin = 2 // interval 30s
		m.instances["sales"] = it
	})
	_ = g9.st.UpsertContactDirect(ctx, "sales", rcptWarm, now.Unix(), now.Unix(), 0)
	for i := 0; i < 3; i++ {
		if dec := g9.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptWarm, Kind: "text"}, now); !dec.Allow {
			t.Fatalf("burst %d seharusnya diizinkan, didapat %+v", i, dec)
		}
	}
	if dec := g9.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptWarm, Kind: "text"}, now); dec.Allow || dec.Code != "rate" {
		t.Fatalf("aturan 9 setelah burst habis harus tunda code=rate, didapat %+v", dec)
	}

	// 10. Aturan 10: seluruh syarat aman -> Allow=true.
	g10, _, _ := newTestGuardian(t)
	if dec := g10.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: rcptCold, Kind: "text", Spec: outbox.Spec{Type: "text", Text: "Halo"}}, now); !dec.Allow {
		t.Fatalf("aturan 10 harus mengizinkan pesan bersih, didapat %+v", dec)
	}
}

// TestDecide_SuspendedSelamaBan memverifikasi penundaan sampai BanUntil + 10m di dalam synctest.
func TestDecide_SuspendedSelamaBan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		g, _, _ := newTestGuardian(t)

		known := "628123@s.whatsapp.net"
		_ = g.st.UpsertContactDirect(ctx, "sales", known, time.Now().Add(-time.Hour).Unix(), time.Now().Add(-time.Hour).Unix(), 0)

		if err := g.ObserveTempBan(ctx, "sales", 104, 30*time.Minute); err != nil {
			t.Fatalf("ObserveTempBan gagal: %v", err)
		}
		now := time.Now()
		job := &outbox.Job{InstanceID: "sales", ToJID: known, Kind: "text"}
		dec := g.Decide(ctx, job, now)
		if dec.Allow || dec.Permanent || dec.Code != "guardian_suspended" || dec.Delay != 40*time.Minute {
			t.Fatalf("diharapkan tunda 40m (BanUntil 30m + 10m) dengan code guardian_suspended, didapat %+v", dec)
		}

		// Pada menit ke-35 (BanUntil sudah lewat, masih di masa cooldown +10m) -> masih tertunda 5m.
		time.Sleep(35 * time.Minute)
		synctest.Wait()
		decMid := g.Decide(ctx, job, time.Now())
		if decMid.Allow || decMid.Code != "guardian_suspended" || decMid.Delay != 5*time.Minute {
			t.Fatalf("pada menit ke-35 seharusnya masih tunda 5m, didapat %+v", decMid)
		}

		// Lewati 5 menit lagi (tepat BanUntil + 10m) -> cooldown selesai -> diizinkan.
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		decAfter := g.Decide(ctx, job, time.Now())
		if !decAfter.Allow {
			t.Fatalf("setelah BanUntil + 10m harus kembali diizinkan, didapat %+v", decAfter)
		}
	})
}

// TestQuietHours_LintasTengahMalam menguji jendela 21:00-07:00 lintas tengah malam, balasan chat aktif <=10m, dan prioritas urgent.
func TestQuietHours_LintasTengahMalam(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		loc, _ := time.LoadLocation("Asia/Jakarta")
		// Mulai test sebelum pukul 23:00 WIB dengan clock synctest.
		g, _, m := newTestGuardian(t, func(o *Options, m *fakeMetaSource) {
			o.DisableJitter = false
			it := m.instances["sales"]
			it.Settings.Timezone = "Asia/Jakarta"
			it.Settings.QuietHours = "21:00-07:00"
			it.PairedAt = time.Now().Add(-30 * 24 * time.Hour).Unix()
			m.instances["sales"] = it
		})
		_ = m

		cold := "628123450001@s.whatsapp.net"
		activeChat := "628123450002@s.whatsapp.net"

		// Jam 23:00 WIB -> tunda sampai 07:00 (+ jitter 0..15m).
		t2300 := time.Date(2026, 11, 16, 23, 0, 0, 0, loc)
		dec23 := g.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: cold, Priority: outbox.PriorityNormal}, t2300)
		if dec23.Allow || dec23.Code != "quiet_hours" || dec23.Delay < 8*time.Hour || dec23.Delay > 8*time.Hour+15*time.Minute {
			t.Fatalf("jam 23:00 seharusnya tunda ~8h (+<=15m jitter), didapat %+v", dec23)
		}

		// Jam 02:30 WIB (sesudah tengah malam) -> tunda sampai 07:00 hari sama (+ jitter 0..15m).
		t0230 := time.Date(2026, 11, 17, 2, 30, 0, 0, loc)
		dec02 := g.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: cold, Priority: outbox.PriorityNormal}, t0230)
		if dec02.Allow || dec02.Code != "quiet_hours" || dec02.Delay < 4*time.Hour+30*time.Minute || dec02.Delay > 4*time.Hour+45*time.Minute {
			t.Fatalf("jam 02:30 seharusnya tunda ~4h30m (+<=15m jitter), didapat %+v", dec02)
		}

		// Jam 12:00 WIB -> diizinkan.
		t1200 := time.Date(2026, 11, 17, 12, 0, 0, 0, loc)
		if dec12 := g.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: cold, Priority: outbox.PriorityNormal}, t1200); !dec12.Allow {
			t.Fatalf("jam 12:00 di luar quiet_hours harus diizinkan, didapat %+v", dec12)
		}

		// Penerima yang mengirim pesan 5 menit lalu tetap boleh dibalas walau jam 23:00.
		_ = g.st.UpsertContactDirect(ctx, "sales", activeChat, t2300.Add(-5*time.Minute).Unix(), t2300.Add(-5*time.Minute).Unix(), 0)
		if decReply := g.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: activeChat, Priority: outbox.PriorityNormal}, t2300); !decReply.Allow {
			t.Fatalf("penerima yang baru mengirim pesan 5m lalu harus diizinkan meski quiet_hours, didapat %+v", decReply)
		}

		// Prioritas urgent melewati quiet hours.
		if decUrgent := g.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: cold, Priority: outbox.PriorityUrgent}, t2300); !decUrgent.Allow {
			t.Fatalf("pesan priority urgent harus melewati quiet_hours, didapat %+v", decUrgent)
		}
	})
}

// TestFirstContactCap_DenganWarmup memverifikasi kuota first-contact hari 0 profil standard = 12 (50 * 0.25).
func TestFirstContactCap_DenganWarmup(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	g, _, _ := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
		it := m.instances["sales"]
		it.PairedAt = now.Unix() // hari ke-0 -> warmup 0.25 -> first_contact_cap = int(50*0.25) = 12
		m.instances["sales"] = it
	})

	h, err := g.Health(ctx, "sales")
	if err != nil {
		t.Fatalf("Health gagal: %v", err)
	}
	if h.Limits.FirstContactCap != 12 || h.Limits.WarmupFactor != 0.25 {
		t.Fatalf("hari 0 standard diharapkan fc_cap=12 & warmup=0.25, didapat %+v", h.Limits)
	}

	g.seedHourlyForTest("sales", now, 12, 12, 12, 0)
	dec := g.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: "6289990001@s.whatsapp.net", Kind: "text"}, now)
	if dec.Allow || dec.Permanent || dec.Code != "guardian_first_contact_cap" {
		t.Fatalf("pesan ke-13 pada hari 0 harus ditunda guardian_first_contact_cap, didapat %+v", dec)
	}
}

// TestKanonik_LIDdanPNSatuKontak memastikan pesan masuk lewat LID dan keluar lewat PN memakai satu baris contacts_seen dan menaikkan replied.
func TestKanonik_LIDdanPNSatuKontak(t *testing.T) {
	ctx := context.Background()
	pn := types.NewJID("628123456789", types.DefaultUserServer)
	lid := types.NewJID("999888777", types.HiddenUserServer)

	g, j, _ := newTestGuardian(t, func(o *Options, _ *fakeMetaSource) {
		o.LIDs = fakeLIDLookup{lid.String(): pn}
	})

	at1 := time.Now().Add(-5 * time.Minute).UnixMilli()
	evIn1 := &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       pn.String(),
		Sender:     lid.String(),
		WAID:       "IN1",
		DedupeKey:  "m:kan1",
		TS:         at1 / 1000,
		ReceivedAt: at1,
		Payload:    mustJSON(t, map[string]any{"id": "IN1", "from": lid.String(), "sender_pn": pn.String(), "type": "text", "text": "Halo"}),
	}
	if _, err := j.Append(ctx, evIn1, g.InboundHook(ctx, evIn1, nil)); err != nil {
		t.Fatalf("Append pesan LID gagal: %v", err)
	}

	// Kirim keluar ke PN -> harus melengkapi baris PN yang sama (bukan duplikat baris LID).
	g.Record(ctx, &outbox.Job{
		InstanceID: "sales",
		ToJID:      pn.String(),
		Kind:       "text",
		Spec:       outbox.Spec{Type: "text", To: pn.String(), Text: "Ya ada yang bisa dibantu?"},
	}, outbox.Outcome{Success: true})

	// Pesan masuk kedua lewat LID lagi -> last_out > last_in sebelumnya -> replied++!
	at2 := time.Now().UnixMilli()
	evIn2 := &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       pn.String(),
		Sender:     lid.String(),
		WAID:       "IN2",
		DedupeKey:  "m:kan2",
		TS:         at2 / 1000,
		ReceivedAt: at2,
		Payload:    mustJSON(t, map[string]any{"id": "IN2", "from": lid.String(), "sender_pn": pn.String(), "type": "text", "text": "Stok ada?"}),
	}
	if _, err := j.Append(ctx, evIn2, g.InboundHook(ctx, evIn2, nil)); err != nil {
		t.Fatalf("Append balasan kedua gagal: %v", err)
	}

	var count int
	if err := j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM contacts_seen WHERE instance_id = 'sales'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("LID dan PN harus menjadi 1 baris contacts_seen kanonik, didapat %d", count)
	}
	h, err := g.Health(ctx, "sales")
	if err != nil {
		t.Fatal(err)
	}
	if h.Counters.Replied24h != 1 {
		t.Fatalf("replied_24h diharapkan 1 setelah balasan ke outbound terakhir, didapat %d", h.Counters.Replied24h)
	}
}

// TestCounters_BertahanSetelahRestart memastikan counter 24 jam bertahan setelah flush ke SQLite dan rekonstruksi Guardian baru.
func TestCounters_BertahanSetelahRestart(t *testing.T) {
	ctx := context.Background()
	g1, j, meta := newTestGuardian(t)

	for k := 0; k < 4; k++ {
		g1.Record(ctx, &outbox.Job{
			InstanceID: "sales",
			ToJID:      fmt.Sprintf("6285000%04d@s.whatsapp.net", k),
			Kind:       "text",
			Spec:       outbox.Spec{Type: "text", Text: fmt.Sprintf("Pesan %d", k)},
		}, outbox.Outcome{Success: true})
	}
	g1.ObserveDelivered("sales", 3)
	if err := g1.Flush(ctx); err != nil {
		t.Fatalf("Flush pertama gagal: %v", err)
	}

	// Buat Guardian baru di DB yang sama (simulasi restart daemon).
	g2, err := New(j.DB(), Options{Settings: meta, DisableJitter: true})
	if err != nil {
		t.Fatalf("New Guardian pasca restart gagal: %v", err)
	}
	h, err := g2.Health(ctx, "sales")
	if err != nil {
		t.Fatal(err)
	}
	if h.Counters.Sent24h != 4 || h.Counters.FirstContact24h != 4 || h.Counters.Delivered24h != 3 {
		t.Fatalf("counter setelah restart tidak cocok: %+v", h.Counters)
	}
}

// TestPrecheck_KodeHTTP memastikan Precheck mengembalikan 409/403/429 yang tepat untuk aturan 1, 3, dan 4.
func TestPrecheck_KodeHTTP(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	g, _, _ := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
		it := m.instances["sales"]
		it.Settings.GuardianProfile = ProfileStrict
		m.instances["sales"] = it
	})

	// 409 recipient_opted_out
	_ = g.st.UpsertContactDirect(ctx, "sales", "6281@s.whatsapp.net", 0, 0, 1)
	err409 := g.Precheck(ctx, &outbox.Job{InstanceID: "sales", ToJID: "6281@s.whatsapp.net", Origin: "api", Spec: outbox.Spec{Type: "text", Text: "x"}})
	var ge *Error
	if !errors.Is(err409, ErrRecipientOptedOut) || !errors.As(err409, &ge) || ge.Status != http.StatusConflict {
		t.Fatalf("aturan 1 diharapkan 409 ErrRecipientOptedOut, didapat %v", err409)
	}

	// 403 guardian_restricted
	g.forcePenaltyForTest("sales", now, 65, 25, 0)
	err403 := g.Precheck(ctx, &outbox.Job{InstanceID: "sales", ToJID: "6282@s.whatsapp.net", Origin: "api", Spec: outbox.Spec{Type: "text", Text: "x"}})
	if !errors.Is(err403, ErrGuardianRestricted) || !errors.As(err403, &ge) || ge.Status != http.StatusForbidden {
		t.Fatalf("aturan 3 diharapkan 403 ErrGuardianRestricted, didapat %v", err403)
	}

	// 429 guardian_limited (fan-out konten identik pada penerima yang tidak restricted/punya last_in)
	g429, _, _ := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
		it := m.instances["sales"]
		it.Settings.GuardianProfile = ProfileStrict // fanout cap = 10
		m.instances["sales"] = it
	})
	day := now.Format("2006-01-02")
	for k := 0; k < 10; k++ {
		_ = g429.st.RecordFanout(ctx, "sales", day, ContentHash("pesan blast"), fmt.Sprintf("62870%02d@s.whatsapp.net", k))
	}
	err429 := g429.Precheck(ctx, &outbox.Job{InstanceID: "sales", ToJID: "6283@s.whatsapp.net", Kind: "text", Spec: outbox.Spec{Type: "text", Text: "  Pesan  Blast "}})
	if !errors.Is(err429, ErrGuardianLimited) || !errors.As(err429, &ge) || ge.Status != http.StatusTooManyRequests || ge.RetryAfter <= 0 {
		t.Fatalf("aturan 4 diharapkan 429 ErrGuardianLimited dengan RetryAfter > 0, didapat %v", err429)
	}
}

// TestGroupGovernor_KuotaGrup memverifikasi grup ke-6 pada hari yang sama (profil standard) ditolak 429 guardian_limited danAllowParticipantAdd ditolak saat restricted.
func TestGroupGovernor_KuotaGrup(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	g, _, _ := newTestGuardian(t)

	for i := 1; i <= 5; i++ {
		if err := g.AllowGroupCreate(ctx, "sales", []string{"628111111111"}); err != nil {
			t.Fatalf("grup ke-%d seharusnya diizinkan, didapat %v", i, err)
		}
		g.RecordGroupCreated(ctx, "sales")
	}

	err6 := g.AllowGroupCreate(ctx, "sales", []string{"628111111111"})
	var ge *Error
	if !errors.Is(err6, ErrGuardianLimited) || !errors.As(err6, &ge) || ge.Status != http.StatusTooManyRequests || ge.RetryAfter <= 0 {
		t.Fatalf("grup ke-6 pada profil standard harus ditolak 429 guardian_limited, didapat %v", err6)
	}

	// Saat state restricted, AllowParticipantAdd ke kontak baru harus ditolak 403.
	g.forcePenaltyForTest("sales", now, 65, 25, 0)
	errAdd := g.AllowParticipantAdd(ctx, "sales", []string{"628999999999@s.whatsapp.net"})
	if !errors.Is(errAdd, ErrGuardianRestricted) || !errors.As(errAdd, &ge) || ge.Status != http.StatusForbidden {
		t.Fatalf("AllowParticipantAdd saat restricted diharapkan 403 guardian_restricted, didapat %v", errAdd)
	}
}

// TestFirstContact_KontakBukuAlamatBukanFirstContact memverifikasi kontak buku alamat dengan FullName terisi melewati aturan 5 dan 6 serta memakai cache LRU 4096.
func TestFirstContact_KontakBukuAlamatBukanFirstContact(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	savedJID := "628123450099@s.whatsapp.net"
	book := &fakeContactBook{saved: map[string]bool{"sales|" + savedJID: true}}

	g, _, _ := newTestGuardian(t, func(o *Options, m *fakeMetaSource) {
		o.Contacts = book
		it := m.instances["sales"]
		it.PairedAt = now.Unix() // kuota FC hari 0 = 12
		m.instances["sales"] = it
	})
	g.seedHourlyForTest("sales", now, 12, 12, 12, 0) // kuota first-contact hari ini habis

	job := &outbox.Job{InstanceID: "sales", ToJID: savedJID, Kind: "text", Spec: outbox.Spec{Type: "text", Text: "Halo pelanggan"}}
	if dec := g.Decide(ctx, job, now); !dec.Allow {
		t.Fatalf("kontak tersimpan di buku alamat bukan first-contact sehingga tidak boleh kena aturan 6, didapat %+v", dec)
	}
	// Panggilan kedua harus hit cache LRU 4096 tanpa menaikkan hits fakeContactBook.
	if dec := g.Decide(ctx, job, now); !dec.Allow {
		t.Fatalf("panggilan kedua gagal: %+v", dec)
	}
	if book.hits != 1 {
		t.Fatalf("cache LRU kontak buku alamat seharusnya hanya memanggil ContactBook 1 kali, didapat %d", book.hits)
	}
}

// TestShadowMode_HanyaMencatat memverifikasi bootstrap +7 hari shadow pada instance lama, pencatatan metric shadow, opt-out tetap ditolak, dan penegakan setelah 7 hari di synctest.
func TestShadowMode_HanyaMencatat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		g, _, meta := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
			it := m.instances["sales"]
			it.PairedAt = time.Now().Unix() // warmup day 0 -> fc cap = 12
			it.Settings.GuardianShadowUntil = 0
			m.instances["sales"] = it
		})

		beforeMetric := obs.MetricGuardianShadowTotal.With("guardian_first_contact_cap").Value()

		// Bootstrap harus menyetel guardian_shadow_until = now + 7 hari sekali untuk instance lama JID != "".
		g.BootstrapShadow(ctx)
		mSales, _ := meta.InstanceMeta(ctx, "sales")
		if mSales.Settings.GuardianShadowUntil <= time.Now().Unix() {
			t.Fatalf("BootstrapShadow harus mengisi guardian_shadow_until +7 hari, didapat %d", mSales.Settings.GuardianShadowUntil)
		}

		now := time.Now()
		g.seedHourlyForTest("sales", now, 12, 12, 12, 0)
		coldJob := &outbox.Job{InstanceID: "sales", ToJID: "628880001@s.whatsapp.net", Kind: "text", Origin: "api"}

		// Saat shadow aktif: aturan 6 cocok -> tetap diizinkan + metric shadow naik.
		if dec := g.Decide(ctx, coldJob, now); !dec.Allow {
			t.Fatalf("selama shadow aktif aturan 6 harus tetap diizinkan, didapat %+v", dec)
		}
		if after := obs.MetricGuardianShadowTotal.With("guardian_first_contact_cap").Value(); after != beforeMetric+1 {
			t.Fatalf("waapi_guardian_shadow_total seharusnya bertambah 1, sebelum=%v sesudah=%v", beforeMetric, after)
		}

		// GroupGovernor saat shadow aktif -> catat shadow & izinkan walau grup ke-6.
		for i := 0; i < 6; i++ {
			if err := g.AllowGroupCreate(ctx, "sales", nil); err != nil {
				t.Fatalf("AllowGroupCreate selama shadow harus diizinkan, iter=%d err=%v", i, err)
			}
			g.RecordGroupCreated(ctx, "sales")
		}

		// Aturan 1 (opt-out) tetap ditegakkan walau shadow aktif!
		_ = g.st.UpsertContactDirect(ctx, "sales", "628880002@s.whatsapp.net", 0, 0, 1)
		if decOpt := g.Decide(ctx, &outbox.Job{InstanceID: "sales", ToJID: "628880002@s.whatsapp.net", Origin: "api"}, now); decOpt.Allow || decOpt.Code != "recipient_opted_out" {
			t.Fatalf("opt-out harus tetap ditolak selama shadow, didapat %+v", decOpt)
		}

		// Lewati 7 hari penuh di synctest -> kuota hari ke-7 (warmup 0.75, cap 37) harus menegakkan aturan 6.
		time.Sleep(7*24*time.Hour + time.Second)
		synctest.Wait()
		later := time.Now()
		g.seedHourlyForTest("sales", later, 37, 37, 37, 0)
		if decAfter := g.Decide(ctx, coldJob, later); decAfter.Allow || decAfter.Code != "guardian_first_contact_cap" {
			t.Fatalf("setelah 7 hari shadow berakhir aturan 6 wajib menunda, didapat %+v", decAfter)
		}

		// Jika operator menyetel guardian_shadow_until = 0, bootstrap ulang tidak boleh mengisinya lagi;
		// begitu pula instance yang di-pairing sesudah Guardian aktif.
		_ = meta.SetShadowUntil(ctx, "sales", 0)
		g.OnInstancePaired(ctx, "fresh-pair")
		meta.mu.Lock()
		meta.instances["fresh-pair"] = store.Instance{
			ID:       "fresh-pair",
			JID:      "628999999@s.whatsapp.net",
			PairedAt: later.Unix(),
			Settings: store.Defaults(),
		}
		meta.mu.Unlock()
		g.BootstrapShadow(ctx)
		if ms, _ := meta.InstanceMeta(ctx, "sales"); ms.Settings.GuardianShadowUntil != 0 {
			t.Fatalf("shadow=0 manual pada sales tidak boleh ditimpa bootstrap, didapat %d", ms.Settings.GuardianShadowUntil)
		}
		if mf, _ := meta.InstanceMeta(ctx, "fresh-pair"); mf.Settings.GuardianShadowUntil != 0 {
			t.Fatalf("pairing baru pasca-Guardian tidak boleh mendapat auto-shadow, didapat %d", mf.Settings.GuardianShadowUntil)
		}
	})
}

// TestOptOut_TahanCrash memverifikasi rollback transaksi journal juga membatalkan opt-out di contacts_seen serta tidak mengubah RAM atau mengirim konfirmasi.
func TestOptOut_TahanCrash(t *testing.T) {
	ctx := context.Background()
	enq := &fakeOutboxEnqueuer{}
	g, j, _ := newTestGuardian(t, func(o *Options, m *fakeMetaSource) {
		o.Enqueuer = enq
		it := m.instances["sales"]
		it.Settings.OptoutConfirmText = "Berhenti diterima"
		m.instances["sales"] = it
	})

	rcpt := "628555444333@s.whatsapp.net"
	// Siapkan last_out sebelumnya agar bisa memeriksa replied RAM tidak bertambah bila rollback.
	_ = g.st.UpsertContactDirect(ctx, "sales", rcpt, 0, time.Now().Unix(), 0)

	ev := &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       rcpt,
		Sender:     rcpt,
		WAID:       "CRASH1",
		DedupeKey:  "m:crash1",
		TS:         time.Now().Unix(),
		ReceivedAt: time.Now().UnixMilli(),
		Payload:    mustJSON(t, map[string]any{"id": "CRASH1", "from": rcpt, "sender_pn": rcpt, "type": "text", "text": "STOP"}),
	}

	if _, err := j.Append(ctx, ev, g.InboundHook(ctx, ev, nil), func(_ context.Context, _ *sql.Tx, _ int64) error {
		return errors.New("simulasi crash")
	}); err == nil {
		t.Fatal("Append diharapkan gagal karena TxHook kedua gagal")
	}

	// Tidak boleh ada event message/consent tersimpan dan contacts_seen.opted_out tetap 0!
	res, _ := j.Read(ctx, journal.Query{InstanceID: "sales", Limit: 20})
	if len(res.Events) != 0 {
		t.Fatalf("event journal seharusnya ikut ter-rollback, didapat %d baris", len(res.Events))
	}
	var optedOut int
	if err := j.DB().QueryRowContext(ctx, `SELECT opted_out FROM contacts_seen WHERE instance_id='sales' AND jid=?`, rcpt).Scan(&optedOut); err != nil || optedOut != 0 {
		t.Fatalf("contacts_seen.opted_out seharusnya tetap 0 pasca rollback, didapat %d err=%v", optedOut, err)
	}
	if len(enq.items) != 0 {
		t.Fatalf("pesan konfirmasi opt-out tidak boleh di-enqueue bila transaksi rollback, didapat %+v", enq.items)
	}
	h, _ := g.Health(ctx, "sales")
	if h.Counters.Replied24h != 0 {
		t.Fatalf("counter replied RAM tidak boleh berubah pasca rollback, didapat %d", h.Counters.Replied24h)
	}
}

// TestAcceptance_FirstContact100Hari0 mensimulasikan 100 pesan first-contact hari 0 profil standard -> 12 terkirim, 88 tertunda guardian_first_contact_cap.
func TestAcceptance_FirstContact100Hari0(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Now()
		g, _, _ := newTestGuardian(t, func(_ *Options, m *fakeMetaSource) {
			it := m.instances["sales"]
			it.PairedAt = now.Unix()
			it.RatePerMin = 60000 // agar token bucket tidak membatasi simulasi acceptance
			m.instances["sales"] = it
		})

		sent := 0
		deferred := 0
		for i := 0; i < 100; i++ {
			rcpt := fmt.Sprintf("62877000%04d@s.whatsapp.net", i)
			job := &outbox.Job{
				InstanceID: "sales",
				ToJID:      rcpt,
				Kind:       "text",
				Spec:       outbox.Spec{Type: "text", To: rcpt, Text: fmt.Sprintf("Pesan unik ke-%d", i)},
			}
			sendAt := now.Add(time.Duration(i) * 5 * time.Millisecond)
			dec := g.Decide(ctx, job, sendAt)
			if dec.Allow {
				sent++
				g.Record(ctx, job, outbox.Outcome{Success: true, Attempts: 1})
			} else if dec.Code == "guardian_first_contact_cap" && !dec.Permanent {
				deferred++
			} else {
				t.Fatalf("keputusan tidak terduga pada pesan ke-%d: %+v", i, dec)
			}
		}

		if sent != 12 || deferred != 88 {
			t.Fatalf("diharapkan 12 terkirim dan 88 tertunda guardian_first_contact_cap, didapat sent=%d deferred=%d", sent, deferred)
		}
		h, err := g.Health(ctx, "sales")
		if err != nil {
			t.Fatal(err)
		}
		if h.Limits.FirstContactCap != 12 || h.Limits.FirstContactRemaining != 0 || h.Counters.FirstContact24h != 12 {
			t.Fatalf("ringkasan Health() tidak mencerminkan batas 12 terpakai penuh: %+v", h)
		}
	})
}
