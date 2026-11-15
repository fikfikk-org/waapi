package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

// fakeJournal menyediakan implementasi journal di memori agar logika waktu deliverer bisa diuji di dalam synctest.
type fakeJournal struct {
	mu      sync.Mutex
	nextSeq int64
	events  []journal.Event
	cursors map[string]int64
	enabled map[string]bool
	subs    map[string][]chan struct{}
	commits []int64
}

func newFakeJournal() *fakeJournal {
	return &fakeJournal{
		cursors: make(map[string]int64),
		enabled: make(map[string]bool),
		subs:    make(map[string][]chan struct{}),
	}
}

func fakeCursorKey(consumer, inst string, lane int) string {
	return consumer + ":" + inst + ":" + strconv.Itoa(lane)
}

func (f *fakeJournal) appendEvent(ev journal.Event) int64 {
	f.mu.Lock()
	f.nextSeq++
	ev.Seq = f.nextSeq
	if ev.TS == 0 {
		ev.TS = time.Now().Unix()
	}
	if ev.Part == 0 && ev.Chat != "" {
		ev.Part = journal.PartForChat(ev.Chat)
	}
	f.events = append(f.events, ev)
	subs := append([]chan struct{}(nil), f.subs[ev.InstanceID]...)
	f.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return ev.Seq
}

func (f *fakeJournal) Read(_ context.Context, q journal.Query) (journal.ReadResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var head int64
	for _, ev := range f.events {
		if ev.InstanceID == q.InstanceID && ev.Seq > head {
			head = ev.Seq
		}
	}
	if head <= q.AfterSeq {
		return journal.ReadResult{ScannedTo: q.AfterSeq, HasMore: false}, nil
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 32
	}
	typeAllowed := func(t string) bool {
		if len(q.Types) == 0 {
			return true
		}
		for _, want := range q.Types {
			if t == want {
				return true
			}
		}
		return false
	}

	var out []journal.Event
	totalBytes := 0
	truncated := false
	for _, ev := range f.events {
		if ev.InstanceID != q.InstanceID || ev.Seq <= q.AfterSeq || ev.Seq > head {
			continue
		}
		if !typeAllowed(ev.Type) {
			continue
		}
		if q.Lanes > 0 && (int(ev.Part)%q.Lanes) != q.Lane {
			continue
		}
		if len(out) >= limit {
			truncated = true
			break
		}
		if q.MaxBytes > 0 && len(out) > 0 && totalBytes+len(ev.Payload) > q.MaxBytes {
			truncated = true
			break
		}
		totalBytes += len(ev.Payload)
		out = append(out, ev)
	}
	scannedTo := head
	if truncated && len(out) > 0 {
		scannedTo = out[len(out)-1].Seq
	}
	return journal.ReadResult{Events: out, ScannedTo: scannedTo, HasMore: truncated}, nil
}

func (f *fakeJournal) Subscribe(instanceID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	f.mu.Lock()
	f.subs[instanceID] = append(f.subs[instanceID], ch)
	f.mu.Unlock()
	return ch, func() {
		f.mu.Lock()
		list := f.subs[instanceID]
		for i, c := range list {
			if c == ch {
				f.subs[instanceID] = append(list[:i], list[i+1:]...)
				break
			}
		}
		f.mu.Unlock()
	}
}

func (f *fakeJournal) Cursor(_ context.Context, consumer, instanceID string, lane int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cursors[fakeCursorKey(consumer, instanceID, lane)], nil
}

func (f *fakeJournal) CommitCursor(_ context.Context, consumer, instanceID string, lane int, seq int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := fakeCursorKey(consumer, instanceID, lane)
	if seq > f.cursors[k] {
		f.cursors[k] = seq
	}
	f.enabled[k] = true
	f.commits = append(f.commits, seq)
	return nil
}

func (f *fakeJournal) SetConsumerEnabled(_ context.Context, consumer, instanceID string, enabled bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := consumer + ":" + instanceID + ":"
	for k := range f.cursors {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			f.enabled[k] = enabled
		}
	}
	return nil
}

func (f *fakeJournal) EnsureConsumerCursors(_ context.Context, consumer, instanceID string, lanes int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for l := 0; l < lanes; l++ {
		k := fakeCursorKey(consumer, instanceID, l)
		if _, exists := f.cursors[k]; !exists {
			f.cursors[k] = 0
		}
		f.enabled[k] = true
	}
	return nil
}

func (f *fakeJournal) ReconfigureConsumerLanes(_ context.Context, consumer, instanceID string, lanes int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := consumer + ":" + instanceID + ":"
	var minSeq int64
	first := true
	for k, v := range f.cursors {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			if first || v < minSeq {
				minSeq = v
				first = false
			}
			delete(f.cursors, k)
			delete(f.enabled, k)
		}
	}
	for l := 0; l < lanes; l++ {
		k := fakeCursorKey(consumer, instanceID, l)
		f.cursors[k] = minSeq
		f.enabled[k] = true
	}
	return nil
}

type fakeDeadStore struct {
	mu      sync.Mutex
	entries []DeadEntry
}

func (d *fakeDeadStore) Record(_ context.Context, entry DeadEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = append(d.entries, entry)
	return nil
}

func (d *fakeDeadStore) list() []DeadEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DeadEntry, len(d.entries))
	copy(out, d.entries)
	return out
}

type fakePauser struct {
	mu     sync.Mutex
	paused map[string]string
}

func (p *fakePauser) PauseWebhook(_ context.Context, instanceID, reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused == nil {
		p.paused = make(map[string]string)
	}
	p.paused[instanceID] = reason
	return nil
}

func findChatForLane(lane, lanes int) string {
	for i := 0; i < 1000; i++ {
		chat := fmt.Sprintf("6281000%04d@s.whatsapp.net", i)
		if int(journal.PartForChat(chat))%lanes == lane {
			return chat
		}
	}
	return "fallback@s.whatsapp.net"
}

// TestDeliverer_UrutanDalamLane memastikan event satu chat dikirim tepat berurutan 1..10 meski event 3 sempat ditolak dua kali.
func TestDeliverer_UrutanDalamLane(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		var delivered []int64
		var ev3Fails atomic.Int32

		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seq, _ := strconv.ParseInt(r.Header.Get("X-WA-Event-Id"), 10, 64)
			if seq == 3 && ev3Fails.Add(1) <= 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			delivered = append(delivered, seq)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Client().CloseIdleConnections()
		defer srv.Close()

		fj := newFakeJournal()
		chat := findChatForLane(0, 4)
		for i := 1; i <= 10; i++ {
			fj.appendEvent(journal.Event{
				InstanceID: "sales",
				Type:       "message",
				Chat:       chat,
				Payload:    []byte(fmt.Sprintf(`{"n":%d}`, i)),
			})
		}

		tc := NewTargetCache()
		tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

		svc := newServiceForTest(fj, tc, &fakeDeadStore{}, &fakePauser{}, srv.Client(), nil, Options{
			CommitEvery:    1,
			CommitInterval: time.Second,
		})
		svc.Start(ctx, []string{"sales"})

		for {
			synctest.Wait()
			mu.Lock()
			n := len(delivered)
			mu.Unlock()
			if n == 10 {
				break
			}
			time.Sleep(time.Second)
		}

		svc.Stop()
		mu.Lock()
		got := append([]int64(nil), delivered...)
		mu.Unlock()

		for i := 0; i < 10; i++ {
			if got[i] != int64(i+1) {
				t.Fatalf("urutan pengiriman terganggu pada indeks %d: didapat %v", i, got)
			}
		}
	})
}

// TestDeliverer_LaneLainTidakTerblokir memastikan kegagalan permanen di lane 0 tidak menghambat pengiriman di lane 1.
func TestDeliverer_LaneLainTidakTerblokir(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var lane1Delivered atomic.Int32
		chatA := findChatForLane(0, 4)
		chatB := findChatForLane(1, 4)

		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var env map[string]any
			_ = json.NewDecoder(r.Body).Decode(&env)
			data, _ := env["data"].(map[string]any)
			if data["chat"] == chatA {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			lane1Delivered.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Client().CloseIdleConnections()
		defer srv.Close()

		fj := newFakeJournal()
		fj.appendEvent(journal.Event{InstanceID: "sales", Type: "message", Chat: chatA, Payload: []byte(fmt.Sprintf(`{"chat":%q}`, chatA))})
		fj.appendEvent(journal.Event{InstanceID: "sales", Type: "message", Chat: chatB, Payload: []byte(fmt.Sprintf(`{"chat":%q}`, chatB))})

		tc := NewTargetCache()
		tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

		svc := newServiceForTest(fj, tc, &fakeDeadStore{}, &fakePauser{}, srv.Client(), nil, Options{CommitEvery: 1})
		svc.Start(ctx, []string{"sales"})

		// Beri waktu sintetik pendek; chatB di lane 1 harus langsung terkirim meski lane 0 terus backoff berjam-jam.
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		if lane1Delivered.Load() != 1 {
			t.Fatalf("chat B di lane 1 seharusnya langsung terkirim meski lane 0 selalu 500, terkirim=%d", lane1Delivered.Load())
		}
		cancel()
		svc.Stop()
	})
}

// TestDeliverer_JadwalRetryDanDLQ memverifikasi SendTest, jadwal retry 1..12 dengan jitter +-20%, pencatatan webhook_dead, dan kemajuan cursor.
func TestDeliverer_JadwalRetryDanDLQ(t *testing.T) {
	// Bagian 1: pertahankan cakupan SendTest dari kontrak v1 (header HMAC X-WA-Signature).
	t.Run("SendTest", func(t *testing.T) {
		ctx := context.Background()
		var gotSig string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotSig = r.Header.Get("X-WA-Signature")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		tc := NewTargetCache()
		tc.Set("toko", Target{URL: srv.URL, Secret: "whsec_test123", EventsCSV: "message,connection", Format: "waapi", Lanes: 4, MaxAttempts: 12})
		svc := NewService(nil, tc, nil, srv.Client(), nil, Options{})

		code, err := svc.SendTest(ctx, "toko")
		if err != nil || code != http.StatusOK {
			t.Fatalf("SendTest gagal: code=%d, err=%v", code, err)
		}
		if len(gotSig) < 10 || gotSig[:7] != "sha256=" {
			t.Fatalf("signature HMAC SendTest tidak valid: %q", gotSig)
		}
	})

	// Bagian 2: uji jadwal backoff +-20% sampai 12 percobaan masuk DLQ lalu cursor maju.
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		var attempts []time.Time
		var attemptHeaders []string

		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			attempts = append(attempts, time.Now())
			attemptHeaders = append(attemptHeaders, r.Header.Get("X-WA-Delivery-Attempt"))
			mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Client().CloseIdleConnections()
		defer srv.Close()

		fj := newFakeJournal()
		fds := &fakeDeadStore{}
		seq := fj.appendEvent(journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       findChatForLane(0, 4),
			Payload:    []byte(`{"text":"selalu 500"}`),
		})

		tc := NewTargetCache()
		tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

		svc := newServiceForTest(fj, tc, fds, &fakePauser{}, srv.Client(), nil, Options{CommitEvery: 1})
		svc.Start(ctx, []string{"sales"})

		for len(fds.list()) == 0 {
			time.Sleep(time.Minute)
			synctest.Wait()
		}
		svc.Stop()

		mu.Lock()
		gotTimes := append([]time.Time(nil), attempts...)
		gotHdrs := append([]string(nil), attemptHeaders...)
		mu.Unlock()

		if len(gotTimes) != 12 {
			t.Fatalf("jumlah percobaan diharapkan 12, didapat %d", len(gotTimes))
		}
		for i, h := range gotHdrs {
			if h != strconv.Itoa(i+1) {
				t.Fatalf("X-WA-Delivery-Attempt ke-%d diharapkan %d, didapat %q", i, i+1, h)
			}
		}

		expected := []time.Duration{
			5 * time.Second,
			15 * time.Second,
			45 * time.Second,
			2 * time.Minute,
			5 * time.Minute,
			15 * time.Minute,
			30 * time.Minute,
			time.Hour,
			time.Hour,
			time.Hour,
			time.Hour,
		}
		for i, exp := range expected {
			gap := gotTimes[i+1].Sub(gotTimes[i])
			lo := time.Duration(float64(exp) * 0.79)
			hi := time.Duration(float64(exp) * 1.21)
			if gap < lo || gap > hi {
				t.Fatalf("jeda sebelum percobaan %d di luar rentang jitter +-20%%: gap=%v, base=%v", i+2, gap, exp)
			}
		}

		dead := fds.list()
		if len(dead) != 1 || dead[0].Seq != seq || dead[0].Attempts != 12 || dead[0].LastStatus != http.StatusInternalServerError {
			t.Fatalf("entri DLQ tidak sesuai: %+v", dead)
		}
		cur, _ := fj.Cursor(ctx, "webhook", "sales", 0)
		if cur != seq {
			t.Fatalf("cursor lane seharusnya maju ke %d setelah DLQ, didapat %d", seq, cur)
		}
	})
}

// TestDeliverer_RetryAfterDihormati memverifikasi respons 429 dengan Retry-After menjeda percobaan berikutnya persis sesuai header (maks 1 jam).
func TestDeliverer_RetryAfterDihormati(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		var times []time.Time

		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			times = append(times, time.Now())
			n := len(times)
			mu.Unlock()
			if n == 1 {
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Client().CloseIdleConnections()
		defer srv.Close()

		fj := newFakeJournal()
		fj.appendEvent(journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       findChatForLane(0, 4),
			Payload:    []byte(`{"text":"rate limit"}`),
		})

		tc := NewTargetCache()
		tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

		svc := newServiceForTest(fj, tc, &fakeDeadStore{}, &fakePauser{}, srv.Client(), nil, Options{CommitEvery: 1})
		svc.Start(ctx, []string{"sales"})

		for {
			mu.Lock()
			done := len(times) >= 2
			mu.Unlock()
			if done {
				break
			}
			time.Sleep(10 * time.Second)
			synctest.Wait()
		}
		svc.Stop()

		mu.Lock()
		delta := times[1].Sub(times[0])
		mu.Unlock()

		if delta < 119*time.Second || delta > 121*time.Second {
			t.Fatalf("jeda setelah 429 Retry-After: 120 diharapkan ~120s, didapat %v", delta)
		}
	})
}

// TestDeliverer_4xxCepatKeDLQ memverifikasi error HTTP 4xx permanen (400) masuk DLQ setelah 3 percobaan lalu cursor maju.
func TestDeliverer_4xxCepatKeDLQ(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var attempts atomic.Int32
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("payload tidak dikenal"))
		}))
		defer srv.Client().CloseIdleConnections()
		defer srv.Close()

		fj := newFakeJournal()
		fds := &fakeDeadStore{}
		seq := fj.appendEvent(journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       findChatForLane(0, 4),
			Payload:    []byte(`{"bad":true}`),
		})

		tc := NewTargetCache()
		tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

		svc := newServiceForTest(fj, tc, fds, &fakePauser{}, srv.Client(), nil, Options{CommitEvery: 1})
		svc.Start(ctx, []string{"sales"})

		for len(fds.list()) == 0 {
			time.Sleep(5 * time.Second)
			synctest.Wait()
		}
		svc.Stop()

		if got := attempts.Load(); got != 3 {
			t.Fatalf("HTTP 400 harus berhenti di percobaan ke-3, didapat %d", got)
		}
		dead := fds.list()
		if len(dead) != 1 || dead[0].Attempts != 3 || dead[0].LastStatus != http.StatusBadRequest {
			t.Fatalf("entri DLQ 4xx tidak sesuai: %+v", dead)
		}
		cur, _ := fj.Cursor(ctx, "webhook", "sales", 0)
		if cur != seq {
			t.Fatalf("cursor lane diharapkan maju ke %d setelah 4xx DLQ, didapat %d", seq, cur)
		}
	})
}

// TestDeliverer_410PauseLaluResume memastikan 410 menghentikan deliverer dengan webhook_paused="gone", menjaga cursor, lalu resume mengirim ulang event yang sama.
func TestDeliverer_410PauseLaluResume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmp := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	var mode410 atomic.Bool
	mode410.Store(true)
	var hits atomic.Int32
	deliveredCh := make(chan string, 4)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if mode410.Load() {
			w.WriteHeader(http.StatusGone)
			return
		}
		deliveredCh <- r.Header.Get("X-WA-Event-Id")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := st.CreateInstance(ctx, store.Instance{
		ID:            "sales",
		WebhookURL:    srv.URL,
		WebhookSecret: "whsec_test",
		WebhookEvents: "*",
		RatePerMin:    20,
		Settings:      store.Defaults(),
	}); err != nil {
		t.Fatalf("CreateInstance gagal: %v", err)
	}

	tc := NewTargetCache()
	_ = tc.Load(ctx, st)
	dead := NewDeadStore(j.DB())

	svc := NewService(j, tc, dead, srv.Client(), nil, Options{
		Store:       st,
		CommitEvery: 1,
	})
	svc.Start(ctx, []string{"sales"})
	defer svc.Stop()

	res, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       findChatForLane(0, 4),
		TS:         1794360100,
		Payload:    []byte(`{"text":"uji 410"}`),
	})
	if err != nil {
		t.Fatalf("Append gagal: %v", err)
	}

	// Tunggu settings.webhook_paused == "gone" tersimpan ke app.db.
	deadline := time.Now().Add(3 * time.Second)
	for {
		inst, err := st.GetInstance(ctx, "sales")
		if err == nil && inst.Settings.WebhookPaused == "gone" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("webhook_paused = 'gone' tidak tersimpan ke app.db tepat waktu")
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Tidak boleh ada retry ulang ataupun entri DLQ, cursor tetap di 0, dan consumer tetap aktif (enabled=1).
	time.Sleep(100 * time.Millisecond)
	if h := hits.Load(); h != 1 {
		t.Fatalf("410 Gone tidak boleh memicu retry, hits=%d", h)
	}
	dlqCount, err := dead.Count(ctx, "sales")
	if err != nil || dlqCount != 0 {
		t.Fatalf("410 Gone tidak boleh masuk DLQ, count=%d err=%v", dlqCount, err)
	}
	cur, err := j.Cursor(ctx, "webhook", "sales", 0)
	if err != nil || cur != 0 {
		t.Fatalf("cursor lane 0 harus tetap 0 setelah 410, didapat %d err=%v", cur, err)
	}
	var enabled int
	if err := j.DB().QueryRowContext(ctx, `SELECT enabled FROM cursors WHERE consumer='webhook' AND instance_id='sales' AND lane=0`).Scan(&enabled); err != nil || enabled != 1 {
		t.Fatalf("consumer harus tetap enabled=1 agar event 410 tidak dipangkas sebagai terkirim: enabled=%d err=%v", enabled, err)
	}

	// Simulasikan `waapi webhook resume sales`: kosongkan webhook_paused dan beritahu Service.
	mode410.Store(false)
	inst, _ := st.GetInstance(ctx, "sales")
	cleared := inst.Settings
	cleared.WebhookPaused = ""
	updated, err := st.UpdateInstance(ctx, "sales", store.InstanceUpdate{Settings: &cleared})
	if err != nil {
		t.Fatalf("clear webhook_paused gagal: %v", err)
	}
	tc.SetFromInstance(updated)
	svc.OnTargetChanged("sales")

	select {
	case id := <-deliveredCh:
		if id != strconv.FormatInt(res.Seq, 10) {
			t.Fatalf("setelah resume harus mengirim ulang seq=%d, didapat %q", res.Seq, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("event lama tidak dikirim ulang setelah webhook di-resume")
	}
}

// TestDeliverer_LaneSepiTetapMaju memastikan lane yang tidak kebagian event ikut memajukan cursor ke ScannedTo agar MinCursor tidak tertahan.
func TestDeliverer_LaneSepiTetapMaju(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	chatLane0 := findChatForLane(0, 4)
	var lastSeq int64
	for i := 0; i < 6; i++ {
		res, err := j.Append(ctx, &journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       chatLane0,
			TS:         1794360100 + int64(i),
			Payload:    []byte(fmt.Sprintf(`{"i":%d}`, i)),
		})
		if err != nil {
			t.Fatalf("Append gagal: %v", err)
		}
		lastSeq = res.Seq
	}

	tc := NewTargetCache()
	tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

	svc := NewService(j, tc, NewDeadStore(j.DB()), srv.Client(), nil, Options{
		CommitEvery:    1,
		CommitInterval: 20 * time.Millisecond,
	})
	svc.Start(ctx, []string{"sales"})

	deadline := time.Now().Add(3 * time.Second)
	for {
		minCur, _ := j.MinCursor(ctx, "sales")
		if received.Load() == 6 && minCur == lastSeq {
			break
		}
		if time.Now().After(deadline) {
			minCur, _ := j.MinCursor(ctx, "sales")
			t.Fatalf("MinCursor tertahan oleh lane sepi: received=%d, minCur=%d, want=%d", received.Load(), minCur, lastSeq)
		}
		time.Sleep(20 * time.Millisecond)
	}
	svc.Stop()

	for lane := 0; lane < 4; lane++ {
		c, err := j.Cursor(ctx, "webhook", "sales", lane)
		if err != nil || c != lastSeq {
			t.Fatalf("cursor lane %d diharapkan %d, didapat %d (err=%v)", lane, lastSeq, c, err)
		}
	}
}

// TestDeliverer_CursorBertahanSetelahRestart memastikan 0 duplikat setelah mengirim 20 event, Stop, lalu Start ulang.
func TestDeliverer_CursorBertahanSetelahRestart(t *testing.T) {
	ctx := context.Background()
	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	var mu sync.Mutex
	seen := make(map[string]int)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Header.Get("X-WA-Event-Id")]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tc := NewTargetCache()
	tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

	for i := 1; i <= 20; i++ {
		chat := findChatForLane(i%4, 4)
		if _, err := j.Append(ctx, &journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       chat,
			TS:         1794360200 + int64(i),
			Payload:    []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("Append %d gagal: %v", i, err)
		}
	}

	ctx1, cancel1 := context.WithCancel(ctx)
	svc1 := NewService(j, tc, NewDeadStore(j.DB()), srv.Client(), nil, Options{CommitEvery: 1})
	svc1.Start(ctx1, []string{"sales"})

	deadline := time.Now().Add(4 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n == 20 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("hanya %d dari 20 event terkirim sebelum restart", n)
		}
		time.Sleep(15 * time.Millisecond)
	}
	cancel1()
	svc1.Stop()

	// Start service baru pada DB yang sama dan tambahkan event ke-21.
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	svc2 := NewService(j, tc, NewDeadStore(j.DB()), srv.Client(), nil, Options{CommitEvery: 1})
	svc2.Start(ctx2, []string{"sales"})
	defer svc2.Stop()

	if _, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       findChatForLane(0, 4),
		TS:         1794360300,
		Payload:    []byte(`{"n":21}`),
	}); err != nil {
		t.Fatalf("Append ke-21 gagal: %v", err)
	}

	deadline = time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n == 21 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("event ke-21 tidak terkirim setelah restart (len=%d)", n)
		}
		time.Sleep(15 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	for id, cnt := range seen {
		if cnt != 1 {
			t.Fatalf("event %s terkirim %d kali setelah restart (seharusnya tepat 1)", id, cnt)
		}
	}
}

// TestDeliverer_GantiLaneTanpaKehilangan memastikan perubahan webhook_lanes 4 -> 2 di tengah aliran mengirim semua event minimal sekali tanpa terlewat.
func TestDeliverer_GantiLaneTanpaKehilangan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	var mu sync.Mutex
	seen := make(map[int64]int)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq, _ := strconv.ParseInt(r.Header.Get("X-WA-Event-Id"), 10, 64)
		mu.Lock()
		seen[seq]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tc := NewTargetCache()
	tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

	svc := NewService(j, tc, NewDeadStore(j.DB()), srv.Client(), nil, Options{CommitEvery: 1})
	svc.Start(ctx, []string{"sales"})
	defer svc.Stop()

	for i := 1; i <= 15; i++ {
		if _, err := j.Append(ctx, &journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       findChatForLane(i%4, 4),
			TS:         1794360400 + int64(i),
			Payload:    []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("Append %d gagal: %v", i, err)
		}
	}

	// Ubah jumlah lane 4 -> 2 sambil menyisipkan 15 event lagi.
	tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 2, MaxAttempts: 12})
	svc.OnTargetChanged("sales")

	for i := 16; i <= 30; i++ {
		if _, err := j.Append(ctx, &journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       findChatForLane(i%4, 4),
			TS:         1794360400 + int64(i),
			Payload:    []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("Append %d gagal: %v", i, err)
		}
	}

	deadline := time.Now().Add(4 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n == 30 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ada event hilang saat ganti lane 4 -> 2: diterima %d/30", n)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Pastikan baris cursor usang (lane >= 2) sudah dibersihkan agar MinCursor/pruner tetap sehat.
	var staleCount int
	if err := j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM cursors WHERE consumer='webhook' AND instance_id='sales' AND lane >= 2`).Scan(&staleCount); err != nil || staleCount != 0 {
		t.Fatalf("cursor lane usang >= 2 harus terhapus, didapat %d (err=%v)", staleCount, err)
	}
}

// TestStop_InFlightTidakHilang memverifikasi R3: request HTTP yang sedang berjalan selesai penuh dan cursor ter-commit saat Stop dipanggil.
func TestStop_InFlightTidakHilang(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		inFlightStarted := make(chan struct{}, 1)
		releaseServer := make(chan struct{})
		completed := atomic.Bool{}

		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case inFlightStarted <- struct{}{}:
			default:
			}
			<-releaseServer
			completed.Store(true)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Client().CloseIdleConnections()
		defer srv.Close()

		fj := newFakeJournal()
		seq := fj.appendEvent(journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       findChatForLane(0, 4),
			Payload:    []byte(`{"text":"in flight"}`),
		})

		tc := NewTargetCache()
		tc.Set("sales", Target{URL: srv.URL, Secret: "whsec_test", EventsCSV: "*", Format: "waapi", Lanes: 4, MaxAttempts: 12})

		svc := newServiceForTest(fj, tc, &fakeDeadStore{}, &fakePauser{}, srv.Client(), nil, Options{
			CommitEvery:    16, // sengajakan > 1 untuk menguji commit paksa saat Stop
			CommitInterval: time.Minute,
		})
		svc.Start(ctx, []string{"sales"})

		<-inFlightStarted

		stopDone := make(chan struct{})
		go func() {
			cancel()
			svc.Stop()
			close(stopDone)
		}()

		// Stop harus menunggu request in-flight, belum selesai sebelum server membalas.
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		select {
		case <-stopDone:
			t.Fatal("Stop tidak boleh kembali sebelum request HTTP in-flight selesai")
		default:
		}

		// Izinkan server membalas 200 OK -> Stop harus segera tuntas dan cursor harus ter-commit ke disk/fake.
		close(releaseServer)
		<-stopDone

		if !completed.Load() {
			t.Fatal("handler HTTP in-flight seharusnya selesai tuntas sebelum Stop kembali")
		}
		cur, _ := fj.Cursor(context.Background(), "webhook", "sales", 0)
		if cur != seq {
			t.Fatalf("cursor harus ter-commit ke %d menggunakan context baru saat Stop, didapat %d", seq, cur)
		}
	})
}
