package wa

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/obs"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

// fakeConnDriver mensimulasikan connDriver untuk menguji Supervisor tanpa koneksi jaringan nyata.
type fakeConnDriver struct {
	mu           sync.Mutex
	connectFn    func(ctx context.Context, attempt int) error
	connectCalls int
	resetCalls   int
	disconnCalls int
	connected    bool
	loggedIn     bool
	contexts     []context.Context
}

func (d *fakeConnDriver) ConnectContext(ctx context.Context) error {
	d.mu.Lock()
	d.connectCalls++
	attempt := d.connectCalls
	d.contexts = append(d.contexts, ctx)
	fn := d.connectFn
	d.mu.Unlock()

	if fn != nil {
		return fn(ctx, attempt)
	}
	return nil
}

func (d *fakeConnDriver) ResetConnection() {
	d.mu.Lock()
	d.resetCalls++
	d.connected = false
	d.mu.Unlock()
}

func (d *fakeConnDriver) Disconnect() {
	d.mu.Lock()
	d.disconnCalls++
	d.connected = false
	d.mu.Unlock()
}

func (d *fakeConnDriver) IsConnected() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.connected
}

func (d *fakeConnDriver) IsLoggedIn() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.loggedIn
}

func (d *fakeConnDriver) stats() (connects, resets, disconns int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.connectCalls, d.resetCalls, d.disconnCalls
}

func (d *fakeConnDriver) setConnected(v bool) {
	d.mu.Lock()
	d.connected = v
	d.loggedIn = v
	d.mu.Unlock()
}

type recordedEmission struct {
	instanceID string
	event      string
	data       any
}

type fakeEmitter struct {
	mu    sync.Mutex
	items []recordedEmission
}

func (e *fakeEmitter) Emit(_ context.Context, instanceID, event string, data any) error {
	e.mu.Lock()
	e.items = append(e.items, recordedEmission{instanceID: instanceID, event: event, data: data})
	e.mu.Unlock()
	return nil
}

func (e *fakeEmitter) all() []recordedEmission {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make([]recordedEmission, len(e.items))
	copy(cp, e.items)
	return cp
}

// TestBackoff_NextDalamRentang memverifikasi equal jitter [d/2, d] dan pembatasan Max pada attempt 0..30.
func TestBackoff_NextDalamRentang(t *testing.T) {
	b := Backoff{Base: 2 * time.Second, Max: 5 * time.Minute, Factor: 2}
	rnds := []float64{0, 0.5, 1}

	for attempt := 0; attempt <= 30; attempt++ {
		// Hitung batas deterministik d = min(Max, Base * Factor^attempt)
		d := b.Base
		for i := 0; i < attempt; i++ {
			if d >= b.Max {
				d = b.Max
				break
			}
			d *= 2
			if d > b.Max {
				d = b.Max
			}
		}
		half := d / 2

		for _, rnd := range rnds {
			got := b.Next(attempt, rnd)
			if got < half || got > d {
				t.Fatalf("attempt=%d rnd=%v: hasil %v di luar rentang [%v, %v]", attempt, rnd, got, half, d)
			}
			if got > b.Max {
				t.Fatalf("attempt=%d rnd=%v: hasil %v melampaui Max %v", attempt, rnd, got, b.Max)
			}
		}
	}
}

// TestSupervisor_ReconnectSetelahDisconnect memverifikasi backoff eksponensial dan recovery setelah disconnect.
func TestSupervisor_ReconnectSetelahDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		var sup *Supervisor
		var fails int
		drv := &fakeConnDriver{}
		drv.connectFn = func(_ context.Context, attempt int) error {
			if attempt == 1 {
				drv.setConnected(true)
				return nil
			}
			// Gagal 3 kali setelah disconnect, lalu sukses pada percobaan reconnect ke-4
			mu.Lock()
			fails++
			cur := fails
			mu.Unlock()
			if cur <= 3 {
				return errors.New("dial gagal sementara")
			}
			drv.setConnected(true)
			return nil
		}

		emit := &fakeEmitter{}
		cfg := SupervisorConfig{
			Backoff:          Backoff{Base: 2 * time.Second, Max: 5 * time.Minute, Factor: 2},
			CircuitThreshold: 10,
			CircuitMax:       15 * time.Minute,
			KeepAliveMaxFail: 75 * time.Second,
			ConnectTimeout:   30 * time.Second,
		}
		sup = newSupervisor("sales", drv, emit, cfg, func() float64 { return 1.0 })
		defer sup.Stop()

		sup.Start(ctx)
		synctest.Wait()
		sup.OnEvent(&events.Connected{})
		synctest.Wait()

		if st := sup.State(); st.State != StateOnline {
			t.Fatalf("state awal diharapkan online, didapat %s", st.State)
		}

		startDisconnect := time.Now()
		drv.setConnected(false)
		sup.OnEvent(&events.Disconnected{})
		synctest.Wait()

		if st := sup.State(); st.State != StateBackoff {
			t.Fatalf("state setelah disconnect diharapkan backoff, didapat %s", st.State)
		}

		// Jitter disconnect awal (0-1s dengan rnd=1 -> 1s) + Backoff(0,1)=2s + Backoff(1,1)=4s + Backoff(2,1)=8s = 15s
		wantDurations := []time.Duration{
			1 * time.Second,
			2 * time.Second,
			4 * time.Second,
			8 * time.Second,
		}
		for idx, wait := range wantDurations {
			time.Sleep(wait - time.Millisecond)
			synctest.Wait()
			if st := sup.State(); st.State != StateBackoff {
				t.Fatalf("langkah %d sebelum timer: state diharapkan backoff, didapat %s", idx, st.State)
			}
			time.Sleep(time.Millisecond)
			synctest.Wait()
		}

		// Percobaan reconnect ke-4 sukses connect, tunggu event Connected masuk
		sup.OnEvent(&events.Connected{})
		synctest.Wait()

		if st := sup.State(); st.State != StateOnline || st.Attempts != 0 {
			t.Fatalf("diharapkan kembali online dengan attempts=0, didapat %+v", st)
		}
		elapsed := time.Since(startDisconnect)
		if elapsed != 15*time.Second {
			t.Fatalf("total jeda backoff diharapkan 15s, didapat %v", elapsed)
		}
	})
}

// TestSupervisor_ConnectAwalGagalTetapDiulang memastikan kegagalan connect awal saat Start terus diulang hingga online (R9).
func TestSupervisor_ConnectAwalGagalTetapDiulang(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		drv := &fakeConnDriver{
			connectFn: func(_ context.Context, attempt int) error {
				if attempt <= 2 {
					return errors.New("jaringan belum siap")
				}
				return nil
			},
		}
		sup := newSupervisor("boot", drv, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 1.0 })
		defer sup.Stop()

		sup.Start(ctx)
		synctest.Wait()

		// Gagal pertama -> backoff 2s, gagal kedua -> backoff 4s
		if st := sup.State(); st.State != StateBackoff || st.Attempts != 1 {
			t.Fatalf("diharapkan state backoff setelah connect awal gagal, didapat %+v", st)
		}

		time.Sleep(2*time.Second + 4*time.Second)
		synctest.Wait()

		sup.OnEvent(&events.Connected{})
		synctest.Wait()

		if st := sup.State(); st.State != StateOnline {
			t.Fatalf("diharapkan akhirnya online tanpa intervensi, didapat %s", st.State)
		}
		if c, _, _ := drv.stats(); c != 3 {
			t.Fatalf("jumlah panggilan connect diharapkan 3, didapat %d", c)
		}
	})
}

// TestSupervisor_StreamReplacedBerhenti memastikan StreamReplaced menghentikan seluruh retry selamanya.
func TestSupervisor_StreamReplacedBerhenti(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		drv := &fakeConnDriver{}
		sup := newSupervisor("dup", drv, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 0.5 })
		defer sup.Stop()

		sup.Start(ctx)
		synctest.Wait()
		sup.OnEvent(&events.Connected{})
		synctest.Wait()

		beforeCalls, _, _ := drv.stats()
		sup.OnEvent(&events.StreamReplaced{})
		synctest.Wait()

		if st := sup.State(); st.State != StateReplaced {
			t.Fatalf("state diharapkan replaced, didapat %s", st.State)
		}

		time.Sleep(1 * time.Hour)
		synctest.Wait()

		afterCalls, _, _ := drv.stats()
		if afterCalls != beforeCalls {
			t.Fatalf("tidak boleh ada panggilan connect baru setelah replaced: sebelum=%d, sesudah=%d", beforeCalls, afterCalls)
		}
	})
}

// TestSupervisor_TemporaryBanMenungguExpiry memverifikasi jeda TemporaryBan = Expire + jitter(30..120s).
func TestSupervisor_TemporaryBanMenungguExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		drv := &fakeConnDriver{}
		sup := newSupervisor("ban", drv, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 0.5 })
		defer sup.Stop()

		sup.Start(ctx)
		synctest.Wait()
		sup.OnEvent(&events.Connected{})
		synctest.Wait()

		beforeCalls, _, _ := drv.stats()
		sup.OnEvent(&events.TemporaryBan{Code: events.TempBanSentToTooManyPeople, Expire: time.Hour})
		synctest.Wait()

		if st := sup.State(); st.State != StateBannedTemp || st.BanCode == 0 {
			t.Fatalf("state diharapkan banned_temp dengan BanCode, didapat %+v", st)
		}

		// Tidak boleh ada connect sebelum 1h + 30s
		time.Sleep(time.Hour + 29*time.Second)
		synctest.Wait()
		if c, _, _ := drv.stats(); c != beforeCalls {
			t.Fatalf("connect terjadi terlalu dini sebelum 1h+30s: %d", c)
		}

		// Dengan rnd=0.5, retry jatuh di 1h + 75s (antara 1h+30s dan 1h+121s)
		time.Sleep(50 * time.Second)
		synctest.Wait()
		if c, _, _ := drv.stats(); c != beforeCalls+1 {
			t.Fatalf("diharapkan tepat 1 percobaan connect sebelum 1h+121s, didapat %d", c-beforeCalls)
		}
		sup.OnEvent(&events.Connected{})
		synctest.Wait()
		if st := sup.State(); st.State != StateOnline {
			t.Fatalf("diharapkan kembali online setelah expiry ban, didapat %s", st.State)
		}

		// Pastikan Expire=0 memakai backoff base 15 menit (rnd=0.5 -> 11m15s)
		beforeZero := func() int { c, _, _ := drv.stats(); return c }()
		sup.OnEvent(&events.TemporaryBan{Code: events.TempBanSentToTooManyPeople, Expire: 0})
		synctest.Wait()
		time.Sleep(7 * time.Minute)
		synctest.Wait()
		if c, _, _ := drv.stats(); c != beforeZero {
			t.Fatalf("TemporaryBan Expire=0 mencoba connect terlalu cepat sebelum 7m30s: %d", c-beforeZero)
		}
		time.Sleep(4*time.Minute + 20*time.Second)
		synctest.Wait()
		if c, _, _ := drv.stats(); c != beforeZero+1 {
			t.Fatalf("TemporaryBan Expire=0 diharapkan mencoba tepat 1 connect dalam [7m30s, 15m], didapat %d", c-beforeZero)
		}
	})
}

// TestSupervisor_KeepAliveDegradedLaluReset memverifikasi transisi degraded lalu ResetConnection sekali saat >= KeepAliveMaxFail.
func TestSupervisor_KeepAliveDegradedLaluReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		drv := &fakeConnDriver{}
		sup := newSupervisor("ka", drv, &fakeEmitter{}, SupervisorConfig{KeepAliveMaxFail: 75 * time.Second}, func() float64 { return 0.5 })
		defer sup.Stop()

		sup.Start(ctx)
		synctest.Wait()
		sup.OnEvent(&events.Connected{})
		synctest.Wait()

		sup.OnEvent(&events.KeepAliveTimeout{ErrorCount: 1, LastSuccess: time.Now().Add(-30 * time.Second)})
		synctest.Wait()
		if st := sup.State(); st.State != StateDegraded {
			t.Fatalf("diharapkan state degraded saat 30s, didapat %s", st.State)
		}
		if _, resets, _ := drv.stats(); resets != 0 {
			t.Fatalf("reset diharapkan 0 saat degraded, didapat %d", resets)
		}

		// KeepAliveRestored memulihkan degraded -> online
		sup.OnEvent(&events.KeepAliveRestored{})
		synctest.Wait()
		if st := sup.State(); st.State != StateOnline {
			t.Fatalf("diharapkan kembali online setelah KeepAliveRestored, didapat %s", st.State)
		}

		// Event kedua saat >= 75s memicu ResetConnection sekali dan masuk backoff
		sup.OnEvent(&events.KeepAliveTimeout{ErrorCount: 3, LastSuccess: time.Now().Add(-80 * time.Second)})
		synctest.Wait()
		if st := sup.State(); st.State != StateBackoff {
			t.Fatalf("diharapkan state backoff saat 80s, didapat %s", st.State)
		}
		if _, resets, _ := drv.stats(); resets != 1 {
			t.Fatalf("reset diharapkan tepat 1 kali, didapat %d", resets)
		}
	})
}

// TestSupervisor_StopMembatalkanTimer memastikan Stop membatalkan timer backoff dan menutup goroutine tanpa sisa.
func TestSupervisor_StopMembatalkanTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		drv := &fakeConnDriver{
			connectFn: func(_ context.Context, _ int) error {
				return errors.New("gagal connect")
			},
		}
		sup := newSupervisor("stoptest", drv, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 1.0 })
		sup.Start(ctx)
		synctest.Wait()

		if st := sup.State(); st.State != StateBackoff {
			t.Fatalf("diharapkan state backoff, didapat %s", st.State)
		}
		beforeCalls, _, _ := drv.stats()

		sup.Stop()
		synctest.Wait()
		if st := sup.State(); st.State != StateStopped {
			t.Fatalf("diharapkan state stopped, didapat %s", st.State)
		}

		time.Sleep(1 * time.Hour)
		synctest.Wait()
		if c, _, _ := drv.stats(); c != beforeCalls {
			t.Fatalf("tidak boleh ada percobaan connect setelah Stop: sebelum=%d, sesudah=%d", beforeCalls, c)
		}
	})
}

// TestSupervisor_CircuitTerbuka memastikan circuit terbuka setelah 10 kegagalan berturut dengan jeda 15 menit.
func TestSupervisor_CircuitTerbuka(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		drv := &fakeConnDriver{
			connectFn: func(_ context.Context, attempt int) error {
				if attempt <= 10 {
					return errors.New("fail")
				}
				return nil
			},
		}
		emit := &fakeEmitter{}
		sup := newSupervisor("cb", drv, emit, SupervisorConfig{}, func() float64 { return 1.0 })
		defer sup.Stop()

		sup.Start(ctx)
		synctest.Wait()

		// Majukan backoff kegagalan 1..9: 2, 4, 8, 16, 32, 64, 128, 256, 300
		delays := []time.Duration{
			2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second,
			64 * time.Second, 128 * time.Second, 256 * time.Second, 300 * time.Second,
		}
		for i, d := range delays {
			time.Sleep(d)
			synctest.Wait()
			if i < 8 && sup.State().Circuit {
				t.Fatalf("circuit tidak boleh terbuka pada kegagalan ke-%d", i+2)
			}
		}

		// Setelah kegagalan ke-10, circuit harus terbuka dan jeda berikutnya 15m (rnd=1)
		st := sup.State()
		if !st.Circuit || st.Attempts != 10 {
			t.Fatalf("circuit diharapkan terbuka pada kegagalan ke-10, didapat %+v", st)
		}
		beforeCalls, _, _ := drv.stats()
		time.Sleep(15*time.Minute - time.Millisecond)
		synctest.Wait()
		if c, _, _ := drv.stats(); c != beforeCalls {
			t.Fatalf("connect terjadi sebelum CircuitMax 15m habis")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if c, _, _ := drv.stats(); c != beforeCalls+1 {
			t.Fatalf("connect diharapkan berjalan tepat pada 15m, didapat %d", c-beforeCalls)
		}

		sup.OnEvent(&events.Connected{})
		synctest.Wait()
		if st := sup.State(); st.State != StateOnline || st.Circuit || st.Attempts != 0 {
			t.Fatalf("circuit & attempts harus reset saat Connected, didapat %+v", st)
		}
	})
}

// TestSupervisor_OutdatedDanBanned memverifikasi transisi terminal tanpa retry untuk ClientOutdated, LoggedOut 406, dan LoggedOut 401.
func TestSupervisor_OutdatedDanBanned(t *testing.T) {
	cases := []struct {
		name      string
		evt       any
		wantState ConnState
	}{
		{name: "ClientOutdated", evt: &events.ClientOutdated{}, wantState: StateOutdated},
		{name: "LoggedOut406_Banned", evt: &events.LoggedOut{Reason: events.ConnectFailureUnknownLogout}, wantState: StateBanned},
		{name: "LoggedOut401", evt: &events.LoggedOut{Reason: events.ConnectFailureLoggedOut}, wantState: StateLoggedOut},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				drv := &fakeConnDriver{}
				sup := newSupervisor("term", drv, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 0.5 })
				defer sup.Stop()

				sup.Start(ctx)
				synctest.Wait()
				sup.OnEvent(&events.Connected{})
				synctest.Wait()

				beforeCalls, _, _ := drv.stats()
				sup.OnEvent(tc.evt)
				synctest.Wait()

				if got := sup.State().State; got != tc.wantState {
					t.Fatalf("state diharapkan %s, didapat %s", tc.wantState, got)
				}
				time.Sleep(1 * time.Hour)
				synctest.Wait()
				if after, _, _ := drv.stats(); after != beforeCalls {
					t.Fatalf("tidak boleh ada retry setelah state terminal %s", tc.wantState)
				}
			})
		})
	}
}

// TestSupervisor_EventNonKritisTidakMemblokir memastikan 100 event non-kritis saat loop tertahan tidak memblokir pemanggil.
func TestSupervisor_EventNonKritisTidakMemblokir(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gate := make(chan struct{})
		drv := &fakeConnDriver{
			connectFn: func(_ context.Context, _ int) error {
				<-gate
				return nil
			},
		}
		sup := newSupervisor("coalesce", drv, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 0.5 })

		beforeCount := obs.MetricSupervisorEventsCoalescedTotal.Value()
		sup.Start(ctx)
		synctest.Wait()

		doneCh := make(chan struct{})
		go func() {
			for i := 0; i < 100; i++ {
				sup.OnEvent(&events.KeepAliveTimeout{ErrorCount: i + 1, LastSuccess: time.Now().Add(-10 * time.Second)})
			}
			close(doneCh)
		}()

		// Semua goroutine (termasuk pengirim 100 event) harus selesai tanpa durably blocked
		synctest.Wait()
		select {
		case <-doneCh:
		default:
			t.Fatal("OnEvent non-kritis memblokir saat channel supervisor penuh")
		}

		afterCount := obs.MetricSupervisorEventsCoalescedTotal.Value()
		if afterCount <= beforeCount {
			t.Fatalf("metrik waapi_supervisor_events_coalesced_total harus bertambah: sebelum=%v, sesudah=%v", beforeCount, afterCount)
		}

		close(gate)
		synctest.Wait()
		sup.Stop()
	})
}

// TestSupervisor_EventKritisTidakHilang memastikan event kritis memblokir saat channel penuh hingga loop memprosesnya.
func TestSupervisor_EventKritisTidakHilang(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gate := make(chan struct{})
		drv := &fakeConnDriver{
			connectFn: func(_ context.Context, _ int) error {
				<-gate
				return nil
			},
		}
		sup := newSupervisor("crit", drv, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 0.5 })
		defer sup.Stop()

		// Tahan loop di connectFn agar channel berkapasitas 64 dapat terisi penuh sementara.
		sup.Start(ctx)
		synctest.Wait()

		// Isi penuh channel kapasitas 64 + trigger coalescing
		for i := 0; i < 64; i++ {
			sup.OnEvent(&events.Connected{})
		}

		sent := make(chan struct{})
		go func() {
			sup.OnEvent(&events.StreamReplaced{})
			close(sent)
		}()
		synctest.Wait()

		// Pastikan event kritis menunggu hingga slot channel tersedia
		select {
		case <-sent:
			t.Fatal("OnEvent kritis seharusnya menunggu saat channel kapasitas 64 penuh")
		default:
		}

		close(gate)
		synctest.Wait()
		<-sent
		synctest.Wait()

		if st := sup.State(); st.State != StateReplaced {
			t.Fatalf("event kritis StreamReplaced hilang: state=%s", st.State)
		}
	})
}

// TestSupervisor_ContextKoneksiTanpaDeadline memverifikasi connCtx tanpa deadline dan ConnectTimeout via timer + Disconnect.
func TestSupervisor_ContextKoneksiTanpaDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var gotHasDeadline bool
		drv := &fakeConnDriver{
			connectFn: func(connCtx context.Context, _ int) error {
				_, gotHasDeadline = connCtx.Deadline()
				return nil // Sengaja tidak pernah mengirim events.Connected
			},
		}
		sup := newSupervisor("conntimeout", drv, &fakeEmitter{}, SupervisorConfig{
			ConnectTimeout: 30 * time.Second,
		}, func() float64 { return 1.0 })
		defer sup.Stop()

		sup.Start(ctx)
		synctest.Wait()

		if gotHasDeadline {
			t.Fatal("context yang diberikan ke ConnectContext tidak boleh memiliki deadline")
		}
		if st := sup.State(); st.State != StateConnecting {
			t.Fatalf("diharapkan state connecting sambil menunggu Connected, didapat %s", st.State)
		}

		// Tepat 30s tanpa events.Connected -> Disconnect dipanggil dan masuk backoff
		time.Sleep(30 * time.Second)
		synctest.Wait()

		if _, _, disconns := drv.stats(); disconns != 1 {
			t.Fatalf("Disconnect harus dipanggil tepat 1 kali setelah ConnectTimeout 30s, didapat %d", disconns)
		}
		st := sup.State()
		if st.State != StateBackoff {
			t.Fatalf("state setelah ConnectTimeout diharapkan backoff, didapat %s", st.State)
		}
		if !strings.Contains(strings.ToLower(st.LastError), "timeout") {
			t.Fatalf("LastError diharapkan menyebut timeout, didapat %q", st.LastError)
		}
	})
}

// TestSupervisor_AdoptSetelahPairing memverifikasi Adopt tanpa ConnectContext kedua dan ErrAlreadyConnected dihitung sukses.
func TestSupervisor_AdoptSetelahPairing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Bagian 1: Adopt langsung ke online tanpa memanggil ConnectContext
		drv1 := &fakeConnDriver{}
		sup1 := newSupervisor("pair1", drv1, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 0.5 })
		defer sup1.Stop()

		sup1.Adopt()
		synctest.Wait()

		if st := sup1.State(); st.State != StateOnline {
			t.Fatalf("state setelah Adopt diharapkan online, didapat %s", st.State)
		}
		if c, _, _ := drv1.stats(); c != 0 {
			t.Fatalf("ConnectContext tidak boleh dipanggil saat Adopt, didapat %d", c)
		}

		// Bagian 2: ErrAlreadyConnected dari ConnectContext dianggap sukses
		var drv2 *fakeConnDriver
		drv2 = &fakeConnDriver{
			connectFn: func(_ context.Context, _ int) error {
				drv2.setConnected(true)
				return whatsmeow.ErrAlreadyConnected
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sup2 := newSupervisor("pair2", drv2, &fakeEmitter{}, SupervisorConfig{}, func() float64 { return 0.5 })
		defer sup2.Stop()

		sup2.Start(ctx)
		synctest.Wait()

		if st := sup2.State(); st.State != StateOnline || st.Attempts != 0 {
			t.Fatalf("ErrAlreadyConnected harus dianggap sukses (online, attempts=0), didapat %+v", st)
		}
	})
}
