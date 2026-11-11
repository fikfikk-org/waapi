package wa

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow"
	"golang.org/x/sync/semaphore"
)

// fakePairDriver menyimulasikan whatsmeow.Client untuk menguji PairSession tanpa koneksi jaringan.
type fakePairDriver struct {
	ch         chan whatsmeow.QRChannelItem
	code       string
	pairErr    error
	jid        string
	pairCalls  atomic.Int32
	disconnect atomic.Bool
}

func newFakeDriver() *fakePairDriver {
	return &fakePairDriver{
		ch:   make(chan whatsmeow.QRChannelItem, 16),
		code: "ABCD-1234",
		jid:  "6281234567890:1@s.whatsapp.net",
	}
}

func (f *fakePairDriver) GetQRChannel(_ context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return f.ch, nil
}

func (f *fakePairDriver) Connect() error { return nil }

func (f *fakePairDriver) PairPhone(_ context.Context, _ string) (string, error) {
	f.pairCalls.Add(1)
	return f.code, f.pairErr
}

func (f *fakePairDriver) Disconnect() {
	f.disconnect.Store(true)
}

func (f *fakePairDriver) JID() string { return f.jid }

func newTestInstance(id string, sem *semaphore.Weighted, drv *fakePairDriver) *Instance {
	mgr := &Manager{
		pairingSem: sem,
	}
	inst := newInstance(mgr, store.Instance{ID: id, Status: store.StatusCreated, RatePerMin: 20})
	inst.driverFactory = func(_ *Instance, _ context.Context) (pairDriver, error) {
		return drv, nil
	}
	return inst
}

// TestPairQRRefreshAndSuccess menguji siklus QR pertama -> refresh -> success dan pelepasan semaphore.
func TestPairQRRefreshAndSuccess(t *testing.T) {
	sem := semaphore.NewWeighted(1)
	drv := newFakeDriver()
	inst := newTestInstance("kostash", sem, drv)
	defer inst.Close()

	sess, err := inst.StartPairing("")
	if err != nil {
		t.Fatalf("StartPairing gagal: %v", err)
	}

	drv.ch <- whatsmeow.QRChannelItem{Event: "code", Code: "2@qr-pertama", Timeout: 60 * time.Second}
	snap, err := sess.WaitReady(context.Background())
	if err != nil {
		t.Fatalf("WaitReady gagal: %v", err)
	}
	if snap.State != PairQRRefreshed || snap.QRRaw == nil || *snap.QRRaw != "2@qr-pertama" {
		t.Fatalf("snapshot QR pertama tidak sesuai: %+v", snap)
	}

	// Uji cache PNG: dua panggilan dengan QR & scale sama harus mengembalikan byte slice yang sama
	png1, _, err := inst.PairingQRPNG(6)
	if err != nil || !bytes.HasPrefix(png1, []byte("\x89PNG\r\n\x1a\n")) {
		t.Fatalf("PNG pertama tidak valid: err=%v", err)
	}
	png2, _, err := inst.PairingQRPNG(6)
	if err != nil || &png1[0] != &png2[0] {
		t.Fatal("diharapkan PNG kedua berasal dari cache memori yang sama")
	}

	// Kirim QR kedua lalu success
	drv.ch <- whatsmeow.QRChannelItem{Event: "code", Code: "2@qr-kedua", Timeout: 20 * time.Second}
	drv.ch <- whatsmeow.QRChannelItem{Event: "success"}
	<-sess.done

	final, err := inst.PairingSnapshot()
	if err != nil || final.State != PairConnected || final.JID == nil || *final.JID != drv.jid {
		t.Fatalf("snapshot akhir tidak sesuai: %+v, err=%v", final, err)
	}
	if !sem.TryAcquire(1) {
		t.Fatal("semaphore pairing tidak dilepaskan setelah sesi selesai")
	}
	sem.Release(1)
}

// TestPairCodeMode memastikan PairPhone hanya dipanggil tepat satu kali walaupun event code muncul berulang.
func TestPairCodeMode(t *testing.T) {
	sem := semaphore.NewWeighted(1)
	drv := newFakeDriver()
	inst := newTestInstance("kostash", sem, drv)
	defer inst.Close()

	sess, err := inst.StartPairing("6281315793349")
	if err != nil {
		t.Fatalf("StartPairing code gagal: %v", err)
	}
	drv.ch <- whatsmeow.QRChannelItem{Event: "code", Code: "2@diabaikan"}
	drv.ch <- whatsmeow.QRChannelItem{Event: "code", Code: "2@diabaikan-2"}

	snap, err := sess.WaitReady(context.Background())
	if err != nil {
		t.Fatalf("WaitReady gagal: %v", err)
	}
	if snap.State != PairCodeGenerated || snap.PairingCode == nil || *snap.PairingCode != "ABCD-1234" || snap.QRRaw != nil {
		t.Fatalf("snapshot mode code tidak sesuai: %+v", snap)
	}
	if drv.pairCalls.Load() != 1 {
		t.Fatalf("PairPhone seharusnya dipanggil 1 kali, didapat %d", drv.pairCalls.Load())
	}

	// QR PNG harus ditolak pada mode code
	if _, _, err := inst.PairingQRPNG(6); !errors.Is(err, ErrQRNotAvailable) {
		t.Fatalf("diharapkan ErrQRNotAvailable pada mode code, didapat %v", err)
	}

	_, _ = inst.CancelPairing()
}

// TestPairIdempotentConflictAndBusy menguji idempotensi, konflik metode, dan batas semaphore lintas instance.
func TestPairIdempotentConflictAndBusy(t *testing.T) {
	sem := semaphore.NewWeighted(1)
	drvA := newFakeDriver()
	instA := newTestInstance("a", sem, drvA)
	defer instA.Close()

	drvB := newFakeDriver()
	instB := newTestInstance("b", sem, drvB)
	defer instB.Close()

	s1, err := instA.StartPairing("")
	if err != nil {
		t.Fatalf("StartPairing A gagal: %v", err)
	}
	// Idempoten: panggilan kedua dengan metode sama harus mengembalikan sesi yang sama
	s2, err := instA.StartPairing("")
	if err != nil || s1 != s2 {
		t.Fatalf("diharapkan sesi idempoten yang sama, err=%v", err)
	}
	// Konflik: minta mode code saat QR sedang jalan pada instance yang sama
	if _, err := instA.StartPairing("6281234567890"); !errors.Is(err, ErrPairingInProgress) {
		t.Fatalf("diharapkan ErrPairingInProgress, didapat %v", err)
	}
	// Lintas instance: instance B harus langsung ditolak ErrPairingBusy tanpa menunggu
	if _, err := instB.StartPairing(""); !errors.Is(err, ErrPairingBusy) {
		t.Fatalf("diharapkan ErrPairingBusy, didapat %v", err)
	}

	snap, err := instA.CancelPairing()
	if err != nil || snap.State != PairCancelled || !drvA.disconnect.Load() {
		t.Fatalf("CancelPairing gagal: snap=%+v, err=%v", snap, err)
	}
}

// TestPairTimeoutTTLAndZeroLeak memastikan sesi mati otomatis saat TTL habis dan tidak meninggalkan goroutine.
func TestPairTimeoutTTLAndZeroLeak(t *testing.T) {
	origTTL := pairSessionTTL
	pairSessionTTL = 120 * time.Millisecond
	defer func() { pairSessionTTL = origTTL }()

	sem := semaphore.NewWeighted(1)
	drv := newFakeDriver()
	inst := newTestInstance("ttl", sem, drv)
	defer inst.Close()

	baseGoroutines := runtime.NumGoroutine()

	sess, err := inst.StartPairing("")
	if err != nil {
		t.Fatalf("StartPairing gagal: %v", err)
	}
	// Daftarkan subscriber lambat yang tidak pernah membaca channel untuk memastikan tidak deadlock
	_, unsub := sess.Subscribe()
	defer unsub()

	for i := 0; i < 20; i++ {
		drv.ch <- whatsmeow.QRChannelItem{Event: "code", Code: "2@spam"}
	}

	select {
	case <-sess.done:
	case <-time.After(2 * time.Second):
		t.Fatal("sesi pairing tidak berhenti setelah TTL habis")
	}

	snap, _ := inst.PairingSnapshot()
	if snap.State != PairTimeout {
		t.Fatalf("diharapkan state timeout, didapat %s", snap.State)
	}

	time.Sleep(30 * time.Millisecond)
	afterGoroutines := runtime.NumGoroutine()
	if afterGoroutines > baseGoroutines+1 {
		t.Fatalf("terdeteksi kebocoran goroutine: sebelum=%d, sesudah=%d", baseGoroutines, afterGoroutines)
	}
}

// TestNormalizePairPhone menolak nomor berawalan 0 atau panjang tidak valid.
func TestNormalizePairPhone(t *testing.T) {
	if _, err := normalizePairPhone("08123456789"); !errors.Is(err, ErrInvalidPhone) {
		t.Fatalf("nomor awalan 0 harus ditolak: %v", err)
	}
	if _, err := normalizePairPhone("62812"); !errors.Is(err, ErrInvalidPhone) {
		t.Fatalf("nomor terlalu pendek harus ditolak: %v", err)
	}
	p, err := normalizePairPhone("+62 813-1579-3349")
	if err != nil || p != "6281315793349" {
		t.Fatalf("nomor valid gagal dinormalisasi: p=%q, err=%v", p, err)
	}
}
