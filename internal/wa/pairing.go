package wa

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow"
	"rsc.io/qr"
)

// Variabel durasi pairing (dibuat var agar dapat dipersingkat saat unit test).
var (
	pairSessionTTL   = 170 * time.Second
	pairStartTimeout = 15 * time.Second
)

const pairSubBuffer = 8

// Sentinel error pairing yang dipetakan ke kode HTTP di layer httpapi.
var (
	ErrAlreadyPaired     = errors.New("instance sudah tertaut, logout terlebih dahulu")
	ErrPairingInProgress = errors.New("sesi pairing dengan metode/nomor berbeda sedang berjalan pada instance ini")
	ErrPairingBusy       = errors.New("instance lain sedang melakukan pairing, coba lagi sebentar")
	ErrNoPairingSession  = errors.New("belum ada sesi pairing pada instance ini")
	ErrNoActivePairing   = errors.New("tidak ada sesi pairing aktif")
	ErrQRNotAvailable    = errors.New("QR code tidak tersedia")
	ErrInvalidPhone      = errors.New("nomor harus format internasional 8-15 digit tanpa awalan 0, contoh 6281234567890")
	ErrPairStartTimeout  = errors.New("QR/kode pairing pertama tidak muncul dalam batas waktu")
)

// PairMethod menentukan mode pairing (qr atau code).
type PairMethod string

const (
	PairMethodQR   PairMethod = "qr"
	PairMethodCode PairMethod = "code"
)

// PairState adalah status siklus hidup sesi pairing.
type PairState string

const (
	PairStarting      PairState = "starting"
	PairQRRefreshed   PairState = "qr_refreshed"
	PairCodeGenerated PairState = "code_generated"
	PairConnected     PairState = "connected"
	PairTimeout       PairState = "timeout"
	PairCancelled     PairState = "cancelled"
	PairError         PairState = "error"
)

// isTerminal menandakan state akhir yang tidak akan berubah lagi.
func (s PairState) isTerminal() bool {
	return s == PairConnected || s == PairTimeout || s == PairCancelled || s == PairError
}

// PairSnapshot adalah representasi publik sesi pairing untuk HTTP API, CLI, dan webhook.
type PairSnapshot struct {
	Instance         string     `json:"instance"`
	Method           PairMethod `json:"method"`
	State            PairState  `json:"state"`
	PairingCode      *string    `json:"pairing_code"`
	QRRaw            *string    `json:"qr_raw"`
	QRImageURL       *string    `json:"qr_image_url"`
	ExpiresIn        int        `json:"expires_in"`
	SessionExpiresIn int        `json:"session_expires_in"`
	JID              *string    `json:"jid"`
	Error            *string    `json:"error"`
}

// pairDriver mengabstraksi operasi whatsmeow yang dibutuhkan sesi pairing (dapat di-mock saat test).
type pairDriver interface {
	GetQRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error)
	Connect() error
	PairPhone(ctx context.Context, phone string) (string, error)
	Disconnect()
	JID() string
}

type whatsmeowPairDriver struct {
	cli *whatsmeow.Client
}

func (d *whatsmeowPairDriver) GetQRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return d.cli.GetQRChannel(ctx)
}

func (d *whatsmeowPairDriver) Connect() error {
	return d.cli.Connect()
}

func (d *whatsmeowPairDriver) PairPhone(ctx context.Context, phone string) (string, error) {
	return d.cli.PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
}

func (d *whatsmeowPairDriver) Disconnect() {
	d.cli.Disconnect()
}

func (d *whatsmeowPairDriver) JID() string {
	if d.cli.Store != nil && d.cli.Store.ID != nil {
		return d.cli.Store.ID.String()
	}
	return ""
}

func defaultPairDriver(inst *Instance, ctx context.Context) (pairDriver, error) {
	dev, err := inst.resolveOrCreateDevice(ctx)
	if err != nil {
		return nil, err
	}
	cli := inst.attachClient(dev)
	return &whatsmeowPairDriver{cli: cli}, nil
}

// mockPairDriver dipakai oleh pengujian di luar package wa (mis. internal/httpapi).
type mockPairDriver struct {
	ch   chan whatsmeow.QRChannelItem
	code string
	jid  string
}

func (m *mockPairDriver) GetQRChannel(_ context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return m.ch, nil
}
func (m *mockPairDriver) Connect() error                                        { return nil }
func (m *mockPairDriver) PairPhone(_ context.Context, _ string) (string, error) { return m.code, nil }
func (m *mockPairDriver) Disconnect()                                           {}
func (m *mockPairDriver) JID() string                                           { return m.jid }

// SetDriverFactoryForTest mengizinkan package test lain menyuntikkan channel QR tiruan pada instance.
func (i *Instance) SetDriverFactoryForTest(fn func() (chan whatsmeow.QRChannelItem, string)) {
	i.driverFactory = func(_ *Instance, _ context.Context) (pairDriver, error) {
		ch, code := fn()
		return &mockPairDriver{
			ch:   ch,
			code: code,
			jid:  "6281234567890:1@s.whatsapp.net",
		}, nil
	}
}

// PairSession menjalankan satu proses pairing di goroutine tunggal dengan TTL keras.
type PairSession struct {
	inst   *Instance
	method PairMethod
	phone  string

	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.RWMutex
	state        PairState
	qrRaw        string
	code         string
	jid          string
	errMsg       string
	qrExpiresAt  time.Time
	sessionEndAt time.Time
	cancelledBy  bool

	pngKey string // kunci cache: qrRaw + scale
	png    []byte

	subs map[chan PairSnapshot]struct{}

	ready     chan struct{} // ditutup saat snapshot pertama (QR/kode/terminal) tersedia
	readyOnce sync.Once
	done      chan struct{} // ditutup saat goroutine sesi selesai total
}

func newPairSession(inst *Instance, method PairMethod, phone string) *PairSession {
	// Context diturunkan dari Background agar tidak mati saat request HTTP selesai
	ctx, cancel := context.WithTimeout(context.Background(), pairSessionTTL)
	return &PairSession{
		inst:         inst,
		method:       method,
		phone:        phone,
		ctx:          ctx,
		cancel:       cancel,
		state:        PairStarting,
		sessionEndAt: time.Now().Add(pairSessionTTL),
		subs:         make(map[chan PairSnapshot]struct{}),
		ready:        make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Active mengembalikan true selama goroutine sesi masih berjalan.
func (s *PairSession) Active() bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

// Snapshot membaca state terkini secara thread-safe.
func (s *PairSession) Snapshot() PairSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

func (s *PairSession) snapshotLocked() PairSnapshot {
	now := time.Now()
	snap := PairSnapshot{
		Instance:         s.inst.id,
		Method:           s.method,
		State:            s.state,
		SessionExpiresIn: secondsUntil(now, s.sessionEndAt),
	}
	if s.state.isTerminal() {
		snap.SessionExpiresIn = 0
	}
	if s.method == PairMethodQR && s.qrRaw != "" && !s.state.isTerminal() {
		raw := s.qrRaw
		url := fmt.Sprintf("/v1/instances/%s/qr.png", s.inst.id)
		snap.QRRaw = &raw
		snap.QRImageURL = &url
		snap.ExpiresIn = secondsUntil(now, s.qrExpiresAt)
	}
	if s.method == PairMethodCode && s.code != "" && !s.state.isTerminal() {
		code := s.code
		snap.PairingCode = &code
		snap.ExpiresIn = snap.SessionExpiresIn
	}
	if s.jid != "" {
		jid := s.jid
		snap.JID = &jid
	}
	if s.errMsg != "" {
		msg := s.errMsg
		snap.Error = &msg
	}
	return snap
}

// WaitReady menunggu snapshot pertama tersedia atau timeout start terlampaui.
func (s *PairSession) WaitReady(ctx context.Context) (PairSnapshot, error) {
	timer := time.NewTimer(pairStartTimeout)
	defer timer.Stop()
	select {
	case <-s.ready:
		return s.Snapshot(), nil
	case <-timer.C:
		return s.Snapshot(), ErrPairStartTimeout
	case <-ctx.Done():
		// Request HTTP dibatalkan klien; sesi tetap berjalan sampai TTL
		return s.Snapshot(), ctx.Err()
	}
}

// Cancel menghentikan sesi secara manual (DELETE /pair atau CLI Ctrl+C).
func (s *PairSession) Cancel() {
	s.mu.Lock()
	s.cancelledBy = true
	s.mu.Unlock()
	s.cancel()
}

// Subscribe mendaftarkan listener event; fungsi unsubscribe wajib dipanggil oleh pemanggil.
func (s *PairSession) Subscribe() (<-chan PairSnapshot, func()) {
	ch := make(chan PairSnapshot, pairSubBuffer)
	s.mu.Lock()
	if !s.Active() {
		ch <- s.snapshotLocked()
		close(ch)
		s.mu.Unlock()
		return ch, func() {}
	}
	s.subs[ch] = struct{}{}
	ch <- s.snapshotLocked()
	s.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			s.mu.Lock()
			if _, ok := s.subs[ch]; ok {
				delete(s.subs, ch)
				close(ch)
			}
			s.mu.Unlock()
		})
	}
}

// update mengubah state di bawah lock lalu menyiarkan snapshot ke subscriber & webhook.
func (s *PairSession) update(fn func()) {
	s.mu.Lock()
	fn()
	snap := s.snapshotLocked()
	for ch := range s.subs {
		// Non-blocking: subscriber lambat tidak boleh menahan goroutine sesi
		select {
		case ch <- snap:
		default:
		}
	}
	s.mu.Unlock()

	s.readyOnce.Do(func() { close(s.ready) })
	if s.inst.mgr != nil && s.inst.mgr.webhook != nil {
		s.inst.mgr.webhook.Enqueue(s.inst.id, "pairing", webhookPairingData(snap))
	}
}

// PNG mengembalikan gambar QR terkini dengan cache per (qrRaw, scale).
func (s *PairSession) PNG(scale int) ([]byte, int, error) {
	s.mu.RLock()
	raw, state, method := s.qrRaw, s.state, s.method
	expiresIn := secondsUntil(time.Now(), s.qrExpiresAt)
	key := fmt.Sprintf("%s|%d", raw, scale)
	if s.pngKey == key && s.png != nil && !state.isTerminal() {
		png := s.png
		s.mu.RUnlock()
		return png, expiresIn, nil
	}
	s.mu.RUnlock()

	if method != PairMethodQR || raw == "" || state.isTerminal() {
		return nil, 0, ErrQRNotAvailable
	}
	code, err := qr.Encode(raw, qr.M)
	if err != nil {
		return nil, 0, fmt.Errorf("encode QR: %w", err)
	}
	code.Scale = scale
	png := code.PNG()

	s.mu.Lock()
	// Simpan ke cache hanya jika QR belum berganti selama proses encode
	if s.qrRaw == raw {
		s.pngKey, s.png = key, png
	}
	s.mu.Unlock()
	return png, expiresIn, nil
}

// run adalah satu-satunya goroutine sesi; seluruh resource dilepas pada defer tunggal.
func (s *PairSession) run() {
	var drv pairDriver
	success := false

	defer func() {
		s.cancel()
		if !success {
			if drv != nil {
				drv.Disconnect()
			}
			s.inst.restoreStatusAfterAbort(context.Background())
		}
		s.inst.mgr.pairingSem.Release(1)

		s.mu.Lock()
		for ch := range s.subs {
			close(ch)
		}
		s.subs = nil
		// Bebaskan slice PNG dari memori segera setelah sesi selesai
		s.png, s.pngKey = nil, ""
		s.mu.Unlock()

		s.readyOnce.Do(func() { close(s.ready) })
		close(s.done)
	}()

	factory := s.inst.driverFactory
	if factory == nil {
		factory = defaultPairDriver
	}
	var err error
	drv, err = factory(s.inst, s.ctx)
	if err != nil {
		s.fail(err)
		return
	}

	s.inst.setStatus(store.StatusPairing, "")
	if s.inst.mgr != nil && s.inst.mgr.store != nil {
		_ = s.inst.mgr.store.UpdateInstanceStatus(s.ctx, s.inst.id, store.StatusPairing, "")
	}

	qrChan, err := drv.GetQRChannel(s.ctx)
	if err != nil {
		s.fail(fmt.Errorf("buka QR channel: %w", err))
		return
	}
	if err := drv.Connect(); err != nil {
		s.fail(fmt.Errorf("connect untuk pairing: %w", err))
		return
	}

	codeRequested := false
	for {
		select {
		case <-s.ctx.Done():
			s.mu.RLock()
			manual := s.cancelledBy
			s.mu.RUnlock()
			if manual {
				s.update(func() { s.state = PairCancelled })
			} else {
				s.update(func() { s.state = PairTimeout })
			}
			return

		case item, ok := <-qrChan:
			if !ok {
				s.mu.RLock()
				manual := s.cancelledBy
				s.mu.RUnlock()
				if manual {
					s.update(func() { s.state = PairCancelled })
				} else {
					s.update(func() { s.state = PairTimeout })
				}
				return
			}
			switch {
			case item.Event == "code":
				if s.method == PairMethodQR {
					to := item.Timeout
					if to <= 0 {
						to = 20 * time.Second
					}
					expiresAt := time.Now().Add(to)
					s.update(func() {
						s.state = PairQRRefreshed
						s.qrRaw = item.Code
						s.qrExpiresAt = expiresAt
					})
					continue
				}
				if codeRequested {
					continue
				}
				codeRequested = true
				code, err := drv.PairPhone(s.ctx, s.phone)
				if err != nil {
					s.fail(fmt.Errorf("PairPhone: %w", err))
					return
				}
				s.update(func() {
					s.state = PairCodeGenerated
					s.code = code
				})

			case item.Event == "success":
				jid := drv.JID()
				success = true
				s.inst.setStatus(store.StatusConnected, jid)
				if s.inst.mgr != nil && s.inst.mgr.store != nil {
					_ = s.inst.mgr.store.UpdateInstanceStatus(context.Background(), s.inst.id, store.StatusConnected, jid)
				}
				s.update(func() {
					s.state = PairConnected
					s.jid = jid
				})
				return

			case item.Event == "timeout":
				s.update(func() { s.state = PairTimeout })
				return

			case item.Event == "error" || strings.HasPrefix(item.Event, "err-"):
				msg := item.Event
				if item.Error != nil {
					msg = item.Error.Error()
				}
				s.fail(errors.New(msg))
				return
			}
		}
	}
}

func (s *PairSession) fail(err error) {
	s.update(func() {
		s.state = PairError
		s.errMsg = err.Error()
	})
}

func webhookPairingData(s PairSnapshot) map[string]any {
	return map[string]any{
		"status":       s.State,
		"method":       s.Method,
		"qr_raw":       s.QRRaw,
		"pairing_code": s.PairingCode,
		"expires_in":   s.ExpiresIn,
		"jid":          s.JID,
		"error":        s.Error,
	}
}

func secondsUntil(now, t time.Time) int {
	if t.IsZero() {
		return 0
	}
	d := int(t.Sub(now).Seconds())
	if d < 0 {
		return 0
	}
	return d
}

// normalizePairPhone memvalidasi nomor untuk mode pairing code (8-15 digit, tanpa awalan 0).
func normalizePairPhone(raw string) (string, error) {
	p := digitsOnly(raw)
	if len(p) < 8 || len(p) > 15 || strings.HasPrefix(p, "0") {
		return "", ErrInvalidPhone
	}
	return p, nil
}
