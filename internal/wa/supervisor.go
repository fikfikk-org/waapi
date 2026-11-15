package wa

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/obs"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

// ConnState merepresentasikan state mesin Supervisor koneksi WhatsApp per instance.
type ConnState string

const (
	StateIdle       ConnState = "idle"
	StateConnecting ConnState = "connecting"
	StateOnline     ConnState = "online"
	StateDegraded   ConnState = "degraded"
	StateBackoff    ConnState = "backoff"
	StateReplaced   ConnState = "replaced"
	StateLoggedOut  ConnState = "logged_out"
	StateBannedTemp ConnState = "banned_temp"
	StateBanned     ConnState = "banned"
	StateOutdated   ConnState = "outdated"
	StateStopped    ConnState = "stopped"
)

// BanInfo merangkum kode dan waktu kedaluwarsa ban sementara/permanen WhatsApp.
type BanInfo struct {
	Code      int   `json:"code"`
	ExpiresAt int64 `json:"expires_at,omitempty"`
}

// Backoff menghitung jeda exponential backoff dengan equal jitter.
type Backoff struct {
	Base, Max time.Duration
	Factor    float64
}

// Next mengembalikan jeda "equal jitter": d/2 + rnd*d/2 dengan d = min(Max, Base*Factor^attempt).
func (b Backoff) Next(attempt int, rnd float64) time.Duration {
	base := b.Base
	if base <= 0 {
		base = 2 * time.Second
	}
	maxD := b.Max
	if maxD <= 0 {
		maxD = 5 * time.Minute
	}
	if maxD < base {
		maxD = base
	}
	factor := b.Factor
	if factor <= 0 {
		factor = 2
	}
	if attempt < 0 {
		attempt = 0
	}
	if math.IsNaN(rnd) {
		rnd = 0.5
	} else if rnd < 0 {
		rnd = 0
	} else if rnd > 1 {
		rnd = 1
	}

	df := float64(base) * math.Pow(factor, float64(attempt))
	var d time.Duration
	if math.IsNaN(df) || math.IsInf(df, 1) || df >= float64(maxD) {
		d = maxD
	} else {
		d = time.Duration(df)
		if d > maxD {
			d = maxD
		}
	}

	half := d / 2
	span := d - half
	out := half + time.Duration(rnd*float64(span))
	if out < half {
		out = half
	}
	if out > d {
		out = d
	}
	return out
}

// SupervisorConfig mengatur parameter backoff, circuit breaker, keepalive, dan timeout connect.
type SupervisorConfig struct {
	Backoff          Backoff       // default {2s, 5m, 2}
	CircuitThreshold int           // default 10 kegagalan berturut-turut
	CircuitMax       time.Duration // default 15m
	KeepAliveMaxFail time.Duration // WAAPI_KEEPALIVE_MAX_FAIL, default 75s
	ConnectTimeout   time.Duration // default 30s, ditegakkan dengan timer (BUKAN context berbatas waktu)
}

func (c SupervisorConfig) withDefaults() SupervisorConfig {
	if c.Backoff.Base <= 0 {
		c.Backoff.Base = 2 * time.Second
	}
	if c.Backoff.Max <= 0 {
		c.Backoff.Max = 5 * time.Minute
	}
	if c.Backoff.Factor <= 0 {
		c.Backoff.Factor = 2
	}
	if c.CircuitThreshold <= 0 {
		c.CircuitThreshold = 10
	}
	if c.CircuitMax <= 0 {
		c.CircuitMax = 15 * time.Minute
	}
	if c.KeepAliveMaxFail <= 0 {
		c.KeepAliveMaxFail = 75 * time.Second
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 30 * time.Second
	}
	return c
}

// StateSnapshot menyimpan potret keadaan Supervisor saat ini.
type StateSnapshot struct {
	State       ConnState
	Since       time.Time
	Attempts    int
	NextRetryAt time.Time
	LastError   string
	BanCode     int
	BanUntil    time.Time
	Circuit     bool
}

// Supervisor mengelola siklus koneksi, reconnect, dan event kritis untuk satu instance secara terpusat (1 goroutine).
type Supervisor struct {
	instanceID string
	drv        connDriver
	emit       Emitter
	cfg        SupervisorConfig
	rnd        func() float64

	mu       sync.RWMutex
	snap     StateSnapshot
	jid      string
	onChange func(StateSnapshot)

	evCh         chan any
	pendingMu    sync.Mutex
	pendingEvent any
	pendingReady chan struct{}

	opMu         sync.Mutex
	loopCancel   context.CancelFunc
	loopDone     chan struct{}
	adoptCh      chan struct{}
	stoppedCh    chan struct{}
	resetPending bool
	inOutage     bool
}

func newSupervisor(instanceID string, drv connDriver, emit Emitter, cfg SupervisorConfig, rnd func() float64) *Supervisor {
	if rnd == nil {
		rnd = rand.Float64
	}
	cfg = cfg.withDefaults()
	return &Supervisor{
		instanceID: instanceID,
		drv:        drv,
		emit:       emit,
		cfg:        cfg,
		rnd:        rnd,
		snap: StateSnapshot{
			State: StateIdle,
			Since: time.Now(),
		},
		evCh:         make(chan any, 64),
		pendingReady: make(chan struct{}, 1),
		adoptCh:      make(chan struct{}, 1),
		stoppedCh:    make(chan struct{}),
	}
}

func (s *Supervisor) setJID(jid string) {
	s.mu.Lock()
	s.jid = jid
	s.mu.Unlock()
}

func (s *Supervisor) setOnChange(fn func(StateSnapshot)) {
	s.mu.Lock()
	s.onChange = fn
	s.mu.Unlock()
}

// Start menyalakan goroutine supervisor dan segera memulai percobaan koneksi dengan ctx tanpa deadline.
func (s *Supervisor) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	s.opMu.Lock()
	defer s.opMu.Unlock()

	s.stopLocked(false)
	if s.isStoppedChanClosed() {
		s.stoppedCh = make(chan struct{})
	}
	s.inOutage = false
	s.resetPending = false

	loopCtx, cancel := context.WithCancel(ctx)
	doneCh := make(chan struct{})
	s.loopCancel = cancel
	s.loopDone = doneCh

	go s.loop(loopCtx, doneCh, false)
}

// Adopt mengambil alih client yang sudah terhubung setelah proses pairing sukses tanpa ConnectContext ulang.
func (s *Supervisor) Adopt() {
	s.opMu.Lock()
	defer s.opMu.Unlock()

	if s.loopDone != nil {
		// Loop sudah aktif, kirim sinyal adopt agar berpindah ke StateOnline
		select {
		case s.adoptCh <- struct{}{}:
		default:
		}
		return
	}

	if s.isStoppedChanClosed() {
		s.stoppedCh = make(chan struct{})
	}
	s.inOutage = false
	s.resetPending = false

	loopCtx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	s.loopCancel = cancel
	s.loopDone = doneCh

	go s.loop(loopCtx, doneCh, true)
}

// Stop menghentikan loop supervisor, membatalkan timer aktif, dan memutus koneksi client.
func (s *Supervisor) Stop() {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.stopLocked(true)
}

func (s *Supervisor) stopLocked(disconnect bool) {
	if !s.isStoppedChanClosed() {
		close(s.stoppedCh)
	}
	if s.loopCancel != nil {
		s.loopCancel()
		s.loopCancel = nil
	}
	if s.loopDone != nil {
		<-s.loopDone
		s.loopDone = nil
	}
	if disconnect && s.drv != nil {
		s.drv.Disconnect()
	}
	s.mu.Lock()
	if s.snap.State != StateStopped {
		s.snap.State = StateStopped
		s.snap.Since = time.Now()
		s.snap.NextRetryAt = time.Time{}
	}
	snap := s.snap
	cb := s.onChange
	s.mu.Unlock()
	if disconnect && cb != nil {
		cb(snap)
	}
}

func (s *Supervisor) isStoppedChanClosed() bool {
	select {
	case <-s.stoppedCh:
		return true
	default:
		return false
	}
}

// State mengembalikan salinan StateSnapshot terbaru secara aman.
func (s *Supervisor) State() StateSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}

// OnEvent menerima event koneksi whatsmeow tanpa spawning goroutine dan menjaga event kritis tetap terkirim.
func (s *Supervisor) OnEvent(evt any) {
	if evt == nil {
		return
	}
	if isCriticalSupervisorEvent(evt) {
		s.opMu.Lock()
		stoppedCh := s.stoppedCh
		s.opMu.Unlock()

		select {
		case s.evCh <- evt:
		case <-stoppedCh:
		}
		return
	}

	select {
	case s.evCh <- evt:
		return
	default:
	}

	// Channel penuh: timpa slot pending terbaru untuk event non-kritis agar pemanggil tidak pernah terblokir
	s.pendingMu.Lock()
	s.pendingEvent = evt
	s.pendingMu.Unlock()
	obs.MetricSupervisorEventsCoalescedTotal.Inc()

	select {
	case s.pendingReady <- struct{}{}:
	default:
	}
}

func isCriticalSupervisorEvent(evt any) bool {
	switch evt.(type) {
	case *events.LoggedOut, *events.StreamReplaced, *events.TemporaryBan, *events.ClientOutdated, *events.ConnectFailure:
		return true
	default:
		return false
	}
}

func isTerminalState(st ConnState) bool {
	switch st {
	case StateReplaced, StateLoggedOut, StateBanned, StateOutdated, StateStopped:
		return true
	default:
		return false
	}
}

func (s *Supervisor) popPending() any {
	s.pendingMu.Lock()
	evt := s.pendingEvent
	s.pendingEvent = nil
	s.pendingMu.Unlock()
	return evt
}

func (s *Supervisor) loop(loopCtx context.Context, doneCh chan struct{}, startOnline bool) {
	var retryTimer *time.Timer
	var connTimer *time.Timer
	defer func() {
		stopTimer(&retryTimer)
		stopTimer(&connTimer)
		close(doneCh)
	}()

	if startOnline {
		s.transitionToOnline(false)
	} else {
		if !s.doConnect(loopCtx, &connTimer, &retryTimer) {
			return
		}
	}

	for {
		var retryC <-chan time.Time
		if retryTimer != nil {
			retryC = retryTimer.C
		}
		var connC <-chan time.Time
		if connTimer != nil {
			connC = connTimer.C
		}

		select {
		case <-loopCtx.Done():
			return

		case <-s.adoptCh:
			stopTimer(&retryTimer)
			stopTimer(&connTimer)
			s.resetPending = false
			s.inOutage = false
			s.transitionToOnline(false)

		case <-connC:
			stopTimer(&connTimer)
			if s.currentState() == StateConnecting {
				// Tegakkan ConnectTimeout lewat Disconnect(), bukan membatalkan context koneksi
				if s.drv != nil {
					s.drv.Disconnect()
				}
				s.scheduleFailureBackoff("connect timeout", s.cfg.Backoff, &retryTimer)
			}

		case <-retryC:
			stopTimer(&retryTimer)
			cur := s.currentState()
			if cur == StateBackoff || cur == StateBannedTemp {
				if !s.doConnect(loopCtx, &connTimer, &retryTimer) {
					return
				}
			}

		case evt := <-s.evCh:
			s.processEvent(evt, &connTimer, &retryTimer)

		case <-s.pendingReady:
			if evt := s.popPending(); evt != nil {
				s.processEvent(evt, &connTimer, &retryTimer)
			}
		}
	}
}

func (s *Supervisor) doConnect(loopCtx context.Context, connTimer **time.Timer, retryTimer **time.Timer) bool {
	if loopCtx.Err() != nil {
		return false
	}

	s.mu.Lock()
	s.snap.State = StateConnecting
	s.snap.Since = time.Now()
	s.snap.NextRetryAt = time.Time{}
	snap := s.snap
	cb := s.onChange
	s.mu.Unlock()
	if cb != nil {
		cb(snap)
	}

	if s.drv == nil {
		s.scheduleFailureBackoff("connDriver belum terpasang", s.cfg.Backoff, retryTimer)
		return true
	}

	// Context ke ConnectContext diwariskan ke lifetime koneksi whatsmeow sehingga tidak boleh memiliki deadline
	err := s.drv.ConnectContext(loopCtx)
	if loopCtx.Err() != nil {
		return false
	}
	if errors.Is(err, whatsmeow.ErrAlreadyConnected) {
		stopTimer(connTimer)
		stopTimer(retryTimer)
		s.transitionToOnline(true)
		return true
	}
	if err != nil {
		stopTimer(connTimer)
		s.scheduleFailureBackoff(err.Error(), s.cfg.Backoff, retryTimer)
		return true
	}

	resetTimer(connTimer, s.cfg.ConnectTimeout)
	return true
}

func (s *Supervisor) processEvent(rawEvt any, connTimer **time.Timer, retryTimer **time.Timer) {
	cur := s.currentState()
	if cur == StateIdle || isTerminalState(cur) {
		return
	}

	switch evt := rawEvt.(type) {
	case *events.Connected:
		s.resetPending = false
		stopTimer(connTimer)
		stopTimer(retryTimer)
		s.transitionToOnline(cur != StateOnline)

	case *events.Disconnected:
		if s.resetPending {
			// Abaikan Disconnected turunan dari ResetConnection() agar timer backoff tidak ganda
			s.resetPending = false
			return
		}
		if cur == StateBannedTemp {
			return
		}
		stopTimer(connTimer)
		if s.currentAttempts() == 0 {
			s.scheduleDisconnectJitter(retryTimer)
		} else {
			s.scheduleFailureBackoff("disconnected", s.cfg.Backoff, retryTimer)
		}

	case *events.KeepAliveTimeout:
		if cur != StateOnline && cur != StateDegraded {
			return
		}
		elapsed := time.Since(evt.LastSuccess)
		if !evt.LastSuccess.IsZero() && elapsed >= s.cfg.KeepAliveMaxFail {
			stopTimer(connTimer)
			s.resetPending = true
			if s.drv != nil {
				s.drv.ResetConnection()
			}
			s.scheduleDisconnectJitter(retryTimer)
			return
		}
		if cur != StateDegraded {
			s.setState(StateDegraded, fmt.Sprintf("keepalive timeout (%dx)", evt.ErrorCount), 0, time.Time{}, time.Time{}, true)
		}

	case *events.KeepAliveRestored:
		if cur == StateDegraded {
			s.transitionToOnline(true)
		}

	case *events.StreamReplaced:
		s.resetPending = false
		stopTimer(connTimer)
		stopTimer(retryTimer)
		slog.Warn("stream WhatsApp digantikan sesi lain (StreamReplaced)", "instance", s.instanceID)
		s.setState(StateReplaced, "stream replaced", 0, time.Time{}, time.Time{}, true)

	case *events.ClientOutdated:
		s.resetPending = false
		stopTimer(connTimer)
		stopTimer(retryTimer)
		slog.Error("versi klien WhatsApp terlalu lama, jalankan waapi update", "instance", s.instanceID)
		s.setState(StateOutdated, "client outdated", 0, time.Time{}, time.Time{}, true)

	case *events.LoggedOut:
		s.resetPending = false
		stopTimer(connTimer)
		stopTimer(retryTimer)
		if evt.Reason == events.ConnectFailureUnknownLogout {
			slog.Error("akun WhatsApp diblokir permanen oleh server (reason 406)", "instance", s.instanceID, "reason", evt.Reason.String())
			s.setState(StateBanned, evt.Reason.String(), int(evt.Reason), time.Time{}, time.Time{}, true)
		} else {
			slog.Warn("instance logged out dari perangkat utama", "instance", s.instanceID, "reason", evt.Reason.String())
			s.setState(StateLoggedOut, evt.Reason.String(), 0, time.Time{}, time.Time{}, true)
		}

	case *events.TemporaryBan:
		s.resetPending = false
		stopTimer(connTimer)
		now := time.Now()
		var delay time.Duration
		var banUntil time.Time
		if evt.Expire > 0 {
			jitter := 30*time.Second + time.Duration(s.rnd()*float64(90*time.Second))
			delay = evt.Expire + jitter
			banUntil = now.Add(evt.Expire)
		} else {
			maxD := maxDuration(15*time.Minute, s.cfg.CircuitMax)
			b := Backoff{Base: 15 * time.Minute, Max: maxD, Factor: 2}
			delay = b.Next(s.currentAttempts(), s.rnd())
			banUntil = now.Add(delay)
		}
		nextAt := now.Add(delay)
		resetTimer(retryTimer, delay)
		slog.Warn("akun WhatsApp terkena temporary ban", "instance", s.instanceID, "code", evt.Code, "expire", evt.Expire, "next_retry", delay)
		s.setState(StateBannedTemp, fmt.Sprintf("temporary ban (code %d)", evt.Code), int(evt.Code), banUntil, nextAt, true)

	case *events.ConnectFailure:
		s.resetPending = false
		stopTimer(connTimer)
		b := Backoff{
			Base:   60 * time.Second,
			Max:    maxDuration(60*time.Second, s.cfg.Backoff.Max),
			Factor: s.cfg.Backoff.Factor,
		}
		s.scheduleFailureBackoff(fmt.Sprintf("connect failure (%s): %s", evt.Reason.String(), evt.Message), b, retryTimer)
	}
}

func (s *Supervisor) transitionToOnline(emitEvt bool) {
	s.mu.Lock()
	prev := s.snap.State
	s.snap.State = StateOnline
	s.snap.Since = time.Now()
	s.snap.Attempts = 0
	s.snap.Circuit = false
	s.snap.LastError = ""
	s.snap.BanCode = 0
	s.snap.BanUntil = time.Time{}
	s.snap.NextRetryAt = time.Time{}
	s.inOutage = false
	snap := s.snap
	cb := s.onChange
	s.mu.Unlock()

	if cb != nil {
		cb(snap)
	}
	if emitEvt {
		s.emitConnection(prev, snap)
	}
}

func (s *Supervisor) scheduleDisconnectJitter(retryTimer **time.Timer) {
	delay := time.Duration(s.rnd() * float64(time.Second))
	now := time.Now()

	s.mu.Lock()
	prev := s.snap.State
	shouldEmit := !s.inOutage
	s.inOutage = true
	s.snap.State = StateBackoff
	s.snap.Since = now
	s.snap.LastError = "disconnected"
	s.snap.NextRetryAt = now.Add(delay)
	snap := s.snap
	cb := s.onChange
	s.mu.Unlock()

	resetTimer(retryTimer, delay)
	if cb != nil {
		cb(snap)
	}
	if shouldEmit {
		s.emitConnection(prev, snap)
	}
}

func (s *Supervisor) scheduleFailureBackoff(reason string, b Backoff, retryTimer **time.Timer) {
	s.mu.Lock()
	prev := s.snap.State
	prevCircuit := s.snap.Circuit
	wasOutage := s.inOutage
	s.inOutage = true

	s.snap.Attempts++
	attempts := s.snap.Attempts
	circuit := s.snap.Circuit
	if attempts >= s.cfg.CircuitThreshold {
		circuit = true
	}

	var delay time.Duration
	if circuit {
		delay = Backoff{Base: s.cfg.CircuitMax, Max: s.cfg.CircuitMax, Factor: 1}.Next(0, s.rnd())
	} else {
		delay = b.Next(attempts-1, s.rnd())
	}

	now := time.Now()
	s.snap.State = StateBackoff
	s.snap.Since = now
	s.snap.LastError = reason
	s.snap.Circuit = circuit
	s.snap.NextRetryAt = now.Add(delay)
	snap := s.snap
	cb := s.onChange
	s.mu.Unlock()

	resetTimer(retryTimer, delay)
	if cb != nil {
		cb(snap)
	}
	if !wasOutage || (!prevCircuit && circuit) {
		s.emitConnection(prev, snap)
	}
}

func (s *Supervisor) setState(next ConnState, reason string, banCode int, banUntil, nextRetryAt time.Time, emitEvt bool) {
	s.mu.Lock()
	prev := s.snap.State
	s.snap.State = next
	s.snap.Since = time.Now()
	s.snap.LastError = reason
	s.snap.BanCode = banCode
	s.snap.BanUntil = banUntil
	s.snap.NextRetryAt = nextRetryAt
	snap := s.snap
	cb := s.onChange
	s.mu.Unlock()

	if cb != nil {
		cb(snap)
	}
	if emitEvt && prev != next {
		s.emitConnection(prev, snap)
	}
}

func (s *Supervisor) currentState() ConnState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap.State
}

func (s *Supervisor) currentAttempts() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap.Attempts
}

func (s *Supervisor) emitConnection(prev ConnState, snap StateSnapshot) {
	if s.emit == nil {
		return
	}
	v1Status := "disconnected"
	switch snap.State {
	case StateOnline:
		v1Status = "connected"
	case StateLoggedOut:
		v1Status = "logged_out"
	}

	s.mu.RLock()
	jid := s.jid
	s.mu.RUnlock()

	nextRetryIn := 0
	if !snap.NextRetryAt.IsZero() {
		sec := int(time.Until(snap.NextRetryAt).Seconds())
		if sec > 0 {
			nextRetryIn = sec
		}
	}

	var ban *BanInfo
	if snap.BanCode != 0 || !snap.BanUntil.IsZero() {
		ban = &BanInfo{Code: snap.BanCode}
		if !snap.BanUntil.IsZero() {
			ban.ExpiresAt = snap.BanUntil.Unix()
		}
	}

	payload := map[string]any{
		"status":        v1Status,
		"state":         string(snap.State),
		"prev_state":    string(prev),
		"jid":           jid,
		"reason":        snap.LastError,
		"attempt":       snap.Attempts,
		"next_retry_in": nextRetryIn,
		"circuit":       snap.Circuit,
		"ban":           ban,
	}
	_ = s.emit.Emit(context.Background(), s.instanceID, "connection", payload)
}

func resetTimer(t **time.Timer, d time.Duration) {
	if d < 0 {
		d = 0
	}
	if *t == nil {
		*t = time.NewTimer(d)
		return
	}
	(*t).Reset(d)
}

func stopTimer(t **time.Timer) {
	if *t != nil {
		(*t).Stop()
		*t = nil
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
