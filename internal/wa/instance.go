package wa

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/logx"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow"
	waStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"golang.org/x/time/rate"
)

// ErrInvalidRecipient dikembalikan jika format nomor atau JID tujuan tidak valid.
var ErrInvalidRecipient = jid.ErrInvalidRecipient

// InstanceStatus adalah representasi status runtime + database sebuah instance.
type InstanceStatus struct {
	ID                string                 `json:"id"`
	Status            string                 `json:"status"`
	State             string                 `json:"state"`
	StateSince        int64                  `json:"state_since,omitempty"`
	ReconnectAttempts int                    `json:"reconnect_attempts"`
	NextRetryIn       int                    `json:"next_retry_in"`
	Ban               *BanInfo               `json:"ban"`
	JID               string                 `json:"jid,omitempty"`
	PushName          string                 `json:"push_name,omitempty"`
	QueueDepth        int                    `json:"queue_depth"`
	RatePerMin        int                    `json:"rate_per_min"`
	WebhookURL        string                 `json:"webhook_url,omitempty"`
	WebhookEvents     string                 `json:"webhook_events"`
	AutoStart         bool                   `json:"auto_start"`
	Settings          store.InstanceSettings `json:"settings"`
	PairedAt          int64                  `json:"paired_at,omitempty"`
	ConnectedSince    int64                  `json:"connected_since,omitempty"`
}

// NumberCheckResult merangkum hasil pemeriksaan satu nomor pada WhatsApp.
type NumberCheckResult struct {
	Number string `json:"number"`
	Exists bool   `json:"exists"`
	JID    string `json:"jid,omitempty"`
}

type cachedCheck struct {
	exists    bool
	jid       string
	expiresAt time.Time
}

// Instance membungkus satu whatsmeow.Client beserta supervisor koneksi dan rate limiter-nya.
type Instance struct {
	id      string
	mgr     *Manager
	baseCtx context.Context

	mu             sync.RWMutex
	client         *whatsmeow.Client
	sup            *Supervisor
	supCancel      context.CancelFunc
	limiter        *rate.Limiter
	ratePerMin     int
	status         string
	jid            string
	connectedSince int64
	settings       store.InstanceSettings
	cancel         context.CancelFunc

	offline atomic.Bool

	checkMu    sync.Mutex
	checkCache map[string]cachedCheck

	pairMu        sync.Mutex
	pairing       *PairSession
	driverFactory func(*Instance, context.Context) (pairDriver, error)
}

func newInstance(m *Manager, meta store.Instance) *Instance {
	ctx, cancel := context.WithCancel(context.Background())
	settings := meta.Settings
	if settings.JournalReceipts == "" {
		settings = store.Defaults()
	}
	inst := &Instance{
		id:         meta.ID,
		mgr:        m,
		baseCtx:    ctx,
		limiter:    newLimiter(meta.RatePerMin),
		ratePerMin: meta.RatePerMin,
		status:     meta.Status,
		jid:        meta.JID,
		settings:   settings,
		cancel:     cancel,
		checkCache: make(map[string]cachedCheck),
	}
	return inst
}

func newLimiter(ratePerMin int) *rate.Limiter {
	if ratePerMin <= 0 {
		ratePerMin = 20
	}
	r := rate.Every(time.Minute / time.Duration(ratePerMin))
	return rate.NewLimiter(r, 3)
}

func (i *Instance) getClient() *whatsmeow.Client {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.client
}

func (i *Instance) getSupervisor() *Supervisor {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.sup
}

func (i *Instance) getJID() string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.jid
}

func (i *Instance) getLimiter() *rate.Limiter {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.limiter
}

// SetRatePerMin memperbarui batas kirim per menit secara dinamis.
func (i *Instance) SetRatePerMin(rpm int) {
	i.mu.Lock()
	i.ratePerMin = rpm
	i.limiter = newLimiter(rpm)
	i.mu.Unlock()
}

// SetSettings menyinkronkan salinan pengaturan instance di memori agar hot path tidak melakukan query DB.
func (i *Instance) SetSettings(s store.InstanceSettings) {
	if i == nil {
		return
	}
	if s.JournalReceipts == "" {
		s.JournalReceipts = store.Defaults().JournalReceipts
	}
	i.mu.Lock()
	i.settings = s
	i.mu.Unlock()
}

func (i *Instance) cachedSettings() store.InstanceSettings {
	if i == nil {
		return store.Defaults()
	}
	i.mu.RLock()
	s := i.settings
	i.mu.RUnlock()
	if s.JournalReceipts == "" {
		s.JournalReceipts = store.Defaults().JournalReceipts
	}
	return s
}

func (i *Instance) lidResolver() LIDResolver {
	cli := i.getClient()
	if cli == nil || cli.Store == nil || cli.Store.LIDs == nil {
		return nil
	}
	return newStoreLIDResolver(cli.Store.LIDs)
}

func (i *Instance) emitter() Emitter {
	if i == nil || i.mgr == nil {
		return nil
	}
	if i.mgr.journal != nil {
		return newEmitterJournal(i.mgr.journal, i.mgr)
	}
	if i.mgr.webhook != nil {
		return newEmitterV1(i.mgr.webhook)
	}
	return nil
}

// IsConnected memeriksa apakah client WhatsApp sedang aktif dan tersambung.
func (i *Instance) IsConnected() bool {
	cli := i.getClient()
	return cli != nil && cli.IsConnected()
}

// Snapshot menghasilkan ringkasan status instance untuk API maupun CLI.
func (i *Instance) Snapshot(meta store.Instance) InstanceStatus {
	i.mu.RLock()
	cli := i.client
	sup := i.sup
	status := i.status
	jid := i.jid
	connSince := i.connectedSince
	rpm := i.ratePerMin
	i.mu.RUnlock()

	var pushName string
	if cli != nil && cli.Store != nil {
		pushName = cli.Store.PushName
		if cli.IsConnected() {
			status = store.StatusConnected
		}
		if cli.Store.ID != nil {
			jid = cli.Store.ID.String()
		}
	}

	queueDepth := 0
	if i.mgr != nil && i.mgr.outbox != nil {
		qCtx, qCancel := context.WithTimeout(context.Background(), time.Second)
		if n, err := i.mgr.outbox.CountQueued(qCtx, i.id); err == nil {
			queueDepth = n
		}
		qCancel()
	}

	out := InstanceStatus{
		ID:             i.id,
		Status:         status,
		State:          string(defaultConnStateForStatus(status)),
		JID:            jid,
		PushName:       pushName,
		QueueDepth:     queueDepth,
		RatePerMin:     rpm,
		WebhookURL:     meta.WebhookURL,
		WebhookEvents:  meta.WebhookEvents,
		AutoStart:      meta.AutoStart,
		Settings:       meta.Settings,
		PairedAt:       meta.PairedAt,
		ConnectedSince: connSince,
	}
	if sup != nil {
		applySupervisorSnapshot(&out, sup.State())
	}
	return out
}

func defaultConnStateForStatus(status string) ConnState {
	switch status {
	case store.StatusConnected:
		return StateOnline
	case store.StatusDisabled:
		return StateStopped
	case store.StatusLoggedOut:
		return StateLoggedOut
	default:
		return StateIdle
	}
}

func snapshotFromMeta(meta store.Instance) InstanceStatus {
	return InstanceStatus{
		ID:            meta.ID,
		Status:        meta.Status,
		State:         string(defaultConnStateForStatus(meta.Status)),
		JID:           meta.JID,
		RatePerMin:    meta.RatePerMin,
		WebhookURL:    meta.WebhookURL,
		WebhookEvents: meta.WebhookEvents,
		AutoStart:     meta.AutoStart,
		Settings:      meta.Settings,
		PairedAt:      meta.PairedAt,
	}
}

func applySupervisorSnapshot(out *InstanceStatus, st StateSnapshot) {
	if st.State != "" {
		out.State = string(st.State)
	}
	if !st.Since.IsZero() {
		out.StateSince = st.Since.Unix()
	}
	out.ReconnectAttempts = st.Attempts
	if !st.NextRetryAt.IsZero() {
		sec := int(time.Until(st.NextRetryAt).Seconds())
		if sec > 0 {
			out.NextRetryIn = sec
		}
	}
	if st.BanCode != 0 || !st.BanUntil.IsZero() {
		ban := &BanInfo{Code: st.BanCode}
		if !st.BanUntil.IsZero() {
			ban.ExpiresAt = st.BanUntil.Unix()
		}
		out.Ban = ban
	}
}

// attachClient memasang whatsmeow.Client baru, menggantikan auto-reconnect bawaan, dan menyiapkan Supervisor.
func (i *Instance) attachClient(dev *waStore.Device) *whatsmeow.Client {
	var (
		clientLog        waLog.Logger
		userTimeout      = 45 * time.Second
		keepaliveMaxFail = 75 * time.Second
		emit             Emitter
	)
	if i.mgr != nil {
		if i.mgr.waLog != nil {
			clientLog = i.mgr.waLog.Sub(i.id)
		}
		if i.mgr.cfg != nil {
			if i.mgr.cfg.TCPUserTimeout > 0 {
				userTimeout = i.mgr.cfg.TCPUserTimeout
			}
			if i.mgr.cfg.KeepaliveMaxFail > 0 {
				keepaliveMaxFail = i.mgr.cfg.KeepaliveMaxFail
			}
		}
		emit = i.emitter()
	}
	if clientLog == nil {
		clientLog = logx.WALogger(slog.Default(), "wa", slog.LevelWarn).Sub(i.id)
	}

	i.stopSupervisor()

	i.mu.Lock()
	if i.client != nil {
		i.client.Disconnect()
	}
	cli := whatsmeow.NewClient(dev, clientLog)
	cli.EnableAutoReconnect = false  // Supervisor satu-satunya pemilik reconnect
	cli.InitialAutoReconnect = false // kegagalan connect awal ditangani Supervisor
	cli.ManualHistorySyncDownload = true
	cli.SetWebsocketHTTPClient(newWebsocketHTTPClient(userTimeout))
	if i.mgr != nil && i.mgr.journal != nil {
		// Ack WhatsApp baru dikirim setelah transaksi journal.db berhasil commit.
		cli.SynchronousAck = true
		cli.EnableDecryptedEventBuffer = true
		cli.UseRetryMessageStore = true
		cli.AddEventHandlerWithSuccessStatus(i.handleEventV2)
	} else {
		cli.AddEventHandler(i.handleEvent)
	}

	sup := newSupervisor(i.id, &whatsmeowConnDriver{cli: cli}, emit, SupervisorConfig{
		KeepAliveMaxFail: keepaliveMaxFail,
	}, nil)
	if i.jid != "" {
		sup.setJID(i.jid)
	}
	sup.setOnChange(i.syncSupervisorState)

	i.client = cli
	i.sup = sup
	i.mu.Unlock()
	return cli
}

// ConnectExisting menyambungkan kembali instance yang sudah memiliki JID di wa.db melalui Supervisor.
func (i *Instance) ConnectExisting(ctx context.Context) error {
	i.mu.RLock()
	rawJID := i.jid
	i.mu.RUnlock()
	if rawJID == "" {
		return errors.New("instance belum di-pair (JID kosong)")
	}
	parsed, err := types.ParseJID(rawJID)
	if err != nil {
		return fmt.Errorf("parse JID %s: %w", rawJID, err)
	}
	dev, err := i.mgr.container.GetDevice(ctx, parsed)
	if err != nil {
		return fmt.Errorf("ambil device dari wa.db: %w", err)
	}
	if dev == nil {
		_ = i.mgr.store.ClearInstanceJID(ctx, i.id, store.StatusLoggedOut)
		i.clearJIDAndStatus(store.StatusLoggedOut)
		return errors.New("device tidak ditemukan di wa.db, silakan pair ulang")
	}
	i.attachClient(dev)
	i.startSupervisor()
	return nil
}

func (i *Instance) startSupervisor() {
	i.mu.Lock()
	if i.supCancel != nil {
		i.supCancel()
	}
	// Context hidup instance terpisah dari context HTTP sementara agar koneksi tidak terputus setelah respons kontrol
	supCtx, supCancel := context.WithCancel(context.Background())
	i.supCancel = supCancel
	sup := i.sup
	i.mu.Unlock()

	if sup != nil {
		sup.Start(supCtx)
	}
	i.startOutboxScheduler()
}

func (i *Instance) startOutboxScheduler() {
	if i == nil || i.mgr == nil || i.mgr.outbox == nil || i.baseCtx == nil {
		return
	}
	i.mgr.outbox.StartScheduler(i.baseCtx, i.id)
}

func (i *Instance) stopOutboxScheduler() {
	if i == nil || i.mgr == nil || i.mgr.outbox == nil {
		return
	}
	i.mgr.outbox.StopScheduler(i.id)
}

func (i *Instance) stopSupervisor() {
	i.mu.Lock()
	sup := i.sup
	cancel := i.supCancel
	i.supCancel = nil
	i.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if sup != nil {
		sup.Stop()
	}
}

func (i *Instance) adoptAfterPairing() {
	i.mu.RLock()
	sup := i.sup
	jid := i.jid
	i.mu.RUnlock()
	if sup != nil {
		if jid != "" {
			sup.setJID(jid)
		}
		sup.Adopt()
	}
	i.startOutboxScheduler()
	if i.mgr != nil && i.mgr.outbox != nil {
		i.mgr.outbox.NotifyReady(i.id)
	}
}

func (i *Instance) isPairingActive() bool {
	i.pairMu.Lock()
	sess := i.pairing
	i.pairMu.Unlock()
	return sess != nil && sess.Active()
}

// Recycle menghentikan supervisor, melepas client lama, memuat ulang device dari wa.db, dan menyalakan ulang supervisor.
func (i *Instance) Recycle(ctx context.Context) error {
	if i.isPairingActive() {
		return ErrPairingInProgress
	}
	rawJID := i.getJID()
	if rawJID == "" {
		return errors.New("instance belum di-pair (JID kosong)")
	}
	parsed, err := types.ParseJID(rawJID)
	if err != nil {
		return fmt.Errorf("parse JID %s: %w", rawJID, err)
	}

	i.stopSupervisor()
	i.mu.Lock()
	cli := i.client
	i.client = nil
	i.connectedSince = 0
	i.mu.Unlock()
	if cli != nil {
		cli.Disconnect()
	}

	dev, err := i.mgr.container.GetDevice(ctx, parsed)
	if err != nil {
		return fmt.Errorf("ambil device baru dari wa.db: %w", err)
	}
	if dev == nil {
		_ = i.mgr.store.ClearInstanceJID(ctx, i.id, store.StatusLoggedOut)
		i.clearJIDAndStatus(store.StatusLoggedOut)
		return errors.New("device tidak ditemukan di wa.db, silakan pair ulang")
	}
	i.attachClient(dev)
	i.startSupervisor()
	return nil
}

// StartPairing memulai sesi pairing baru atau mengembalikan sesi identik yang sedang berjalan.
func (i *Instance) StartPairing(rawPhone string) (*PairSession, error) {
	method, phone := PairMethodQR, ""
	if strings.TrimSpace(rawPhone) != "" {
		p, err := normalizePairPhone(rawPhone)
		if err != nil {
			return nil, err
		}
		method, phone = PairMethodCode, p
	}

	if i.IsConnected() {
		return nil, ErrAlreadyPaired
	}
	i.mu.RLock()
	st := i.status
	i.mu.RUnlock()
	if st == store.StatusConnected {
		return nil, ErrAlreadyPaired
	}

	i.pairMu.Lock()
	defer i.pairMu.Unlock()

	if cur := i.pairing; cur != nil && cur.Active() {
		if cur.method == method && cur.phone == phone {
			return cur, nil
		}
		return nil, ErrPairingInProgress
	}

	// Tolak cepat alih-alih menunggu agar request HTTP tidak menggantung
	if !i.mgr.pairingSem.TryAcquire(1) {
		return nil, ErrPairingBusy
	}

	sess := newPairSession(i, method, phone)
	i.pairing = sess
	go sess.run()
	return sess, nil
}

// PairingSnapshot mengembalikan snapshot sesi terakhir (aktif maupun terminal).
func (i *Instance) PairingSnapshot() (PairSnapshot, error) {
	i.pairMu.Lock()
	sess := i.pairing
	i.pairMu.Unlock()
	if sess == nil {
		return PairSnapshot{}, ErrNoPairingSession
	}
	return sess.Snapshot(), nil
}

// PairingQRPNG mengembalikan PNG QR dari sesi aktif.
func (i *Instance) PairingQRPNG(scale int) ([]byte, int, error) {
	i.pairMu.Lock()
	sess := i.pairing
	i.pairMu.Unlock()
	if sess == nil || !sess.Active() {
		return nil, 0, ErrQRNotAvailable
	}
	return sess.PNG(scale)
}

// CancelPairing membatalkan sesi aktif dan menunggu goroutine sesi selesai (maks 5 detik).
func (i *Instance) CancelPairing() (PairSnapshot, error) {
	i.pairMu.Lock()
	sess := i.pairing
	i.pairMu.Unlock()
	if sess == nil || !sess.Active() {
		return PairSnapshot{}, ErrNoActivePairing
	}
	sess.Cancel()
	select {
	case <-sess.done:
	case <-time.After(5 * time.Second):
	}
	return sess.Snapshot(), nil
}

func (i *Instance) resolveOrCreateDevice(ctx context.Context) (*waStore.Device, error) {
	i.mu.RLock()
	rawJID := i.jid
	i.mu.RUnlock()
	if rawJID != "" {
		if parsed, err := types.ParseJID(rawJID); err == nil {
			if dev, err := i.mgr.container.GetDevice(ctx, parsed); err == nil && dev != nil {
				_ = dev.Delete(ctx)
			}
		}
	}
	return i.mgr.container.NewDevice(), nil
}

func (i *Instance) restoreStatusAfterAbort(ctx context.Context) {
	i.mu.RLock()
	jid := i.jid
	i.mu.RUnlock()
	next := store.StatusCreated
	if jid != "" {
		next = store.StatusLoggedOut
	}
	i.setStatus(next, jid)
	if i.mgr != nil && i.mgr.store != nil {
		_ = i.mgr.store.UpdateInstanceStatus(ctx, i.id, next, jid)
	}
}

// Disconnect memutus koneksi WebSocket dan menghentikan supervisor tanpa menghapus sesi kriptografi.
func (i *Instance) Disconnect() {
	i.stopSupervisor()
	i.mu.Lock()
	cli := i.client
	i.connectedSince = 0
	i.mu.Unlock()
	if cli != nil {
		cli.Disconnect()
	}
}

// Close menghentikan sesi pairing, supervisor, scheduler outbox, dan koneksi WhatsApp.
func (i *Instance) Close() {
	i.pairMu.Lock()
	sess := i.pairing
	i.pairMu.Unlock()
	if sess != nil && sess.Active() {
		sess.Cancel()
		<-sess.done
	}
	i.stopSupervisor()
	if i.cancel != nil {
		i.cancel()
	}
	i.stopOutboxScheduler()
	i.mu.Lock()
	cli := i.client
	i.connectedSince = 0
	i.mu.Unlock()
	if cli != nil {
		cli.Disconnect()
	}
}

// Logout melepas tautan perangkat dari WhatsApp dan menghapus device dari wa.db.
func (i *Instance) Logout(ctx context.Context) error {
	_, _ = i.CancelPairing()
	i.stopSupervisor()
	i.stopOutboxScheduler()
	cli := i.getClient()
	if cli != nil {
		if err := cli.Logout(ctx); err != nil {
			cli.Disconnect()
			if cli.Store != nil {
				_ = cli.Store.Delete(ctx)
			}
		}
	}
	i.clearJIDAndStatus(store.StatusLoggedOut)
	if i.mgr != nil && i.mgr.store != nil {
		return i.mgr.store.ClearInstanceJID(ctx, i.id, store.StatusLoggedOut)
	}
	return nil
}

// CheckNumbers memeriksa daftar nomor (maks 20) dengan cache TTL 24 jam.
func (i *Instance) CheckNumbers(ctx context.Context, numbers []string) ([]NumberCheckResult, error) {
	if len(numbers) == 0 || len(numbers) > 20 {
		return nil, errors.New("jumlah nomor harus antara 1 sampai 20")
	}
	cli := i.getClient()
	if cli == nil || !cli.IsConnected() {
		return nil, ErrNotConnected
	}

	now := time.Now()
	results := make([]NumberCheckResult, len(numbers))
	var toQuery []string
	var queryIdx []int

	i.checkMu.Lock()
	for idx, raw := range numbers {
		clean := jid.DigitsOnly(raw)
		if len(clean) < 7 || len(clean) > 16 {
			i.checkMu.Unlock()
			return nil, fmt.Errorf("nomor tidak valid: %s", raw)
		}
		results[idx].Number = clean
		if c, ok := i.checkCache[clean]; ok && now.Before(c.expiresAt) {
			results[idx].Exists = c.exists
			results[idx].JID = c.jid
			continue
		}
		toQuery = append(toQuery, "+"+clean)
		queryIdx = append(queryIdx, idx)
	}
	i.checkMu.Unlock()

	if len(toQuery) > 0 {
		resp, err := cli.IsOnWhatsApp(ctx, toQuery)
		if err != nil {
			return nil, fmt.Errorf("periksa nomor ke WhatsApp: %w", err)
		}
		byPhone := make(map[string]types.IsOnWhatsAppResponse, len(resp))
		for _, item := range resp {
			byPhone[jid.DigitsOnly(item.Query)] = item
		}

		i.checkMu.Lock()
		// Jaga batas cache maksimal 1024 entri agar memori tetap terkendali
		if len(i.checkCache) > 1024 {
			i.checkCache = make(map[string]cachedCheck)
		}
		for _, idx := range queryIdx {
			num := results[idx].Number
			if item, ok := byPhone[num]; ok {
				results[idx].Exists = item.IsIn
				if item.IsIn {
					results[idx].JID = item.JID.ToNonAD().String()
				}
				i.checkCache[num] = cachedCheck{
					exists:    results[idx].Exists,
					jid:       results[idx].JID,
					expiresAt: now.Add(24 * time.Hour),
				}
			}
		}
		i.checkMu.Unlock()
	}

	return results, nil
}

func (i *Instance) routeSupervisorEvent(rawEvt any) bool {
	switch evt := rawEvt.(type) {
	case *events.Connected:
		var jid string
		if cli := i.getClient(); cli != nil && cli.Store != nil && cli.Store.ID != nil {
			jid = cli.Store.ID.String()
		}
		if sup := i.getSupervisor(); sup != nil {
			if jid != "" {
				sup.setJID(jid)
			}
			sup.OnEvent(evt)
		}
		return true
	case *events.Disconnected,
		*events.LoggedOut,
		*events.StreamReplaced,
		*events.TemporaryBan,
		*events.ClientOutdated,
		*events.ConnectFailure,
		*events.KeepAliveTimeout,
		*events.KeepAliveRestored:
		if sup := i.getSupervisor(); sup != nil {
			sup.OnEvent(evt)
		}
		return true
	default:
		return false
	}
}

// handleEvent menangani event dari whatsmeow secara non-blocking untuk mode kompatibilitas v1 tanpa journal.
func (i *Instance) handleEvent(rawEvt any) {
	if i.routeSupervisorEvent(rawEvt) {
		return
	}
	switch evt := rawEvt.(type) {
	case *events.Message:
		// Di V2 seluruh event pesan mengalir lewat journal.db melalui handleEventV2.
		_ = extractMessage(evt)
	case *events.Receipt:
		// Tidak lagi memakai antrian webhook v1 di RAM; gunakan mode journal.
	case *events.HistorySync:
		// Drop seluruh history sync agar tidak memakan RAM maupun disk
	}
}

// handleEventV2 menjalankan pipeline inbound effectively-once di hot path tanpa query app.db atau I/O jaringan.
func (i *Instance) handleEventV2(rawEvt any) bool {
	if i.routeSupervisorEvent(rawEvt) {
		return true
	}

	switch rawEvt.(type) {
	case *events.HistorySync:
		// Drop seluruh history sync agar tidak memakan RAM maupun disk.
		return true
	case *events.OfflineSyncPreview:
		i.offline.Store(true)
		return true
	case *events.OfflineSyncCompleted:
		i.offline.Store(false)
		return true
	}

	var j *journal.Journal
	if i.mgr != nil {
		j = i.mgr.journal
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc := NormalizeContext{
		Now:      time.Now(),
		Offline:  i.offline.Load(),
		Receipts: i.cachedSettings().JournalReceipts,
		LID:      i.lidResolver(),
	}
	obsCtx := withObserverInstanceID(ctx, i.id)
	ev, ok := NormalizeEvent(ctx, i.id, rawEvt, nc)
	if !ok {
		// Tetap jalankan InboundObserver pada event non-journal seperti receipt grup sebelum ack ke WhatsApp.
		if i.mgr != nil && j != nil {
			if hooks := i.mgr.collectHooks(obsCtx, nil, rawEvt); len(hooks) > 0 {
				if err := j.RunTx(ctx, hooks...); err != nil {
					slog.Warn("gagal menjalankan hook non-journal", "instance", i.id, "err", err)
					return false
				}
				j.Notify(i.id)
			}
		}
		return true
	}
	if j == nil {
		return false
	}

	var hooks []journal.TxHook
	if i.mgr != nil {
		hooks = i.mgr.collectHooks(obsCtx, ev, rawEvt)
	}
	res, err := j.Append(ctx, ev, hooks...)
	if err != nil {
		slog.Warn("gagal menyimpan event masuk ke journal", "instance", i.id, "type", ev.Type, "err", err)
		return false
	}
	ev.Seq = res.Seq
	return true
}

func (i *Instance) syncSupervisorState(snap StateSnapshot) {
	var nextV1 string
	var jid string
	var clearJID bool

	switch snap.State {
	case StateOnline:
		nextV1 = store.StatusConnected
		if cli := i.getClient(); cli != nil && cli.Store != nil && cli.Store.ID != nil {
			jid = cli.Store.ID.String()
		}
		if jid == "" {
			jid = i.getJID()
		}
	case StateLoggedOut, StateBanned:
		nextV1 = store.StatusLoggedOut
		clearJID = true
	case StateStopped:
		i.mu.Lock()
		i.connectedSince = 0
		i.mu.Unlock()
		return
	case StateIdle:
		return
	default:
		nextV1 = store.StatusDisconnected
	}

	i.mu.Lock()
	prevV1 := i.status
	changed := prevV1 != nextV1 || (jid != "" && i.jid != jid) || (clearJID && i.jid != "")
	if clearJID {
		i.status = store.StatusLoggedOut
		i.jid = ""
		i.connectedSince = 0
	} else {
		i.status = nextV1
		if jid != "" {
			i.jid = jid
		}
		if nextV1 == store.StatusConnected {
			if i.connectedSince == 0 {
				i.connectedSince = time.Now().Unix()
			}
		} else {
			i.connectedSince = 0
		}
	}
	i.mu.Unlock()

	if changed && i.mgr != nil && i.mgr.store != nil {
		dbCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if clearJID {
			_ = i.mgr.store.ClearInstanceJID(dbCtx, i.id, store.StatusLoggedOut)
		} else {
			_ = i.mgr.store.UpdateInstanceStatus(dbCtx, i.id, nextV1, jid)
		}
		cancel()
	}

	if snap.State == StateOnline && i.mgr != nil && i.mgr.outbox != nil {
		i.mgr.outbox.NotifyReady(i.id)
	}
}

func (i *Instance) clearJIDAndStatus(status string) {
	i.mu.Lock()
	i.status = status
	i.jid = ""
	i.connectedSince = 0
	i.mu.Unlock()
}

func (i *Instance) setStatus(status, jid string) {
	i.mu.Lock()
	i.status = status
	if jid != "" {
		i.jid = jid
	}
	if status == store.StatusConnected {
		i.connectedSince = time.Now().Unix()
	} else if status == store.StatusDisconnected || status == store.StatusLoggedOut || status == store.StatusDisabled {
		i.connectedSince = 0
	}
	i.mu.Unlock()
}

// ParseRecipient mengubah string nomor telepon atau JID menjadi types.JID WhatsApp.
func ParseRecipient(raw string) (types.JID, error) {
	return jid.ParseRecipient(raw)
}
