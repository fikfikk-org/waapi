package wa

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/logx"
	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/netguard"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/webhook"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"golang.org/x/sync/semaphore"
)

var slugRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)

// InboundObserver mendaftarkan hook transaksi journal.db untuk setiap event masuk sebelum ack ke WhatsApp.
type InboundObserver func(ctx context.Context, ev *journal.Event, raw any) journal.TxHook

// ManagerOption mengonfigurasi opsi tambahan pada konstruksi Manager.
type ManagerOption func(*Manager)

// WithJournal memasang Journal v2 ke Manager dan seluruh instance yang dikelola.
func WithJournal(j *journal.Journal) ManagerOption {
	return func(m *Manager) {
		m.journal = j
	}
}

// WithTargetCache memasang cache target webhook agar operasi CRUD instance langsung menyinkronkan cache.
func WithTargetCache(tc *webhook.TargetCache) ManagerOption {
	return func(m *Manager) {
		m.targetCache = tc
	}
}

// WithWebhookService memasang service webhook v2 agar perubahan konfigurasi target langsung me-restart deliverer.
func WithWebhookService(ws *webhook.Service) ManagerOption {
	return func(m *Manager) {
		m.webhookSvc = ws
	}
}

// RateUpdater diperbarui saat rate_per_min instance diubah lewat API kontrol.
type RateUpdater interface {
	SetRate(instanceID string, rpm int)
}

// TempBanHandler menangani sinyal TemporaryBan WhatsApp ke komponen kesehatan akun.
type TempBanHandler func(ctx context.Context, instanceID string, code int, expire time.Duration) error

// WithOutbox memasang Service durable outbox beserta observer receipt ke Manager.
func WithOutbox(svc *outbox.Service, gov RateUpdater) ManagerOption {
	return func(m *Manager) {
		m.outbox = svc
		m.outboxGov = gov
	}
}

// WithHealthHooks memasang callback delivered post-commit, temporary ban, pairing selesai, dan ringkasan health untuk Guardian.
func WithHealthHooks(onDelivered func(string, int), onTempBan TempBanHandler, onPaired func(string), summaryFn func(context.Context, string) (*HealthSummary, error)) ManagerOption {
	return func(m *Manager) {
		m.onDelivered = onDelivered
		m.onTempBan = onTempBan
		m.onPaired = onPaired
		m.healthSummary = summaryFn
	}
}

// WithHealthSummary memasang penyedia ringkasan state/score kesehatan akun pada snapshot status instance.
func WithHealthSummary(fn func(context.Context, string) (*HealthSummary, error)) ManagerOption {
	return func(m *Manager) {
		m.healthSummary = fn
	}
}

// WithMedia memasang Pipeline media v2, UploadStore, dan RetryWaiter ke Manager.
func WithMedia(pipe *media.Pipeline, uploads *media.UploadStore, waiter *media.RetryWaiter) ManagerOption {
	return func(m *Manager) {
		m.mediaPipe = pipe
		m.uploadStore = uploads
		m.retryWaiter = waiter
	}
}

// WithPolls memasang PollStore untuk menyimpan polling masuk/keluar serta suara terdekripsi ke journal.db.
func WithPolls(ps *PollStore) ManagerOption {
	return func(m *Manager) {
		m.pollStore = ps
	}
}

// Manager mengelola siklus hidup seluruh instance WhatsApp di dalam satu proses daemon.
type Manager struct {
	cfg         *config.Config
	store       *store.Store
	waDB        *sql.DB
	container   *sqlstore.Container
	webhook     *webhook.Dispatcher
	webhookSvc  *webhook.Service
	journal     *journal.Journal
	targetCache *webhook.TargetCache
	outbox        *outbox.Service
	outboxGov     RateUpdater
	onDelivered   func(string, int)
	onTempBan     TempBanHandler
	onPaired      func(string)
	healthSummary func(context.Context, string) (*HealthSummary, error)
	mediaPipe   *media.Pipeline
	mediaOnce   sync.Once
	uploadStore *media.UploadStore
	retryWaiter  *media.RetryWaiter
	pollStore    *PollStore
	mediaHTTP    *http.Client
	waLog        waLog.Logger
	pairingSem   *semaphore.Weighted
	mediaSem     *semaphore.Weighted
	groupListLRU *LRU[string, []GroupSummary]
	groupGov     GroupGovernor
	recycleCoord *recycleCoordinator
	freeOSMu     sync.Mutex
	freeOSFn     func()
	lastFreeOS   time.Time

	obsMu     sync.RWMutex
	observers []InboundObserver

	mu        sync.RWMutex
	instances map[string]*Instance
}

// NewManager membuka wa.db, menjalankan migrasi sqlstore, dan menyiapkan registry instance.
func NewManager(ctx context.Context, cfg *config.Config, st *store.Store, wh *webhook.Dispatcher, opts ...ManagerOption) (*Manager, error) {
	cfg.Normalize()
	ConfigureDeviceProps()

	waLogger := logx.WALogger(slog.Default(), "wa", logx.ParseLevel(cfg.WALogLevel))

	waDB, err := store.OpenDB(ctx, cfg.WADBPath(), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    cfg.WACacheKiB,
		IdleTimeout: 5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("buka wa.db: %w", err)
	}
	container := sqlstore.NewWithDB(waDB, "sqlite3", waLogger.Sub("sqlstore"))
	if err := container.Upgrade(ctx); err != nil {
		_ = waDB.Close()
		return nil, fmt.Errorf("migrasi wa.db: %w", err)
	}

	m := &Manager{
		cfg:        cfg,
		store:      st,
		waDB:       waDB,
		container:  container,
		webhook:    wh,
		waLog:        waLogger,
		mediaHTTP:    netguard.NewClient(netguard.Policy{}, 60*time.Second),
		pairingSem:   semaphore.NewWeighted(1), // maksimal 1 proses pairing dalam satu waktu
		mediaSem:     semaphore.NewWeighted(2), // maksimal 2 stream media aktif bersamaan
		groupListLRU: NewLRU[string, []GroupSummary](128, 60*time.Second),
		groupGov:     noopGroupGovernor{},
		instances:    make(map[string]*Instance),
	}
	m.recycleCoord = newRecycleCoordinator(managerRecycleBackend{mgr: m}, governor.AlwaysGreen)
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	if m.outbox != nil {
		m.AddInboundObserver(NewReceiptObserverWithCallback(m.outbox, m.onDelivered))
	}

	rows, err := st.ListInstances(ctx)
	if err != nil {
		_ = waDB.Close()
		return nil, err
	}
	for _, row := range rows {
		m.instances[row.ID] = newInstance(m, row)
	}
	return m, nil
}

// Journal mengembalikan instance Journal v2 yang terpasang (dapat bernilai nil pada mode kompatibilitas v1).
func (m *Manager) Journal() *journal.Journal {
	if m == nil {
		return nil
	}
	return m.journal
}

// AddInboundObserver mendaftarkan observer yang menghasilkan TxHook saat event masuk disimpan ke journal.db.
func (m *Manager) AddInboundObserver(fn InboundObserver) {
	if m == nil || fn == nil {
		return
	}
	m.obsMu.Lock()
	m.observers = append(m.observers, fn)
	m.obsMu.Unlock()
}

// AttachOutbox menghubungkan service outbox setelah konstruksi Manager, mendaftarkan observer receipt, dan menyalakan scheduler instance aktif.
func (m *Manager) AttachOutbox(svc *outbox.Service, gov RateUpdater) {
	if m == nil {
		return
	}
	m.outbox = svc
	m.outboxGov = gov
	if svc != nil {
		m.AddInboundObserver(NewReceiptObserverWithCallback(svc, m.onDelivered))
		m.mu.RLock()
		for _, inst := range m.instances {
			if inst.getSupervisor() != nil || inst.IsConnected() {
				inst.startOutboxScheduler()
				if inst.IsConnected() {
					svc.NotifyReady(inst.id)
				}
			}
		}
		m.mu.RUnlock()
	}
}

// RatePerMin mengembalikan batas kirim per menit untuk instance tertentu (default 20).
func (m *Manager) RatePerMin(instanceID string) int {
	if m == nil {
		return 20
	}
	inst, ok := m.Get(instanceID)
	if !ok {
		return 20
	}
	inst.mu.RLock()
	rpm := inst.ratePerMin
	inst.mu.RUnlock()
	if rpm <= 0 {
		return 20
	}
	return rpm
}

func (m *Manager) collectHooks(ctx context.Context, ev *journal.Event, raw any) []journal.TxHook {
	if m == nil {
		return nil
	}
	m.obsMu.RLock()
	if len(m.observers) == 0 {
		m.obsMu.RUnlock()
		return nil
	}
	obs := make([]InboundObserver, len(m.observers))
	copy(obs, m.observers)
	m.obsMu.RUnlock()

	var hooks []journal.TxHook
	for _, fn := range obs {
		if h := fn(ctx, ev, raw); h != nil {
			hooks = append(hooks, h)
		}
	}
	return hooks
}

// Store mengembalikan handle Store (app.db) milik wa Manager.
func (m *Manager) Store() *store.Store {
	if m == nil {
		return nil
	}
	return m.store
}

// WADB mengembalikan handle *sql.DB milik wa.db.
func (m *Manager) WADB() *sql.DB {
	if m == nil {
		return nil
	}
	return m.waDB
}

// AutoStartAll menyalakan instance dengan auto_start=true secara berurutan (stagger 2 s + jitter 0-1 s) agar tidak memicu spike RAM.
func (m *Manager) AutoStartAll(ctx context.Context) {
	rows, err := m.store.ListInstances(ctx)
	if err != nil {
		slog.Warn("gagal membaca daftar instance untuk auto-start", "err", err)
		return
	}
	first := true
	for _, row := range rows {
		if !row.AutoStart || row.JID == "" || row.Status == store.StatusDisabled || row.Status == store.StatusLoggedOut {
			continue
		}
		if !first {
			jitter := time.Duration(rand.IntN(1000)) * time.Millisecond
			timer := time.NewTimer(2*time.Second + jitter)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		first = false

		inst, ok := m.Get(row.ID)
		if !ok {
			continue
		}
		if err := inst.ConnectExisting(ctx); err != nil {
			slog.Warn("auto-start instance gagal", "instance", row.ID, "err", err)
		} else {
			slog.Info("supervisor instance dimulai saat startup", "instance", row.ID, "jid", row.JID)
		}
	}
}

// Close menutup semua koneksi WhatsApp dan database wa.db secara berurutan.
func (m *Manager) Close() error {
	m.mu.Lock()
	list := make([]*Instance, 0, len(m.instances))
	for _, inst := range m.instances {
		list = append(list, inst)
	}
	m.mu.Unlock()

	for _, inst := range list {
		inst.Close()
	}
	if m.mediaHTTP != nil {
		m.mediaHTTP.CloseIdleConnections()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = m.waDB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return m.waDB.Close()
}

// Create membuat instance baru di app.db dan mendaftarkannya ke memori.
func (m *Manager) Create(ctx context.Context, id string, ratePerMin int, autoStart bool) (*InstanceStatus, error) {
	if !slugRegex.MatchString(id) {
		return nil, errors.New("id instance harus huruf kecil/angka/strip (1-32 karakter)")
	}
	secret, err := auth.GenerateSecret()
	if err != nil {
		return nil, err
	}
	meta := store.Instance{
		ID:            id,
		Status:        store.StatusCreated,
		WebhookSecret: secret,
		WebhookEvents: store.DefaultWebhookEvents,
		RatePerMin:    ratePerMin,
		AutoStart:     autoStart,
		Settings:      store.Defaults(),
	}
	if err := m.store.CreateInstance(ctx, meta); err != nil {
		return nil, err
	}

	inst := newInstance(m, meta)
	m.mu.Lock()
	m.instances[id] = inst
	m.mu.Unlock()
	if m.targetCache != nil {
		m.targetCache.SetFromInstance(&meta)
	}

	snap := inst.Snapshot(meta)
	return &snap, nil
}

// Get mengambil runtime instance berdasarkan ID.
func (m *Manager) Get(id string) (*Instance, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inst, ok := m.instances[id]
	return inst, ok
}

// Status mengambil status gabungan (DB + runtime) untuk satu instance tanpa efek samping membuat instance (R17).
func (m *Manager) Status(ctx context.Context, id string) (*InstanceStatus, error) {
	inst, ok := m.Get(id)
	if !ok {
		return nil, store.ErrNotFound
	}
	meta, err := m.store.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	snap := inst.Snapshot(*meta)
	return &snap, nil
}

// ListStatuses mengembalikan daftar status seluruh instance tanpa membuat instance/goroutine baru (R17).
func (m *Manager) ListStatuses(ctx context.Context) ([]InstanceStatus, error) {
	rows, err := m.store.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]InstanceStatus, 0, len(rows))
	for _, row := range rows {
		if inst, ok := m.Get(row.ID); ok {
			out = append(out, inst.Snapshot(row))
			continue
		}
		out = append(out, snapshotFromMeta(row))
	}
	return out, nil
}

// Recycle menyegarkan penuh whatsmeow.Client dan cache sqlstore milik instance yang sudah tertaut.
func (m *Manager) Recycle(ctx context.Context, id string) (*InstanceStatus, error) {
	meta, err := m.store.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	inst, ok := m.Get(id)
	if !ok {
		return nil, store.ErrNotFound
	}
	if meta.JID == "" && inst.getJID() == "" {
		return nil, errors.New("instance belum di-pair")
	}
	if err := inst.Recycle(ctx); err != nil {
		return nil, err
	}
	return m.Status(ctx, id)
}

// UpdateSettings memperbarui konfigurasi InstanceSettings secara aman sekaligus menyinkronkan TargetCache dan deliverer.
func (m *Manager) UpdateSettings(ctx context.Context, id string, settings store.InstanceSettings) (*InstanceStatus, error) {
	return m.Update(ctx, id, store.InstanceUpdate{Settings: &settings})
}

// Update memperbarui konfigurasi instance di DB dan menyinkronkan rate limiter, settings, cursor, serta deliverer webhook.
func (m *Manager) Update(ctx context.Context, id string, up store.InstanceUpdate) (*InstanceStatus, error) {
	prev, err := m.store.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	meta, err := m.store.UpdateInstance(ctx, id, up)
	if err != nil {
		return nil, err
	}
	if inst, ok := m.Get(id); ok {
		if up.RatePerMin != nil {
			inst.SetRatePerMin(*up.RatePerMin)
			if m.outboxGov != nil {
				m.outboxGov.SetRate(id, *up.RatePerMin)
			}
		}
		inst.SetSettings(meta.Settings)
	}

	if m.journal != nil && up.WebhookURL != nil {
		prevEmpty := strings.TrimSpace(prev.WebhookURL) == ""
		newEmpty := strings.TrimSpace(meta.WebhookURL) == ""
		if !newEmpty && (prevEmpty || strings.TrimSpace(up.WebhookFrom) != "") {
			// Hentikan deliverer aktif sejenak jika sudah menyala sebelum mengatur ulang cursor awal.
			if m.webhookSvc != nil && m.targetCache != nil && !prevEmpty {
				m.targetCache.Set(id, webhook.Target{})
				m.webhookSvc.OnTargetChanged(id)
			}
			var startSeq int64
			if !strings.EqualFold(strings.TrimSpace(up.WebhookFrom), "retained") {
				if head, hErr := m.journal.HeadSeq(ctx, id); hErr == nil {
					startSeq = head
				}
			}
			_ = m.journal.ResetConsumerCursors(ctx, "webhook", id, meta.Settings.WebhookLanes, startSeq, true)
		}
	}

	if m.targetCache != nil {
		m.targetCache.SetFromInstance(meta)
	}
	if m.webhookSvc != nil {
		m.webhookSvc.OnTargetChanged(id)
	}
	return m.Status(ctx, meta.ID)
}

// Restart memutus dan menyambungkan ulang koneksi instance.
func (m *Manager) Restart(ctx context.Context, id string) (*InstanceStatus, error) {
	meta, err := m.store.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	inst, ok := m.Get(id)
	if !ok {
		return nil, store.ErrNotFound
	}
	inst.Disconnect()
	auto := true
	_, _ = m.store.UpdateInstance(ctx, id, store.InstanceUpdate{AutoStart: &auto})
	if meta.JID != "" {
		inst.setStatus(store.StatusDisconnected, meta.JID)
		if err := inst.ConnectExisting(ctx); err != nil {
			return nil, err
		}
	}
	return m.Status(ctx, id)
}

// Stop menghentikan koneksi instance dan menonaktifkan auto_start.
func (m *Manager) Stop(ctx context.Context, id string) (*InstanceStatus, error) {
	inst, ok := m.Get(id)
	if !ok {
		return nil, store.ErrNotFound
	}
	inst.Disconnect()
	inst.stopOutboxScheduler()
	inst.setStatus(store.StatusDisabled, "")
	auto := false
	if _, err := m.store.UpdateInstance(ctx, id, store.InstanceUpdate{AutoStart: &auto}); err != nil {
		return nil, err
	}
	if err := m.store.UpdateInstanceStatus(ctx, id, store.StatusDisabled, ""); err != nil {
		return nil, err
	}
	return m.Status(ctx, id)
}

// Logout mengeluarkan perangkat dari WhatsApp tanpa menghapus baris instance di app.db.
func (m *Manager) Logout(ctx context.Context, id string) (*InstanceStatus, error) {
	inst, ok := m.Get(id)
	if !ok {
		return nil, store.ErrNotFound
	}
	if err := inst.Logout(ctx); err != nil {
		return nil, err
	}
	return m.Status(ctx, id)
}

// Delete melakukan logout (jika masih tertaut) lalu menghapus instance dari memori dan DB.
func (m *Manager) Delete(ctx context.Context, id string) error {
	inst, ok := m.Get(id)
	if ok {
		_, _ = inst.CancelPairing()
		_ = inst.Logout(ctx)
		inst.Close()
	}
	if m.outbox != nil {
		m.outbox.StopScheduler(id)
	}
	m.mu.Lock()
	delete(m.instances, id)
	m.mu.Unlock()
	if m.targetCache != nil {
		m.targetCache.Delete(id)
	}
	if m.webhookSvc != nil {
		m.webhookSvc.OnTargetChanged(id)
	}
	return m.store.DeleteInstance(ctx, id)
}

// PollStore mengembalikan PollStore yang terpasang pada Manager.
func (m *Manager) PollStore() *PollStore {
	if m == nil {
		return nil
	}
	return m.pollStore
}

// MarkRead menandai satu atau lebih pesan chat telah dibaca atau diputar (played).
func (m *Manager) MarkRead(ctx context.Context, instanceID string, chat, sender types.JID, ids []types.MessageID, played bool) error {
	inst, ok := m.Get(instanceID)
	if !ok {
		return store.ErrNotFound
	}
	cli := inst.getClient()
	if cli == nil || !cli.IsConnected() {
		return ErrNotConnected
	}
	if played {
		return cli.MarkRead(ctx, ids, time.Now(), chat, sender, types.ReceiptTypePlayed)
	}
	return cli.MarkRead(ctx, ids, time.Now(), chat, sender)
}

// SendPresence memperbarui status online/offline instance pada server WhatsApp.
func (m *Manager) SendPresence(ctx context.Context, instanceID string, state types.Presence) error {
	inst, ok := m.Get(instanceID)
	if !ok {
		return store.ErrNotFound
	}
	cli := inst.getClient()
	if cli == nil || !cli.IsConnected() {
		return ErrNotConnected
	}
	return cli.SendPresence(ctx, state)
}

// SendChatPresence mengirim indikator mengetik atau merekam suara ke sebuah chat.
func (m *Manager) SendChatPresence(ctx context.Context, instanceID string, chat types.JID, state types.ChatPresence, media types.ChatPresenceMedia) error {
	inst, ok := m.Get(instanceID)
	if !ok {
		return store.ErrNotFound
	}
	cli := inst.getClient()
	if cli == nil || !cli.IsConnected() {
		return ErrNotConnected
	}
	return cli.SendChatPresence(ctx, chat, state, media)
}

// SubscribePresence mendaftarkan pemantauan status online/offline untuk JID tertentu (maks 100 JID per instance).
func (m *Manager) SubscribePresence(ctx context.Context, instanceID string, target types.JID) error {
	inst, ok := m.Get(instanceID)
	if !ok {
		return store.ErrNotFound
	}
	return inst.SubscribePresence(ctx, target)
}

// CountByStatus menghitung jumlah instance per status untuk laporan doctor.
func (m *Manager) CountByStatus(ctx context.Context) map[string]int {
	counts := map[string]int{
		store.StatusConnected:    0,
		store.StatusDisconnected: 0,
		store.StatusLoggedOut:    0,
		store.StatusCreated:      0,
		store.StatusDisabled:     0,
	}
	list, err := m.ListStatuses(ctx)
	if err != nil {
		return counts
	}
	for _, it := range list {
		counts[it.Status]++
	}
	return counts
}

// SetGroupGovernor memasang atau mengganti GroupGovernor aktif setelah Manager dikonstruksi.
func (m *Manager) SetGroupGovernor(gov GroupGovernor) {
	if m == nil || gov == nil {
		return
	}
	m.groupGov = gov
}

// SetHealthHooks menyambungkan callback Guardian setelah konstruksi Manager (sebelum AttachOutbox & AutoStartAll).
func (m *Manager) SetHealthHooks(onDelivered func(string, int), onTempBan TempBanHandler, onPaired func(string), summaryFn func(context.Context, string) (*HealthSummary, error)) {
	if m == nil {
		return
	}
	m.onDelivered = onDelivered
	m.onTempBan = onTempBan
	m.onPaired = onPaired
	m.healthSummary = summaryFn
}

// notifyPaired dipanggil setelah pairing perangkat berhasil.
func (m *Manager) notifyPaired(id string) {
	if m != nil && m.onPaired != nil {
		m.onPaired(id)
	}
}

// IsAddressBookContact memeriksa buku alamat WhatsApp lokal (Found && FullName != "") tanpa panggilan jaringan.
func (m *Manager) IsAddressBookContact(ctx context.Context, instanceID string, target types.JID) bool {
	if m == nil || target.IsEmpty() {
		return false
	}
	inst, ok := m.Get(instanceID)
	if !ok {
		return false
	}
	cli := inst.getClient()
	if cli == nil || cli.Store == nil || cli.Store.Contacts == nil {
		return false
	}
	nonAD := target.ToNonAD()
	if info, err := cli.Store.Contacts.GetContact(ctx, nonAD); err == nil && info.Found && strings.TrimSpace(info.FullName) != "" {
		return true
	}
	if nonAD.Server == types.HiddenUserServer && cli.Store.LIDs != nil {
		if pn, err := cli.Store.LIDs.GetPNForLID(ctx, nonAD); err == nil && !pn.IsEmpty() {
			if info, err := cli.Store.Contacts.GetContact(ctx, pn.ToNonAD()); err == nil && info.Found && strings.TrimSpace(info.FullName) != "" {
				return true
			}
		}
	}
	return false
}

// ResolveLIDToPN memetakan JID LID ke PN lewat store lokal milik instance jika tersedia.
func (m *Manager) ResolveLIDToPN(ctx context.Context, instanceID string, target types.JID) (types.JID, bool) {
	if m == nil || target.IsEmpty() {
		return types.EmptyJID, false
	}
	inst, ok := m.Get(instanceID)
	if !ok {
		return types.EmptyJID, false
	}
	res := inst.lidResolver()
	if res == nil {
		return types.EmptyJID, false
	}
	return res.PNForLID(ctx, target)
}

// EmitHealthEvent mencatat event health instance melalui Emitter aktif ke journal/webhook.
func (m *Manager) EmitHealthEvent(ctx context.Context, instanceID string, payload any) error {
	if m == nil {
		return nil
	}
	inst, ok := m.Get(instanceID)
	if !ok {
		return nil
	}
	emit := inst.emitter()
	if emit == nil {
		return nil
	}
	return emit.Emit(ctx, instanceID, "health", payload)
}

// EnqueueSystemText mengantrekan pesan teks sistem (seperti konfirmasi opt-out) lewat durable outbox.
func (m *Manager) EnqueueSystemText(ctx context.Context, instanceID, to, text, idemKey string) error {
	if m == nil || m.outbox == nil {
		return nil
	}
	_, _, err := m.outbox.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: instanceID,
		IdemKey:    idemKey,
		Spec: outbox.Spec{
			Type: "text",
			To:   to,
			Text: text,
		},
		Origin: outbox.Origin{Kind: "system"},
	})
	return err
}

// SetFreeOSMemoryFn menyetel fungsi pelepas memori OS pasca-pairing (disambungkan ke Governor dari daemon).
func (m *Manager) SetFreeOSMemoryFn(fn func()) {
	if m == nil {
		return
	}
	m.freeOSMu.Lock()
	m.freeOSFn = fn
	m.freeOSMu.Unlock()
}

func (m *Manager) freeOSMemoryAfterPairing() {
	if m == nil {
		return
	}
	m.freeOSMu.Lock()
	fn := m.freeOSFn
	if fn != nil {
		m.freeOSMu.Unlock()
		fn()
		return
	}
	now := time.Now()
	if !m.lastFreeOS.IsZero() && now.Sub(m.lastFreeOS) < 60*time.Second {
		m.freeOSMu.Unlock()
		return
	}
	m.lastFreeOS = now
	m.freeOSMu.Unlock()
	debug.FreeOSMemory()
}

// RecordActivity memperbarui stempel waktu aktivitas pesan terbaru sebuah instance (dipakai outbox Enqueue).
func (m *Manager) RecordActivity(instanceID string, at time.Time) {
	if m == nil {
		return
	}
	if inst, ok := m.Get(instanceID); ok {
		inst.recordActivity(at)
	}
}

// StartRecycleLoop menjalankan scheduler recycle harian (recycle_at) hingga ctx dibatalkan.
func (m *Manager) StartRecycleLoop(ctx context.Context) {
	if m == nil || m.recycleCoord == nil {
		return
	}
	go m.recycleCoord.Run(ctx)
}

// RecycleNextEligible me-recycle satu instance secara round-robin dengan cooldown >= 6 jam jika tidak ada job sending.
func (m *Manager) RecycleNextEligible(ctx context.Context) (bool, error) {
	if m == nil || m.recycleCoord == nil {
		return false, nil
	}
	return m.recycleCoord.RecycleNextEligible(ctx)
}

