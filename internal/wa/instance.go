package wa

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow"
	waStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"golang.org/x/time/rate"
)

// ErrInvalidRecipient dikembalikan jika format nomor atau JID tujuan tidak valid.
var ErrInvalidRecipient = errors.New("nomor telepon atau JID tidak valid")

// PairEvent dikirim secara streaming (NDJSON) ke CLI selama proses pairing berlangsung.
type PairEvent struct {
	Type    string `json:"type"`              // qr | code | connected | timeout | error
	Code    string `json:"code,omitempty"`    // string QR atau kode 8 digit
	JID     string `json:"jid,omitempty"`     // JID setelah terhubung
	Message string `json:"message,omitempty"` // keterangan tambahan atau pesan error
}

// InstanceStatus adalah representasi status runtime + database sebuah instance.
type InstanceStatus struct {
	ID             string `json:"id"`
	Status         string `json:"status"`
	JID            string `json:"jid,omitempty"`
	PushName       string `json:"push_name,omitempty"`
	QueueDepth     int    `json:"queue_depth"`
	RatePerMin     int    `json:"rate_per_min"`
	WebhookURL     string `json:"webhook_url,omitempty"`
	WebhookEvents  string `json:"webhook_events"`
	AutoStart      bool   `json:"auto_start"`
	ConnectedSince int64  `json:"connected_since,omitempty"`
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

// Instance membungkus satu whatsmeow.Client beserta antrian kirim dan rate limiter-nya.
type Instance struct {
	id     string
	mgr    *Manager
	sendCh chan sendRequest

	mu             sync.RWMutex
	client         *whatsmeow.Client
	limiter        *rate.Limiter
	ratePerMin     int
	status         string
	jid            string
	connectedSince int64
	cancel         context.CancelFunc

	checkMu    sync.Mutex
	checkCache map[string]cachedCheck
}

func newInstance(m *Manager, meta store.Instance) *Instance {
	ctx, cancel := context.WithCancel(context.Background())
	inst := &Instance{
		id:         meta.ID,
		mgr:        m,
		sendCh:     make(chan sendRequest, 256),
		limiter:    newLimiter(meta.RatePerMin),
		ratePerMin: meta.RatePerMin,
		status:     meta.Status,
		jid:        meta.JID,
		cancel:     cancel,
		checkCache: make(map[string]cachedCheck),
	}
	go inst.senderLoop(ctx)
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

// IsConnected memeriksa apakah client WhatsApp sedang aktif dan tersambung.
func (i *Instance) IsConnected() bool {
	cli := i.getClient()
	return cli != nil && cli.IsConnected()
}

// Snapshot menghasilkan ringkasan status instance untuk API maupun CLI.
func (i *Instance) Snapshot(meta store.Instance) InstanceStatus {
	i.mu.RLock()
	cli := i.client
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
	return InstanceStatus{
		ID:             i.id,
		Status:         status,
		JID:            jid,
		PushName:       pushName,
		QueueDepth:     len(i.sendCh),
		RatePerMin:     rpm,
		WebhookURL:     meta.WebhookURL,
		WebhookEvents:  meta.WebhookEvents,
		AutoStart:      meta.AutoStart,
		ConnectedSince: connSince,
	}
}

// attachClient memasang whatsmeow.Client baru dan mendaftarkan event handler.
func (i *Instance) attachClient(dev *waStore.Device) *whatsmeow.Client {
	i.mu.Lock()
	if i.client != nil {
		i.client.Disconnect()
	}
	cli := whatsmeow.NewClient(dev, waLog.Noop)
	cli.EnableAutoReconnect = true
	cli.ManualHistorySyncDownload = true
	cli.AddEventHandler(i.handleEvent)
	i.client = cli
	i.mu.Unlock()
	return cli
}

// ConnectExisting menyambungkan kembali instance yang sudah memiliki JID di wa.db.
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
		i.setStatus(store.StatusLoggedOut, "")
		return errors.New("device tidak ditemukan di wa.db, silakan pair ulang")
	}
	cli := i.attachClient(dev)
	if err := cli.Connect(); err != nil {
		return fmt.Errorf("connect whatsapp: %w", err)
	}
	return nil
}

// Pair menjalankan proses penautan perangkat via QR code atau 8-digit pairing code.
func (i *Instance) Pair(ctx context.Context, phone string, emit func(PairEvent)) error {
	// Semaphore global = 1 agar hanya satu proses pairing berjalan pada satu waktu
	if err := i.mgr.pairingSem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("proses pairing lain sedang berlangsung: %w", err)
	}
	defer i.mgr.pairingSem.Release(1)

	dev, err := i.resolveOrCreateDevice(ctx)
	if err != nil {
		return err
	}
	cli := i.attachClient(dev)
	i.setStatus(store.StatusPairing, "")
	_ = i.mgr.store.UpdateInstanceStatus(ctx, i.id, store.StatusPairing, "")

	qrChan, err := cli.GetQRChannel(ctx)
	if err != nil {
		return fmt.Errorf("buka qr channel: %w", err)
	}
	if err := cli.Connect(); err != nil {
		return fmt.Errorf("connect untuk pairing: %w", err)
	}

	cleanPhone := digitsOnly(phone)
	pairedWithCode := false

	for {
		select {
		case <-ctx.Done():
			cli.Disconnect()
			i.restoreStatusAfterAbort(context.Background())
			return ctx.Err()
		case item, ok := <-qrChan:
			if !ok {
				return nil
			}
			switch item.Event {
			case "code":
				if cleanPhone != "" {
					if !pairedWithCode {
						code, err := cli.PairPhone(ctx, cleanPhone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
						if err != nil {
							cli.Disconnect()
							i.restoreStatusAfterAbort(context.Background())
							emit(PairEvent{Type: "error", Message: err.Error()})
							return err
						}
						pairedWithCode = true
						emit(PairEvent{Type: "code", Code: code})
					}
				} else {
					emit(PairEvent{Type: "qr", Code: item.Code})
				}
			case "success":
				var jidStr string
				if cli.Store != nil && cli.Store.ID != nil {
					jidStr = cli.Store.ID.String()
				}
				i.setStatus(store.StatusConnected, jidStr)
				_ = i.mgr.store.UpdateInstanceStatus(context.Background(), i.id, store.StatusConnected, jidStr)
				emit(PairEvent{Type: "connected", JID: jidStr})
				return nil
			case "timeout":
				cli.Disconnect()
				i.restoreStatusAfterAbort(context.Background())
				emit(PairEvent{Type: "timeout", Message: "waktu pairing habis"})
				return errors.New("waktu pairing habis")
			default:
				if strings.HasPrefix(item.Event, "err-") {
					cli.Disconnect()
					i.restoreStatusAfterAbort(context.Background())
					emit(PairEvent{Type: "error", Message: item.Event})
					return fmt.Errorf("pairing gagal: %s", item.Event)
				}
			}
		}
	}
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
	_ = i.mgr.store.UpdateInstanceStatus(ctx, i.id, next, jid)
}

// Disconnect memutus koneksi WebSocket tanpa menghapus sesi kriptografi.
func (i *Instance) Disconnect() {
	i.mu.Lock()
	cli := i.client
	i.connectedSince = 0
	i.mu.Unlock()
	if cli != nil {
		cli.Disconnect()
	}
}

// Close menghentikan goroutine pengirim dan memutus koneksi WhatsApp.
func (i *Instance) Close() {
	if i.cancel != nil {
		i.cancel()
	}
	i.Disconnect()
}

// Logout melepas tautan perangkat dari WhatsApp dan menghapus device dari wa.db.
func (i *Instance) Logout(ctx context.Context) error {
	cli := i.getClient()
	if cli != nil {
		if err := cli.Logout(ctx); err != nil {
			cli.Disconnect()
			if cli.Store != nil {
				_ = cli.Store.Delete(ctx)
			}
		}
	}
	i.setStatus(store.StatusLoggedOut, "")
	return i.mgr.store.ClearInstanceJID(ctx, i.id, store.StatusLoggedOut)
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
		clean := digitsOnly(raw)
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
			byPhone[digitsOnly(item.Query)] = item
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

// handleEvent menangani event dari whatsmeow secara non-blocking.
func (i *Instance) handleEvent(rawEvt any) {
	switch evt := rawEvt.(type) {
	case *events.Connected:
		var jid string
		if cli := i.getClient(); cli != nil && cli.Store != nil && cli.Store.ID != nil {
			jid = cli.Store.ID.String()
		}
		i.setStatus(store.StatusConnected, jid)
		_ = i.mgr.store.UpdateInstanceStatus(context.Background(), i.id, store.StatusConnected, jid)
		i.mgr.webhook.Enqueue(i.id, "connection", map[string]any{
			"status": store.StatusConnected,
			"jid":    jid,
		})
	case *events.Disconnected:
		i.setStatus(store.StatusDisconnected, "")
		_ = i.mgr.store.UpdateInstanceStatus(context.Background(), i.id, store.StatusDisconnected, "")
		i.mgr.webhook.Enqueue(i.id, "connection", map[string]any{
			"status": store.StatusDisconnected,
		})
	case *events.LoggedOut:
		slog.Warn("instance logged out dari perangkat utama", "instance", i.id, "reason", evt.Reason.String())
		i.setStatus(store.StatusLoggedOut, "")
		_ = i.mgr.store.ClearInstanceJID(context.Background(), i.id, store.StatusLoggedOut)
		i.mgr.webhook.Enqueue(i.id, "connection", map[string]any{
			"status": store.StatusLoggedOut,
			"reason": evt.Reason.String(),
		})
	case *events.Message:
		if p := extractMessage(evt); p != nil {
			i.mgr.webhook.Enqueue(i.id, "message", p)
		}
	case *events.Receipt:
		ids := make([]string, len(evt.MessageIDs))
		for idx, id := range evt.MessageIDs {
			ids[idx] = string(id)
		}
		i.mgr.webhook.Enqueue(i.id, "receipt", ReceiptPayload{
			MessageIDs: ids,
			Chat:       evt.Chat.ToNonAD().String(),
			Sender:     evt.Sender.ToNonAD().String(),
			Type:       string(evt.Type),
			Timestamp:  evt.Timestamp.Unix(),
		})
	case *events.HistorySync:
		// Drop seluruh history sync agar tidak memakan RAM maupun disk
	}
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
	s := strings.TrimSpace(raw)
	if s == "" {
		return types.JID{}, ErrInvalidRecipient
	}
	if strings.Contains(s, "@") {
		jid, err := types.ParseJID(s)
		if err != nil || jid.User == "" {
			return types.JID{}, ErrInvalidRecipient
		}
		return jid, nil
	}
	digits := digitsOnly(s)
	if len(digits) < 7 || len(digits) > 16 {
		return types.JID{}, ErrInvalidRecipient
	}
	return types.NewJID(digits, types.DefaultUserServer), nil
}

func digitsOnly(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

