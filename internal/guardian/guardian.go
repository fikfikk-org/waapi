package guardian

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow/types"
)

// Error sentinel domain Guardian untuk pemetaan HTTP pada outbox dan API grup.
var (
	ErrRecipientOptedOut  = errors.New("penerima telah memilih opt-out dari pesan")
	ErrGuardianRestricted = errors.New("akun berada dalam status restricted oleh Guardian")
	ErrGuardianLimited    = errors.New("batas kuota Guardian tercapai")
)

// Error membawa status HTTP, kode mesin API, retry_after, dan sentinel pembungkus dari Guardian.
type Error struct {
	Status     int
	Code       string
	Message    string
	RetryAfter int
	Err        error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Code
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// HTTPStatus mengembalikan kode status HTTP error Guardian.
func (e *Error) HTTPStatus() int { return e.Status }

// ErrorCode mengembalikan kode mesin error Guardian.
func (e *Error) ErrorCode() string { return e.Code }

// ErrorRetryAfter mengembalikan durasi tunggu Retry-After dalam detik bila ada.
func (e *Error) ErrorRetryAfter() int { return e.RetryAfter }

// ErrorDetails mengembalikan detail tambahan error bila diperlukan.
func (e *Error) ErrorDetails() map[string]any { return nil }

// SettingsSource membaca konfigurasi instance dan menyimpan pembaruan shadow mode bootstrap.
type SettingsSource interface {
	InstanceMeta(ctx context.Context, instanceID string) (store.Instance, bool)
	ListInstances(ctx context.Context) ([]store.Instance, error)
	SetShadowUntil(ctx context.Context, instanceID string, until int64) error
}

// ContactBook memeriksa apakah JID penerima tersimpan di buku alamat WhatsApp dengan nama terisi.
type ContactBook interface {
	IsSavedContact(ctx context.Context, instanceID string, target types.JID) bool
}

// LIDLookup meresolusi alamat LID ke JID nomor telepon kanonik secara lokal.
type LIDLookup interface {
	ResolvePN(ctx context.Context, instanceID string, target types.JID) (types.JID, bool)
}

// OutboxEnqueuer mengantrekan pesan sistem (seperti konfirmasi opt-out) ke durable outbox.
type OutboxEnqueuer interface {
	EnqueueSystemText(ctx context.Context, instanceID, to, text, idemKey string) error
}

// HealthEmitter memancarkan event health ke journal saat terjadi perubahan state kesehatan akun.
type HealthEmitter interface {
	EmitHealth(ctx context.Context, instanceID string, ev HealthEventPayload) error
}

// Options mengonfigurasi dependensi dan opsi runtime Guardian.
type Options struct {
	Settings      SettingsSource
	Contacts      ContactBook
	LIDs          LIDLookup
	Enqueuer      OutboxEnqueuer
	Emitter       HealthEmitter
	FlushInterval time.Duration
	DisableJitter bool
}

// HealthCounters merangkum penghitung pesan 24 jam terakhir.
type HealthCounters struct {
	Sent24h         int `json:"sent_24h"`
	FirstContact24h int `json:"first_contact_24h"`
	Delivered24h    int `json:"delivered_24h"`
	Replied24h      int `json:"replied_24h"`
}

// HealthLimits merangkum kapasitas harian dan warm-up aktif instance.
type HealthLimits struct {
	DailyCap              int     `json:"daily_cap"`
	DailyRemaining        int     `json:"daily_remaining"`
	FirstContactCap       int     `json:"first_contact_cap"`
	FirstContactRemaining int     `json:"first_contact_remaining"`
	WarmupFactor          float64 `json:"warmup_factor"`
	QuietHours            string  `json:"quiet_hours"`
}

// HealthBan merangkum status ban sementara WhatsApp terkini.
type HealthBan struct {
	Active   bool   `json:"active"`
	LastCode int    `json:"last_code,omitempty"`
	LastAt   int64  `json:"last_at,omitempty"`
	Until    *int64 `json:"until"`
}

// HealthReport adalah respons lengkap endpoint GET /v2/instances/{id}/health dan waapi health.
type HealthReport struct {
	Instance string         `json:"instance"`
	State    string         `json:"state"`
	Score    int            `json:"score"`
	Profile  string         `json:"profile"`
	Shadow   bool           `json:"shadow"`
	Factors  []Factor       `json:"factors"`
	Counters HealthCounters `json:"counters"`
	Limits   HealthLimits   `json:"limits"`
	Ban      HealthBan      `json:"ban"`
}

// HealthSummary adalah ringkasan singkat state dan score untuk field aditif GET /v1/instances/{id}.
type HealthSummary struct {
	State string `json:"state"`
	Score int    `json:"score"`
}

// HealthEventPayload adalah isi JSON payload untuk event journal bertipe "health".
type HealthEventPayload struct {
	State     string `json:"state"`
	PrevState string `json:"prev_state"`
	Score     int    `json:"score"`
	Profile   string `json:"profile"`
	Shadow    bool   `json:"shadow"`
	Reason    string `json:"reason,omitempty"`
	BanCode   int    `json:"ban_code,omitempty"`
	BanUntil  int64  `json:"ban_until,omitempty"`
}

// ConsentEvent adalah isi JSON payload untuk event journal bertipe "consent".
type ConsentEvent struct {
	JID     string `json:"jid"`
	Action  string `json:"action"` // opt_out | opt_in
	Keyword string `json:"keyword"`
}

const (
	ringBuckets         = 25
	contactLRUCap       = 4096
	contactLRUTTL       = time.Hour
	defaultFlushEvery   = 30 * time.Second
	banPostCooldown     = 10 * time.Minute
	maxInboundReplyIdle = 10 * time.Minute
)

type hourBucket struct {
	hour             int64
	sent             int
	firstContactSent int
	delivered        int
	replied          int
	groupsCreated    int
	rateOverlimit    int

	dirtySent          int
	dirtyFC            int
	dirtyDelivered     int
	dirtyReplied       int
	dirtyGroups        int
	dirtyRateOverlimit int
}

type instanceRing struct {
	buckets     [ringBuckets]hourBucket
	lastState   string
	banLoaded   bool
	lastBanCode int
	lastBanAt   int64
	banUntil    time.Time
}

type contactBookLRU struct {
	mu    sync.Mutex
	cap   int
	ttl   time.Duration
	ll    *list.List
	items map[string]*list.Element
}

type contactLRUItem struct {
	key       string
	saved     bool
	expiresAt time.Time
}

func newContactBookLRU(capacity int, ttl time.Duration) *contactBookLRU {
	return &contactBookLRU{
		cap:   capacity,
		ttl:   ttl,
		ll:    list.New(),
		items: make(map[string]*list.Element),
	}
}

func (c *contactBookLRU) get(key string, now time.Time) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return false, false
	}
	item := el.Value.(*contactLRUItem)
	if !now.Before(item.expiresAt) {
		delete(c.items, key)
		c.ll.Remove(el)
		return false, false
	}
	c.ll.MoveToFront(el)
	return item.saved, true
}

func (c *contactBookLRU) set(key string, saved bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	exp := now.Add(c.ttl)
	if el, ok := c.items[key]; ok {
		item := el.Value.(*contactLRUItem)
		item.saved = saved
		item.expiresAt = exp
		c.ll.MoveToFront(el)
		return
	}
	for c.ll.Len() >= c.cap {
		tail := c.ll.Back()
		if tail == nil {
			break
		}
		item := tail.Value.(*contactLRUItem)
		delete(c.items, item.key)
		c.ll.Remove(tail)
	}
	el := c.ll.PushFront(&contactLRUItem{key: key, saved: saved, expiresAt: exp})
	c.items[key] = el
}

// Guardian mengimplementasikan outbox.Governor dan wa.GroupGovernor untuk menjaga kesehatan akun WhatsApp.
type Guardian struct {
	st            *GuardianStore
	settings      SettingsSource
	contacts      ContactBook
	lids          LIDLookup
	enq           OutboxEnqueuer
	emitter       HealthEmitter
	bucket        *outbox.TokenBucketGovernor
	bookLRU       *contactBookLRU
	flushInterval time.Duration
	disableJitter bool

	mu    sync.Mutex
	rings map[string]*instanceRing

	loopMu     sync.Mutex
	loopCancel context.CancelFunc
	loopDone   chan struct{}
}

// New menginisialisasi Guardian di atas *sql.DB journal.db dan memuat bucket 25 jam terakhir ke RAM.
func New(journalDB *sql.DB, opts Options) (*Guardian, error) {
	if journalDB == nil {
		return nil, errors.New("journal db wajib diisi untuk Guardian")
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushEvery
	}

	g := &Guardian{
		st:            NewStore(journalDB),
		settings:      opts.Settings,
		contacts:      opts.Contacts,
		lids:          opts.LIDs,
		enq:           opts.Enqueuer,
		emitter:       opts.Emitter,
		bookLRU:       newContactBookLRU(contactLRUCap, contactLRUTTL),
		flushInterval: opts.FlushInterval,
		disableJitter: opts.DisableJitter,
		rings:         make(map[string]*instanceRing),
	}
	g.bucket = outbox.NewTokenBucketGovernor(func(instanceID string) int {
		meta := g.metaFor(context.Background(), instanceID)
		if meta.RatePerMin > 0 {
			return meta.RatePerMin
		}
		return 20
	})

	if err := g.loadHourly(context.Background(), time.Now()); err != nil {
		return nil, err
	}
	return g, nil
}

// Store mengembalikan akses GuardianStore internal (misal untuk pengujian atau audit).
func (g *Guardian) Store() *GuardianStore {
	if g == nil {
		return nil
	}
	return g.st
}

// SetRate meneruskan perubahan rate_per_min dari wa.Manager.Update ke token bucket internal.
func (g *Guardian) SetRate(instanceID string, rpm int) {
	if g != nil && g.bucket != nil {
		g.bucket.SetRate(instanceID, rpm)
	}
}

// BootstrapShadow menetapkan masa shadow 7 hari sekali bagi instance lama yang sudah tertaut sebelum Guardian aktif.
func (g *Guardian) BootstrapShadow(ctx context.Context) {
	if g == nil || g.settings == nil || g.st == nil {
		return
	}
	list, err := g.settings.ListInstances(ctx)
	if err != nil {
		slog.Warn("gagal membaca daftar instance saat bootstrap shadow guardian", "err", err)
		return
	}
	now := time.Now()
	nowSec := now.Unix()
	shadowUntil := now.Add(shadowBootstrapDuration).Unix()

	for _, meta := range list {
		done, bErr := g.st.IsShadowBootstrapped(ctx, meta.ID)
		if bErr != nil || done {
			continue
		}
		if strings.TrimSpace(meta.JID) != "" {
			if meta.Settings.GuardianShadowUntil == 0 {
				if err := g.settings.SetShadowUntil(ctx, meta.ID, shadowUntil); err != nil {
					slog.Warn("gagal menyetel guardian_shadow_until bootstrap", "instance", meta.ID, "err", err)
					continue
				}
			}
			_ = g.st.MarkShadowBootstrapped(ctx, meta.ID, nowSec)
		}
	}
}

// OnInstancePaired menandai instance baru pasca-pairing agar tidak menerima auto-shadow bootstrap lama.
func (g *Guardian) OnInstancePaired(ctx context.Context, instanceID string) {
	if g == nil || g.st == nil || instanceID == "" {
		return
	}
	_ = g.st.MarkShadowBootstrapped(ctx, instanceID, time.Now().Unix())
}

// StartFlushLoop menyalakan goroutine periodik tiap 30 detik untuk menulis dirty bucket ke journal.db.
func (g *Guardian) StartFlushLoop(ctx context.Context) {
	if g == nil {
		return
	}
	g.loopMu.Lock()
	defer g.loopMu.Unlock()
	if g.loopCancel != nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	g.loopCancel = cancel
	g.loopDone = done

	go func() {
		defer close(done)
		t := time.NewTicker(g.flushInterval)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				fCtx, fCancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := g.Flush(fCtx); err != nil {
					slog.Warn("gagal flush bucket guardian ke journal.db", "err", err)
				}
				fCancel()
			}
		}
	}()
}

// Stop menghentikan loop periodik dan melakukan flush akhir seluruh dirty bucket.
func (g *Guardian) Stop() {
	if g == nil {
		return
	}
	g.loopMu.Lock()
	cancel := g.loopCancel
	done := g.loopDone
	g.loopCancel = nil
	g.loopDone = nil
	g.loopMu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
	fCtx, fCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer fCancel()
	if err := g.Flush(fCtx); err != nil {
		slog.Warn("gagal flush akhir bucket guardian", "err", err)
	}
}

// Flush menyimpan seluruh delta penghitung RAM yang kotor ke tabel guardian_hourly.
func (g *Guardian) Flush(ctx context.Context) error {
	if g == nil || g.st == nil {
		return nil
	}
	type pendingDelta struct {
		instIdx int
		bIdx    int
		row     HourlyRow
	}

	g.mu.Lock()
	var batch []pendingDelta
	var rows []HourlyRow
	for id, ring := range g.rings {
		for idx := range ring.buckets {
			b := &ring.buckets[idx]
			if b.hour <= 0 {
				continue
			}
			if b.dirtySent == 0 && b.dirtyFC == 0 && b.dirtyDelivered == 0 &&
				b.dirtyReplied == 0 && b.dirtyGroups == 0 && b.dirtyRateOverlimit == 0 {
				continue
			}
			r := HourlyRow{
				InstanceID:       id,
				Hour:             b.hour,
				Sent:             b.dirtySent,
				FirstContactSent: b.dirtyFC,
				Delivered:        b.dirtyDelivered,
				Replied:          b.dirtyReplied,
				GroupsCreated:    b.dirtyGroups,
				RateOverlimit:    b.dirtyRateOverlimit,
			}
			batch = append(batch, pendingDelta{instIdx: 0, bIdx: idx, row: r})
			rows = append(rows, r)
		}
	}
	g.mu.Unlock()

	if len(rows) == 0 {
		return nil
	}
	if err := g.st.FlushHourlyDeltas(ctx, rows); err != nil {
		return err
	}

	g.mu.Lock()
	for _, p := range batch {
		ring, ok := g.rings[p.row.InstanceID]
		if !ok {
			continue
		}
		b := &ring.buckets[p.bIdx]
		if b.hour != p.row.Hour {
			continue
		}
		b.dirtySent -= p.row.Sent
		b.dirtyFC -= p.row.FirstContactSent
		b.dirtyDelivered -= p.row.Delivered
		b.dirtyReplied -= p.row.Replied
		b.dirtyGroups -= p.row.GroupsCreated
		b.dirtyRateOverlimit -= p.row.RateOverlimit
	}
	g.mu.Unlock()
	return nil
}

// Prune menghapus bucket > 8 hari dan baris fan-out > 2 hari di journal.db.
func (g *Guardian) Prune(ctx context.Context, now time.Time) error {
	if g == nil || g.st == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	return g.st.PruneOld(ctx, now)
}

func (g *Guardian) loadHourly(ctx context.Context, now time.Time) error {
	sinceHour := (now.Unix() / 3600) - (ringBuckets - 1)
	rows, err := g.st.LoadRecentHourly(ctx, sinceHour)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, r := range rows {
		ring := g.ringLocked(r.InstanceID)
		b := ring.bucketForHour(r.Hour)
		b.sent = r.Sent
		b.firstContactSent = r.FirstContactSent
		b.delivered = r.Delivered
		b.replied = r.Replied
		b.groupsCreated = r.GroupsCreated
		b.rateOverlimit = r.RateOverlimit
	}
	return nil
}

func (r *instanceRing) bucketForHour(hour int64) *hourBucket {
	slot := int(hour % ringBuckets)
	if slot < 0 {
		slot += ringBuckets
	}
	b := &r.buckets[slot]
	if b.hour != hour {
		*b = hourBucket{hour: hour}
	}
	return b
}

func (g *Guardian) ringLocked(instanceID string) *instanceRing {
	ring, ok := g.rings[instanceID]
	if !ok {
		ring = &instanceRing{}
		g.rings[instanceID] = ring
	}
	return ring
}

func (g *Guardian) ensureBanLoaded(ctx context.Context, instanceID string) (code int, at int64, until time.Time) {
	g.mu.Lock()
	ring := g.ringLocked(instanceID)
	loaded := ring.banLoaded
	code = ring.lastBanCode
	at = ring.lastBanAt
	until = ring.banUntil
	g.mu.Unlock()

	if !loaded && g.st != nil {
		lCode, lAt, err := g.st.LatestTempBan(ctx, instanceID)
		if err == nil {
			g.mu.Lock()
			ring := g.ringLocked(instanceID)
			if !ring.banLoaded {
				ring.banLoaded = true
				if lAt > ring.lastBanAt {
					ring.lastBanCode = lCode
					ring.lastBanAt = lAt
				}
			}
			code = ring.lastBanCode
			at = ring.lastBanAt
			until = ring.banUntil
			g.mu.Unlock()
		}
	}
	return code, at, until
}

func (g *Guardian) metaFor(ctx context.Context, instanceID string) store.Instance {
	if g.settings != nil {
		if meta, ok := g.settings.InstanceMeta(ctx, instanceID); ok {
			if meta.Settings.JournalReceipts == "" {
				meta.Settings = store.Defaults()
			}
			return meta
		}
	}
	return store.Instance{
		ID:         instanceID,
		RatePerMin: 20,
		Settings:   store.Defaults(),
	}
}

func (g *Guardian) canonicalRecipient(ctx context.Context, instanceID, raw string) (canon string, jidVal types.JID, isUser bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", types.EmptyJID, false
	}
	parsed, err := types.ParseJID(s)
	if err != nil || parsed.IsEmpty() {
		return s, types.EmptyJID, false
	}
	nonAD := parsed.ToNonAD()
	switch nonAD.Server {
	case types.DefaultUserServer:
		return nonAD.String(), nonAD, true
	case types.HiddenUserServer:
		if g.lids != nil {
			if pn, ok := g.lids.ResolvePN(ctx, instanceID, nonAD); ok && !pn.IsEmpty() && pn.Server == types.DefaultUserServer {
				pnNonAD := pn.ToNonAD()
				return pnNonAD.String(), pnNonAD, true
			}
		}
		return nonAD.String(), nonAD, true
	default:
		return nonAD.String(), nonAD, false
	}
}

func (g *Guardian) isSavedContact(ctx context.Context, instanceID string, target types.JID) bool {
	if g.contacts == nil || target.IsEmpty() {
		return false
	}
	now := time.Now()
	key := instanceID + "|" + target.ToNonAD().String()
	if saved, ok := g.bookLRU.get(key, now); ok {
		return saved
	}
	saved := g.contacts.IsSavedContact(ctx, instanceID, target)
	g.bookLRU.set(key, saved, now)
	return saved
}

func (g *Guardian) isFirstContact(ctx context.Context, instanceID, canon string, rcptJID types.JID, isUser bool) bool {
	if !isUser || canon == "" {
		return false
	}
	row, found, err := g.st.GetContact(ctx, instanceID, canon)
	if err == nil && found && (row.FirstIn > 0 || row.FirstOut > 0) {
		return false
	}
	if !rcptJID.IsEmpty() && g.isSavedContact(ctx, instanceID, rcptJID) {
		return false
	}
	return true
}

func (g *Guardian) computeEval(ctx context.Context, instanceID string, meta store.Instance, now time.Time) (evalSnapshot, error) {
	prof := ProfileFor(meta.Settings.GuardianProfile)
	loc := LoadLocation(meta.Settings.Timezone)
	day := now.In(loc).Format("2006-01-02")

	banCode, banAt, banUntil := g.ensureBanLoaded(ctx, instanceID)
	banActive := !banUntil.IsZero() && now.Before(banUntil)
	sinceBan := time.Duration(-1)
	if banAt > 0 {
		diff := now.Sub(time.Unix(banAt, 0))
		if diff >= 0 {
			sinceBan = diff
		}
	}

	curHour := now.Unix() / 3600
	dayStartHour := startOfDayLocal(now, loc).Unix() / 3600

	var (
		sent24h, fc24h, delivered24h, replied24h int
		sentEligible, deliveredEligible          int
		rateOverlimit1h                          int
		todaySent, todayFC, todayGroups          int
	)

	g.mu.Lock()
	ring := g.ringLocked(instanceID)
	for _, b := range ring.buckets {
		if b.hour <= 0 {
			continue
		}
		if b.hour >= curHour-23 && b.hour <= curHour {
			sent24h += b.sent
			fc24h += b.firstContactSent
			delivered24h += b.delivered
			replied24h += b.replied
		}
		// Jendela eligible untuk rasio delivery: [now-25j, now-1j).
		if b.hour >= curHour-24 && b.hour <= curHour-1 {
			sentEligible += b.sent
			deliveredEligible += b.delivered
		}
		if b.hour == curHour {
			rateOverlimit1h += b.rateOverlimit
		}
		if b.hour >= dayStartHour && b.hour <= curHour {
			todaySent += b.sent
			todayFC += b.firstContactSent
			todayGroups += b.groupsCreated
		}
	}
	g.mu.Unlock()

	recipients24h, _ := g.st.CountRecipients24h(ctx, instanceID, now.Add(-24*time.Hour).Unix())
	maxFanout, _ := g.st.MaxFanoutDay(ctx, instanceID, day)
	fanoutRatio := 0.0
	if prof.ContentFanoutCap > 0 && maxFanout > 0 {
		fanoutRatio = float64(maxFanout) / float64(prof.ContentFanoutCap)
	}

	score, factors := Score(ScoreInput{
		SinceTempBan:    sinceBan,
		Sent24h:         sent24h,
		SentEligible24h: sentEligible,
		Delivered24h:    deliveredEligible,
		FirstContact24h: fc24h,
		Recipients24h:   recipients24h,
		Replied24h:      replied24h,
		FanoutMaxRatio:  fanoutRatio,
		RateOverlimit1h: rateOverlimit1h,
	})
	if prof.Name == ProfileOff {
		score = 100
		factors = []Factor{}
	}

	state := StateForScore(prof.Name, banActive, score)
	shadow := prof.Name != ProfileOff && meta.Settings.GuardianShadowUntil > 0 && now.Unix() < meta.Settings.GuardianShadowUntil
	pairedAt := time.Time{}
	if meta.PairedAt > 0 {
		pairedAt = time.Unix(meta.PairedAt, 0)
	}
	warmup := WarmupFactor(pairedAt, now)
	dailyCap, fcCap := EffectiveLimits(prof, state, warmup)

	return evalSnapshot{
		prof:            prof,
		loc:             loc,
		day:             day,
		state:           state,
		score:           score,
		factors:         factors,
		shadow:          shadow,
		warmup:          warmup,
		dailyCap:        dailyCap,
		fcCap:           fcCap,
		banActive:       banActive,
		banCode:         banCode,
		banAt:           banAt,
		banUntil:        banUntil,
		sent24h:         sent24h,
		fc24h:           fc24h,
		delivered24h:    delivered24h,
		replied24h:      replied24h,
		todaySent:       todaySent,
		todayFC:         todayFC,
		todayGroups:     todayGroups,
		effectiveQuiet:  EffectiveQuietHours(meta.Settings, prof),
		settings:        meta.Settings,
		graceBanSeconds: meta.PairedAt,
	}, nil
}

type evalSnapshot struct {
	prof            ProfileSpec
	loc             *time.Location
	day             string
	state           string
	score           int
	factors         []Factor
	shadow          bool
	warmup          float64
	dailyCap        int
	fcCap           int
	banActive       bool
	banCode         int
	banAt           int64
	banUntil        time.Time
	sent24h         int
	fc24h           int
	delivered24h    int
	replied24h      int
	todaySent       int
	todayFC         int
	todayGroups     int
	effectiveQuiet  string
	settings        store.InstanceSettings
	graceBanSeconds int64
}

func (g *Guardian) maybeTransitionState(ctx context.Context, instanceID string, eval evalSnapshot, reason string, now time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	ring := g.ringLocked(instanceID)
	prev := ring.lastState
	if prev == "" {
		ring.lastState = eval.state
		g.mu.Unlock()
		return
	}
	if prev == eval.state {
		g.mu.Unlock()
		return
	}
	ring.lastState = eval.state
	g.mu.Unlock()

	if g.st != nil {
		_ = g.st.InsertHealthLog(ctx, instanceID, now.Unix(), "state_change", eval.score, fmt.Sprintf("%s -> %s (%s)", prev, eval.state, reason))
	}
	if g.emitter != nil {
		var banUntilUnix int64
		if !eval.banUntil.IsZero() {
			banUntilUnix = eval.banUntil.Unix()
		}
		_ = g.emitter.EmitHealth(ctx, instanceID, HealthEventPayload{
			State:     eval.state,
			PrevState: prev,
			Score:     eval.score,
			Profile:   eval.prof.Name,
			Shadow:    eval.shadow,
			Reason:    reason,
			BanCode:   eval.banCode,
			BanUntil:  banUntilUnix,
		})
	}
}

// InboundObserver mengembalikan wa.InboundObserver untuk memproses pesan masuk dan kata kunci opt-out/opt-in sebelum ack ke WhatsApp.
func (g *Guardian) InboundObserver() func(ctx context.Context, ev *journal.Event, raw any) journal.TxHook {
	return g.InboundHook
}

// InboundHook menyusun TxHook transaksi journal.db untuk pesan masuk (upserts contacts_seen, consent, dan reply counter pasca-commit).
func (g *Guardian) InboundHook(ctx context.Context, ev *journal.Event, _ any) journal.TxHook {
	if g == nil || g.st == nil || ev == nil || ev.Type != "message" || ev.InstanceID == "" {
		return nil
	}
	var payload struct {
		From     string `json:"from"`
		SenderPN string `json:"sender_pn"`
		ChatType string `json:"chat_type"`
		IsGroup  bool   `json:"is_group"`
		IsFromMe bool   `json:"is_from_me"`
		FromMe   bool   `json:"from_me"`
		Type     string `json:"type"`
		Text     string `json:"text"`
	}
	if len(ev.Payload) > 0 {
		_ = json.Unmarshal(ev.Payload, &payload)
	}
	if payload.IsFromMe || payload.FromMe || payload.IsGroup {
		return nil
	}
	if payload.ChatType != "" && payload.ChatType != "dm" {
		return nil
	}

	rawSender := payload.SenderPN
	if rawSender == "" {
		rawSender = ev.Sender
	}
	if rawSender == "" {
		rawSender = payload.From
	}
	canonJID, _, isUser := g.canonicalRecipient(ctx, ev.InstanceID, rawSender)
	if !isUser || canonJID == "" {
		return nil
	}

	atMs := ev.ReceivedAt
	if atMs <= 0 {
		atMs = time.Now().UnixMilli()
	}
	atSec := atMs / 1000
	meta := g.metaFor(ctx, ev.InstanceID)

	var (
		optedPtr  *bool
		optAction string
		matchedKW string
	)
	if kw, ok := MatchKeyword(payload.Text, meta.Settings.OptoutKeywords); ok {
		v := true
		optedPtr = &v
		optAction = "opt_out"
		matchedKW = kw
	} else if kw, ok := MatchKeyword(payload.Text, meta.Settings.OptinKeywords); ok {
		v := false
		optedPtr = &v
		optAction = "opt_in"
		matchedKW = kw
	}

	confirmText := strings.TrimSpace(meta.Settings.OptoutConfirmText)

	return func(txCtx context.Context, tx *sql.Tx, _ int64) error {
		replied, err := g.st.UpsertInboundTx(txCtx, tx, ev.InstanceID, canonJID, atSec, optedPtr)
		if err != nil {
			return err
		}
		if optAction != "" {
			cBytes, err := json.Marshal(ConsentEvent{
				JID:     canonJID,
				Action:  optAction,
				Keyword: matchedKW,
			})
			if err != nil {
				return err
			}
			consentEv := &journal.Event{
				InstanceID: ev.InstanceID,
				Type:       "consent",
				Chat:       canonJID,
				Sender:     canonJID,
				TS:         atSec,
				ReceivedAt: atMs,
				Part:       journal.PartForChat(canonJID),
				Payload:    cBytes,
			}
			if _, err := journal.InsertTx(txCtx, tx, consentEv); err != nil {
				return fmt.Errorf("simpan event consent ke journal: %w", err)
			}
		}

		journal.OnCommit(txCtx, func() {
			if replied {
				g.mu.Lock()
				b := g.ringLocked(ev.InstanceID).bucketForHour(atSec / 3600)
				b.replied++
				b.dirtyReplied++
				g.mu.Unlock()
			}
			if optAction == "opt_out" && confirmText != "" && g.enq != nil {
				idem := fmt.Sprintf("consent:%s:%d", canonJID, atMs)
				if err := g.enq.EnqueueSystemText(context.Background(), ev.InstanceID, canonJID, confirmText, idem); err != nil {
					slog.Warn("gagal mengantrekan pesan konfirmasi opt-out", "instance", ev.InstanceID, "jid", canonJID, "err", err)
				}
			}
		})
		return nil
	}
}

// ObserveDelivered memperbarui counter pengiriman tersampaikan di RAM setelah transaksi receipt commit sukses.
func (g *Guardian) ObserveDelivered(instanceID string, n int) {
	if g == nil || instanceID == "" || n <= 0 {
		return
	}
	hour := time.Now().Unix() / 3600
	g.mu.Lock()
	b := g.ringLocked(instanceID).bucketForHour(hour)
	b.delivered += n
	b.dirtyDelivered += n
	g.mu.Unlock()
}

// ObserveTempBan mencatat ban sementara WhatsApp dari Supervisor, memperbarui state suspended, dan memancarkan event health.
func (g *Guardian) ObserveTempBan(ctx context.Context, instanceID string, code int, expire time.Duration) error {
	if g == nil || instanceID == "" {
		return nil
	}
	now := time.Now()
	nowSec := now.Unix()
	var until time.Time
	if expire > 0 {
		until = now.Add(expire)
	}

	g.mu.Lock()
	ring := g.ringLocked(instanceID)
	ring.banLoaded = true
	ring.lastBanCode = code
	ring.lastBanAt = nowSec
	ring.banUntil = until
	g.mu.Unlock()

	if g.st != nil {
		if err := g.st.InsertHealthLog(ctx, instanceID, nowSec, "temp_ban", code, fmt.Sprintf("temporary ban expire=%s", expire)); err != nil {
			return err
		}
	}

	meta := g.metaFor(ctx, instanceID)
	eval, err := g.computeEval(ctx, instanceID, meta, now)
	if err == nil {
		g.maybeTransitionState(ctx, instanceID, eval, fmt.Sprintf("temporary ban kode %d", code), now)
	}
	return nil
}

// Precheck menerapkan penolakan definitif saat enqueue untuk aturan 1 (opt-out), aturan 3 (restricted), dan aturan 4 (fan-out).
func (g *Guardian) Precheck(ctx context.Context, job *outbox.Job) error {
	if g == nil || job == nil {
		return nil
	}
	now := time.Now()
	canon, rcptJID, isUser := g.canonicalRecipient(ctx, job.InstanceID, job.ToJID)
	var (
		cRow      ContactRow
		seenFound bool
	)
	if isUser && canon != "" {
		cRow, seenFound, _ = g.st.GetContact(ctx, job.InstanceID, canon)
	}

	// Aturan 1 selalu ditegakkan (termasuk profil off & shadow mode), kecuali pesan sistem.
	if seenFound && cRow.OptedOut && job.Origin != "system" {
		return &Error{
			Status:  http.StatusConflict,
			Code:    "recipient_opted_out",
			Message: "penerima telah berhenti berlangganan (opt-out)",
			Err:     ErrRecipientOptedOut,
		}
	}

	meta := g.metaFor(ctx, job.InstanceID)
	if ProfileFor(meta.Settings.GuardianProfile).Name == ProfileOff {
		return nil
	}

	eval, err := g.computeEval(ctx, job.InstanceID, meta, now)
	if err != nil {
		return nil
	}

	// Aturan 3: restricted & tidak ada pesan masuk dari penerima dalam 24 jam.
	if isUser && eval.state == StateRestricted {
		hasRecentIn := seenFound && cRow.LastIn > 0 && now.Sub(time.Unix(cRow.LastIn, 0)) <= 24*time.Hour
		if !hasRecentIn && job.Origin != "system" {
			if eval.shadow {
				g.recordShadowDecision("guardian_restricted", job.InstanceID)
			} else {
				return &Error{
					Status:  http.StatusForbidden,
					Code:    "guardian_restricted",
					Message: "skor kesehatan akun berada pada level restricted untuk penerima ini",
					Err:     ErrGuardianRestricted,
				}
			}
		}
	}

	// Aturan 4: fan-out konten identik (teks/caption non-kosong) mencapai kuota.
	normText := NormalizeContent(job.Spec)
	if normText != "" && eval.prof.ContentFanoutCap > 0 && canon != "" {
		cnt, contains, fErr := g.st.FanoutStatus(ctx, job.InstanceID, eval.day, ContentHash(normText), canon)
		if fErr == nil && !contains && cnt >= eval.prof.ContentFanoutCap {
			if eval.shadow {
				g.recordShadowDecision("guardian_content_fanout", job.InstanceID)
			} else {
				retrySec := int(nextMidnightLocal(now, eval.loc).Sub(now).Seconds())
				if retrySec < 1 {
					retrySec = 1
				}
				return &Error{
					Status:     http.StatusTooManyRequests,
					Code:       "guardian_limited",
					Message:    fmt.Sprintf("konten identik sudah dikirim ke %d penerima hari ini", eval.prof.ContentFanoutCap),
					RetryAfter: retrySec,
					Err:        ErrGuardianLimited,
				}
			}
		}
	}
	_ = rcptJID
	return nil
}

// Decide mengevaluasi 10 aturan Guardian berurutan ketika job siap dieksekusi scheduler outbox.
func (g *Guardian) Decide(ctx context.Context, job *outbox.Job, now time.Time) outbox.Decision {
	if g == nil || job == nil {
		return outbox.Decision{Allow: true}
	}
	if now.IsZero() {
		now = time.Now()
	}

	canon, rcptJID, isUser := g.canonicalRecipient(ctx, job.InstanceID, job.ToJID)
	var (
		cRow      ContactRow
		seenFound bool
	)
	if isUser && canon != "" {
		cRow, seenFound, _ = g.st.GetContact(ctx, job.InstanceID, canon)
	}

	// 1. Penerima opted_out (tetap ditegakkan meski shadow/off, kecuali pesan konfirmasi system).
	if seenFound && cRow.OptedOut && job.Origin != "system" {
		return outbox.Decision{
			Allow:     false,
			Permanent: true,
			Code:      "recipient_opted_out",
			Reason:    "penerima berstatus opt-out",
		}
	}

	meta := g.metaFor(ctx, job.InstanceID)
	eval, err := g.computeEval(ctx, job.InstanceID, meta, now)
	if err != nil {
		return outbox.Decision{Allow: true}
	}

	// 2. Ban aktif atau masa pendinginan pasca-ban (+10 menit setelah BanUntil) -> tunda hingga BanUntil + 10m.
	if !eval.banUntil.IsZero() && now.Before(eval.banUntil.Add(banPostCooldown)) {
		delay := eval.banUntil.Add(banPostCooldown).Sub(now)
		if delay < time.Second {
			delay = time.Second
		}
		return outbox.Decision{
			Allow:  false,
			Delay:  delay,
			Code:   "guardian_suspended",
			Reason: "akun sedang dalam masa temporary ban",
		}
	}

	if eval.prof.Name != ProfileOff {
		// 3. Restricted & penerima tanpa last_in 24 jam -> Permanent guardian_restricted.
		if isUser && eval.state == StateRestricted {
			hasRecentIn := seenFound && cRow.LastIn > 0 && now.Sub(time.Unix(cRow.LastIn, 0)) <= 24*time.Hour
			if !hasRecentIn && job.Origin != "system" {
				if eval.shadow {
					g.recordShadowDecision("guardian_restricted", job.InstanceID)
				} else {
					return outbox.Decision{
						Allow:     false,
						Permanent: true,
						Code:      "guardian_restricted",
						Reason:    "state guardian restricted dan belum ada interaksi masuk 24 jam",
					}
				}
			}
		}

		// 4. Konten identik mencapai kuota dan penerima belum tercatat.
		if normText := NormalizeContent(job.Spec); normText != "" && eval.prof.ContentFanoutCap > 0 && canon != "" {
			cnt, contains, fErr := g.st.FanoutStatus(ctx, job.InstanceID, eval.day, ContentHash(normText), canon)
			if fErr == nil && !contains && cnt >= eval.prof.ContentFanoutCap {
				if eval.shadow {
					g.recordShadowDecision("guardian_content_fanout", job.InstanceID)
				} else {
					return outbox.Decision{
						Allow:     false,
						Permanent: true,
						Code:      "guardian_content_fanout",
						Reason:    "batas fan-out konten identik harian tercapai",
					}
				}
			}
		}

		firstContact := g.isFirstContact(ctx, job.InstanceID, canon, rcptJID, isUser)

		// 5. First-contact dan skor < minimal profil.
		if firstContact && eval.score < eval.prof.MinFirstContactScore && job.Origin != "system" {
			if eval.shadow {
				g.recordShadowDecision("guardian_low_score", job.InstanceID)
			} else {
				return outbox.Decision{
					Allow:     false,
					Permanent: true,
					Code:      "guardian_low_score",
					Reason:    fmt.Sprintf("skor kesehatan %d di bawah batas minimal first-contact (%d)", eval.score, eval.prof.MinFirstContactScore),
				}
			}
		}

		// 6. First-contact dan kuota hari ini habis -> tunda tengah malam lokal + jitter 0-30m.
		if firstContact && eval.todayFC >= eval.fcCap && job.Origin != "system" {
			if eval.shadow {
				g.recordShadowDecision("guardian_first_contact_cap", job.InstanceID)
			} else {
				delay := nextMidnightLocal(now, eval.loc).Sub(now) + g.jitterDuration(30*time.Minute)
				if delay < time.Second {
					delay = time.Second
				}
				return outbox.Decision{
					Allow:  false,
					Delay:  delay,
					Code:   "guardian_first_contact_cap",
					Reason: "kuota pengiriman first-contact harian habis",
				}
			}
		}

		// 7. Kuota harian total habis -> tunda ke hari berikutnya.
		if eval.todaySent >= eval.dailyCap && job.Origin != "system" {
			if eval.shadow {
				g.recordShadowDecision("guardian_daily_cap", job.InstanceID)
			} else {
				delay := nextMidnightLocal(now, eval.loc).Sub(now) + g.jitterDuration(30*time.Minute)
				if delay < time.Second {
					delay = time.Second
				}
				return outbox.Decision{
					Allow:  false,
					Delay:  delay,
					Code:   "guardian_daily_cap",
					Reason: "kuota pengiriman harian habis",
				}
			}
		}
	}

	// 8. Quiet hours, prioritas bukan urgent, dan penerima tidak mengirim pesan dalam 10 menit terakhir.
	if eval.effectiveQuiet != "" && job.Priority < outbox.PriorityUrgent {
		recentInbound := seenFound && cRow.LastIn > 0 && now.Sub(time.Unix(cRow.LastIn, 0)) <= maxInboundReplyIdle
		if !recentInbound {
			if active, endsAt := CheckQuietHours(eval.effectiveQuiet, meta.Settings.Timezone, now); active {
				delay := endsAt.Sub(now) + g.jitterDuration(15*time.Minute)
				if delay < time.Second {
					delay = time.Second
				}
				return outbox.Decision{
					Allow:  false,
					Delay:  delay,
					Code:   "quiet_hours",
					Reason: "berada dalam jam tenang (quiet hours)",
				}
			}
		}
	}

	// 9. Token bucket rate_per_min (x0.5 saat state cautious).
	if g.bucket != nil {
		scale := 1.0
		if eval.state == StateCautious {
			scale = 0.5
		}
		if dec := g.bucket.DecideWithScale(ctx, job, now, scale); !dec.Allow {
			return dec
		}
	}

	// 10. Izinkan pengiriman.
	return outbox.Decision{Allow: true}
}

// Record mencatat hasil percobaan eksekusi outbox ke penghitung RAM, kontak keluar, fan-out, serta log rate_overlimit.
func (g *Guardian) Record(ctx context.Context, job *outbox.Job, out outbox.Outcome) {
	if g == nil || job == nil {
		return
	}
	if g.bucket != nil {
		g.bucket.Record(ctx, job, out)
	}

	now := time.Now()
	nowSec := now.Unix()
	hour := nowSec / 3600

	if out.Success {
		canon, rcptJID, isUser := g.canonicalRecipient(ctx, job.InstanceID, job.ToJID)
		firstContact := g.isFirstContact(ctx, job.InstanceID, canon, rcptJID, isUser)

		g.mu.Lock()
		b := g.ringLocked(job.InstanceID).bucketForHour(hour)
		b.sent++
		b.dirtySent++
		if firstContact {
			b.firstContactSent++
			b.dirtyFC++
		}
		g.mu.Unlock()

		if isUser && canon != "" {
			_ = g.st.RecordOutbound(ctx, job.InstanceID, canon, nowSec)
		}
		if norm := NormalizeContent(job.Spec); norm != "" && canon != "" {
			meta := g.metaFor(ctx, job.InstanceID)
			day := now.In(LoadLocation(meta.Settings.Timezone)).Format("2006-01-02")
			_ = g.st.RecordFanout(ctx, job.InstanceID, day, ContentHash(norm), canon)
		}
	}

	if out.RateLimited {
		g.mu.Lock()
		b := g.ringLocked(job.InstanceID).bucketForHour(hour)
		b.rateOverlimit++
		b.dirtyRateOverlimit++
		g.mu.Unlock()

		if g.st != nil {
			_ = g.st.InsertHealthLog(ctx, job.InstanceID, nowSec, "rate_overlimit", 0, "server menolak laju pengiriman")
		}
	}

	meta := g.metaFor(ctx, job.InstanceID)
	if eval, err := g.computeEval(ctx, job.InstanceID, meta, now); err == nil {
		reason := "pembaruan skor pasca kirim"
		if out.RateLimited {
			reason = "rate overlimit dari WhatsApp"
		}
		g.maybeTransitionState(ctx, job.InstanceID, eval, reason, now)
	}
}

// AllowGroupCreate memeriksa kuota pembuatan grup harian (strict: 2, standard: 5, relaxed: 10, tanpa pengali warm-up).
func (g *Guardian) AllowGroupCreate(ctx context.Context, instanceID string, participants []string) error {
	if g == nil {
		return nil
	}
	now := time.Now()
	meta := g.metaFor(ctx, instanceID)
	eval, err := g.computeEval(ctx, instanceID, meta, now)
	if err != nil || eval.prof.Name == ProfileOff {
		return nil
	}

	if eval.banActive || eval.state == StateRestricted {
		if eval.shadow && !eval.banActive {
			g.recordShadowDecision("guardian_restricted", instanceID)
		} else {
			return &Error{
				Status:  http.StatusForbidden,
				Code:    "guardian_restricted",
				Message: "status kesehatan akun tidak mengizinkan pembuatan grup baru",
				Err:     ErrGuardianRestricted,
			}
		}
	}

	if eval.prof.GroupsDailyCap > 0 && eval.todayGroups >= eval.prof.GroupsDailyCap {
		if eval.shadow {
			g.recordShadowDecision("guardian_group_daily_cap", instanceID)
		} else {
			retrySec := int(nextMidnightLocal(now, eval.loc).Sub(now).Seconds())
			if retrySec < 1 {
				retrySec = 1
			}
			return &Error{
				Status:     http.StatusTooManyRequests,
				Code:       "guardian_limited",
				Message:    fmt.Sprintf("kuota pembuatan grup harian (%d) sudah habis", eval.prof.GroupsDailyCap),
				RetryAfter: retrySec,
				Err:        ErrGuardianLimited,
			}
		}
	}

	if len(participants) > 0 {
		return g.AllowParticipantAdd(ctx, instanceID, participants)
	}
	return nil
}

// AllowParticipantAdd memeriksa status restricted/first-contact untuk peserta yang akan ditambahkan ke grup.
func (g *Guardian) AllowParticipantAdd(ctx context.Context, instanceID string, participants []string) error {
	if g == nil || len(participants) == 0 {
		return nil
	}
	now := time.Now()
	meta := g.metaFor(ctx, instanceID)
	eval, err := g.computeEval(ctx, instanceID, meta, now)
	if err != nil || eval.prof.Name == ProfileOff {
		return nil
	}

	if eval.banActive || eval.state == StateRestricted {
		if eval.shadow && !eval.banActive {
			g.recordShadowDecision("guardian_restricted", instanceID)
		} else {
			return &Error{
				Status:  http.StatusForbidden,
				Code:    "guardian_restricted",
				Message: "status kesehatan akun restricted menolak penambahan peserta grup",
				Err:     ErrGuardianRestricted,
			}
		}
	}

	newFC := 0
	for _, raw := range participants {
		canon, rcptJID, isUser := g.canonicalRecipient(ctx, instanceID, raw)
		if g.isFirstContact(ctx, instanceID, canon, rcptJID, isUser) {
			newFC++
		}
	}
	if newFC == 0 {
		return nil
	}

	if eval.score < eval.prof.MinFirstContactScore || eval.todayFC+newFC > eval.fcCap {
		if eval.shadow {
			g.recordShadowDecision("guardian_first_contact_cap", instanceID)
			return nil
		}
		if eval.score < eval.prof.MinFirstContactScore {
			return &Error{
				Status:  http.StatusForbidden,
				Code:    "guardian_restricted",
				Message: "skor kesehatan akun belum memenuhi syarat menambah peserta first-contact",
				Err:     ErrGuardianRestricted,
			}
		}
		retrySec := int(nextMidnightLocal(now, eval.loc).Sub(now).Seconds())
		if retrySec < 1 {
			retrySec = 1
		}
		return &Error{
			Status:     http.StatusTooManyRequests,
			Code:       "guardian_limited",
			Message:    "kuota first-contact harian tidak mencukupi untuk menambah peserta baru",
			RetryAfter: retrySec,
			Err:        ErrGuardianLimited,
		}
	}
	return nil
}

// RecordGroupCreated menambah penghitung grup terbuat pada jam berjalan setelah CreateGroup berhasil di server WhatsApp.
func (g *Guardian) RecordGroupCreated(ctx context.Context, instanceID string) {
	if g == nil || instanceID == "" {
		return
	}
	hour := time.Now().Unix() / 3600
	g.mu.Lock()
	b := g.ringLocked(instanceID).bucketForHour(hour)
	b.groupsCreated++
	b.dirtyGroups++
	g.mu.Unlock()
	_ = ctx
}

// Health mengembalikan laporan lengkap skor, faktor, penghitung 24h, limit harian, dan status ban untuk endpoint /health & CLI.
func (g *Guardian) Health(ctx context.Context, instanceID string) (HealthReport, error) {
	if g == nil {
		return HealthReport{}, errors.New("guardian belum aktif")
	}
	meta, ok := g.settings.InstanceMeta(ctx, instanceID)
	if !ok {
		return HealthReport{}, store.ErrNotFound
	}
	if meta.Settings.JournalReceipts == "" {
		meta.Settings = store.Defaults()
	}
	now := time.Now()
	eval, err := g.computeEval(ctx, instanceID, meta, now)
	if err != nil {
		return HealthReport{}, err
	}

	dailyRem := eval.dailyCap - eval.todaySent
	if dailyRem < 0 {
		dailyRem = 0
	}
	fcRem := eval.fcCap - eval.todayFC
	if fcRem < 0 {
		fcRem = 0
	}
	var untilPtr *int64
	if !eval.banUntil.IsZero() && now.Before(eval.banUntil) {
		v := eval.banUntil.Unix()
		untilPtr = &v
	}

	factors := eval.factors
	if factors == nil {
		factors = []Factor{}
	}

	return HealthReport{
		Instance: instanceID,
		State:    eval.state,
		Score:    eval.score,
		Profile:  eval.prof.Name,
		Shadow:   eval.shadow,
		Factors:  factors,
		Counters: HealthCounters{
			Sent24h:         eval.sent24h,
			FirstContact24h: eval.fc24h,
			Delivered24h:    eval.delivered24h,
			Replied24h:      eval.replied24h,
		},
		Limits: HealthLimits{
			DailyCap:              eval.dailyCap,
			DailyRemaining:        dailyRem,
			FirstContactCap:       eval.fcCap,
			FirstContactRemaining: fcRem,
			WarmupFactor:          eval.warmup,
			QuietHours:            eval.effectiveQuiet,
		},
		Ban: HealthBan{
			Active:   eval.banActive,
			LastCode: eval.banCode,
			LastAt:   eval.banAt,
			Until:    untilPtr,
		},
	}, nil
}

// Summary mengembalikan ringkasan state dan score ringkas untuk disematkan ke status instance v1.
func (g *Guardian) Summary(ctx context.Context, instanceID string) (HealthSummary, error) {
	rep, err := g.Health(ctx, instanceID)
	if err != nil {
		return HealthSummary{}, err
	}
	return HealthSummary{
		State: rep.State,
		Score: rep.Score,
	}, nil
}

func (g *Guardian) recordShadowDecision(code, instanceID string) {
	obs.MetricGuardianShadowTotal.With(code).Inc()
	slog.Debug("keputusan guardian dilewati oleh shadow mode", "instance", instanceID, "code", code)
}

func (g *Guardian) jitterDuration(max time.Duration) time.Duration {
	if g.disableJitter || max <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(max) + 1))
}

// forcePenaltyForTest menyuntikkan sinyal penalti melalui bucket jam nyata untuk pengujian tabel skor dan Decide.
func (g *Guardian) forcePenaltyForTest(instanceID string, now time.Time, penaltyTarget int, _ int, _ int) {
	curHour := now.Unix() / 3600
	g.mu.Lock()
	defer g.mu.Unlock()
	ring := g.ringLocked(instanceID)
	// Jendela eligible [curHour-24, curHour-1] -> -40 dari undelivered 100% pada 40 pesan.
	bElig := ring.bucketForHour(curHour - 2)
	bElig.sent = 40
	bElig.delivered = 0

	bCur := ring.bucketForHour(curHour)
	if penaltyTarget >= 60 {
		// -40 (undelivered) - 30 (rate overlimit 2x) = -70 -> skor 30 (restricted).
		bCur.rateOverlimit = 2
	} else {
		// -40 (undelivered) - 15 (rate overlimit 1x) = -55 -> skor 45 (cautious).
		bCur.rateOverlimit = 1
	}
}

// seedHourlyForTest mengisi bucket jam berjalan di RAM untuk menguji batas kuota harian dan first-contact.
func (g *Guardian) seedHourlyForTest(instanceID string, now time.Time, sent, fc, delivered, replied int) {
	curHour := now.Unix() / 3600
	g.mu.Lock()
	defer g.mu.Unlock()
	b := g.ringLocked(instanceID).bucketForHour(curHour)
	b.sent = sent
	b.firstContactSent = fc
	b.delivered = delivered
	b.replied = replied
}
