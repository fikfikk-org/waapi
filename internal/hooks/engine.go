package hooks

import (
	"bytes"
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/netguard"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/webhook"
	"go.mau.fi/whatsmeow/types"
)

// RuleStore memasok daftar rule hooks tersimpan.
type RuleStore interface {
	ListHooks(ctx context.Context, instanceID string) ([]Rule, error)
	ListHookInstances(ctx context.Context) ([]string, error)
}

// RunStore menyimpan status eksekusi per (instance_id, seq, hook_id) untuk idempotency dan audit.
type RunStore interface {
	GetRun(ctx context.Context, instanceID string, seq int64, hookID string) (RunRecord, bool, error)
	UpsertRun(ctx context.Context, rec RunRecord) error
	IsSeqSuppressed(ctx context.Context, instanceID string, seq int64) (bool, error)
}

// JournalAccess menyediakan pembacaan berurutan, notifikasi, dan cursor konsumen hooks pada journal.
type JournalAccess interface {
	Read(ctx context.Context, q journal.Query) (journal.ReadResult, error)
	Subscribe(instanceID string) (<-chan struct{}, func())
	Cursor(ctx context.Context, consumer, instanceID string, lane int) (int64, error)
	CommitCursor(ctx context.Context, consumer, instanceID string, lane int, seq int64) error
	SetConsumerEnabled(ctx context.Context, consumer, instanceID string, enabled bool) error
}

// OutboxSender mengirim balasan/send/react ke durable outbox dan mengecek kepemilikan wa_id outbox.
type OutboxSender interface {
	Enqueue(ctx context.Context, req outbox.EnqueueRequest) (*outbox.Job, bool, error)
	HasWAID(ctx context.Context, instanceID, waID string) (bool, error)
}

// MarkReader menandai pesan telah dibaca pada WhatsApp.
type MarkReader interface {
	MarkRead(ctx context.Context, instanceID string, chat, sender types.JID, ids []types.MessageID, played bool) error
}

// ConversationReader membaca cuplikan riwayat chat dari ContextStore (opsional).
type ConversationReader interface {
	Messages(ctx context.Context, instanceID, chat string, limit int, before string) (ctxstore.MessagesPage, error)
}

// WebhookTargetReader menyediakan format & secret webhook instance untuk penandatanganan aksi HTTP.
type WebhookTargetReader interface {
	Get(instanceID string) (webhook.Target, bool)
}

// InstanceSettingsReader memasok InstanceSettings serta self JID aktif instance.
type InstanceSettingsReader interface {
	InstanceMeta(ctx context.Context, instanceID string) (store.InstanceSettings, string, error)
}

// EngineDeps menyatukan seluruh dependensi Engine (AppStore & Journal otomatis dipakai jika field adapter nil).
type EngineDeps struct {
	AppStore          *store.Store
	Journal           *journal.Journal
	Rules             RuleStore
	Runs              RunStore
	Jr                JournalAccess // alias opsional bila tidak memakai *journal.Journal
	Outbox            OutboxSender
	WA                MarkReader
	CtxStore          ConversationReader
	Targets           WebhookTargetReader
	Governor          governor.Source
	Settings          InstanceSettingsReader
	HTTPClientFactory func(trusted bool, timeout time.Duration) *http.Client
	Now               func() time.Time
}

// Untuk kenyamanan konstruksi di pengujian synctest yang memakai field bernama `Journal` ber-interface.
// Catatan: EngineDeps.Journal tetap *journal.Journal sedangkan EngineDeps.Jr adalah interface JournalAccess.

type instanceRuleSet struct {
	rules       []*Compiled
	eventTypes  []string
	canSuppress bool
	needMsg     bool
}

type instanceRuntime struct {
	ptr         atomic.Pointer[instanceRuleSet]
	takeoverMu  sync.Mutex
	takeover    *boundedLRU[string, time.Time]
	throttleMu  sync.Mutex
	throttles   map[string]*ruleThrottleCache
	quotaMu     sync.Mutex
	sendStamps  []time.Time

	workerMu     sync.Mutex
	workerCancel context.CancelFunc
	workerDone   chan struct{}
}

func newInstanceRuntime() *instanceRuntime {
	return &instanceRuntime{
		throttles: make(map[string]*ruleThrottleCache),
	}
}

// Engine mengeksekusi rule Reactive Hooks per instance secara berurutan di atas journal v2.
type Engine struct {
	rules    RuleStore
	runs     RunStore
	jr       JournalAccess
	outbox   OutboxSender
	wa       MarkReader
	ctxStore ConversationReader
	targets  WebhookTargetReader
	gov      governor.Source
	settings InstanceSettingsReader
	httpFact func(trusted bool, timeout time.Duration) *http.Client
	now      func() time.Time

	gate *Gate

	mu       sync.Mutex
	baseCtx  context.Context
	started  bool
	runtimes map[string]*instanceRuntime
}

// NewEngine membangun Engine Reactive Hooks beserta Webhook Gate terkait.
func NewEngine(deps EngineDeps) *Engine {
	rules := deps.Rules
	if rules == nil && deps.AppStore != nil {
		rules = sqlRuleStore{st: deps.AppStore}
	}
	runs := deps.Runs
	if runs == nil && deps.Journal != nil {
		runs = NewSQLiteRunStore(deps.Journal.DB())
	}
	jr := deps.Jr
	if jr == nil && deps.Journal != nil {
		jr = deps.Journal
	}
	settings := deps.Settings
	if settings == nil && deps.AppStore != nil {
		settings = sqlSettingsReader{st: deps.AppStore}
	}
	gov := deps.Governor
	if gov == nil {
		gov = governor.AlwaysGreen
	}
	nowFn := deps.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	httpFact := deps.HTTPClientFactory
	if httpFact == nil {
		httpFact = netguard.NewHookClient
	}

	return &Engine{
		rules:    rules,
		runs:     runs,
		jr:       jr,
		outbox:   deps.Outbox,
		wa:       deps.WA,
		ctxStore: deps.CtxStore,
		targets:  deps.Targets,
		gov:      gov,
		settings: settings,
		httpFact: httpFact,
		now:      nowFn,
		gate:     NewGate(runs, nowFn),
		runtimes: make(map[string]*instanceRuntime),
	}
}

// Gate mengembalikan implementasi webhook.Gate untuk disambungkan ke deliverer webhook v2.
func (e *Engine) Gate() *Gate {
	if e == nil {
		return nil
	}
	return e.gate
}

// Start memuat rule awal seluruh instance ber-hook aktif dan menjalankan loop pekerja per instance (0 goroutine jika 0 rule).
func (e *Engine) Start(ctx context.Context) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.started = true
	e.baseCtx = ctx
	e.mu.Unlock()

	if e.rules == nil {
		return
	}
	ids, err := e.rules.ListHookInstances(ctx)
	if err != nil {
		slog.Warn("gagal memuat daftar instance hooks", "err", err)
		return
	}
	for _, id := range ids {
		if err := e.ReloadInstance(ctx, id); err != nil {
			slog.Warn("gagal memuat hooks awal instance", "instance", id, "err", err)
		}
	}
}

// Stop menghentikan seluruh goroutine pekerja hooks per instance dengan menunggu drain bersih.
func (e *Engine) Stop() {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.started = false
	var stops []func()
	for _, rt := range e.runtimes {
		rt.workerMu.Lock()
		cancel := rt.workerCancel
		done := rt.workerDone
		rt.workerCancel = nil
		rt.workerDone = nil
		rt.workerMu.Unlock()
		if cancel != nil {
			cancel()
		}
		if done != nil {
			d := done
			stops = append(stops, func() { <-d })
		}
	}
	e.mu.Unlock()

	for _, wait := range stops {
		wait()
	}
}

// ReloadInstance mengompilasi ulang seluruh rule aktif milik instanceID secara atomik serta menyalakan/mematikan worker-nya.
func (e *Engine) ReloadInstance(ctx context.Context, instanceID string) error {
	if e == nil || e.rules == nil || instanceID == "" {
		return nil
	}
	rawRules, err := e.rules.ListHooks(ctx, instanceID)
	if err != nil {
		return err
	}

	compiled := make([]*Compiled, 0, len(rawRules))
	typeSet := make(map[string]struct{})
	canSuppress := false
	needMsg := false

	for _, r := range rawRules {
		if !r.IsEnabled() {
			continue
		}
		c, cErr := CompileWithOptions(r, CompileOptions{Trusted: r.Trusted})
		if cErr != nil {
			slog.Warn("mengabaikan rule hook rusak saat reload", "instance", instanceID, "hook", r.ID, "err", cErr)
			continue
		}
		compiled = append(compiled, c)
		for tp := range c.OnTypes {
			typeSet[tp] = struct{}{}
		}
		if c.CanSuppress {
			canSuppress = true
		}
		if c.RespectTake {
			needMsg = true
		}
	}

	if needMsg && len(compiled) > 0 {
		typeSet["message"] = struct{}{}
	}

	sort.Slice(compiled, func(i, j int) bool {
		if compiled[i].Priority != compiled[j].Priority {
			return compiled[i].Priority < compiled[j].Priority
		}
		return compiled[i].Rule.ID < compiled[j].Rule.ID
	})

	evTypes := make([]string, 0, len(typeSet))
	for tp := range typeSet {
		evTypes = append(evTypes, tp)
	}
	sort.Strings(evTypes)

	rt := e.runtimeFor(instanceID)
	rt.ptr.Store(&instanceRuleSet{
		rules:       compiled,
		eventTypes:  evTypes,
		canSuppress: canSuppress,
		needMsg:     needMsg,
	})

	var curSeq int64
	if e.jr != nil {
		if seq, cErr := e.jr.Cursor(ctx, ConsumerName, instanceID, 0); cErr == nil {
			curSeq = seq
		}
	}
	e.gate.ConfigureInstance(instanceID, canSuppress && len(compiled) > 0, curSeq)

	e.mu.Lock()
	started := e.started
	baseCtx := e.baseCtx
	e.mu.Unlock()

	if !started {
		return nil
	}

	if len(compiled) == 0 {
		stopWorker(rt)
		if e.jr != nil {
			_ = e.jr.SetConsumerEnabled(ctx, ConsumerName, instanceID, false)
		}
		return nil
	}

	ensureWorkerRunning(e, rt, instanceID, baseCtx)
	return nil
}

func (e *Engine) runtimeFor(instanceID string) *instanceRuntime {
	e.mu.Lock()
	defer e.mu.Unlock()
	rt := e.runtimes[instanceID]
	if rt == nil {
		rt = newInstanceRuntime()
		e.runtimes[instanceID] = rt
	}
	return rt
}

func stopWorker(rt *instanceRuntime) {
	rt.workerMu.Lock()
	cancel := rt.workerCancel
	done := rt.workerDone
	rt.workerCancel = nil
	rt.workerDone = nil
	rt.workerMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func ensureWorkerRunning(e *Engine, rt *instanceRuntime, instanceID string, baseCtx context.Context) {
	rt.workerMu.Lock()
	if rt.workerCancel != nil {
		rt.workerMu.Unlock()
		return
	}
	if baseCtx == nil || baseCtx.Err() != nil {
		rt.workerMu.Unlock()
		return
	}
	wCtx, cancel := context.WithCancel(baseCtx)
	done := make(chan struct{})
	rt.workerCancel = cancel
	rt.workerDone = done
	rt.workerMu.Unlock()

	go func() {
		defer close(done)
		e.runInstanceLoop(wCtx, rt, instanceID)
	}()
}

func (e *Engine) runInstanceLoop(ctx context.Context, rt *instanceRuntime, instanceID string) {
	if e.jr == nil {
		return
	}
	cursor, err := e.jr.Cursor(ctx, ConsumerName, instanceID, 0)
	if err != nil && ctx.Err() == nil {
		slog.Warn("gagal membaca cursor awal hooks", "instance", instanceID, "err", err)
	}
	_ = e.jr.CommitCursor(ctx, ConsumerName, instanceID, 0, cursor)
	e.gate.AdvanceCursor(instanceID, cursor)

	subCh, unsub := e.jr.Subscribe(instanceID)
	defer unsub()

	for {
		if ctx.Err() != nil {
			return
		}
		rs := rt.ptr.Load()
		if rs == nil || len(rs.rules) == 0 {
			return
		}

		res, rErr := e.jr.Read(ctx, journal.Query{
			InstanceID: instanceID,
			AfterSeq:   cursor,
			Types:      rs.eventTypes,
			Limit:      32,
			MaxBytes:   256 << 10,
		})
		if rErr != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("gagal membaca event journal untuk hooks", "instance", instanceID, "err", rErr)
			if !waitSleepCtx(ctx, time.Second) {
				return
			}
			continue
		}

		if len(res.Events) == 0 {
			if res.ScannedTo > cursor {
				cursor = res.ScannedTo
				_ = e.jr.CommitCursor(ctx, ConsumerName, instanceID, 0, cursor)
				e.gate.AdvanceCursor(instanceID, cursor)
			}
			if res.HasMore {
				continue
			}
			timer := time.NewTimer(30 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-subCh:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}

		for idx, ev := range res.Events {
			if ctx.Err() != nil {
				return
			}
			_ = e.processWithRuntime(ctx, rt, instanceID, ev)
			if ev.Seq > cursor {
				cursor = ev.Seq
			}
			if idx == len(res.Events)-1 && res.ScannedTo > cursor {
				cursor = res.ScannedTo
			}
			if cErr := e.jr.CommitCursor(ctx, ConsumerName, instanceID, 0, cursor); cErr != nil && ctx.Err() == nil {
				slog.Warn("gagal commit cursor hooks", "instance", instanceID, "seq", cursor, "err", cErr)
			}
			e.gate.AdvanceCursor(instanceID, cursor)
		}
	}
}

// ProcessOne memproses satu event journal secara sinkron terhadap rule aktif instance (berguna untuk pengujian unit/replay).
func (e *Engine) ProcessOne(ctx context.Context, instanceID string, ev journal.Event) error {
	if e == nil || instanceID == "" {
		return nil
	}
	rt := e.runtimeFor(instanceID)
	err := e.processWithRuntime(ctx, rt, instanceID, ev)
	e.gate.AdvanceCursor(instanceID, ev.Seq)
	return err
}

func (e *Engine) processWithRuntime(ctx context.Context, rt *instanceRuntime, instanceID string, ev journal.Event) error {
	rs := rt.ptr.Load()
	if rs == nil || len(rs.rules) == 0 {
		return nil
	}

	settings, selfJID := e.loadInstanceMeta(ctx, instanceID)
	now := e.now()
	view, err := EventViewFromJournal(ev, selfJID, now)
	if err != nil {
		return nil
	}

	// Catat human takeover jika ada pesan from_me dari perangkat lain yang wa_id-nya tidak terdaftar di outbox.
	if ev.Type == "message" && view.FromMe && !view.Offline && ev.WAID != "" && view.Chat != "" && e.outbox != nil {
		takeoverDur := settings.TakeoverPause.Duration()
		if takeoverDur <= 0 {
			takeoverDur = 30 * time.Minute
		}
		if ev.ReceivedAt == 0 || now.Sub(time.UnixMilli(ev.ReceivedAt)) <= takeoverDur {
			fromOutbox, checkErr := e.outbox.HasWAID(ctx, instanceID, ev.WAID)
			if checkErr == nil && !fromOutbox {
				rt.setTakeoverUntil(view.Chat, now.Add(takeoverDur))
			}
		}
	}

	loc := loadTimezone(settings.Timezone)
	maxAge := settings.HooksMaxAge.Duration()
	if maxAge <= 0 {
		maxAge = 120 * time.Second
	}
	isStale := view.Offline || (ev.ReceivedAt > 0 && now.Sub(time.UnixMilli(ev.ReceivedAt)) > maxAge)

	for _, c := range rs.rules {
		if !c.MatchesEventType(ev.Type) {
			continue
		}
		if view.FromMe && !c.Rule.OnFromMe {
			continue
		}
		if c.ActiveWindow.enabled && !c.ActiveWindow.allows(now.In(loc)) {
			continue
		}

		// Cek status idempotency hook_runs sebelum mengeksekusi guard atau aksi ulang.
		var isReplay bool
		if e.runs != nil {
			prev, found, getErr := e.runs.GetRun(ctx, instanceID, ev.Seq, c.Rule.ID)
			if getErr == nil && found {
				if prev.Status != "matched" {
					if prev.Suppressed {
						e.gate.MarkSuppressed(instanceID, ev.Seq)
					}
					continue
				}
				isReplay = true
			}
		}

		if !isReplay {
			if isStale {
				e.writeRun(ctx, instanceID, ev.Seq, c.Rule.ID, "skipped_stale", "", false, now)
				continue
			}
			if c.RespectTake && view.Chat != "" && rt.isTakenOver(view.Chat, now) {
				e.writeRun(ctx, instanceID, ev.Seq, c.Rule.ID, "skipped_takeover", "", false, now)
				continue
			}
			if !c.Match(view) {
				continue
			}
			if c.Heavy && e.gov != nil && e.gov.Level() == governor.Red {
				e.writeRun(ctx, instanceID, ev.Seq, c.Rule.ID, "skipped_pressure", "", false, now)
				continue
			}
			if c.StaticSends > 0 && !rt.canAffordSendQuota(now, c.StaticSends) {
				e.writeRun(ctx, instanceID, ev.Seq, c.Rule.ID, "throttled", "instance_quota", false, now)
				continue
			}
			if c.Throttle.enabled && !rt.allowThrottle(c.Rule.ID, c.Throttle, view.Chat, now) {
				e.writeRun(ctx, instanceID, ev.Seq, c.Rule.ID, "throttled", "per_chat", false, now)
				continue
			}
			if c.StaticSends > 0 {
				rt.recordSendQuota(now, c.StaticSends)
			}
			e.writeRun(ctx, instanceID, ev.Seq, c.Rule.ID, "matched", "", false, now)
		}

		suppressed, execErr := e.executeRuleActions(ctx, rt, instanceID, c, ev, view, loc, now, isReplay)
		if execErr != nil {
			e.writeRun(ctx, instanceID, ev.Seq, c.Rule.ID, "error", execErr.Error(), suppressed, now)
			// Sesuai kontrak HTTPTimeout: error pada rule tidak menghentikan evaluasi rule berikutnya walau stop=true.
			continue
		}
		e.writeRun(ctx, instanceID, ev.Seq, c.Rule.ID, "done", "", suppressed, now)
		if c.Rule.Stop {
			break
		}
	}
	return nil
}

// DryRun mensimulasikan evaluasi satu rule terhadap event tanpa enqueue outbox, HTTP I/O, mutasi throttle, atau menulis hook_runs.
func (e *Engine) DryRun(ctx context.Context, instanceID string, c *Compiled, ev journal.Event) (DryRunResult, error) {
	if c == nil {
		return DryRunResult{}, errors.New("rule terkompilasi nil")
	}
	settings, selfJID := e.loadInstanceMeta(ctx, instanceID)
	now := e.now()
	if ev.ReceivedAt == 0 {
		ev.ReceivedAt = now.UnixMilli()
	}
	view, err := EventViewFromJournal(ev, selfJID, now)
	if err != nil {
		return DryRunResult{}, err
	}
	if !c.MatchesEventType(ev.Type) {
		return DryRunResult{Matched: false, SkippedReason: "event_type_mismatch", Actions: []DryRunAction{}}, nil
	}
	if view.FromMe && !c.Rule.OnFromMe {
		return DryRunResult{Matched: false, SkippedReason: "from_me", Actions: []DryRunAction{}}, nil
	}

	loc := loadTimezone(settings.Timezone)
	if c.ActiveWindow.enabled && !c.ActiveWindow.allows(now.In(loc)) {
		return DryRunResult{Matched: false, SkippedReason: "outside_active_hours", Actions: []DryRunAction{}}, nil
	}
	maxAge := settings.HooksMaxAge.Duration()
	if maxAge <= 0 {
		maxAge = 120 * time.Second
	}
	if view.Offline || now.Sub(time.UnixMilli(ev.ReceivedAt)) > maxAge {
		return DryRunResult{Matched: false, SkippedReason: "skipped_stale", Actions: []DryRunAction{}}, nil
	}
	if rt, ok := e.getRuntimeIfPresent(instanceID); ok {
		if c.RespectTake && view.Chat != "" && rt.isTakenOver(view.Chat, now) {
			return DryRunResult{Matched: false, SkippedReason: "skipped_takeover", Actions: []DryRunAction{}}, nil
		}
	}
	if !c.Match(view) {
		return DryRunResult{Matched: false, Actions: []DryRunAction{}}, nil
	}
	if c.Heavy && e.gov != nil && e.gov.Level() == governor.Red {
		return DryRunResult{Matched: true, SkippedReason: "skipped_pressure", Actions: []DryRunAction{}}, nil
	}

	actions := make([]DryRunAction, 0, len(c.Actions))
	for _, ca := range c.Actions {
		a := ca.raw
		da := DryRunAction{Action: a.Action}
		switch a.Action {
		case "reply":
			da.Text = RenderTemplate(a.Text, view, loc, now)
			da.To = view.Chat
		case "send":
			da.To = RenderTemplate(a.To, view, loc, now)
			da.Text = RenderTemplate(a.Text, view, loc, now)
		case "react":
			da.Emoji = a.Emoji
			da.To = view.Chat
		case "http":
			da.URL = a.URL
		}
		actions = append(actions, da)
	}
	return DryRunResult{Matched: true, Actions: actions}, nil
}

func (e *Engine) getRuntimeIfPresent(instanceID string) (*instanceRuntime, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rt, ok := e.runtimes[instanceID]
	return rt, ok
}

func (e *Engine) loadInstanceMeta(ctx context.Context, instanceID string) (store.InstanceSettings, string) {
	if e.settings != nil {
		s, sj, err := e.settings.InstanceMeta(ctx, instanceID)
		if err == nil {
			return s, sj
		}
	}
	return store.Defaults(), ""
}

func (e *Engine) writeRun(ctx context.Context, instanceID string, seq int64, hookID, status, detail string, suppressed bool, now time.Time) {
	if e.runs == nil || seq <= 0 {
		return
	}
	if len(detail) > 512 {
		detail = detail[:512]
	}
	_ = e.runs.UpsertRun(ctx, RunRecord{
		InstanceID: instanceID,
		Seq:        seq,
		HookID:     hookID,
		Status:     status,
		Detail:     detail,
		Suppressed: suppressed,
		At:         now.UnixMilli(),
	})
}

func (e *Engine) executeRuleActions(
	ctx context.Context,
	rt *instanceRuntime,
	instanceID string,
	c *Compiled,
	ev journal.Event,
	view EventView,
	loc *time.Location,
	now time.Time,
	isReplay bool,
) (suppressed bool, err error) {
	for idx, ca := range c.Actions {
		a := ca.raw
		idemBase := fmt.Sprintf("hook:%s:%d:%d", c.Rule.ID, ev.Seq, idx)
		origin := outbox.Origin{Kind: "hook", KeyID: c.Rule.ID}

		switch a.Action {
		case "reply":
			if view.Chat == "" {
				return suppressed, errors.New("event tidak memiliki target chat")
			}
			if e.outbox == nil {
				return suppressed, errors.New("service outbox belum siap")
			}
			spec := outbox.Spec{
				Type:   "text",
				To:     view.Chat,
				Text:   RenderTemplate(a.Text, view, loc, now),
				Typing: a.Typing,
			}
			if a.Quote && view.WAID != "" {
				spec.ReplyTo = &outbox.ReplyRef{ID: view.WAID, Sender: view.ReplySender}
			}
			req := outbox.EnqueueRequest{
				InstanceID: instanceID,
				IdemKey:    idemBase,
				Spec:       spec,
				Origin:     origin,
			}
			if _, _, err := e.outbox.Enqueue(ctx, req); err != nil {
				return suppressed, err
			}

		case "send":
			if e.outbox == nil {
				return suppressed, errors.New("service outbox belum siap")
			}
			toStr := strings.TrimSpace(RenderTemplate(a.To, view, loc, now))
			if toStr == "" {
				return suppressed, errors.New("target send hasil template kosong")
			}
			if ca.toHasTmpl {
				if _, jErr := jid.ParseRecipient(toStr); jErr != nil {
					return suppressed, fmt.Errorf("target send hasil template tidak valid: %w", jErr)
				}
			}
			if _, _, err := e.outbox.Enqueue(ctx, outbox.EnqueueRequest{
				InstanceID: instanceID,
				IdemKey:    idemBase,
				Spec: outbox.Spec{
					Type: "text",
					To:   toStr,
					Text: RenderTemplate(a.Text, view, loc, now),
				},
				Origin: origin,
			}); err != nil {
				return suppressed, err
			}

		case "react":
			if view.Chat == "" || view.WAID == "" {
				return suppressed, errors.New("event tidak memiliki target pesan untuk reaksi")
			}
			if e.outbox == nil {
				return suppressed, errors.New("service outbox belum siap")
			}
			if _, _, err := e.outbox.Enqueue(ctx, outbox.EnqueueRequest{
				InstanceID: instanceID,
				IdemKey:    idemBase,
				Spec: outbox.Spec{
					Type: "reaction",
					To:   view.Chat,
					Reaction: &outbox.Reaction{
						MessageID: view.WAID,
						Sender:    view.ReplySender,
						Emoji:     a.Emoji,
					},
				},
				Origin: origin,
			}); err != nil {
				return suppressed, err
			}

		case "mark_read":
			if e.wa != nil && !view.FromMe && view.WAID != "" && view.Chat != "" {
				chatJID, cErr := types.ParseJID(view.Chat)
				if cErr == nil {
					var senderJID types.JID
					if view.Sender != "" {
						senderJID, _ = types.ParseJID(view.Sender)
					}
					if err := e.wa.MarkRead(ctx, instanceID, chatJID, senderJID, []types.MessageID{types.MessageID(view.WAID)}, false); err != nil {
						return suppressed, err
					}
				}
			}

		case "suppress_webhook":
			suppressed = true
			e.gate.MarkSuppressed(instanceID, ev.Seq)

		case "http":
			httpSup, hErr := e.executeHTTPAction(ctx, rt, instanceID, c, ca, ev, view, idemBase, origin, now, isReplay)
			if httpSup {
				suppressed = true
				e.gate.MarkSuppressed(instanceID, ev.Seq)
			}
			if hErr != nil {
				return suppressed, hErr
			}
		}
	}
	return suppressed, nil
}

func (e *Engine) executeHTTPAction(
	ctx context.Context,
	rt *instanceRuntime,
	instanceID string,
	c *Compiled,
	ca compiledAction,
	ev journal.Event,
	view EventView,
	idemBase string,
	origin outbox.Origin,
	now time.Time,
	isReplay bool,
) (suppressed bool, err error) {
	a := ca.raw
	envRaw, err := ev.MarshalEnvelope()
	if err != nil {
		return false, err
	}

	type httpPayload struct {
		V            int                    `json:"v"`
		Instance     string                 `json:"instance"`
		HookID       string                 `json:"hook_id"`
		Seq          int64                  `json:"seq"`
		Event        json.RawMessage        `json:"event"`
		Conversation []ctxstore.MessageItem `json:"conversation,omitempty"`
	}

	reqPayload := httpPayload{
		V:        2,
		Instance: instanceID,
		HookID:   c.Rule.ID,
		Seq:      ev.Seq,
		Event:    json.RawMessage(envRaw),
	}
	includeConv := a.Include == "conversation"
	if includeConv {
		reqPayload.Conversation = []ctxstore.MessageItem{}
		if e.ctxStore != nil && view.Chat != "" {
			if page, mErr := e.ctxStore.Messages(ctx, instanceID, view.Chat, DefaultConversationMsgs, ""); mErr == nil && len(page.Messages) > 0 {
				reqPayload.Conversation = page.Messages
			}
		}
	}

	body, err := json.Marshal(reqPayload)
	if err != nil {
		return false, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, ca.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, a.URL, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "waapi-hook/2.0")
	req.Header.Set("X-WA-Event", ev.Type)
	req.Header.Set("X-WA-Instance", instanceID)
	req.Header.Set("X-WA-Hook-Run", fmt.Sprintf("%d:%s", ev.Seq, c.Rule.ID))

	if e.targets != nil {
		if target, ok := e.targets.Get(instanceID); ok {
			ts := now.Unix()
			if strings.EqualFold(target.Format, "standard") {
				whID := fmt.Sprintf("%s_hook_%d_%s", instanceID, ev.Seq, c.Rule.ID)
				req.Header.Set("webhook-id", whID)
				req.Header.Set("webhook-timestamp", strconv.FormatInt(ts, 10))
				if target.Secret != "" {
					if sig, sErr := webhook.SignStandard(target.Secret, whID, ts, body); sErr == nil {
						req.Header.Set("webhook-signature", sig)
					}
				}
			} else {
				req.Header.Set("X-WA-Timestamp", strconv.FormatInt(ts, 10))
				if target.Secret != "" {
					req.Header.Set("X-WA-Signature", webhook.Sign(target.Secret, ts, body))
				}
			}
		}
	}

	client := e.httpFact(c.Rule.Trusted, ca.timeout)
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, MaxHTTPRespBytes))
	if err != nil {
		return false, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf("status HTTP hook %d", resp.StatusCode)
	}
	if !a.ExpectReply || len(bytes.TrimSpace(respBytes)) == 0 {
		return false, nil
	}

	var replyObj struct {
		Reply           string `json:"reply"`
		React           string `json:"react"`
		SuppressWebhook bool   `json:"suppress_webhook"`
	}
	if err := json.Unmarshal(respBytes, &replyObj); err != nil {
		return false, fmt.Errorf("parse JSON balasan HTTP hook: %w", err)
	}

	if replyObj.SuppressWebhook {
		suppressed = true
	}

	if strings.TrimSpace(replyObj.Reply) != "" && e.outbox != nil && view.Chat != "" {
		if !isReplay && !rt.tryConsumeSendQuota(now, 1) {
			return suppressed, errors.New("kuota kirim hooks per menit terlampaui saat membalas HTTP")
		}
		if _, _, err := e.outbox.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: instanceID,
			IdemKey:    idemBase + ":reply",
			Spec: outbox.Spec{
				Type: "text",
				To:   view.Chat,
				Text: replyObj.Reply,
			},
			Origin: origin,
		}); err != nil {
			return suppressed, err
		}
	}
	if strings.TrimSpace(replyObj.React) != "" && e.outbox != nil && view.Chat != "" && view.WAID != "" {
		if !isReplay && !rt.tryConsumeSendQuota(now, 1) {
			return suppressed, errors.New("kuota kirim hooks per menit terlampaui saat reaksi HTTP")
		}
		if _, _, err := e.outbox.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: instanceID,
			IdemKey:    idemBase + ":react",
			Spec: outbox.Spec{
				Type: "reaction",
				To:   view.Chat,
				Reaction: &outbox.Reaction{
					MessageID: view.WAID,
					Sender:    view.ReplySender,
					Emoji:     replyObj.React,
				},
			},
			Origin: origin,
		}); err != nil {
			return suppressed, err
		}
	}
	return suppressed, nil
}

// --- Guard helpers (Takeover LRU, Throttle LRU, dan Quota 30/menit per instance) ---

func (rt *instanceRuntime) setTakeoverUntil(chat string, until time.Time) {
	rt.takeoverMu.Lock()
	if rt.takeover == nil {
		rt.takeover = newBoundedLRU[string, time.Time](TakeoverLRUCap)
	}
	rt.takeover.Set(chat, until)
	rt.takeoverMu.Unlock()
}

func (rt *instanceRuntime) isTakenOver(chat string, now time.Time) bool {
	rt.takeoverMu.Lock()
	defer rt.takeoverMu.Unlock()
	if rt.takeover == nil {
		return false
	}
	until, ok := rt.takeover.Get(chat)
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}
	rt.takeover.Delete(chat)
	return false
}

type ruleBucket struct {
	tokens float64
	last   time.Time
}

type ruleThrottleCache struct {
	spec string
	lru  *boundedLRU[string, *ruleBucket]
}

func (rt *instanceRuntime) allowThrottle(ruleID string, cfg compiledThrottle, chat string, now time.Time) bool {
	if !cfg.enabled || chat == "" {
		return true
	}
	rt.throttleMu.Lock()
	defer rt.throttleMu.Unlock()

	cache := rt.throttles[ruleID]
	if cache == nil || cache.spec != cfg.spec {
		cache = &ruleThrottleCache{
			spec: cfg.spec,
			lru:  newBoundedLRU[string, *ruleBucket](ThrottleLRUCap),
		}
		rt.throttles[ruleID] = cache
	}

	b, ok := cache.lru.Get(chat)
	if !ok {
		b = &ruleBucket{
			tokens: float64(cfg.capacity),
			last:   now,
		}
		cache.lru.Set(chat, b)
	} else {
		elapsed := now.Sub(b.last)
		if elapsed > 0 {
			ratePerSec := float64(cfg.capacity) / cfg.window.Seconds()
			b.tokens += elapsed.Seconds() * ratePerSec
			if b.tokens > float64(cfg.capacity) {
				b.tokens = float64(cfg.capacity)
			}
			b.last = now
		}
	}

	if b.tokens < 1.0 {
		return false
	}
	b.tokens -= 1.0
	return true
}

func (rt *instanceRuntime) canAffordSendQuota(now time.Time, need int) bool {
	rt.quotaMu.Lock()
	defer rt.quotaMu.Unlock()
	rt.pruneQuotaLocked(now)
	return len(rt.sendStamps)+need <= MaxHooksSendsPerMin
}

func (rt *instanceRuntime) recordSendQuota(now time.Time, count int) {
	rt.quotaMu.Lock()
	defer rt.quotaMu.Unlock()
	rt.pruneQuotaLocked(now)
	for i := 0; i < count; i++ {
		rt.sendStamps = append(rt.sendStamps, now)
	}
}

func (rt *instanceRuntime) tryConsumeSendQuota(now time.Time, count int) bool {
	rt.quotaMu.Lock()
	defer rt.quotaMu.Unlock()
	rt.pruneQuotaLocked(now)
	if len(rt.sendStamps)+count > MaxHooksSendsPerMin {
		return false
	}
	for i := 0; i < count; i++ {
		rt.sendStamps = append(rt.sendStamps, now)
	}
	return true
}

func (rt *instanceRuntime) pruneQuotaLocked(now time.Time) {
	cutoff := now.Add(-time.Minute)
	keep := 0
	for _, ts := range rt.sendStamps {
		if ts.After(cutoff) {
			rt.sendStamps[keep] = ts
			keep++
		}
	}
	rt.sendStamps = rt.sendStamps[:keep]
}

// boundedLRU menyimpan pasangan key->value dengan batas kapasitas tetap dan eviksi LRU.
type boundedLRU[K comparable, V any] struct {
	cap   int
	ll    *list.List
	items map[K]*list.Element
}

type lruEntry[K comparable, V any] struct {
	key K
	val V
}

func newBoundedLRU[K comparable, V any](capacity int) *boundedLRU[K, V] {
	return &boundedLRU[K, V]{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[K]*list.Element),
	}
}

func (l *boundedLRU[K, V]) Get(k K) (V, bool) {
	el, ok := l.items[k]
	if !ok {
		var zero V
		return zero, false
	}
	l.ll.MoveToFront(el)
	return el.Value.(*lruEntry[K, V]).val, true
}

func (l *boundedLRU[K, V]) Set(k K, v V) {
	if el, ok := l.items[k]; ok {
		el.Value.(*lruEntry[K, V]).val = v
		l.ll.MoveToFront(el)
		return
	}
	el := l.ll.PushFront(&lruEntry[K, V]{key: k, val: v})
	l.items[k] = el
	for l.ll.Len() > l.cap {
		back := l.ll.Back()
		if back == nil {
			break
		}
		l.ll.Remove(back)
		delete(l.items, back.Value.(*lruEntry[K, V]).key)
	}
}

func (l *boundedLRU[K, V]) Delete(k K) {
	if el, ok := l.items[k]; ok {
		l.ll.Remove(el)
		delete(l.items, k)
	}
}

// --- Adapter SQLite bawaan untuk produksi & integrasi ---

type sqlRuleStore struct {
	st *store.Store
}

func (s sqlRuleStore) ListHooks(ctx context.Context, instanceID string) ([]Rule, error) {
	rows, err := s.st.ListHooks(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	out := make([]Rule, 0, len(rows))
	for _, rec := range rows {
		r, pErr := ParseRuleJSON([]byte(rec.Spec))
		if pErr != nil {
			continue
		}
		r.ID = rec.ID
		r.Name = rec.Name
		en := rec.Enabled
		pr := rec.Priority
		r.Enabled = &en
		r.Priority = &pr
		r.Trusted = rec.Trusted
		out = append(out, r)
	}
	return out, nil
}

func (s sqlRuleStore) ListHookInstances(ctx context.Context) ([]string, error) {
	return s.st.ListHookInstances(ctx)
}

type sqlSettingsReader struct {
	st *store.Store
}

func (s sqlSettingsReader) InstanceMeta(ctx context.Context, instanceID string) (store.InstanceSettings, string, error) {
	inst, err := s.st.GetInstance(ctx, instanceID)
	if err != nil {
		return store.Defaults(), "", err
	}
	return inst.Settings, inst.JID, nil
}

// SQLiteRunStore menyimpan jejak hook_runs di tabel hook_runs pada journal.db.
type SQLiteRunStore struct {
	db *sql.DB
}

// NewSQLiteRunStore membungkus handle *sql.DB milik journal.db untuk persistensi hook_runs.
func NewSQLiteRunStore(db *sql.DB) *SQLiteRunStore {
	return &SQLiteRunStore{db: db}
}

// GetRun membaca satu baris status eksekusi hook.
func (s *SQLiteRunStore) GetRun(ctx context.Context, instanceID string, seq int64, hookID string) (RunRecord, bool, error) {
	if s == nil || s.db == nil {
		return RunRecord{}, false, nil
	}
	var (
		rec RunRecord
		sup int
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT instance_id, seq, hook_id, status, detail, suppressed, at
		FROM hook_runs
		WHERE instance_id = ? AND seq = ? AND hook_id = ?`,
		instanceID, seq, hookID,
	).Scan(&rec.InstanceID, &rec.Seq, &rec.HookID, &rec.Status, &rec.Detail, &sup, &rec.At)
	if errors.Is(err, sql.ErrNoRows) {
		return RunRecord{}, false, nil
	}
	if err != nil {
		return RunRecord{}, false, fmt.Errorf("baca hook_runs: %w", err)
	}
	rec.Suppressed = sup == 1
	return rec, true, nil
}

// UpsertRun menulis atau memperbarui status eksekusi hook secara idempoten.
func (s *SQLiteRunStore) UpsertRun(ctx context.Context, rec RunRecord) error {
	if s == nil || s.db == nil {
		return nil
	}
	sup := 0
	if rec.Suppressed {
		sup = 1
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO hook_runs (instance_id, seq, hook_id, status, detail, suppressed, at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id, seq, hook_id) DO UPDATE SET
			status = excluded.status,
			detail = excluded.detail,
			suppressed = MAX(hook_runs.suppressed, excluded.suppressed),
			at = excluded.at`,
		rec.InstanceID, rec.Seq, rec.HookID, rec.Status, rec.Detail, sup, rec.At,
	)
	if err != nil {
		return fmt.Errorf("simpan hook_runs: %w", err)
	}
	return nil
}

// IsSeqSuppressed memeriksa apakah event seq pada instance memiliki penanda suppressed dari rule hooks mana pun.
func (s *SQLiteRunStore) IsSeqSuppressed(ctx context.Context, instanceID string, seq int64) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	var exists int
	if err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM hook_runs WHERE instance_id = ? AND seq = ? AND suppressed = 1)`,
		instanceID, seq,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("cek suppress hook_runs: %w", err)
	}
	return exists == 1, nil
}

// ListRuns mengambil riwayat eksekusi hooks milik suatu instance dengan filter hookID dan cutoff waktu.
func (s *SQLiteRunStore) ListRuns(ctx context.Context, instanceID, hookID string, since time.Time, limit int) ([]RunRecord, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	args := []any{instanceID}
	q := `SELECT instance_id, seq, hook_id, status, detail, suppressed, at FROM hook_runs WHERE instance_id = ?`
	if strings.TrimSpace(hookID) != "" {
		q += ` AND hook_id = ?`
		args = append(args, hookID)
	}
	if !since.IsZero() {
		q += ` AND at >= ?`
		args = append(args, since.UnixMilli())
	}
	q += ` ORDER BY at DESC, seq DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("daftar hook_runs: %w", err)
	}
	defer rows.Close()

	var out []RunRecord
	for rows.Next() {
		var (
			rec RunRecord
			sup int
		)
		if err := rows.Scan(&rec.InstanceID, &rec.Seq, &rec.HookID, &rec.Status, &rec.Detail, &sup, &rec.At); err != nil {
			return nil, fmt.Errorf("scan hook_runs: %w", err)
		}
		rec.Suppressed = sup == 1
		out = append(out, rec)
	}
	return out, rows.Err()
}

func loadTimezone(tz string) *time.Location {
	if strings.TrimSpace(tz) != "" {
		if loc, err := time.LoadLocation(tz); err == nil {
			return loc
		}
	}
	return time.UTC
}

func waitSleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
