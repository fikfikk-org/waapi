package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

// Gate memblokir pengiriman webhook untuk suatu event sampai diperbolehkan (mis. grace window di batch berikutnya).
type Gate interface {
	Wait(ctx context.Context, instanceID string, seq int64) (deliver bool, err error)
}

// Options mengatur dependensi tambahan dan tuning deliverer webhook v2.
type Options struct {
	Store          *store.Store
	CommitEvery    int
	CommitInterval time.Duration
	AttemptTimeout time.Duration
	IdleWait       time.Duration
	OnPause        func(ctx context.Context, instanceID, reason string) error
}

// Abstraksi internal agar logika waktu deliverer dapat diuji di dalam synctest tanpa SQLite.
type journalReader interface {
	Read(ctx context.Context, q journal.Query) (journal.ReadResult, error)
	Subscribe(instanceID string) (<-chan struct{}, func())
	Cursor(ctx context.Context, consumer, instanceID string, lane int) (int64, error)
	CommitCursor(ctx context.Context, consumer, instanceID string, lane int, seq int64) error
	SetConsumerEnabled(ctx context.Context, consumer, instanceID string, enabled bool) error
	EnsureConsumerCursors(ctx context.Context, consumer, instanceID string, lanes int) error
	ReconfigureConsumerLanes(ctx context.Context, consumer, instanceID string, lanes int) error
}

type deadRecorder interface {
	Record(ctx context.Context, entry DeadEntry) error
}

type targetPauser interface {
	PauseWebhook(ctx context.Context, instanceID, reason string) error
}

type realJournalAdapter struct {
	j *journal.Journal
}

func (a realJournalAdapter) Read(ctx context.Context, q journal.Query) (journal.ReadResult, error) {
	return a.j.Read(ctx, q)
}

func (a realJournalAdapter) Subscribe(instanceID string) (<-chan struct{}, func()) {
	return a.j.Subscribe(instanceID)
}

func (a realJournalAdapter) Cursor(ctx context.Context, consumer, instanceID string, lane int) (int64, error) {
	return a.j.Cursor(ctx, consumer, instanceID, lane)
}

func (a realJournalAdapter) CommitCursor(ctx context.Context, consumer, instanceID string, lane int, seq int64) error {
	return a.j.CommitCursor(ctx, consumer, instanceID, lane, seq)
}

func (a realJournalAdapter) SetConsumerEnabled(ctx context.Context, consumer, instanceID string, enabled bool) error {
	return a.j.SetConsumerEnabled(ctx, consumer, instanceID, enabled)
}

func (a realJournalAdapter) EnsureConsumerCursors(ctx context.Context, consumer, instanceID string, lanes int) error {
	return a.j.EnsureConsumerCursors(ctx, consumer, instanceID, lanes)
}

func (a realJournalAdapter) ReconfigureConsumerLanes(ctx context.Context, consumer, instanceID string, lanes int) error {
	minSeq, _, err := a.j.ConsumerCursorMin(ctx, consumer, instanceID)
	if err != nil {
		return err
	}
	return a.j.ResetConsumerCursors(ctx, consumer, instanceID, lanes, minSeq, true)
}

type storePauser struct {
	st *store.Store
}

func (p storePauser) PauseWebhook(ctx context.Context, instanceID, reason string) error {
	if p.st == nil {
		return nil
	}
	inst, err := p.st.GetInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	next := inst.Settings
	if next.WebhookPaused == reason {
		return nil
	}
	next.WebhookPaused = reason
	_, err = p.st.UpdateInstance(ctx, instanceID, store.InstanceUpdate{Settings: &next})
	return err
}

// Service mengoordinasikan deliverer webhook berbasis cursor journal per instance dan per lane.
type Service struct {
	journal *journal.Journal
	jr      journalReader
	targets *TargetCache
	dead    *DeadStore
	dr      deadRecorder
	pauser  targetPauser
	client  *http.Client
	gate    Gate
	opt     Options

	mu         sync.Mutex
	started    bool
	baseCtx    context.Context
	deliverers map[string]*laneDeliverer
}

// NewService membuat Service pengiriman webhook v2 yang membaca batch terikat dari journal.db.
func NewService(j *journal.Journal, targets *TargetCache, dead *DeadStore, client *http.Client, gate Gate, opt Options) *Service {
	var jr journalReader
	if j != nil {
		jr = realJournalAdapter{j: j}
	}
	var dr deadRecorder
	if dead != nil {
		dr = dead
	}
	var pauser targetPauser
	if opt.Store != nil {
		pauser = storePauser{st: opt.Store}
	}
	svc := newServiceWithDeps(j, jr, targets, dead, dr, pauser, client, gate, opt)
	return svc
}

func newServiceForTest(jr journalReader, targets *TargetCache, dr deadRecorder, pauser targetPauser, client *http.Client, gate Gate, opt Options) *Service {
	return newServiceWithDeps(nil, jr, targets, nil, dr, pauser, client, gate, opt)
}

func newServiceWithDeps(
	j *journal.Journal,
	jr journalReader,
	targets *TargetCache,
	dead *DeadStore,
	dr deadRecorder,
	pauser targetPauser,
	client *http.Client,
	gate Gate,
	opt Options,
) *Service {
	if client == nil {
		client = defaultWebhookHTTPClient()
	}
	return &Service{
		journal:    j,
		jr:         jr,
		targets:    targets,
		dead:       dead,
		dr:         dr,
		pauser:     pauser,
		client:     client,
		gate:       gate,
		opt:        opt,
		deliverers: make(map[string]*laneDeliverer),
	}
}

func defaultWebhookHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        32,
			MaxIdleConnsPerHost: 8,
			IdleConnTimeout:     30 * time.Second,
		},
	}
}

// Start menyalakan deliverer lane untuk seluruh instance yang memiliki webhook aktif (tidak di-pause).
func (s *Service) Start(ctx context.Context, instanceIDs []string) {
	if s == nil || s.jr == nil {
		return
	}
	s.mu.Lock()
	s.started = true
	s.baseCtx = ctx
	s.mu.Unlock()

	for _, id := range instanceIDs {
		s.OnTargetChanged(id)
	}
}

// OnTargetChanged mengevaluasi ulang konfigurasi webhook instance dan me-restart deliverer bila berubah.
func (s *Service) OnTargetChanged(instanceID string) {
	if s == nil || s.jr == nil || instanceID == "" {
		return
	}

	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	baseCtx := s.baseCtx
	old := s.deliverers[instanceID]
	target, ok := s.resolveTarget(baseCtx, instanceID)
	if old != nil && old.loopCtx.Err() == nil && ok && strings.TrimSpace(target.URL) != "" && target.Paused == "" && targetsEqual(old.target, target) {
		s.mu.Unlock()
		return
	}
	if old != nil {
		delete(s.deliverers, instanceID)
		old.stopLoop()
	}
	s.mu.Unlock()

	if old != nil {
		drainDeliverer(old)
	}

	opCtx, cancel := context.WithTimeout(context.Background(), commitFlushTimeout)
	defer cancel()

	if !ok || strings.TrimSpace(target.URL) == "" {
		_ = s.jr.SetConsumerEnabled(opCtx, webhookConsumerName, instanceID, false)
		return
	}
	if target.Paused != "" {
		// Tetap simpan posisi cursor dan status enabled saat paused (410) agar pruner tidak membuang event.
		return
	}

	// Jika jumlah lane berubah di tengah aliran, reset semua lane baru ke min(cursor lama).
	needReconfig := false
	if old != nil && old.target.Lanes != target.Lanes {
		needReconfig = true
	} else if s.journal != nil {
		if _, curLanes, err := s.journal.ConsumerCursorMin(opCtx, webhookConsumerName, instanceID); err == nil && curLanes > 0 && curLanes != target.Lanes {
			needReconfig = true
		}
	}
	if needReconfig {
		if err := s.jr.ReconfigureConsumerLanes(opCtx, webhookConsumerName, instanceID, target.Lanes); err != nil {
			slog.Warn("gagal mengatur ulang lane webhook", "instance", instanceID, "lanes", target.Lanes, "err", err)
		}
	} else if err := s.jr.EnsureConsumerCursors(opCtx, webhookConsumerName, instanceID, target.Lanes); err != nil {
		slog.Warn("gagal inisialisasi cursor awal webhook", "instance", instanceID, "lanes", target.Lanes, "err", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started || s.baseCtx.Err() != nil {
		return
	}
	// Pastikan tidak tertimpa jika ada OnTargetChanged konkuren saat old sedang berhenti.
	if _, exists := s.deliverers[instanceID]; exists {
		return
	}
	d := newLaneDeliverer(s, s.baseCtx, instanceID, target)
	s.deliverers[instanceID] = d
	d.start()
}

// Stop menghentikan seluruh deliverer dengan menunggu request in-flight <= 10 detik dan mem-flush cursor (R3).
func (s *Service) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.started = false
	list := make([]*laneDeliverer, 0, len(s.deliverers))
	for id, d := range s.deliverers {
		list = append(list, d)
		delete(s.deliverers, id)
		d.stopLoop()
	}
	s.mu.Unlock()

	if len(list) == 0 {
		return
	}
	done := make(chan struct{})
	go func() {
		for _, d := range list {
			d.wait()
		}
		close(done)
	}()

	timer := time.NewTimer(stopDrainTimeout)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		for _, d := range list {
			d.hardCancel()
		}
		<-done
	}
}

// SendTest mengirim satu event percobaan secara sinkron untuk perintah CLI `waapi webhook test`.
func (s *Service) SendTest(ctx context.Context, instanceID string) (int, error) {
	if s == nil {
		return 0, errors.New("service webhook belum siap")
	}
	target, ok := s.resolveTarget(ctx, instanceID)
	if !ok || strings.TrimSpace(target.URL) == "" {
		return 0, fmt.Errorf("webhook_url belum diset pada instance %s", instanceID)
	}
	now := time.Now()
	body, err := json.Marshal(Envelope{
		Event:     "test",
		Instance:  instanceID,
		Timestamp: now.Unix(),
		Data:      map[string]any{"message": "webhook test dari waapi"},
	})
	if err != nil {
		return 0, err
	}
	timeout := s.opt.AttemptTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	status, _, _, sendErr := sendWebhookHTTP(reqCtx, s.client, target, instanceID, "test", 0, 1, now, body)
	return status, sendErr
}

// Replay mengirim ulang daftar seq dari webhook_dead ke target terkini dan menghapusnya dari DLQ saat 2xx.
func (s *Service) Replay(ctx context.Context, instanceID string, seqs []int64) (ReplayResult, error) {
	if s == nil || s.journal == nil || s.dead == nil {
		return ReplayResult{}, errors.New("journal atau dead store belum aktif")
	}
	target, ok := s.resolveTarget(ctx, instanceID)
	if !ok || strings.TrimSpace(target.URL) == "" {
		return ReplayResult{}, fmt.Errorf("webhook_url belum diset pada instance %s", instanceID)
	}
	timeout := s.opt.AttemptTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	var res ReplayResult
	for _, seq := range seqs {
		ev, err := s.journal.Get(ctx, instanceID, seq)
		if err != nil {
			if errors.Is(err, journal.ErrNotFound) {
				_ = s.dead.Delete(ctx, instanceID, seq)
				res.Failed++
				continue
			}
			return res, err
		}
		body, err := ev.MarshalEnvelope()
		if err != nil {
			res.Failed++
			continue
		}

		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		status, _, _, sendErr := sendWebhookHTTP(reqCtx, s.client, target, instanceID, ev.Type, ev.Seq, 1, time.Now(), body)
		cancel()

		if ClassifyResponse(status, sendErr) == ClassSuccess {
			if err := s.dead.Delete(ctx, instanceID, seq); err != nil {
				return res, err
			}
			res.Replayed++
			res.Seqs = append(res.Seqs, seq)
		} else {
			res.Failed++
		}
	}
	return res, nil
}

func (s *Service) resolveTarget(ctx context.Context, instanceID string) (Target, bool) {
	if s.targets != nil {
		if t, ok := s.targets.Get(instanceID); ok {
			return normalizeTarget(t), true
		}
	}
	if s.opt.Store != nil {
		inst, err := s.opt.Store.GetInstance(ctx, instanceID)
		if err == nil && inst != nil {
			t := normalizeTarget(Target{
				URL:         inst.WebhookURL,
				Secret:      inst.WebhookSecret,
				EventsCSV:   inst.WebhookEvents,
				Format:      inst.Settings.WebhookFormat,
				Lanes:       inst.Settings.WebhookLanes,
				MaxAttempts: inst.Settings.WebhookMaxAttempts,
				Paused:      inst.Settings.WebhookPaused,
			})
			if s.targets != nil {
				s.targets.Set(instanceID, t)
			}
			return t, true
		}
	}
	return Target{}, false
}

func (s *Service) pauseTarget(ctx context.Context, instanceID, reason string) {
	if s.targets != nil {
		s.targets.SetPaused(instanceID, reason)
	}
	if s.opt.OnPause != nil {
		if err := s.opt.OnPause(ctx, instanceID, reason); err != nil {
			slog.Warn("callback pause webhook gagal", "instance", instanceID, "err", err)
		}
	}
	if s.pauser != nil {
		if err := s.pauser.PauseWebhook(ctx, instanceID, reason); err != nil {
			slog.Warn("gagal menyimpan status webhook_paused ke app.db", "instance", instanceID, "err", err)
		}
	}
}

func drainDeliverer(d *laneDeliverer) {
	done := make(chan struct{})
	go func() {
		d.wait()
		close(done)
	}()
	timer := time.NewTimer(stopDrainTimeout)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		d.hardCancel()
		<-done
	}
}

func targetsEqual(a, b Target) bool {
	return a.URL == b.URL &&
		a.Secret == b.Secret &&
		a.EventsCSV == b.EventsCSV &&
		a.Format == b.Format &&
		a.Lanes == b.Lanes &&
		a.MaxAttempts == b.MaxAttempts &&
		a.Paused == b.Paused
}
