package outbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/idgen"
	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/journal"
)

const (
	defaultOutboxTTL     = 24 * time.Hour
	defaultMaxQueued     = 1000
	defaultLeaseDuration = 2 * time.Minute
	defaultExecTimeout   = 90 * time.Second
	defaultRecoverEvery  = 60 * time.Second
	maxIdleSleep         = 30 * time.Second
	notReadySleep        = 5 * time.Second
)

// Options mengatur parameter runtime service outbox.
type Options struct {
	DefaultTTL         time.Duration
	MaxQueued          int
	LeaseDuration      time.Duration
	ExecTimeout        time.Duration
	RecoverInterval    time.Duration
	OnActivity         func(instanceID string, at time.Time) // dipanggil saat pesan baru diterima ke antrian outbox
	DisableSendJitter  bool // khusus pengujian synctest agar jeda 300-1500 ms tidak menggeser jadwal deterministik
	DisableRetryJitter bool // khusus pengujian synctest agar backoff retry deterministik
}

func (o Options) withDefaults() Options {
	if o.DefaultTTL <= 0 {
		o.DefaultTTL = defaultOutboxTTL
	}
	if o.MaxQueued <= 0 {
		o.MaxQueued = defaultMaxQueued
	}
	if o.LeaseDuration <= 0 {
		o.LeaseDuration = defaultLeaseDuration
	}
	if o.ExecTimeout <= 0 {
		o.ExecTimeout = defaultExecTimeout
	}
	if o.RecoverInterval <= 0 {
		o.RecoverInterval = defaultRecoverEvery
	}
	return o
}

type journalNotifier interface {
	Append(ctx context.Context, ev *journal.Event, hooks ...journal.TxHook) (journal.AppendResult, error)
	Notify(instanceID string)
}

type schedulerHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
	wake   chan struct{}
}

// Service mengatur enqueue idempoten, scheduler per instance, pemulihan lease, receipt, dan retensi outbox.
type Service struct {
	st     Store
	exec   Executor
	gov    Governor
	policy PolicyChecker
	opt    Options

	jrn journalNotifier
	rec EventRecorder

	schedMu    sync.Mutex
	schedulers map[string]*schedulerHandle
	recover    *schedulerHandle
	wg         sync.WaitGroup

	waitMu  sync.Mutex
	waiters map[string]chan struct{}
}

// NewService membuat Service outbox baru. Parameter sink menerima *journal.Journal, EventRecorder, atau nil.
func NewService(st Store, sink any, exec Executor, gov Governor, policy PolicyChecker, opt Options) *Service {
	opt = opt.withDefaults()
	s := &Service{
		st:         st,
		exec:       exec,
		gov:        gov,
		policy:     policy,
		opt:        opt,
		schedulers: make(map[string]*schedulerHandle),
		waiters:    make(map[string]chan struct{}),
	}

	switch v := sink.(type) {
	case *journal.Journal:
		if v != nil {
			s.jrn = v
			if sq, ok := st.(*SQLiteStore); ok {
				sq.setEventTxHook(func(ctx context.Context, tx *sql.Tx, ev *journal.Event) error {
					_, err := journal.InsertTx(ctx, tx, ev)
					return err
				})
			}
		}
	case EventRecorder:
		s.rec = v
	}
	return s
}

// Enqueue memvalidasi, mengecek idempotency dan batas antrian, lalu menyimpan job ke store sebelum mengembalikan job.
func (s *Service) Enqueue(ctx context.Context, req EnqueueRequest) (job *Job, created bool, err error) {
	if s == nil || s.st == nil || s.exec == nil {
		return nil, false, errors.New("service outbox belum siap")
	}
	if strings.TrimSpace(req.InstanceID) == "" {
		return nil, false, fmt.Errorf("%w: instance_id wajib diisi", ErrInvalidSpec)
	}

	// 1. Instance wajib sudah di-pairing agar NewMessageID menghasilkan ID deterministik WhatsApp.
	waID, err := s.exec.NewMessageID(req.InstanceID)
	if err != nil {
		return nil, false, err
	}

	// 2. Validasi dan kanonisasi spesifikasi pesan serta hitung hash permintaan.
	spec, sendAtMS, ttl, err := s.validateAndNormalize(req)
	if err != nil {
		return nil, false, err
	}
	reqHash, err := computeReqHash(spec, sendAtMS, req.Priority, ttl)
	if err != nil {
		return nil, false, err
	}

	// 3. Cek idempotency key sebelum kuota antrian agar replay tetap berhasil meski antrian penuh.
	if req.IdemKey != "" {
		existing, err := s.st.GetByIdemKey(ctx, req.InstanceID, req.IdemKey)
		if err == nil {
			if bytes.Equal(existing.ReqHash, reqHash) {
				return existing, false, nil
			}
			return nil, false, ErrIdempotencyMismatch
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, false, err
		}
	}

	// 4. Batas kapasitas antrian queued per instance.
	queued, err := s.st.CountQueued(ctx, req.InstanceID)
	if err != nil {
		return nil, false, err
	}
	if queued >= s.opt.MaxQueued {
		return nil, false, ErrQueueFull
	}

	now := time.Now()
	nowMs := now.UnixMilli()
	notBeforeMs := nowMs
	if sendAtMS > notBeforeMs {
		notBeforeMs = sendAtMS
	}
	expiresAtMs := notBeforeMs + ttl.Milliseconds()

	originKind := strings.TrimSpace(req.Origin.Kind)
	if originKind == "" {
		originKind = "api"
	}

	candidate := &Job{
		ID:         idgen.NewAt("msg_", now),
		InstanceID: req.InstanceID,
		IdemKey:    req.IdemKey,
		ReqHash:    reqHash,
		WAID:       waID,
		ToJID:      spec.To,
		Kind:       spec.Type,
		Spec:       spec,
		Priority:   req.Priority,
		Origin:     originKind,
		KeyID:      req.Origin.KeyID,
		Status:     StatusQueued,
		NotBefore:  notBeforeMs,
		ExpiresAt:  expiresAtMs,
		CreatedAt:  nowMs,
		UpdatedAt:  nowMs,
	}

	// 5. Governor Precheck dan PolicyChecker.CheckSend sebelum menyisipkan job ke SQLite.
	if s.gov != nil {
		if err := s.gov.Precheck(ctx, candidate); err != nil {
			return nil, false, err
		}
	}
	if s.policy != nil {
		if err := s.policy.CheckSend(ctx, req.InstanceID, req.Origin, spec); err != nil {
			return nil, false, err
		}
	}

	// 6-7. Simpan ke antrian durable; tangani balapan unique index idem_key dengan membaca ulang job pemenang.
	if err := s.st.Insert(ctx, candidate); err != nil {
		if errors.Is(err, ErrIdempotencyConflict) && req.IdemKey != "" {
			existing, getErr := s.st.GetByIdemKey(ctx, req.InstanceID, req.IdemKey)
			if getErr == nil {
				if bytes.Equal(existing.ReqHash, reqHash) {
					return existing, false, nil
				}
				return nil, false, ErrIdempotencyMismatch
			}
			return nil, false, getErr
		}
		return nil, false, err
	}

	// 8. Bangunkan scheduler agar job langsung dijadwalkan.
	if s.opt.OnActivity != nil {
		s.opt.OnActivity(req.InstanceID, now)
	}
	s.wakeInstance(req.InstanceID)
	return candidate, true, nil
}

type sendingChecker interface {
	HasSending(ctx context.Context, instanceID string) (bool, error)
}

// HasSending memeriksa apakah instance sedang memiliki job berstatus sending di outbox.
func (s *Service) HasSending(ctx context.Context, instanceID string) (bool, error) {
	if s == nil || s.st == nil {
		return false, nil
	}
	sc, ok := s.st.(sendingChecker)
	if !ok {
		return false, nil
	}
	return sc.HasSending(ctx, instanceID)
}

// Get mengambil detail satu job outbox milik instance tertentu.
func (s *Service) Get(ctx context.Context, instanceID, id string) (*Job, error) {
	if s == nil || s.st == nil {
		return nil, ErrNotFound
	}
	return s.st.GetByID(ctx, instanceID, id)
}

// Cancel membatalkan job yang masih berstatus queued.
func (s *Service) Cancel(ctx context.Context, instanceID, id string) (*Job, error) {
	if s == nil || s.st == nil {
		return nil, ErrNotFound
	}
	job, err := s.st.Cancel(ctx, instanceID, id, time.Now())
	if err != nil {
		return nil, err
	}
	s.notifyJob(job.ID)
	return job, nil
}

// Wait memblokir hingga job mencapai status target (mis. sent) atau berhenti pada status final gagal/timeout ctx.
func (s *Service) Wait(ctx context.Context, instanceID, id string, until Status) (*Job, error) {
	if s == nil || s.st == nil {
		return nil, ErrNotFound
	}
	targetRank := until.StatusRank()
	for {
		// Daftar waiter sebelum membaca DB agar sinyal perubahan status tidak terlewat.
		ch := s.subscribeJob(id)
		job, err := s.st.GetByID(ctx, instanceID, id)
		if err != nil {
			return nil, err
		}
		if hasReachedStatus(job.Status, until, targetRank) {
			return job, nil
		}
		if job.Status.IsFailureTerminal() {
			return job, fmt.Errorf("%w: status=%s code=%s", ErrSendFailed, job.Status, job.ErrorCode)
		}

		select {
		case <-ch:
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return job, ErrWaitTimeout
			}
			return job, ctx.Err()
		}
	}
}

// CountQueued mengembalikan jumlah job berstatus queued pada instance tertentu untuk metrik status.
func (s *Service) CountQueued(ctx context.Context, instanceID string) (int, error) {
	if s == nil || s.st == nil {
		return 0, nil
	}
	return s.st.CountQueued(ctx, instanceID)
}

// MarkReceiptTx memajukan status receipt searah di dalam transaksi journal dan mengembalikan jumlah pesan yang baru naik ke delivered.
func (s *Service) MarkReceiptTx(ctx context.Context, tx *sql.Tx, instanceID string, waIDs []string, rtype string, group bool) (int, error) {
	if s == nil || s.st == nil {
		return 0, nil
	}
	rs, ok := s.st.(ReceiptStore)
	if !ok {
		return 0, nil
	}
	n, updatedIDs, err := rs.MarkReceiptTx(ctx, tx, instanceID, waIDs, rtype, group, time.Now())
	for _, id := range updatedIDs {
		s.notifyJob(id)
	}
	return n, err
}

// RecoverExpiredLeases memulihkan job sending dengan lease kedaluwarsa menjadi queued tanpa mengubah wa_id.
func (s *Service) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	if s == nil || s.st == nil {
		return 0, nil
	}
	return s.st.RecoverExpiredLeases(ctx, time.Now())
}

// PruneOutbox menghapus job berstatus final yang lebih tua dari retention per batch berukuran chunk.
func (s *Service) PruneOutbox(ctx context.Context, now time.Time, retention time.Duration, chunk int) (int64, error) {
	if s == nil || s.st == nil {
		return 0, nil
	}
	if retention <= 0 {
		retention = 7 * 24 * time.Hour
	}
	if chunk <= 0 {
		chunk = 1000
	}
	if now.IsZero() {
		now = time.Now()
	}
	cutoff := now.Add(-retention)
	var total int64
	for {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n, err := s.st.PruneFinal(ctx, cutoff, chunk)
		total += n
		if err != nil {
			return total, err
		}
		if n < int64(chunk) {
			break
		}
	}
	return total, nil
}

// Ready meneruskan status kesiapan transport WhatsApp instance dari Executor yang terpasang.
func (s *Service) Ready(instanceID string) bool {
	if s == nil || s.exec == nil || instanceID == "" {
		return false
	}
	return s.exec.Ready(instanceID)
}

// NotifyReady membangunkan scheduler instance segera setelah koneksi WhatsApp kembali online.
func (s *Service) NotifyReady(instanceID string) {
	s.wakeInstance(instanceID)
}

// StartScheduler menjalankan tepat satu goroutine scheduler untuk instance tertentu hingga ctx dibatalkan atau StopScheduler dipanggil.
func (s *Service) StartScheduler(ctx context.Context, instanceID string) {
	if s == nil || instanceID == "" {
		return
	}
	s.schedMu.Lock()
	if _, exists := s.schedulers[instanceID]; exists {
		s.schedMu.Unlock()
		return
	}
	sCtx, cancel := context.WithCancel(ctx)
	h := &schedulerHandle{
		cancel: cancel,
		done:   make(chan struct{}),
		wake:   make(chan struct{}, 1),
	}
	s.schedulers[instanceID] = h
	s.wg.Add(1)
	s.schedMu.Unlock()

	go func() {
		defer s.wg.Done()
		defer close(h.done)
		defer func() {
			s.schedMu.Lock()
			if cur, ok := s.schedulers[instanceID]; ok && cur == h {
				delete(s.schedulers, instanceID)
			}
			s.schedMu.Unlock()
		}()

		// Pulihkan lease tertunda ketika scheduler instance mulai berjalan.
		if _, err := s.st.RecoverExpiredLeases(sCtx, time.Now()); err != nil && sCtx.Err() == nil {
			slog.Warn("gagal memulihkan lease outbox saat start", "instance", instanceID, "err", err)
		}
		s.runScheduler(sCtx, instanceID, h.wake)
	}()
}

// StopScheduler menghentikan goroutine scheduler instance tertentu secara bersih dan menunggu keluar.
func (s *Service) StopScheduler(instanceID string) {
	if s == nil || instanceID == "" {
		return
	}
	s.schedMu.Lock()
	h := s.schedulers[instanceID]
	if h != nil {
		delete(s.schedulers, instanceID)
	}
	s.schedMu.Unlock()
	if h != nil {
		h.cancel()
		<-h.done
	}
}

// StartLeaseRecovery menjalankan loop global setiap 60 detik untuk memulihkan lease sending yang kedaluwarsa.
func (s *Service) StartLeaseRecovery(ctx context.Context) {
	if s == nil || s.st == nil {
		return
	}
	s.schedMu.Lock()
	if s.recover != nil {
		s.schedMu.Unlock()
		return
	}
	rCtx, cancel := context.WithCancel(ctx)
	h := &schedulerHandle{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	s.recover = h
	s.wg.Add(1)
	s.schedMu.Unlock()

	go func() {
		defer s.wg.Done()
		defer close(h.done)
		ticker := time.NewTicker(s.opt.RecoverInterval)
		defer ticker.Stop()

		_, _ = s.st.RecoverExpiredLeases(rCtx, time.Now())
		for {
			select {
			case <-rCtx.Done():
				return
			case now := <-ticker.C:
				if _, err := s.st.RecoverExpiredLeases(rCtx, now); err != nil && rCtx.Err() == nil {
					slog.Warn("gagal memulihkan lease outbox periodik", "err", err)
				}
			}
		}
	}()
}

// Stop menghentikan seluruh scheduler per instance serta loop pemulihan lease secara sinkron.
func (s *Service) Stop() {
	if s == nil {
		return
	}
	s.schedMu.Lock()
	handles := make([]*schedulerHandle, 0, len(s.schedulers)+1)
	for id, h := range s.schedulers {
		handles = append(handles, h)
		delete(s.schedulers, id)
	}
	if s.recover != nil {
		handles = append(handles, s.recover)
		s.recover = nil
	}
	s.schedMu.Unlock()

	for _, h := range handles {
		h.cancel()
	}
	s.wg.Wait()
}

func (s *Service) runScheduler(ctx context.Context, instanceID string, wake <-chan struct{}) {
	for {
		if ctx.Err() != nil {
			return
		}
		if s.exec != nil && !s.exec.Ready(instanceID) {
			if !sleepOrWake(ctx, notReadySleep, wake) {
				return
			}
			continue
		}

		now := time.Now()
		job, err := s.st.ClaimNext(ctx, instanceID, now, now.Add(s.opt.LeaseDuration))
		if err != nil {
			if !sleepOrWake(ctx, time.Second, wake) {
				return
			}
			continue
		}
		if job == nil {
			waitDur := maxIdleSleep
			if nextDue, ok, dueErr := s.st.NextDue(ctx, instanceID); dueErr == nil && ok {
				if d := nextDue.Sub(now); d <= 0 {
					waitDur = time.Millisecond
				} else if d < waitDur {
					waitDur = d
				}
			}
			if !sleepOrWake(ctx, waitDur, wake) {
				return
			}
			continue
		}

		if now.UnixMilli() > job.ExpiresAt {
			if expJob, mErr := s.st.MarkExpired(ctx, job.ID, now); mErr == nil {
				s.afterFinalTransition(ctx, expJob, StatusExpired)
			}
			continue
		}

		if s.gov != nil {
			dec := s.gov.Decide(ctx, job, now)
			if !dec.Allow {
				if dec.Permanent {
					if fJob, mErr := s.st.MarkFailed(ctx, job.ID, now, dec.Code, dec.Reason); mErr == nil {
						s.afterFinalTransition(ctx, fJob, StatusFailed)
					}
					continue
				}
				delay := dec.Delay
				if delay <= 0 {
					delay = time.Second
				}
				// Penundaan karena Governor tidak menambah attempts.
				_, _ = s.st.Release(ctx, job.ID, now.Add(delay), now, "", "")
				continue
			}
		}

		if jitter := s.sendJitter(); jitter > 0 {
			if !sleepWithCancel(ctx, jitter) {
				_, _ = s.st.Release(context.Background(), job.ID, time.Now(), time.Now(), "", "")
				return
			}
		}

		execStart := time.Now()
		if execStart.UnixMilli() > job.ExpiresAt {
			if expJob, mErr := s.st.MarkExpired(ctx, job.ID, execStart); mErr == nil {
				s.afterFinalTransition(ctx, expJob, StatusExpired)
			}
			continue
		}

		attempts, err := s.st.BeginAttempt(ctx, job.ID, execStart.Add(s.opt.LeaseDuration), execStart)
		if err != nil {
			_, _ = s.st.Release(ctx, job.ID, execStart.Add(time.Second), execStart, "", "")
			continue
		}
		job.Attempts = attempts

		execCtx, cancelExec := context.WithTimeout(ctx, s.opt.ExecTimeout)
		res, execErr := s.exec.Execute(execCtx, job)
		cancelExec()

		doneAt := time.Now()
		if execErr == nil {
			serverTS := res.ServerTS
			if serverTS.IsZero() {
				serverTS = doneAt
			}
			if sentJob, mErr := s.st.MarkSent(ctx, job.ID, serverTS, doneAt); mErr == nil {
				if s.gov != nil {
					s.gov.Record(ctx, sentJob, Outcome{Success: true, Attempts: attempts})
				}
				s.afterFinalTransition(ctx, sentJob, StatusSent)
			}
			continue
		}

		if ctx.Err() != nil {
			// Kembalikan job saat proses berhenti agar tidak dihitung gagal permanen karena shutdown.
			_, _ = s.st.Release(context.Background(), job.ID, doneAt, doneAt, "", "")
			return
		}

		transient, code, minDelay := Classify(execErr)
		errMsg := execErr.Error()
		if s.gov != nil {
			s.gov.Record(ctx, job, Outcome{
				Success:     false,
				Code:        code,
				Transient:   transient,
				RateLimited: code == "rate_overlimit",
				Attempts:    attempts,
				Err:         execErr,
			})
		}

		maxAttempts := 8
		if code == "send_failed" || code == "media_fetch_failed" {
			maxAttempts = 3
		}
		if !transient || attempts >= maxAttempts {
			if fJob, mErr := s.st.MarkFailed(ctx, job.ID, doneAt, code, errMsg); mErr == nil {
				s.afterFinalTransition(ctx, fJob, StatusFailed)
			}
			continue
		}

		nextTry := doneAt.Add(s.retryBackoff(attempts, minDelay))
		if nextTry.UnixMilli() > job.ExpiresAt {
			if fJob, mErr := s.st.MarkFailed(ctx, job.ID, doneAt, code, errMsg); mErr == nil {
				s.afterFinalTransition(ctx, fJob, StatusFailed)
			}
			continue
		}

		_, _ = s.st.Release(ctx, job.ID, nextTry, doneAt, code, errMsg)
	}
}

func (s *Service) afterFinalTransition(ctx context.Context, job *Job, st Status) {
	if job == nil {
		return
	}
	tsSec := time.Now().Unix()
	if st == StatusSent && job.SentAt > 0 {
		tsSec = job.SentAt / 1000
	}

	if s.jrn != nil {
		// Untuk SQLiteStore + Journal, event sudah tersisip atomik di transaksi DB; cukup bangunkan subscriber.
		if _, isSQL := s.st.(*SQLiteStore); !isSQL {
			ev, err := buildOutboxJournalEvent(job, st, tsSec, time.Now().UnixMilli())
			if err == nil {
				_, _ = s.jrn.Append(ctx, ev)
			}
		} else {
			s.jrn.Notify(job.InstanceID)
		}
	} else if s.rec != nil {
		errText := ""
		if st == StatusFailed {
			if job.ErrorMsg != "" {
				errText = job.ErrorMsg
			} else {
				errText = job.ErrorCode
			}
		}
		_ = s.rec.RecordOutboxEvent(ctx, OutboxEvent{
			ID:          job.ID,
			WAMessageID: job.WAID,
			To:          job.ToJID,
			Type:        job.Kind,
			Status:      st,
			Attempts:    job.Attempts,
			Timestamp:   tsSec,
			Error:       errText,
		})
	}

	s.notifyJob(job.ID)
}

func (s *Service) validateAndNormalize(req EnqueueRequest) (Spec, int64, time.Duration, error) {
	spec := req.Spec
	spec.Type = strings.ToLower(strings.TrimSpace(spec.Type))

	parsedTo, err := jid.ParseRecipient(spec.To)
	if err != nil {
		return Spec{}, 0, 0, err
	}
	spec.To = parsedTo.String()

	switch spec.Type {
	case "text":
		if tErr := ValidateTextBody(spec.Text); tErr != nil {
			return Spec{}, 0, 0, tErr
		}
	case "image", "video", "audio", "document", "sticker":
		normMedia, mErr := validateMediaSpec(spec.Type, spec.Media)
		if mErr != nil {
			return Spec{}, 0, 0, mErr
		}
		spec.Media = normMedia
	case "location":
		if lErr := ValidateLocation(spec.Location); lErr != nil {
			return Spec{}, 0, 0, lErr
		}
	case "contact":
		if cErr := ValidateContacts(spec.Contacts); cErr != nil {
			return Spec{}, 0, 0, cErr
		}
	case "poll":
		if pErr := ValidatePoll(spec.Poll); pErr != nil {
			return Spec{}, 0, 0, pErr
		}
	case "reaction":
		if rErr := ValidateReaction(spec.Reaction); rErr != nil {
			return Spec{}, 0, 0, rErr
		}
	case "edit":
		if eErr := ValidateEdit(spec.Edit); eErr != nil {
			return Spec{}, 0, 0, eErr
		}
	case "revoke":
		if rvErr := ValidateRevoke(spec.Revoke); rvErr != nil {
			return Spec{}, 0, 0, rvErr
		}
	default:
		return Spec{}, 0, 0, fmt.Errorf("%w: tipe pesan %q tidak didukung", ErrInvalidSpec, spec.Type)
	}

	if spec.ReplyTo != nil && strings.TrimSpace(spec.ReplyTo.ID) == "" {
		return Spec{}, 0, 0, fmt.Errorf("%w: reply_to.id wajib diisi", ErrInvalidSpec)
	}
	if len(spec.Mentions) > 0 {
		norm := make([]string, 0, len(spec.Mentions))
		for _, m := range spec.Mentions {
			mj, err := jid.ParseRecipient(m)
			if err != nil {
				return Spec{}, 0, 0, fmt.Errorf("%w: mention %q tidak valid", ErrInvalidSpec, m)
			}
			norm = append(norm, mj.String())
		}
		spec.Mentions = norm
	}
	if req.Priority != PriorityNormal && req.Priority != PriorityUrgent {
		return Spec{}, 0, 0, fmt.Errorf("%w: priority harus normal (0) atau urgent (10)", ErrInvalidSpec)
	}

	var sendAtMS int64
	if !req.SendAt.IsZero() {
		sendAtMS = req.SendAt.UnixMilli()
	}
	ttl := req.TTL
	if ttl <= 0 {
		ttl = s.opt.DefaultTTL
	}
	return spec, sendAtMS, ttl, nil
}

func validateMediaSpec(msgType string, m *MediaRef) (*MediaRef, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: objek media wajib diisi untuk tipe %s", ErrInvalidSpec, msgType)
	}
	cp := *m
	cp.UploadID = strings.TrimSpace(cp.UploadID)
	cp.URL = strings.TrimSpace(cp.URL)
	if (cp.UploadID == "") == (cp.URL == "") {
		return nil, fmt.Errorf("%w: isi tepat satu antara media.upload_id atau media.url", ErrInvalidSpec)
	}
	if cp.URL != "" {
		u, err := url.Parse(cp.URL)
		if err != nil || !strings.EqualFold(u.Scheme, "https") || strings.TrimSpace(u.Host) == "" {
			return nil, fmt.Errorf("%w: media.url harus berupa URL https yang valid", ErrInvalidSpec)
		}
	}
	if cp.PTT && msgType != "audio" {
		return nil, fmt.Errorf("%w: media.ptt hanya valid untuk tipe audio", ErrInvalidSpec)
	}
	if cp.GIF && msgType != "video" {
		return nil, fmt.Errorf("%w: media.gif hanya valid untuk tipe video", ErrInvalidSpec)
	}
	if cp.Thumbnail != "" {
		if msgType != "image" && msgType != "video" {
			return nil, fmt.Errorf("%w: media.thumbnail hanya didukung untuk image atau video", ErrInvalidSpec)
		}
		clean := strings.TrimSpace(cp.Thumbnail)
		raw, err := base64.StdEncoding.DecodeString(clean)
		if err != nil {
			return nil, fmt.Errorf("%w: media.thumbnail harus berupa base64 yang valid", ErrInvalidSpec)
		}
		if len(raw) > 16<<10 {
			return nil, fmt.Errorf("%w: ukuran thumbnail melebihi batas 16 KiB", ErrInvalidSpec)
		}
		cp.Thumbnail = clean
	}
	return &cp, nil
}

func computeReqHash(spec Spec, sendAtMS int64, priority int, ttl time.Duration) ([]byte, error) {
	type canonicalReq struct {
		Spec     Spec  `json:"spec"`
		SendAtMS int64 `json:"send_at_ms"`
		Priority int   `json:"priority"`
		TTLMS    int64 `json:"ttl_ms"`
	}
	raw, err := json.Marshal(canonicalReq{
		Spec:     spec,
		SendAtMS: sendAtMS,
		Priority: priority,
		TTLMS:    ttl.Milliseconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("hash permintaan outbox: %w", err)
	}
	sum := sha256.Sum256(raw)
	return sum[:], nil
}

func (s *Service) sendJitter() time.Duration {
	if s.opt.DisableSendJitter {
		return 0
	}
	// Paritas v1: jeda acak 300-1500 ms sebelum mengirim ke server WhatsApp.
	return time.Duration(300+rand.IntN(1201)) * time.Millisecond
}

func (s *Service) retryBackoff(attempts int, minDelay time.Duration) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	base := 5 * time.Second
	shift := attempts - 1
	if shift > 10 {
		shift = 10
	}
	backoff := base << shift
	if backoff > 10*time.Minute {
		backoff = 10 * time.Minute
	}
	if !s.opt.DisableRetryJitter {
		// Jitter ±20% untuk mencegah thundering herd saat konektivitas pulih.
		factor := 0.8 + rand.Float64()*0.4
		backoff = time.Duration(float64(backoff) * factor)
	}
	if minDelay > backoff {
		backoff = minDelay
	}
	return backoff
}

func (s *Service) wakeInstance(instanceID string) {
	if s == nil || instanceID == "" {
		return
	}
	s.schedMu.Lock()
	h := s.schedulers[instanceID]
	s.schedMu.Unlock()
	if h == nil {
		return
	}
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (s *Service) subscribeJob(id string) <-chan struct{} {
	s.waitMu.Lock()
	ch, ok := s.waiters[id]
	if !ok {
		ch = make(chan struct{})
		s.waiters[id] = ch
	}
	s.waitMu.Unlock()
	return ch
}

func (s *Service) notifyJob(id string) {
	if s == nil || id == "" {
		return
	}
	s.waitMu.Lock()
	if ch, ok := s.waiters[id]; ok {
		close(ch)
		delete(s.waiters, id)
	}
	s.waitMu.Unlock()
}

func hasReachedStatus(cur, until Status, targetRank int) bool {
	if targetRank > 0 {
		return cur.StatusRank() >= targetRank
	}
	return cur == until
}

func sleepOrWake(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return true
	case <-timer.C:
		return true
	}
}

func sleepWithCancel(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
