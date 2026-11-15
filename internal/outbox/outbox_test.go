package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
)

// fakeExecutor mensimulasikan pengirim WhatsApp untuk unit test dan synctest outbox.
type fakeExecutor struct {
	mu       sync.Mutex
	ready    bool
	paired   bool
	nextSeq  int64
	calls    []execCall
	execFn   func(ctx context.Context, job *outbox.Job) (outbox.ExecResult, error)
}

type execCall struct {
	id   string
	waID string
	at   time.Time
}

func (f *fakeExecutor) Ready(_ string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready
}

func (f *fakeExecutor) SetReady(ready bool) {
	f.mu.Lock()
	f.ready = ready
	f.mu.Unlock()
}

func (f *fakeExecutor) NewMessageID(instanceID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.paired {
		return "", outbox.ErrNotPaired
	}
	f.nextSeq++
	return fmt.Sprintf("3EB0%s%06d", instanceID, f.nextSeq), nil
}

func (f *fakeExecutor) Execute(ctx context.Context, job *outbox.Job) (outbox.ExecResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, execCall{id: job.ID, waID: job.WAID, at: time.Now()})
	fn := f.execFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, job)
	}
	return outbox.ExecResult{ServerTS: time.Now()}, nil
}

func (f *fakeExecutor) Calls() []execCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]execCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// fakeOutboxRecorder mencatat event outbox yang dipancarkan oleh service di dalam synctest.
type fakeOutboxRecorder struct {
	mu     sync.Mutex
	events []outbox.OutboxEvent
}

func (r *fakeOutboxRecorder) RecordOutboxEvent(_ context.Context, ev outbox.OutboxEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	return nil
}

func (r *fakeOutboxRecorder) Events() []outbox.OutboxEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]outbox.OutboxEvent, len(r.events))
	copy(out, r.events)
	return out
}

// memStore mengimplementasikan outbox.Store sepenuhnya di memori agar aman dijalankan di dalam synctest tanpa I/O SQLite.
type memStore struct {
	mu   sync.Mutex
	jobs map[string]*outbox.Job
	idem map[string]string // instanceID + "\x00" + idemKey -> jobID
}

func newMemStore() *memStore {
	return &memStore{
		jobs: make(map[string]*outbox.Job),
		idem: make(map[string]string),
	}
}

func cloneJob(j *outbox.Job) *outbox.Job {
	if j == nil {
		return nil
	}
	cp := *j
	if j.ReqHash != nil {
		cp.ReqHash = append([]byte(nil), j.ReqHash...)
	}
	return &cp
}

func (m *memStore) GetByID(_ context.Context, instanceID, id string) (*outbox.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok || j.InstanceID != instanceID {
		return nil, outbox.ErrNotFound
	}
	return cloneJob(j), nil
}

func (m *memStore) GetByIdemKey(_ context.Context, instanceID, idemKey string) (*outbox.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.idem[instanceID+"\x00"+idemKey]
	if !ok {
		return nil, outbox.ErrNotFound
	}
	return cloneJob(m.jobs[id]), nil
}

func (m *memStore) CountQueued(_ context.Context, instanceID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, j := range m.jobs {
		if j.InstanceID == instanceID && j.Status == outbox.StatusQueued {
			n++
		}
	}
	return n, nil
}

func (m *memStore) Insert(_ context.Context, job *outbox.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job.IdemKey != "" {
		k := job.InstanceID + "\x00" + job.IdemKey
		if _, exists := m.idem[k]; exists {
			return outbox.ErrIdempotencyConflict
		}
		m.idem[k] = job.ID
	}
	m.jobs[job.ID] = cloneJob(job)
	return nil
}

func (m *memStore) Cancel(_ context.Context, instanceID, id string, now time.Time) (*outbox.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok || j.InstanceID != instanceID {
		return nil, outbox.ErrNotFound
	}
	if j.Status != outbox.StatusQueued {
		return nil, outbox.ErrNotCancellable
	}
	j.Status = outbox.StatusCancelled
	j.UpdatedAt = now.UnixMilli()
	return cloneJob(j), nil
}

func (m *memStore) ClaimNext(_ context.Context, instanceID string, now, leaseUntil time.Time) (*outbox.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nowMs := now.UnixMilli()
	var best *outbox.Job
	for _, j := range m.jobs {
		if j.InstanceID != instanceID || j.Status != outbox.StatusQueued {
			continue
		}
		if j.NotBefore > nowMs && j.ExpiresAt >= nowMs {
			continue
		}
		if best == nil ||
			j.Priority > best.Priority ||
			(j.Priority == best.Priority && j.NotBefore < best.NotBefore) ||
			(j.Priority == best.Priority && j.NotBefore == best.NotBefore && j.ID < best.ID) {
			best = j
		}
	}
	if best == nil {
		return nil, nil
	}
	best.Status = outbox.StatusSending
	best.LeaseUntil = leaseUntil.UnixMilli()
	best.UpdatedAt = nowMs
	return cloneJob(best), nil
}

func (m *memStore) NextDue(_ context.Context, instanceID string) (time.Time, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var minMs int64
	found := false
	for _, j := range m.jobs {
		if j.InstanceID != instanceID || j.Status != outbox.StatusQueued {
			continue
		}
		due := min(j.NotBefore, j.ExpiresAt+1)
		if !found || due < minMs {
			minMs = due
			found = true
		}
	}
	if !found {
		return time.Time{}, false, nil
	}
	return time.UnixMilli(minMs), true, nil
}

func (m *memStore) BeginAttempt(_ context.Context, id string, leaseUntil, now time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return 0, outbox.ErrNotFound
	}
	j.Attempts++
	j.LeaseUntil = leaseUntil.UnixMilli()
	j.UpdatedAt = now.UnixMilli()
	return j.Attempts, nil
}

func (m *memStore) Release(_ context.Context, id string, notBefore, now time.Time, errCode, errMsg string) (*outbox.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, outbox.ErrNotFound
	}
	j.Status = outbox.StatusQueued
	j.LeaseUntil = 0
	j.NotBefore = notBefore.UnixMilli()
	j.ErrorCode = errCode
	j.ErrorMsg = errMsg
	j.UpdatedAt = now.UnixMilli()
	return cloneJob(j), nil
}

func (m *memStore) MarkSent(_ context.Context, id string, sentAt, now time.Time) (*outbox.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, outbox.ErrNotFound
	}
	j.Status = outbox.StatusSent
	j.LeaseUntil = 0
	j.SentAt = sentAt.UnixMilli()
	j.ErrorCode = ""
	j.ErrorMsg = ""
	j.UpdatedAt = now.UnixMilli()
	return cloneJob(j), nil
}

func (m *memStore) MarkFailed(_ context.Context, id string, now time.Time, errCode, errMsg string) (*outbox.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, outbox.ErrNotFound
	}
	j.Status = outbox.StatusFailed
	j.LeaseUntil = 0
	j.ErrorCode = errCode
	j.ErrorMsg = errMsg
	j.UpdatedAt = now.UnixMilli()
	return cloneJob(j), nil
}

func (m *memStore) MarkExpired(_ context.Context, id string, now time.Time) (*outbox.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, outbox.ErrNotFound
	}
	j.Status = outbox.StatusExpired
	j.LeaseUntil = 0
	j.UpdatedAt = now.UnixMilli()
	return cloneJob(j), nil
}

func (m *memStore) RecoverExpiredLeases(_ context.Context, now time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nowMs := now.UnixMilli()
	var n int64
	for _, j := range m.jobs {
		if j.Status == outbox.StatusSending && j.LeaseUntil > 0 && j.LeaseUntil < nowMs {
			j.Status = outbox.StatusQueued
			j.LeaseUntil = 0
			j.UpdatedAt = nowMs
			n++
		}
	}
	return n, nil
}

func (m *memStore) PruneFinal(_ context.Context, cutoff time.Time, limit int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutMs := cutoff.UnixMilli()
	var n int64
	for id, j := range m.jobs {
		if n >= int64(limit) {
			break
		}
		if j.Status.IsTerminal() && j.UpdatedAt < cutMs {
			delete(m.jobs, id)
			n++
		}
	}
	return n, nil
}

func openTestJournal(t *testing.T) *journal.Journal {
	t.Helper()
	ctx := context.Background()
	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    2,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j
}

func textSpec(to, text string) outbox.Spec {
	return outbox.Spec{Type: "text", To: to, Text: text}
}

// TestEnqueue_Idempoten memverifikasi replay key + payload yang sama mengembalikan job lama dan created=false.
func TestEnqueue_Idempoten(t *testing.T) {
	j := openTestJournal(t)
	exec := &fakeExecutor{paired: true, ready: true}
	svc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{})

	req := outbox.EnqueueRequest{
		InstanceID: "sales",
		IdemKey:    "order-1024",
		Spec:       textSpec("6281234567890", "Pesanan diterima"),
		Priority:   outbox.PriorityNormal,
		TTL:        24 * time.Hour,
		Origin:     outbox.Origin{Kind: "api"},
	}

	ctx := context.Background()
	job1, created1, err := svc.Enqueue(ctx, req)
	if err != nil {
		t.Fatalf("Enqueue pertama gagal: %v", err)
	}
	if !created1 || job1.ID == "" || job1.WAID == "" || job1.Status != outbox.StatusQueued {
		t.Fatalf("hasil enqueue pertama tidak sesuai: created=%v job=%+v", created1, job1)
	}

	job2, created2, err := svc.Enqueue(ctx, req)
	if err != nil {
		t.Fatalf("Enqueue replay gagal: %v", err)
	}
	if created2 {
		t.Fatalf("replay idempoten harus mengembalikan created=false")
	}
	if job2.ID != job1.ID || job2.WAID != job1.WAID {
		t.Fatalf("replay harus mengembalikan job yang sama: %s != %s", job2.ID, job1.ID)
	}
}

// TestEnqueue_IdemKeyBedaPayload422 memverifikasi pemakaian ulang Idempotency-Key dengan isi berbeda menolak 422.
func TestEnqueue_IdemKeyBedaPayload422(t *testing.T) {
	j := openTestJournal(t)
	exec := &fakeExecutor{paired: true, ready: true}
	svc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{})

	ctx := context.Background()
	_, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		IdemKey:    "k-unik",
		Spec:       textSpec("6281234567890", "isi satu"),
	})
	if err != nil {
		t.Fatalf("Enqueue awal gagal: %v", err)
	}

	_, _, err = svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		IdemKey:    "k-unik",
		Spec:       textSpec("6281234567890", "isi berbeda"),
	})
	if !errors.Is(err, outbox.ErrIdempotencyMismatch) {
		t.Fatalf("diharapkan ErrIdempotencyMismatch, didapat %v", err)
	}
}

// TestEnqueue_KonkurenKunciSama memastikan 20 goroutine bersamaan dengan idem_key sama hanya menghasilkan tepat satu baris.
func TestEnqueue_KonkurenKunciSama(t *testing.T) {
	j := openTestJournal(t)
	exec := &fakeExecutor{paired: true, ready: true}
	svc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{})

	const workers = 20
	req := outbox.EnqueueRequest{
		InstanceID: "sales",
		IdemKey:    "race-key",
		Spec:       textSpec("6281234567890", "halo konkuren"),
		TTL:        2 * time.Hour,
	}

	var wg sync.WaitGroup
	var createdCount atomic.Int32
	ids := make([]string, workers)
	errs := make([]error, workers)
	start := make(chan struct{})

	for idx := range workers {
		wg.Go(func() {
			<-start
			job, created, err := svc.Enqueue(context.Background(), req)
			errs[idx] = err
			if err == nil {
				ids[idx] = job.ID
			}
			if created {
				createdCount.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()

	for idx, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d gagal: %v", idx, err)
		}
		if ids[idx] != ids[0] {
			t.Fatalf("ID tidak sama antar goroutine: %q != %q", ids[idx], ids[0])
		}
	}
	if got := createdCount.Load(); got != 1 {
		t.Fatalf("tepat satu goroutine harus mengembalikan created=true, didapat %d", got)
	}

	var count int
	if err := j.DB().QueryRow(`SELECT COUNT(*) FROM outbox WHERE instance_id = 'sales' AND idem_key = 'race-key'`).Scan(&count); err != nil {
		t.Fatalf("hitung baris outbox gagal: %v", err)
	}
	if count != 1 {
		t.Fatalf("diharapkan 1 baris di SQLite, didapat %d", count)
	}
}

// TestEnqueue_QueueFull memastikan kapasitas antrian maksimal menegakkan ErrQueueFull kecuali replay idempoten.
func TestEnqueue_QueueFull(t *testing.T) {
	j := openTestJournal(t)
	exec := &fakeExecutor{paired: true, ready: true}
	svc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{
		MaxQueued: 3,
	})
	ctx := context.Background()

	for i := range 3 {
		_, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: "sales",
			IdemKey:    fmt.Sprintf("full-%d", i),
			Spec:       textSpec("6281234567890", fmt.Sprintf("msg %d", i)),
		})
		if err != nil {
			t.Fatalf("enqueue ke-%d gagal: %v", i, err)
		}
	}

	_, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		IdemKey:    "full-4",
		Spec:       textSpec("6281234567890", "msg 4"),
	})
	if !errors.Is(err, outbox.ErrQueueFull) {
		t.Fatalf("enqueue ke-4 harus mengembalikan ErrQueueFull, didapat %v", err)
	}

	// Replay idempoten pada key yang sudah ada tetap diizinkan walau antrian penuh.
	_, created, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		IdemKey:    "full-1",
		Spec:       textSpec("6281234567890", "msg 1"),
	})
	if err != nil || created {
		t.Fatalf("replay ketika penuh harus berhasil dengan created=false: created=%v err=%v", created, err)
	}
}

// TestScheduler_UrutanDanRate menguji pengiriman serial mengikuti prioritas, token bucket 60/menit burst 3, di dalam synctest.
func TestScheduler_UrutanDanRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st := newMemStore()
		exec := &fakeExecutor{paired: true, ready: true}
		gov := outbox.NewTokenBucketGovernor(func(string) int { return 60 })
		rec := &fakeOutboxRecorder{}
		svc := outbox.NewService(st, rec, exec, gov, nil, outbox.Options{
			DisableSendJitter: true,
		})

		startTS := time.Now()
		svc.StartScheduler(ctx, "sales")
		synctest.Wait()

		for i := range 5 {
			_, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
				InstanceID: "sales",
				Spec:       textSpec("6281234567890", fmt.Sprintf("pesan-%d", i)),
			})
			if err != nil {
				t.Fatalf("enqueue %d gagal: %v", i, err)
			}
		}

		// Tunggu 3 detik virtual: 3 pesan langsung terkirim di burst t=0, lalu t+1s dan t+2s.
		time.Sleep(3 * time.Second)
		synctest.Wait()

		calls := exec.Calls()
		if len(calls) != 5 {
			t.Fatalf("diharapkan 5 panggilan Execute, didapat %d", len(calls))
		}
		for idx, wantDelay := range []time.Duration{0, 0, 0, time.Second, 2 * time.Second} {
			gotDelay := calls[idx].at.Sub(startTS)
			if gotDelay != wantDelay {
				t.Fatalf("waktu eksekusi pesan ke-%d tidak sesuai: dapat=%v ingin=%v", idx, gotDelay, wantDelay)
			}
		}

		cancel()
		svc.Stop()
	})
}

// TestScheduler_CrashLeasePakaiWAIDSama mensimulasikan crash saat sending lalu pemulihan lease dan pengiriman ulang memakai WAID yang sama.
func TestScheduler_CrashLeasePakaiWAIDSama(t *testing.T) {
	j := openTestJournal(t)
	st := outbox.NewSQLiteStore(j.DB())
	exec := &fakeExecutor{paired: true, ready: true}
	svc := outbox.NewService(st, j, exec, nil, nil, outbox.Options{
		DisableSendJitter: true,
	})

	ctx := context.Background()
	job, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Spec:       textSpec("6281234567890", "tahan crash"),
	})
	if err != nil {
		t.Fatalf("Enqueue gagal: %v", err)
	}
	origWAID := job.WAID

	// Klaim seolah worker crash sebelum menandai sent.
	now := time.Now()
	claimed, err := st.ClaimNext(ctx, "sales", now, now.Add(2*time.Minute))
	if err != nil || claimed == nil || claimed.Status != outbox.StatusSending {
		t.Fatalf("ClaimNext gagal: %+v err=%v", claimed, err)
	}

	recovered, err := st.RecoverExpiredLeases(ctx, now.Add(3*time.Minute))
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverExpiredLeases harus memulihkan 1 baris, didapat %d err=%v", recovered, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	svc.StartScheduler(runCtx, "sales")

	got, err := svc.Wait(ctx, "sales", job.ID, outbox.StatusSent)
	if err != nil {
		t.Fatalf("Wait gagal: %v", err)
	}
	if got.WAID != origWAID {
		t.Fatalf("WAID berubah setelah pemulihan lease: dapat=%q ingin=%q", got.WAID, origWAID)
	}
	calls := exec.Calls()
	if len(calls) != 1 || calls[0].waID != origWAID {
		t.Fatalf("Execute harus memakai WAID semula %q: %+v", origWAID, calls)
	}
	cancel()
	svc.Stop()
}

// TestScheduler_TransientLaluFailed menguji retry 8 kali pada error transient sebelum menjadi failed/not_connected.
func TestScheduler_TransientLaluFailed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st := newMemStore()
		rec := &fakeOutboxRecorder{}
		exec := &fakeExecutor{
			paired: true,
			ready:  true,
			execFn: func(context.Context, *outbox.Job) (outbox.ExecResult, error) {
				return outbox.ExecResult{}, &outbox.SendError{
					Transient: true,
					Code:      "not_connected",
					Err:       errors.New("terputus"),
				}
			},
		}
		svc := outbox.NewService(st, rec, exec, nil, nil, outbox.Options{
			DefaultTTL:        48 * time.Hour,
			DisableSendJitter: true,
			DisableRetryJitter: true,
		})

		svc.StartScheduler(ctx, "sales")
		job, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: "sales",
			Spec:       textSpec("6281234567890", "coba terus"),
			TTL:        48 * time.Hour,
		})
		if err != nil {
			t.Fatalf("Enqueue gagal: %v", err)
		}

		// Total backoff 7 jeda: 5 + 10 + 20 + 40 + 80 + 160 + 320 = 635s (< 15 menit).
		time.Sleep(15 * time.Minute)
		synctest.Wait()

		got, err := svc.Get(ctx, "sales", job.ID)
		if err != nil {
			t.Fatalf("Get gagal: %v", err)
		}
		if got.Status != outbox.StatusFailed || got.Attempts != 8 || got.ErrorCode != "not_connected" {
			t.Fatalf("status/attempts/error_code tidak sesuai: %+v", got)
		}

		events := rec.Events()
		if len(events) != 1 || events[0].Status != outbox.StatusFailed {
			t.Fatalf("event outbox failed harus tercatat tepat sekali: %+v", events)
		}

		cancel()
		svc.Stop()
	})
}

// TestScheduler_PermanenLangsungFailed memverifikasi error permanen menghentikan job setelah 1 percobaan.
func TestScheduler_PermanenLangsungFailed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st := newMemStore()
		rec := &fakeOutboxRecorder{}
		exec := &fakeExecutor{
			paired: true,
			ready:  true,
			execFn: func(context.Context, *outbox.Job) (outbox.ExecResult, error) {
				return outbox.ExecResult{}, &outbox.SendError{
					Transient: false,
					Code:      "invalid_recipient",
					Err:       errors.New("jid salah"),
				}
			},
		}
		svc := outbox.NewService(st, rec, exec, nil, nil, outbox.Options{
			DisableSendJitter: true,
		})

		svc.StartScheduler(ctx, "sales")
		job, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: "sales",
			Spec:       textSpec("6281234567890", "permanen"),
		})
		if err != nil {
			t.Fatalf("Enqueue gagal: %v", err)
		}

		synctest.Wait()

		got, err := svc.Get(ctx, "sales", job.ID)
		if err != nil {
			t.Fatalf("Get gagal: %v", err)
		}
		if got.Status != outbox.StatusFailed || got.Attempts != 1 || got.ErrorCode != "invalid_recipient" {
			t.Fatalf("harus langsung failed setelah 1 percobaan: %+v", got)
		}

		cancel()
		svc.Stop()
	})
}

// TestScheduler_SendAtDihormati memastikan job terjadwal di masa depan tidak dieksekusi sebelum waktunya.
func TestScheduler_SendAtDihormati(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st := newMemStore()
		exec := &fakeExecutor{paired: true, ready: true}
		svc := outbox.NewService(st, nil, exec, nil, nil, outbox.Options{
			DisableSendJitter: true,
		})

		svc.StartScheduler(ctx, "sales")
		sendAt := time.Now().Add(10 * time.Minute)
		job, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: "sales",
			Spec:       textSpec("6281234567890", "terjadwal"),
			SendAt:     sendAt,
			TTL:        24 * time.Hour,
		})
		if err != nil {
			t.Fatalf("Enqueue gagal: %v", err)
		}

		time.Sleep(9*time.Minute + 59*time.Second)
		synctest.Wait()
		if n := len(exec.Calls()); n != 0 {
			t.Fatalf("pesan belum boleh dieksekusi sebelum send_at, panggilan=%d", n)
		}

		time.Sleep(2 * time.Second)
		synctest.Wait()
		got, _ := svc.Get(ctx, "sales", job.ID)
		if got.Status != outbox.StatusSent || len(exec.Calls()) != 1 {
			t.Fatalf("pesan harus terkirim setelah send_at terlewati: status=%s calls=%d", got.Status, len(exec.Calls()))
		}

		cancel()
		svc.Stop()
	})
}

// TestScheduler_ExpiredSetelahTTL memverifikasi job melewati TTL berubah menjadi expired dan mencatat event outbox.
func TestScheduler_ExpiredSetelahTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st := newMemStore()
		rec := &fakeOutboxRecorder{}
		exec := &fakeExecutor{paired: true, ready: false}
		svc := outbox.NewService(st, rec, exec, nil, nil, outbox.Options{
			DisableSendJitter: true,
		})

		svc.StartScheduler(ctx, "sales")
		job, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: "sales",
			Spec:       textSpec("6281234567890", "kedaluwarsa"),
			TTL:        5 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Enqueue gagal: %v", err)
		}

		time.Sleep(6 * time.Minute)
		exec.SetReady(true)
		svc.NotifyReady("sales")
		synctest.Wait()

		got, err := svc.Get(ctx, "sales", job.ID)
		if err != nil {
			t.Fatalf("Get gagal: %v", err)
		}
		if got.Status != outbox.StatusExpired {
			t.Fatalf("status harus expired setelah melewati TTL, didapat %q", got.Status)
		}
		if len(exec.Calls()) != 0 {
			t.Fatalf("job expired tidak boleh dieksekusi ke WhatsApp")
		}
		events := rec.Events()
		if len(events) != 1 || events[0].Status != outbox.StatusExpired {
			t.Fatalf("event outbox expired harus tercatat: %+v", events)
		}

		cancel()
		svc.Stop()
	})
}

type deferringGovernor struct {
	mu     sync.Mutex
	defers int
}

func (g *deferringGovernor) Precheck(context.Context, *outbox.Job) error { return nil }

func (g *deferringGovernor) Decide(context.Context, *outbox.Job, time.Time) outbox.Decision {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.defers > 0 {
		g.defers--
		return outbox.Decision{Allow: false, Delay: 5 * time.Second, Code: "rate", Reason: "tunda"}
	}
	return outbox.Decision{Allow: true}
}

func (g *deferringGovernor) Record(context.Context, *outbox.Job, outbox.Outcome) {}

// TestScheduler_PenundaanTidakMenambahAttempts memastikan penundaan oleh Governor tidak menaikkan counter attempts.
func TestScheduler_PenundaanTidakMenambahAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st := newMemStore()
		exec := &fakeExecutor{paired: true, ready: true}
		gov := &deferringGovernor{defers: 2}
		svc := outbox.NewService(st, nil, exec, gov, nil, outbox.Options{
			DisableSendJitter: true,
		})

		svc.StartScheduler(ctx, "sales")
		job, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: "sales",
			Spec:       textSpec("6281234567890", "tunda governor"),
		})
		if err != nil {
			t.Fatalf("Enqueue gagal: %v", err)
		}

		time.Sleep(15 * time.Second)
		synctest.Wait()

		got, err := svc.Get(ctx, "sales", job.ID)
		if err != nil {
			t.Fatalf("Get gagal: %v", err)
		}
		if got.Status != outbox.StatusSent || got.Attempts != 1 {
			t.Fatalf("penundaan governor tidak boleh menambah attempts: status=%s attempts=%d", got.Status, got.Attempts)
		}

		cancel()
		svc.Stop()
	})
}

// TestMarkReceipt_Monoton menguji kenaikan status searah (sent < delivered < read < played), counter grup, dan event outbox DM.
func TestMarkReceipt_Monoton(t *testing.T) {
	j := openTestJournal(t)
	st := outbox.NewSQLiteStore(j.DB())
	exec := &fakeExecutor{paired: true, ready: true}
	svc := outbox.NewService(st, j, exec, nil, nil, outbox.Options{})
	ctx := context.Background()

	dmJob, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Spec:       textSpec("6281234567890", "dm receipt"),
	})
	if err != nil {
		t.Fatalf("Enqueue DM gagal: %v", err)
	}
	if _, err := st.MarkSent(ctx, dmJob.ID, time.Now(), time.Now()); err != nil {
		t.Fatalf("MarkSent DM gagal: %v", err)
	}

	grpJob, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Spec:       textSpec("1203630123456789@g.us", "grup receipt"),
	})
	if err != nil {
		t.Fatalf("Enqueue grup gagal: %v", err)
	}
	if _, err := st.MarkSent(ctx, grpJob.ID, time.Now(), time.Now()); err != nil {
		t.Fatalf("MarkSent grup gagal: %v", err)
	}

	runMark := func(waIDs []string, rtype string, group bool) int {
		var transitioned int
		err := j.RunTx(ctx, func(txCtx context.Context, tx *sql.Tx, _ int64) error {
			var err error
			transitioned, err = svc.MarkReceiptTx(txCtx, tx, "sales", waIDs, rtype, group)
			return err
		})
		if err != nil {
			t.Fatalf("RunTx MarkReceiptTx gagal: %v", err)
		}
		return transitioned
	}

	// DM: delivered -> read -> delivered lagi (tidak boleh turun ke delivered).
	if tr := runMark([]string{dmJob.WAID}, "", false); tr != 1 {
		t.Fatalf("delivered DM pertama harus transitioned=1, dapat=%d", tr)
	}
	if tr := runMark([]string{dmJob.WAID}, "read", false); tr != 0 {
		t.Fatalf("read DM harus transitioned=0, dapat=%d", tr)
	}
	if tr := runMark([]string{dmJob.WAID}, "", false); tr != 0 {
		t.Fatalf("delivered ulang DM harus transitioned=0, dapat=%d", tr)
	}

	gotDM, _ := svc.Get(ctx, "sales", dmJob.ID)
	if gotDM.Status != outbox.StatusRead || gotDM.DeliveredAt == 0 || gotDM.ReadAt == 0 {
		t.Fatalf("status DM harus tetap read tanpa turun: %+v", gotDM)
	}

	// Grup: counter delivered/read bertambah, status naik pada receipt pertama dan tetap monoton.
	if tr := runMark([]string{grpJob.WAID}, "", true); tr != 1 {
		t.Fatalf("delivered grup pertama harus transitioned=1, dapat=%d", tr)
	}
	_ = runMark([]string{grpJob.WAID}, "", true)
	_ = runMark([]string{grpJob.WAID}, "read", true)
	_ = runMark([]string{grpJob.WAID}, "", true)

	gotGrp, _ := svc.Get(ctx, "sales", grpJob.ID)
	if gotGrp.Status != outbox.StatusRead || gotGrp.RcptDelivered != 3 || gotGrp.RcptRead != 1 {
		t.Fatalf("counter atau status grup tidak sesuai: %+v", gotGrp)
	}

	// Pastikan journal mencatat event outbox untuk DM delivered dan read, namun tidak untuk receipt grup.
	rr, err := j.Read(ctx, journal.Query{InstanceID: "sales", Types: []string{"outbox"}, Limit: 50})
	if err != nil {
		t.Fatalf("journal.Read gagal: %v", err)
	}
	var statuses []string
	for _, ev := range rr.Events {
		var p struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(ev.Payload, &p); err == nil {
			statuses = append(statuses, p.ID+":"+p.Status)
		}
	}
	if !slices.Contains(statuses, dmJob.ID+":delivered") || !slices.Contains(statuses, dmJob.ID+":read") {
		t.Fatalf("event outbox DM delivered/read harus tercatat di journal, didapat %v", statuses)
	}
	for _, st := range statuses {
		if st == grpJob.ID+":delivered" || st == grpJob.ID+":read" {
			t.Fatalf("receipt grup tidak boleh memancarkan event outbox delivered/read: %v", statuses)
		}
	}
}

// TestPruneOutbox_HanyaJobFinal memastikan pruner menghapus job final > 7 hari tetapi mempertahankan job queued berumur sama.
func TestPruneOutbox_HanyaJobFinal(t *testing.T) {
	j := openTestJournal(t)
	st := outbox.NewSQLiteStore(j.DB())
	exec := &fakeExecutor{paired: true, ready: true}
	svc := outbox.NewService(st, j, exec, nil, nil, outbox.Options{})
	ctx := context.Background()

	oldQueued, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Spec:       textSpec("6281234567890", "antri lama"),
		TTL:        30 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("Enqueue oldQueued gagal: %v", err)
	}
	oldSent, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Spec:       textSpec("6281234567890", "final lama"),
	})
	if err != nil {
		t.Fatalf("Enqueue oldSent gagal: %v", err)
	}
	if _, err := st.MarkSent(ctx, oldSent.ID, time.Now(), time.Now()); err != nil {
		t.Fatalf("MarkSent gagal: %v", err)
	}

	oldMS := time.Now().Add(-8 * 24 * time.Hour).UnixMilli()
	if _, err := j.DB().ExecContext(ctx, `UPDATE outbox SET updated_at = ? WHERE id IN (?, ?)`, oldMS, oldQueued.ID, oldSent.ID); err != nil {
		t.Fatalf("backdate updated_at gagal: %v", err)
	}

	deleted, err := svc.PruneOutbox(ctx, time.Now(), 7*24*time.Hour, 1000)
	if err != nil {
		t.Fatalf("PruneOutbox gagal: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("harus menghapus tepat 1 job final, didapat %d", deleted)
	}

	if _, err := svc.Get(ctx, "sales", oldSent.ID); !errors.Is(err, outbox.ErrNotFound) {
		t.Fatalf("job final > 7 hari seharusnya terhapus, err=%v", err)
	}
	if got, err := svc.Get(ctx, "sales", oldQueued.ID); err != nil || got.Status != outbox.StatusQueued {
		t.Fatalf("job queued tidak boleh terhapus oleh pruner: got=%+v err=%v", got, err)
	}
}

// TestWait_SentDanTimeout menguji Wait kembali saat status sent tercapai maupun ErrWaitTimeout saat batas waktu habis.
func TestWait_SentDanTimeout(t *testing.T) {
	j := openTestJournal(t)
	st := outbox.NewSQLiteStore(j.DB())

	gate := make(chan struct{})
	exec := &fakeExecutor{
		paired: true,
		ready:  true,
		execFn: func(ctx context.Context, job *outbox.Job) (outbox.ExecResult, error) {
			<-gate
			return outbox.ExecResult{ServerTS: time.Now()}, nil
		},
	}
	svc := outbox.NewService(st, j, exec, nil, nil, outbox.Options{
		DisableSendJitter: true,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.StartScheduler(runCtx, "sales")
	defer svc.Stop()

	job, _, err := svc.Enqueue(runCtx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Spec:       textSpec("6281234567890", "uji wait"),
	})
	if err != nil {
		t.Fatalf("Enqueue gagal: %v", err)
	}

	waitShort, cancelShort := context.WithTimeout(runCtx, 50*time.Millisecond)
	defer cancelShort()
	if _, err := svc.Wait(waitShort, "sales", job.ID, outbox.StatusSent); !errors.Is(err, outbox.ErrWaitTimeout) {
		t.Fatalf("diharapkan ErrWaitTimeout, didapat %v", err)
	}

	close(gate)
	waitOK, cancelOK := context.WithTimeout(runCtx, 2*time.Second)
	defer cancelOK()
	doneJob, err := svc.Wait(waitOK, "sales", job.ID, outbox.StatusSent)
	if err != nil || doneJob.Status != outbox.StatusSent {
		t.Fatalf("Wait setelah gate dibuka gagal: job=%+v err=%v", doneJob, err)
	}
}

// BenchmarkEnqueue mengukur latensi ns/op Enqueue menuju journal.db.
func BenchmarkEnqueue(b *testing.B) {
	ctx := context.Background()
	j, err := journal.Open(ctx, filepath.Join(b.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    1024,
		IdleTimeout: 60 * time.Second,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	})
	if err != nil {
		b.Fatalf("journal.Open gagal: %v", err)
	}
	b.Cleanup(func() { _ = j.Close() })

	exec := &fakeExecutor{paired: true, ready: true}
	svc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{
		MaxQueued: 1000000,
	})

	req := outbox.EnqueueRequest{
		InstanceID: "sales",
		Spec:       textSpec("6281234567890", "benchmark outbox enqueue"),
		TTL:        24 * time.Hour,
		Origin:     outbox.Origin{Kind: "api"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := svc.Enqueue(ctx, req); err != nil {
			b.Fatalf("Enqueue benchmark gagal: %v", err)
		}
	}
}
