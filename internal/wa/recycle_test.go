package wa

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
)

type fakeRecycleBackend struct {
	mu           sync.Mutex
	instances    []RecycleCandidate
	hasSending   map[string]bool
	lastActivity map[string]time.Time
	recycled     []string
	recycledAt   []time.Time
}

func (f *fakeRecycleBackend) RecycleCandidates(_ context.Context) ([]RecycleCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]RecycleCandidate, len(f.instances))
	copy(out, f.instances)
	return out, nil
}

func (f *fakeRecycleBackend) HasSending(_ context.Context, instanceID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hasSending[instanceID], nil
}

func (f *fakeRecycleBackend) LastActivity(instanceID string) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastActivity[instanceID]
}

func (f *fakeRecycleBackend) RecycleInstance(_ context.Context, instanceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recycled = append(f.recycled, instanceID)
	f.recycledAt = append(f.recycledAt, time.Now())
	return nil
}

type fakeRecycleGov struct {
	lvl atomic.Int32
}

func (g *fakeRecycleGov) Level() governor.Level {
	return governor.Level(g.lvl.Load())
}

// TestRecycleTerjadwal memverifikasi recycle terjadwal pada pukul 04:00 lokal, skip saat sending/aktif 5 menit terakhir, serta auto-recycle round-robin.
func TestRecycleTerjadwal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		locJkt, err := time.LoadLocation("Asia/Jakarta")
		if err != nil {
			t.Fatal(err)
		}

		// Di dalam synctest, jam awal selalu 2000-01-01 00:00:00 UTC (= 07:00 WIB).
		// Jadwalkan sales pukul 07:10 WIB (dalam 10 menit) dan support pukul 07:12 WIB (dalam 12 menit).
		// Juga uji pukul "04:00" lokal lewat timezone "UTC" / offset ke pukul 04:00 hari berikutnya!
		backend := &fakeRecycleBackend{
			instances: []RecycleCandidate{
				{ID: "sales", RecycleAt: "04:00", Timezone: "Asia/Jakarta", Paired: true},
				{ID: "busy-send", RecycleAt: "04:00", Timezone: "Asia/Jakarta", Paired: true},
				{ID: "busy-recent", RecycleAt: "04:00", Timezone: "Asia/Jakarta", Paired: true},
			},
			hasSending: map[string]bool{
				"busy-send": true,
			},
			lastActivity: make(map[string]time.Time),
		}

		gov := &fakeRecycleGov{}
		coord := newRecycleCoordinator(backend, gov)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			coord.Run(ctx)
		}()
		synctest.Wait()

		// Hitung durasi tepat menuju pukul 04:00 Asia/Jakarta berikutnya dari jam synctest saat ini.
		nowJkt := time.Now().In(locJkt)
		next0400 := time.Date(nowJkt.Year(), nowJkt.Month(), nowJkt.Day()+1, 4, 0, 0, 0, locJkt)
		to0358 := next0400.Add(-2 * time.Minute).Sub(time.Now())

		// Maju sampai 03:58 WIB lalu catat aktivitas pada busy-recent (< 5 menit sebelum 04:00)
		// dan aktivitas lama (10 menit lalu) pada sales.
		time.Sleep(to0358)
		synctest.Wait()

		backend.mu.Lock()
		backend.lastActivity["busy-recent"] = time.Now()
		backend.lastActivity["sales"] = time.Now().Add(-10 * time.Minute)
		backend.mu.Unlock()

		// Maju melewati 04:00 WIB -> hanya sales yang di-recycle 1x; busy-send dan busy-recent dilewati hari itu
		time.Sleep(3 * time.Minute)
		synctest.Wait()

		// Bebaskan busy-send & tunggu lewat 04:01..04:05 -> karena sudah dilewati hari itu, tidak boleh jalan susulan
		backend.mu.Lock()
		backend.hasSending["busy-send"] = false
		backend.mu.Unlock()
		time.Sleep(5 * time.Minute)
		synctest.Wait()

		backend.mu.Lock()
		gotRecycled := append([]string(nil), backend.recycled...)
		backend.mu.Unlock()
		if len(gotRecycled) != 1 || gotRecycled[0] != "sales" {
			t.Fatalf("diharapkan hanya [sales] yang di-recycle pukul 04:00 lokal, didapat %v", gotRecycled)
		}

		// Pastikan tidak di-recycle lagi pada hari yang sama, dan baru jalan lagi hari berikutnya
		time.Sleep(24 * time.Hour)
		synctest.Wait()

		backend.mu.Lock()
		gotDay2 := append([]string(nil), backend.recycled...)
		backend.mu.Unlock()
		// Hari ke-2 (t+24h) ketiga instance kini idle -> bertambah 3
		if len(gotDay2) != 4 {
			t.Fatalf("hari ke-2 seluruh 3 instance idle seharusnya ikut recycle terjadwal (total 4), didapat %v", gotDay2)
		}

		// Uji auto-recycle round-robin dan cooldown 6 jam per instance:
		// Memanggil RecycleNextEligible 3x dalam < 6 jam memilih instance bergilir lalu berhenti setelah habis kuota 6h.
		backend.mu.Lock()
		backend.recycled = nil
		backend.mu.Unlock()

		// Set lastAutoRecycle sales ke sekarang supaya sales menunggu 6h
		ok1, err := coord.RecycleNextEligible(ctx)
		if err != nil || !ok1 {
			t.Fatalf("auto-recycle pertama seharusnya berhasil, ok=%v err=%v", ok1, err)
		}
		ok2, err := coord.RecycleNextEligible(ctx)
		if err != nil || !ok2 {
			t.Fatalf("auto-recycle kedua seharusnya berhasil, ok=%v err=%v", ok2, err)
		}
		ok3, err := coord.RecycleNextEligible(ctx)
		if err != nil || !ok3 {
			t.Fatalf("auto-recycle ketiga seharusnya berhasil, ok=%v err=%v", ok3, err)
		}
		// Ke-4 dalam < 6 jam -> semua kena cooldown 6 jam -> false
		ok4, err := coord.RecycleNextEligible(ctx)
		if err != nil || ok4 {
			t.Fatalf("auto-recycle ke-4 dalam < 6 jam seharusnya ditunda cooldown, ok=%v err=%v", ok4, err)
		}

		backend.mu.Lock()
		rr := append([]string(nil), backend.recycled...)
		backend.mu.Unlock()
		if len(rr) != 3 || rr[0] == rr[1] || rr[1] == rr[2] || rr[0] == rr[2] {
			t.Fatalf("round-robin auto-recycle harus memilih 3 instance berbeda secara bergilir, didapat %v", rr)
		}

		cancel()
		<-done
	})
}

// TestOutboxStore_HasSending memastikan query HasSending membaca status sending di tabel outbox secara akurat.
func TestOutboxStore_HasSending(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 2, CacheKiB: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	st := outbox.NewSQLiteStore(j.DB())
	has, err := st.HasSending(ctx, "sales")
	if err != nil || has {
		t.Fatalf("awal kosong seharusnya HasSending=false, has=%v err=%v", has, err)
	}

	nowMs := time.Now().UnixMilli()
	job := &outbox.Job{
		ID:         "msg_sending_1",
		InstanceID: "sales",
		ReqHash:    []byte("h1"),
		WAID:       "3EB0123",
		ToJID:      "628123@s.whatsapp.net",
		Kind:       "text",
		Spec:       outbox.Spec{Type: "text", To: "628123", Text: "hi"},
		Origin:     "api",
		Status:     outbox.StatusSending,
		ExpiresAt:  nowMs + 60000,
		CreatedAt:  nowMs,
		UpdatedAt:  nowMs,
	}
	if err := st.Insert(ctx, job); err != nil {
		t.Fatal(err)
	}

	has, err = st.HasSending(ctx, "sales")
	if err != nil || !has {
		t.Fatalf("dengan job sending seharusnya HasSending=true, has=%v err=%v", has, err)
	}
	other, err := st.HasSending(ctx, "other")
	if err != nil || other {
		t.Fatalf("instance lain seharusnya HasSending=false, other=%v err=%v", other, err)
	}
}
