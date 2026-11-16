package wa

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/governor"
)

const (
	recycleTickInterval    = 30 * time.Second
	recycleActivityWindow  = 5 * time.Minute
	autoRecycleMinInterval = 6 * time.Hour
)

// RecycleCandidate merangkum konfigurasi jadwal recycle serta kesiapan pairing sebuah instance.
type RecycleCandidate struct {
	ID        string
	RecycleAt string
	Timezone  string
	Paired    bool
}

type recycleBackend interface {
	RecycleCandidates(ctx context.Context) ([]RecycleCandidate, error)
	HasSending(ctx context.Context, instanceID string) (bool, error)
	LastActivity(instanceID string) time.Time
	RecycleInstance(ctx context.Context, instanceID string) error
}

type recycleCoordinator struct {
	backend recycleBackend
	gov     governor.Source

	mu        sync.Mutex
	dailyDone map[string]string    // instance_id -> YYYY-MM-DD zona lokal yang sudah diproses/dilewati hari ini
	lastAuto  map[string]time.Time // instance_id -> waktu auto-recycle terakhir akibat level Red
	rrCursor  int
}

func newRecycleCoordinator(backend recycleBackend, gov governor.Source) *recycleCoordinator {
	if gov == nil {
		gov = governor.AlwaysGreen
	}
	return &recycleCoordinator{
		backend:   backend,
		gov:       gov,
		dailyDone: make(map[string]string),
		lastAuto:  make(map[string]time.Time),
	}
}

// Run menjalankan loop pemeriksaan recycle terjadwal hingga ctx dibatalkan.
func (c *recycleCoordinator) Run(ctx context.Context) {
	if c == nil || c.backend == nil {
		return
	}
	ticker := time.NewTicker(recycleTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			c.checkScheduledTick(ctx, now)
		}
	}
}

func (c *recycleCoordinator) checkScheduledTick(ctx context.Context, now time.Time) {
	cands, err := c.backend.RecycleCandidates(ctx)
	if err != nil {
		slog.Warn("gagal membaca kandidat recycle terjadwal", "err", err)
		return
	}

	for _, cand := range cands {
		targetHHMM := strings.TrimSpace(cand.RecycleAt)
		if !cand.Paired || targetHHMM == "" {
			continue
		}

		loc := loadInstanceLocation(cand.Timezone)
		localNow := now.In(loc)
		if localNow.Format("15:04") != targetHHMM {
			continue
		}
		today := localNow.Format("2006-01-02")

		c.mu.Lock()
		if c.dailyDone[cand.ID] == today {
			c.mu.Unlock()
			continue
		}
		// Batasi ukuran map harian agar tidak tumbuh melebihi batas instance aktif.
		if len(c.dailyDone) > 512 {
			clear(c.dailyDone)
		}
		// Tandai hari ini sebelum cek sibuk sehingga instance sibuk dilewati sampai jadwal besok.
		c.dailyDone[cand.ID] = today
		c.mu.Unlock()

		if sending, sErr := c.backend.HasSending(ctx, cand.ID); sErr != nil {
			slog.Warn("gagal memeriksa job sending sebelum recycle terjadwal", "instance", cand.ID, "err", sErr)
			continue
		} else if sending {
			slog.Info("recycle terjadwal dilewati karena ada job sending aktif", "instance", cand.ID, "recycle_at", targetHHMM)
			continue
		}

		if lastAct := c.backend.LastActivity(cand.ID); !lastAct.IsZero() && now.Sub(lastAct) <= recycleActivityWindow {
			slog.Info("recycle terjadwal dilewati karena ada aktivitas pesan 5 menit terakhir",
				"instance", cand.ID, "recycle_at", targetHHMM)
			continue
		}

		if err := c.backend.RecycleInstance(ctx, cand.ID); err != nil {
			slog.Warn("recycle terjadwal gagal", "instance", cand.ID, "err", err)
			continue
		}
		slog.Info("recycle terjadwal selesai dijalankan", "instance", cand.ID, "recycle_at", targetHHMM)
	}
}

// RecycleNextEligible memilih satu instance secara round-robin (>= 6 jam per instance, tanpa job sending) dan me-recycle-nya.
func (c *recycleCoordinator) RecycleNextEligible(ctx context.Context) (bool, error) {
	if c == nil || c.backend == nil {
		return false, nil
	}
	cands, err := c.backend.RecycleCandidates(ctx)
	if err != nil {
		return false, err
	}
	if len(cands) == 0 {
		return false, nil
	}

	now := time.Now()
	c.mu.Lock()
	startIdx := c.rrCursor % len(cands)
	c.mu.Unlock()

	for offset := 0; offset < len(cands); offset++ {
		idx := (startIdx + offset) % len(cands)
		cand := cands[idx]
		if !cand.Paired {
			continue
		}

		c.mu.Lock()
		last := c.lastAuto[cand.ID]
		c.mu.Unlock()
		if !last.IsZero() && now.Sub(last) < autoRecycleMinInterval {
			continue
		}

		sending, sErr := c.backend.HasSending(ctx, cand.ID)
		if sErr != nil {
			slog.Warn("gagal memeriksa status sending sebelum auto-recycle", "instance", cand.ID, "err", sErr)
			continue
		}
		if sending {
			continue
		}

		c.mu.Lock()
		if len(c.lastAuto) > 512 {
			clear(c.lastAuto)
		}
		c.lastAuto[cand.ID] = now
		c.rrCursor = (idx + 1) % len(cands)
		c.mu.Unlock()

		if err := c.backend.RecycleInstance(ctx, cand.ID); err != nil {
			slog.Warn("auto-recycle instance pada level Red gagal", "instance", cand.ID, "err", err)
			return false, err
		}
		slog.Warn("auto-recycle instance dijalankan karena memori Red >= 10 menit", "instance", cand.ID)
		return true, nil
	}
	return false, nil
}

func loadInstanceLocation(tz string) *time.Location {
	tz = strings.TrimSpace(tz)
	if tz != "" {
		if loc, err := time.LoadLocation(tz); err == nil {
			return loc
		}
	}
	if jkt, err := time.LoadLocation("Asia/Jakarta"); err == nil {
		return jkt
	}
	return time.UTC
}

type managerRecycleBackend struct {
	mgr *Manager
}

func (b managerRecycleBackend) RecycleCandidates(ctx context.Context) ([]RecycleCandidate, error) {
	rows, err := b.mgr.store.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]RecycleCandidate, 0, len(rows))
	for _, row := range rows {
		paired := strings.TrimSpace(row.JID) != ""
		if inst, ok := b.mgr.Get(row.ID); ok && inst.getJID() != "" {
			paired = true
		}
		out = append(out, RecycleCandidate{
			ID:        row.ID,
			RecycleAt: row.Settings.RecycleAt,
			Timezone:  row.Settings.Timezone,
			Paired:    paired,
		})
	}
	return out, nil
}

func (b managerRecycleBackend) HasSending(ctx context.Context, instanceID string) (bool, error) {
	if b.mgr.outbox == nil {
		return false, nil
	}
	return b.mgr.outbox.HasSending(ctx, instanceID)
}

func (b managerRecycleBackend) LastActivity(instanceID string) time.Time {
	inst, ok := b.mgr.Get(instanceID)
	if !ok {
		return time.Time{}
	}
	return inst.lastActivityTime()
}

func (b managerRecycleBackend) RecycleInstance(ctx context.Context, instanceID string) error {
	_, err := b.mgr.Recycle(ctx, instanceID)
	return err
}
