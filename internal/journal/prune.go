package journal

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/fikfikk-org/waapi/internal/obs"
)

const (
	pruneChunkSize      = 1000
	defaultPruneEvery   = 5 * time.Minute
	defaultRetention    = 72 * time.Hour
	defaultReplayWindow = 1 * time.Hour
	defaultMaxRows      = 50000
)

// PrunePolicy mengatur retensi event dan batas jumlah baris per instance pada journal.db.
type PrunePolicy struct {
	Retention    time.Duration // umur maksimal event yang belum terkirim (default 72h)
	ReplayWindow time.Duration // umur maksimal event yang sudah terkirim (default 1h)
	MaxRows      int           // batas baris maksimal per instance (default 50000)
	Interval     time.Duration // interval pruner periodik (default 5m)
}

func (p PrunePolicy) withDefaults() PrunePolicy {
	if p.Retention <= 0 {
		p.Retention = defaultRetention
	}
	if p.ReplayWindow <= 0 {
		p.ReplayWindow = defaultReplayWindow
	}
	if p.MaxRows <= 0 {
		p.MaxRows = defaultMaxRows
	}
	if p.Interval <= 0 {
		p.Interval = defaultPruneEvery
	}
	return p
}

// Prune menghapus event lama dan kelebihan kapasitas per instance secara bertahap (maks 1000 baris per transaksi).
func (j *Journal) Prune(ctx context.Context, policy PrunePolicy, now time.Time) (int64, error) {
	if j == nil || j.db == nil {
		return 0, nil
	}
	policy = policy.withDefaults()
	if now.IsZero() {
		now = time.Now()
	}

	instances, err := j.listEventInstances(ctx)
	if err != nil {
		return 0, err
	}
	if len(instances) == 0 {
		return 0, nil
	}

	replayCutoff := now.Add(-policy.ReplayWindow).UnixMilli()
	retentionCutoff := now.Add(-policy.Retention).UnixMilli()

	var totalDeleted int64
	for _, inst := range instances {
		if ctx.Err() != nil {
			return totalDeleted, ctx.Err()
		}

		n, dropped, err := j.pruneInstance(ctx, inst, replayCutoff, retentionCutoff, policy.MaxRows)
		totalDeleted += n
		if dropped > 0 {
			obs.MetricJournalDroppedUndeliveredTotal.With(inst).Add(float64(dropped))
			slog.Warn("event journal belum terkirim terhapus akibat retensi atau batas baris",
				"instance", inst,
				"dropped", dropped,
			)
		}
		if err != nil {
			return totalDeleted, err
		}
	}
	return totalDeleted, nil
}

func (j *Journal) listEventInstances(ctx context.Context) ([]string, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT DISTINCT instance_id FROM events`)
	if err != nil {
		return nil, fmt.Errorf("daftar instance journal: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan instance journal: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (j *Journal) pruneInstance(ctx context.Context, inst string, replayCutoff, retentionCutoff int64, maxRows int) (int64, int64, error) {
	var minSeq sql.NullInt64
	if err := j.db.QueryRowContext(ctx,
		`SELECT MIN(seq) FROM cursors WHERE instance_id = ? AND enabled = 1`,
		inst,
	).Scan(&minSeq); err != nil {
		return 0, 0, fmt.Errorf("baca cursor aktif %s: %w", inst, err)
	}
	hasConsumer := minSeq.Valid

	var deleted, dropped int64

	// Langkah 1: hapus event yang sudah terkirim dan melewati WAAPI_JOURNAL_REPLAY_WINDOW
	for {
		if ctx.Err() != nil {
			return deleted, dropped, ctx.Err()
		}
		var (
			res sql.Result
			err error
		)
		if !hasConsumer {
			// Tanpa konsumen aktif, seluruh event dianggap sudah terkirim
			res, err = j.db.ExecContext(ctx, `
				DELETE FROM events
				WHERE seq IN (
					SELECT seq FROM events
					WHERE instance_id = ? AND received_at < ?
					ORDER BY seq ASC LIMIT ?
				)`,
				inst, replayCutoff, pruneChunkSize,
			)
		} else {
			res, err = j.db.ExecContext(ctx, `
				DELETE FROM events
				WHERE seq IN (
					SELECT seq FROM events
					WHERE instance_id = ? AND seq <= ? AND received_at < ?
					ORDER BY seq ASC LIMIT ?
				)`,
				inst, minSeq.Int64, replayCutoff, pruneChunkSize,
			)
		}
		if err != nil {
			return deleted, dropped, fmt.Errorf("prune replay window %s: %w", inst, err)
		}
		n, _ := res.RowsAffected()
		deleted += n
		if n < pruneChunkSize {
			break
		}
	}

	// Langkah 2: hapus event yang melewati WAAPI_JOURNAL_RETENTION (meski belum terkirim)
	for {
		if ctx.Err() != nil {
			return deleted, dropped, ctx.Err()
		}
		n, chunkDropped, err := j.deleteReturningCount(ctx, `
			DELETE FROM events
			WHERE seq IN (
				SELECT seq FROM events
				WHERE instance_id = ? AND received_at < ?
				ORDER BY seq ASC LIMIT ?
			) RETURNING seq`,
			hasConsumer, minSeq.Int64, inst, retentionCutoff, pruneChunkSize,
		)
		if err != nil {
			return deleted, dropped, fmt.Errorf("prune retensi %s: %w", inst, err)
		}
		deleted += n
		dropped += chunkDropped
		if n < pruneChunkSize {
			break
		}
	}

	// Langkah 3: hapus kelebihan baris di atas WAAPI_JOURNAL_MAX_ROWS (yang tertua lebih dulu)
	var totalRows int
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE instance_id = ?`, inst).Scan(&totalRows); err != nil {
		return deleted, dropped, fmt.Errorf("hitung baris journal %s: %w", inst, err)
	}
	excess := totalRows - maxRows
	for excess > 0 {
		if ctx.Err() != nil {
			return deleted, dropped, ctx.Err()
		}
		limit := min(excess, pruneChunkSize)
		n, chunkDropped, err := j.deleteReturningCount(ctx, `
			DELETE FROM events
			WHERE seq IN (
				SELECT seq FROM events
				WHERE instance_id = ?
				ORDER BY seq ASC LIMIT ?
			) RETURNING seq`,
			hasConsumer, minSeq.Int64, inst, limit,
		)
		if err != nil {
			return deleted, dropped, fmt.Errorf("prune max_rows %s: %w", inst, err)
		}
		deleted += n
		dropped += chunkDropped
		if n == 0 {
			break
		}
		excess -= int(n)
	}

	return deleted, dropped, nil
}

func (j *Journal) deleteReturningCount(ctx context.Context, query string, hasConsumer bool, minCursor int64, args ...any) (int64, int64, error) {
	rows, err := j.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var deleted, undelivered int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return deleted, undelivered, err
		}
		deleted++
		if hasConsumer && seq > minCursor {
			undelivered++
		}
	}
	return deleted, undelivered, rows.Err()
}

// RunPruner menjalankan Prune secara periodik di latar belakang sampai ctx dibatalkan.
func (j *Journal) RunPruner(ctx context.Context, policy PrunePolicy) {
	if j == nil || j.db == nil {
		return
	}
	policy = policy.withDefaults()
	ticker := time.NewTicker(policy.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if _, err := j.Prune(pCtx, policy, time.Now()); err != nil && !errorsIsCanceled(ctx, err) {
				slog.Warn("pruner journal gagal", "err", err)
			}
			cancel()
		}
	}
}

func errorsIsCanceled(ctx context.Context, err error) bool {
	return ctx.Err() != nil || err == context.Canceled
}
