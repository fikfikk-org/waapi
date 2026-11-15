package journal

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/store"
)

func readPromCounterVecValue(t *testing.T, metricName, instanceLabel string) float64 {
	t.Helper()
	var buf bytes.Buffer
	if err := obs.Default.WritePrometheus(&buf); err != nil {
		t.Fatalf("WritePrometheus gagal: %v", err)
	}
	prefix := fmt.Sprintf("%s{instance=%q} ", metricName, instanceLabel)
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(line, prefix) {
			raw := strings.TrimSpace(strings.TrimPrefix(line, prefix))
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				t.Fatalf("parse nilai metrik %q gagal: %v", line, err)
			}
			return v
		}
	}
	return 0
}

// TestJournal_PruneRetensiDanMaxRows memverifikasi pruning event delivered/undelivered, max_rows, dan kenaikan metrik dropped.
func TestJournal_PruneRetensiDanMaxRows(t *testing.T) {
	ctx := context.Background()
	j := openTestJournal(t)

	now := time.Unix(1800000000, 0)
	policy := PrunePolicy{
		ReplayWindow: 1 * time.Hour,
		Retention:    72 * time.Hour,
		MaxRows:      5,
	}

	insertAt := func(inst string, age time.Duration, n int) []int64 {
		seqs := make([]int64, 0, n)
		ts := now.Add(-age)
		for i := 0; i < n; i++ {
			res, err := j.Append(ctx, &Event{
				InstanceID: inst,
				Type:       "message",
				Chat:       "6281@s.whatsapp.net",
				WAID:       fmt.Sprintf("%s-%d-%d", inst, int64(age.Seconds()), i),
				TS:         ts.Unix(),
				ReceivedAt: ts.UnixMilli(),
				Payload:    []byte(`{"text":"prune"}`),
			})
			if err != nil {
				t.Fatalf("Append gagal: %v", err)
			}
			seqs = append(seqs, res.Seq)
		}
		return seqs
	}

	// Instance A ("sales"): punya konsumen aktif pada seq ke-3
	// - 3 event umur 2 jam (delivered, > ReplayWindow 1h -> terhapus step 1, TIDAK dihitung dropped)
	// - 2 event umur 80 jam (> Retention 72h, undelivered seq > 3 -> terhapus step 2, dihitung dropped = 2)
	// - 8 event umur 5 menit (undelivered seq > 3 -> step 3 sisakan 5 baris, hapus 3 tertua, dihitung dropped = 3)
	deliveredSeqs := insertAt("sales", 2*time.Hour, 3)
	if err := j.CommitCursor(ctx, "webhook", "sales", 0, deliveredSeqs[len(deliveredSeqs)-1]); err != nil {
		t.Fatalf("CommitCursor gagal: %v", err)
	}
	insertAt("sales", 80*time.Hour, 2)
	insertAt("sales", 5*time.Minute, 8)

	// Instance B ("noconsumer"): tanpa konsumen aktif -> seluruh event dianggap sudah terkirim
	// - 4 event umur 2 jam (> ReplayWindow 1h -> semua terhapus step 1)
	// - 7 event umur 5 menit (> MaxRows=5 -> terhapus 2 di step 3, tanpa menaikkan dropped)
	insertAt("noconsumer", 2*time.Hour, 4)
	insertAt("noconsumer", 5*time.Minute, 7)

	beforeSales := readPromCounterVecValue(t, "waapi_journal_dropped_undelivered_total", "sales")
	beforeNoCons := readPromCounterVecValue(t, "waapi_journal_dropped_undelivered_total", "noconsumer")

	deleted, err := j.Prune(ctx, policy, now)
	if err != nil {
		t.Fatalf("Prune gagal: %v", err)
	}
	// Total terhapus: sales (3 + 2 + 3 = 8) + noconsumer (4 + 2 = 6) = 14
	if deleted != 14 {
		t.Fatalf("total baris terhapus diharapkan 14, didapat %d", deleted)
	}

	var remainSales, remainNoCons int
	_ = j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE instance_id = 'sales'`).Scan(&remainSales)
	_ = j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE instance_id = 'noconsumer'`).Scan(&remainNoCons)
	if remainSales != 5 || remainNoCons != 5 {
		t.Fatalf("sisa baris setelah MaxRows=5 diharapkan (5, 5), didapat (%d, %d)", remainSales, remainNoCons)
	}

	afterSales := readPromCounterVecValue(t, "waapi_journal_dropped_undelivered_total", "sales")
	afterNoCons := readPromCounterVecValue(t, "waapi_journal_dropped_undelivered_total", "noconsumer")
	if delta := afterSales - beforeSales; delta != 5 {
		t.Fatalf("kenaikan metrik dropped_undelivered untuk sales diharapkan 5, didapat %v", delta)
	}
	if delta := afterNoCons - beforeNoCons; delta != 0 {
		t.Fatalf("noconsumer tanpa konsumen aktif tidak boleh menaikkan dropped_undelivered, delta=%v", delta)
	}
}

// TestPrune_TidakHapusWebhookDead memastikan pruner journal tidak pernah menghapus seq yang masih tercatat di webhook_dead.
func TestPrune_TidakHapusWebhookDead(t *testing.T) {
	ctx := context.Background()
	j, err := Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    2,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	now := time.Unix(1800000000, 0)
	for i := 1; i <= 4; i++ {
		if _, err := j.Append(ctx, &Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       "628111@s.whatsapp.net",
			TS:         now.Add(-48 * time.Hour).Unix(),
			Payload:    []byte(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("Append %d gagal: %v", i, err)
		}
	}
	_, _ = j.db.ExecContext(ctx, `UPDATE events SET received_at = ?`, now.Add(-48*time.Hour).UnixMilli())

	// Tandai seq=2 di webhook_dead; seq lain boleh dipangkas karena retensi & max_rows.
	if _, err := j.db.ExecContext(ctx,
		`INSERT INTO webhook_dead (instance_id, seq, attempts, last_status, last_error, dead_at) VALUES ('sales', 2, 12, 500, 'err', ?)`,
		now.UnixMilli(),
	); err != nil {
		t.Fatalf("insert webhook_dead gagal: %v", err)
	}

	deleted, err := j.Prune(ctx, PrunePolicy{
		Retention:    1 * time.Hour,
		ReplayWindow: 1 * time.Minute,
		MaxRows:      1,
	}, now)
	if err != nil {
		t.Fatalf("Prune gagal: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("event yang boleh dihapus diharapkan 3 (seq=2 dilindungi DLQ), didapat %d", deleted)
	}

	var remainSeq int64
	if err := j.db.QueryRowContext(ctx, `SELECT seq FROM events WHERE instance_id = 'sales'`).Scan(&remainSeq); err != nil {
		t.Fatalf("query sisa event gagal: %v", err)
	}
	if remainSeq != 2 {
		t.Fatalf("event yang tersisa harus seq=2 (DLQ), didapat seq=%d", remainSeq)
	}
}
