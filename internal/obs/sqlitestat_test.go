package obs

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fikfikk-org/waapi/internal/store"
)

// TestCollectDBStats_SQLiteNyata memastikan statistik sqlite.DBStatus terbaca pada koneksi modernc sqlite nyata.
func TestCollectDBStats_SQLiteNyata(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")

	st, err := store.Open(ctx, dbPath, store.DBOptions{MaxConns: 2, CacheKiB: 512})
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	// Jalankan satu query nyata agar page cache SQLite terisi
	var n int
	if err := st.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM instances").Scan(&n); err != nil {
		t.Fatalf("query SELECT gagal: %v", err)
	}

	got := CollectDBStats(ctx, "app.db", st.DB, dbPath)
	if got.SampledConns < 1 {
		t.Fatalf("SampledConns diharapkan >= 1, didapat %d", got.SampledConns)
	}
	if got.CacheUsed <= 0 {
		t.Fatalf("CacheUsed diharapkan > 0 setelah query, didapat %d", got.CacheUsed)
	}
	if got.FileBytes <= 0 {
		t.Fatalf("FileBytes diharapkan > 0, didapat %d", got.FileBytes)
	}
}
