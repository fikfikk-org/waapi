package wa

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/store"
)

// TestManager_StatusTidakMembuatInstance memastikan operasi baca Status/ListStatuses tidak membuat instance maupun goroutine baru (R17).
func TestManager_StatusTidakMembuatInstance(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	cfg := &config.Config{DataDir: tmpDir}
	cfg.Normalize()
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs gagal: %v", err)
	}

	st, err := store.Open(ctx, filepath.Join(tmpDir, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	mgr, err := NewManager(ctx, cfg, st, nil)
	if err != nil {
		t.Fatalf("NewManager gagal: %v", err)
	}
	defer mgr.Close()

	// Sisipkan baris langsung ke app.db tanpa melewati mgr.Create
	if err := st.CreateInstance(ctx, store.Instance{
		ID:     "siluman",
		Status: store.StatusCreated,
	}); err != nil {
		t.Fatalf("CreateInstance langsung gagal: %v", err)
	}

	beforeGoroutines := runtime.NumGoroutine()

	if _, err := mgr.Status(ctx, "siluman"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Status pada instance yang tidak terdaftar di Manager harus ErrNotFound, didapat %v", err)
	}
	if _, exists := mgr.Get("siluman"); exists {
		t.Fatal("Status tidak boleh mendaftarkan instance baru ke map Manager")
	}

	// ListStatuses juga tidak boleh membuat instance baru
	list, err := mgr.ListStatuses(ctx)
	if err != nil {
		t.Fatalf("ListStatuses gagal: %v", err)
	}
	if len(list) != 1 || list[0].ID != "siluman" {
		t.Fatalf("ListStatuses diharapkan membaca baris dari DB tanpa mutasi Manager, didapat %+v", list)
	}
	if _, exists := mgr.Get("siluman"); exists {
		t.Fatal("ListStatuses tidak boleh mendaftarkan instance baru ke map Manager")
	}

	afterGoroutines := runtime.NumGoroutine()
	if afterGoroutines != beforeGoroutines {
		t.Fatalf("jumlah goroutine berubah akibat operasi baca Status/ListStatuses: sebelum=%d, sesudah=%d", beforeGoroutines, afterGoroutines)
	}
}
