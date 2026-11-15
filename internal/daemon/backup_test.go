package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/store"
)

// TestPreV2Backup_SekaliSaja memastikan backup pra-v2 hanya dibuat saat upgrade dari skema v1 dan tidak diulang pada start berikutnya atau data dir kosong.
func TestPreV2Backup_SekaliSaja(t *testing.T) {
	ctx := context.Background()

	// 1. Data dir kosong tidak membuat backup
	emptyCfg := &config.Config{DataDir: t.TempDir()}
	emptyCfg.Normalize()
	if err := BackupPreV2(ctx, emptyCfg); err != nil {
		t.Fatalf("BackupPreV2 pada dir kosong gagal: %v", err)
	}
	matches, _ := filepath.Glob(filepath.Join(emptyCfg.DataDir, "backups", "pre-v2-*"))
	if len(matches) != 0 {
		t.Fatalf("data dir kosong tidak boleh membuat folder backup, ditemukan %v", matches)
	}

	// 2. Data dir v1 (user_version=2 + wa.db) membuat tepat 1 backup berisi app.db dan wa.db
	v1Dir := t.TempDir()
	cfg := &config.Config{DataDir: v1Dir}
	cfg.Normalize()
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs gagal: %v", err)
	}

	appDB, err := store.OpenDB(ctx, cfg.AppDBPath(), store.DBOptions{MaxConns: 1, CacheKiB: 512})
	if err != nil {
		t.Fatalf("buka app.db v1: %v", err)
	}
	m1, err := os.ReadFile(filepath.Join("..", "store", "migrations", "0001_init.sql"))
	if err != nil {
		t.Fatalf("baca 0001_init.sql: %v", err)
	}
	m2, err := os.ReadFile(filepath.Join("..", "store", "migrations", "0002_pairing_event.sql"))
	if err != nil {
		t.Fatalf("baca 0002_pairing_event.sql: %v", err)
	}
	v1FS := fstest.MapFS{
		"migrations/0001_init.sql":          {Data: m1},
		"migrations/0002_pairing_event.sql": {Data: m2},
	}
	if err := store.Migrate(ctx, appDB, v1FS, "migrations"); err != nil {
		_ = appDB.Close()
		t.Fatalf("migrasi v1 gagal: %v", err)
	}
	if _, err := appDB.ExecContext(ctx, `INSERT INTO instances (id, jid, status, webhook_secret, created_at, updated_at) VALUES ('sales', '628111:1@s.whatsapp.net', 'connected', 'whsec_test', 1700000000, 1700000000)`); err != nil {
		_ = appDB.Close()
		t.Fatalf("insert instance v1 gagal: %v", err)
	}
	_ = appDB.Close()

	waDB, err := store.OpenDB(ctx, cfg.WADBPath(), store.DBOptions{MaxConns: 1, CacheKiB: 512})
	if err != nil {
		t.Fatalf("buka wa.db v1: %v", err)
	}
	if _, err := waDB.ExecContext(ctx, `CREATE TABLE whatsmeow_device (jid TEXT PRIMARY KEY)`); err != nil {
		_ = waDB.Close()
		t.Fatalf("buat tabel dummy wa.db: %v", err)
	}
	_ = waDB.Close()

	if err := BackupPreV2(ctx, cfg); err != nil {
		t.Fatalf("BackupPreV2 pada data dir v1 gagal: %v", err)
	}

	backups, err := filepath.Glob(filepath.Join(cfg.DataDir, "backups", "pre-v2-*"))
	if err != nil || len(backups) != 1 {
		t.Fatalf("diharapkan tepat 1 folder backup pre-v2-*, didapat %v (err=%v)", backups, err)
	}

	// Pastikan isi backup app.db dan wa.db dapat dibuka
	bakApp, err := store.OpenDB(ctx, filepath.Join(backups[0], "app.db"), store.DBOptions{MaxConns: 1, CacheKiB: 256})
	if err != nil {
		t.Fatalf("backup app.db tidak dapat dibuka: %v", err)
	}
	var instCount int
	if err := bakApp.QueryRowContext(ctx, `SELECT COUNT(*) FROM instances WHERE id = 'sales'`).Scan(&instCount); err != nil || instCount != 1 {
		_ = bakApp.Close()
		t.Fatalf("data instance pada backup app.db tidak utuh: count=%d err=%v", instCount, err)
	}
	_ = bakApp.Close()

	bakWA, err := store.OpenDB(ctx, filepath.Join(backups[0], "wa.db"), store.DBOptions{MaxConns: 1, CacheKiB: 256})
	if err != nil {
		t.Fatalf("backup wa.db tidak dapat dibuka: %v", err)
	}
	var waCount int
	if err := bakWA.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='whatsmeow_device'`).Scan(&waCount); err != nil || waCount != 1 {
		_ = bakWA.Close()
		t.Fatalf("tabel pada backup wa.db tidak utuh: count=%d err=%v", waCount, err)
	}
	_ = bakWA.Close()

	// Jalankan migrasi v2 penuh via store.Open, lalu start kedua tidak boleh membuat backup lagi
	st, err := store.Open(ctx, cfg.AppDBPath())
	if err != nil {
		t.Fatalf("store.Open upgrade ke v2 gagal: %v", err)
	}
	_ = st.Close()

	if err := BackupPreV2(ctx, cfg); err != nil {
		t.Fatalf("BackupPreV2 kedua gagal: %v", err)
	}
	backupsAfter, _ := filepath.Glob(filepath.Join(cfg.DataDir, "backups", "pre-v2-*"))
	if len(backupsAfter) != 1 {
		t.Fatalf("start kedua tidak boleh membuat backup baru, didapat %v", backupsAfter)
	}
}
