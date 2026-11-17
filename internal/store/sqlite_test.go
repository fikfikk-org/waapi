package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

// TestBuildDSN_PragmaDanTxLock memastikan BuildDSN menyusun pragma sesuai opsi dan hanya menyertakan _txlock saat immediate.
func TestBuildDSN_PragmaDanTxLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	dsnNormal := BuildDSN(path, DBOptions{
		CacheKiB: 512,
		Sync:     "NORMAL",
	})
	wantOrder := []string{
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(NORMAL)",
		"_pragma=busy_timeout(5000)",
		"_pragma=foreign_keys(1)",
		"_pragma=cache_size(-512)",
		"_pragma=mmap_size(0)",
		"_pragma=temp_store(FILE)",
		"_pragma=journal_size_limit(4194304)",
	}
	prevIdx := -1
	for _, token := range wantOrder {
		idx := strings.Index(dsnNormal, token)
		if idx < 0 {
			t.Fatalf("DSN tidak mengandung %q: %s", token, dsnNormal)
		}
		if idx <= prevIdx {
			t.Fatalf("urutan pragma salah pada %q dalam DSN: %s", token, dsnNormal)
		}
		prevIdx = idx
	}
	if strings.Contains(dsnNormal, "_txlock=immediate") {
		t.Fatalf("DSN tidak boleh mengandung _txlock=immediate bila TxLock kosong: %s", dsnNormal)
	}

	dsnImm := BuildDSN(path, DBOptions{
		CacheKiB: 512,
		TxLock:   "immediate",
		Sync:     "FULL",
	})
	if !strings.Contains(dsnImm, "_pragma=synchronous(FULL)") {
		t.Fatalf("DSN seharusnya mengandung synchronous(FULL): %s", dsnImm)
	}
	if !strings.Contains(dsnImm, "&_txlock=immediate") {
		t.Fatalf("DSN seharusnya mengandung &_txlock=immediate: %s", dsnImm)
	}
}

// TestMigrate_DuaDBTerpisah memastikan Migrate dapat menjalankan skema berbeda pada dua DB secara idempoten.
func TestMigrate_DuaDBTerpisah(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	db1, err := OpenDB(ctx, filepath.Join(tmp, "db1.db"), DBOptions{MaxConns: 1, CacheKiB: 256})
	if err != nil {
		t.Fatalf("OpenDB db1 gagal: %v", err)
	}
	defer db1.Close()

	db2, err := OpenDB(ctx, filepath.Join(tmp, "db2.db"), DBOptions{MaxConns: 1, CacheKiB: 256})
	if err != nil {
		t.Fatalf("OpenDB db2 gagal: %v", err)
	}
	defer db2.Close()

	fs1 := fstest.MapFS{
		"migrations/0001_a.sql": {Data: []byte("CREATE TABLE a1 (id INTEGER PRIMARY KEY);")},
		"migrations/0002_b.sql": {Data: []byte("CREATE TABLE a2 (id INTEGER PRIMARY KEY);")},
	}
	fs2 := fstest.MapFS{
		"migrations/0001_x.sql": {Data: []byte("CREATE TABLE x1 (id INTEGER PRIMARY KEY);")},
	}

	if err := Migrate(ctx, db1, fs1, "migrations"); err != nil {
		t.Fatalf("Migrate db1 gagal: %v", err)
	}
	if err := Migrate(ctx, db2, fs2, "migrations"); err != nil {
		t.Fatalf("Migrate db2 gagal: %v", err)
	}

	// Jalankan ulang untuk memastikan idempotensi
	if err := Migrate(ctx, db1, fs1, "migrations"); err != nil {
		t.Fatalf("Migrate ulang db1 gagal: %v", err)
	}
	if err := Migrate(ctx, db2, fs2, "migrations"); err != nil {
		t.Fatalf("Migrate ulang db2 gagal: %v", err)
	}

	var v1, v2 int
	if err := db1.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v1); err != nil {
		t.Fatalf("baca user_version db1: %v", err)
	}
	if err := db2.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v2); err != nil {
		t.Fatalf("baca user_version db2: %v", err)
	}
	if v1 != 2 {
		t.Fatalf("user_version db1 diharapkan 2, didapat %d", v1)
	}
	if v2 != 1 {
		t.Fatalf("user_version db2 diharapkan 1, didapat %d", v2)
	}
}

// TestMigrate_CelahNomorDitolak memastikan Migrate menolak daftar migrasi yang nomornya melompat sebelum mengeksekusi apa pun.
func TestMigrate_CelahNomorDitolak(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDB(ctx, filepath.Join(t.TempDir(), "gap.db"), DBOptions{MaxConns: 1, CacheKiB: 256})
	if err != nil {
		t.Fatalf("OpenDB gagal: %v", err)
	}
	defer db.Close()

	fsGap := fstest.MapFS{
		"migrations/0001_init.sql":  {Data: []byte("CREATE TABLE harusnya_tidak_ada (id INTEGER PRIMARY KEY);")},
		"migrations/0003_loncat.sql": {Data: []byte("CREATE TABLE lain (id INTEGER PRIMARY KEY);")},
	}
	if err := Migrate(ctx, db, fsGap, "migrations"); err == nil {
		t.Fatal("Migrate seharusnya mengembalikan error saat nomor migrasi melompat")
	}

	var ver int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&ver); err != nil {
		t.Fatalf("baca user_version: %v", err)
	}
	if ver != 0 {
		t.Fatalf("user_version seharusnya tetap 0, didapat %d", ver)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='harusnya_tidak_ada'").Scan(&count); err != nil {
		t.Fatalf("cek sqlite_master: %v", err)
	}
	if count != 0 {
		t.Fatal("tabel dari 0001 tidak boleh dibuat jika validasi nomor migrasi gagal")
	}
}

// TestMigrate_DataDirV1KeV2 memastikan upgrade dari skema v1 (0001+0002) ke v2 mengisi settings '{}' dan paired_at sesuai jid.
func TestMigrate_DataDirV1KeV2(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "v1.db")

	db, err := OpenDB(ctx, dbPath, DBOptions{MaxConns: 1, CacheKiB: 512})
	if err != nil {
		t.Fatalf("OpenDB gagal: %v", err)
	}
	defer db.Close()

	m1, err := migrationFS.ReadFile("migrations/0001_init.sql")
	if err != nil {
		t.Fatalf("baca 0001_init.sql: %v", err)
	}
	m2, err := migrationFS.ReadFile("migrations/0002_pairing_event.sql")
	if err != nil {
		t.Fatalf("baca 0002_pairing_event.sql: %v", err)
	}

	v1FS := fstest.MapFS{
		"migrations/0001_init.sql":          {Data: m1},
		"migrations/0002_pairing_event.sql": {Data: m2},
	}
	if err := Migrate(ctx, db, v1FS, "migrations"); err != nil {
		t.Fatalf("Migrate v1 gagal: %v", err)
	}

	const createdAt = int64(1700000000)
	_, err = db.ExecContext(ctx, `
		INSERT INTO instances (id, jid, status, webhook_secret, created_at, updated_at)
		VALUES ('lama', '628123456789:1@s.whatsapp.net', 'connected', 'whsec_lama', ?, ?),
		       ('baru', NULL, 'created', 'whsec_baru', ?, ?)`,
		createdAt, createdAt, createdAt+10, createdAt+10)
	if err != nil {
		t.Fatalf("insert instance v1 gagal: %v", err)
	}

	if err := Migrate(ctx, db, migrationFS, "migrations"); err != nil {
		t.Fatalf("Migrate penuh ke v2 gagal: %v", err)
	}

	var ver int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&ver); err != nil {
		t.Fatalf("baca user_version: %v", err)
	}
	if ver != 5 {
		t.Fatalf("user_version diharapkan 5, didapat %d", ver)
	}

	var settingsLama string
	var pairedLama sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT settings, paired_at FROM instances WHERE id = 'lama'").Scan(&settingsLama, &pairedLama); err != nil {
		t.Fatalf("query instance lama: %v", err)
	}
	if settingsLama != "{}" {
		t.Fatalf("settings instance lama diharapkan '{}', didapat %q", settingsLama)
	}
	if !pairedLama.Valid || pairedLama.Int64 != createdAt {
		t.Fatalf("paired_at instance ber-JID diharapkan %d, didapat %+v", createdAt, pairedLama)
	}

	var settingsBaru string
	var pairedBaru sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT settings, paired_at FROM instances WHERE id = 'baru'").Scan(&settingsBaru, &pairedBaru); err != nil {
		t.Fatalf("query instance baru: %v", err)
	}
	if settingsBaru != "{}" {
		t.Fatalf("settings instance baru diharapkan '{}', didapat %q", settingsBaru)
	}
	if pairedBaru.Valid {
		t.Fatalf("paired_at instance tanpa JID seharusnya NULL, didapat %d", pairedBaru.Int64)
	}
}

// TestUpdateInstanceStatus_PairedAt memastikan paired_at terisi saat pairing, tetap saat reconnect JID sama, dan reset saat JID berganti.
func TestUpdateInstanceStatus_PairedAt(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("Open gagal: %v", err)
	}
	defer st.Close()

	if err := st.CreateInstance(ctx, Instance{ID: "sales", WebhookSecret: "whsec_1"}); err != nil {
		t.Fatalf("CreateInstance gagal: %v", err)
	}
	if err := st.UpdateInstanceStatus(ctx, "sales", StatusConnected, "628111:1@s.whatsapp.net"); err != nil {
		t.Fatalf("UpdateInstanceStatus pertama gagal: %v", err)
	}
	inst1, err := st.GetInstance(ctx, "sales")
	if err != nil || inst1.PairedAt <= 0 {
		t.Fatalf("paired_at seharusnya terisi setelah pairing: %+v, err=%v", inst1, err)
	}

	// Set manual paired_at ke masa lalu untuk membedakan reconnect vs ganti JID
	const oldPaired = int64(1700000123)
	if _, err := st.DB.ExecContext(ctx, `UPDATE instances SET paired_at = ? WHERE id = 'sales'`, oldPaired); err != nil {
		t.Fatalf("update paired_at manual gagal: %v", err)
	}
	if err := st.UpdateInstanceStatus(ctx, "sales", StatusConnected, "628111:1@s.whatsapp.net"); err != nil {
		t.Fatalf("UpdateInstanceStatus reconnect gagal: %v", err)
	}
	inst2, err := st.GetInstance(ctx, "sales")
	if err != nil || inst2.PairedAt != oldPaired {
		t.Fatalf("reconnect dengan JID sama harus mempertahankan paired_at (%d), didapat %d", oldPaired, inst2.PairedAt)
	}

	// Ganti JID baru harus me-reset paired_at
	if err := st.UpdateInstanceStatus(ctx, "sales", StatusConnected, "628222:1@s.whatsapp.net"); err != nil {
		t.Fatalf("UpdateInstanceStatus ganti JID gagal: %v", err)
	}
	inst3, err := st.GetInstance(ctx, "sales")
	if err != nil || inst3.PairedAt <= oldPaired {
		t.Fatalf("ganti JID harus me-reset paired_at menjadi lebih baru dari %d, didapat %d", oldPaired, inst3.PairedAt)
	}
}
