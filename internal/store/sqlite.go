// Package store menangani akses SQLite untuk metadata aplikasi (app.db) dan database pendukung lainnya.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	// Driver SQLite pure Go agar binary bisa dibangun dengan CGO_ENABLED=0
	_ "modernc.org/sqlite"
)

// ErrNotFound dikembalikan saat baris yang dicari tidak ada.
var ErrNotFound = errors.New("not found")

// DBOptions mengatur parameter pool koneksi dan pragma SQLite per database.
type DBOptions struct {
	MaxConns    int           // ukuran pool maksimum
	CacheKiB    int           // dipakai sebagai cache_size(-CacheKiB)
	IdleTimeout time.Duration // SetConnMaxIdleTime
	TxLock      string        // "" atau "immediate"
	Sync        string        // "NORMAL" atau "FULL"
}

//go:embed migrations/*.sql
var migrationFS embed.FS

// BuildDSN menyusun DSN SQLite dengan urutan pragma yang terprediksi untuk RSS rendah.
func BuildDSN(dbPath string, opt DBOptions) string {
	cacheKiB := opt.CacheKiB
	if cacheKiB <= 0 {
		cacheKiB = 2000
	}
	syncMode := strings.ToUpper(strings.TrimSpace(opt.Sync))
	if syncMode == "" {
		syncMode = "NORMAL"
	}

	dsn := "file:" + filepath.ToSlash(dbPath) +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(" + syncMode + ")" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=foreign_keys(1)" +
		"&_pragma=cache_size(-" + strconv.Itoa(cacheKiB) + ")" +
		"&_pragma=mmap_size(0)" +
		"&_pragma=temp_store(FILE)" +
		"&_pragma=journal_size_limit(4194304)"

	if strings.EqualFold(strings.TrimSpace(opt.TxLock), "immediate") {
		dsn += "&_txlock=immediate"
	}
	return dsn
}

// OpenDB membuka koneksi SQLite dengan pool terbatas dan pragma dari opt.
func OpenDB(ctx context.Context, dbPath string, opt DBOptions) (*sql.DB, error) {
	maxConns := opt.MaxConns
	if maxConns <= 0 {
		maxConns = 1
	}
	idleTimeout := opt.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 5 * time.Minute
	}

	dsn := BuildDSN(dbPath, opt)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxIdleTime(idleTimeout) // lepas cache koneksi yang lama menganggur

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping %s: %w", dbPath, err)
	}
	return db, nil
}

// Store membungkus koneksi app.db.
type Store struct {
	DB *sql.DB
}

// Open membuka app.db dengan opsi opsional lalu menjalankan migrasi yang belum diterapkan.
func Open(ctx context.Context, dbPath string, opts ...DBOptions) (*Store, error) {
	opt := DBOptions{
		MaxConns:    2,
		CacheKiB:    512,
		IdleTimeout: 5 * time.Minute,
		Sync:        "NORMAL",
	}
	if len(opts) > 0 {
		o := opts[0]
		if o.MaxConns > 0 {
			opt.MaxConns = o.MaxConns
		}
		if o.CacheKiB > 0 {
			opt.CacheKiB = o.CacheKiB
		}
		if o.IdleTimeout > 0 {
			opt.IdleTimeout = o.IdleTimeout
		}
		if o.TxLock != "" {
			opt.TxLock = o.TxLock
		}
		if o.Sync != "" {
			opt.Sync = o.Sync
		}
	}

	db, err := OpenDB(ctx, dbPath, opt)
	if err != nil {
		return nil, err
	}
	if err := Migrate(ctx, db, migrationFS, "migrations"); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{DB: db}, nil
}

// Close melakukan checkpoint WAL lalu menutup koneksi.
func (s *Store) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = s.DB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return s.DB.Close()
}

type migrationFile struct {
	version int
	name    string
}

// Migrate menerapkan file migrasi SQL secara berurutan berdasarkan angka prefix dan mencatat versinya di user_version.
func Migrate(ctx context.Context, db *sql.DB, fsys fs.FS, dir string) error {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return fmt.Errorf("baca migrasi %s: %w", dir, err)
	}

	migs := make([]migrationFile, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if !ok || prefix == "" {
			return fmt.Errorf("nama file migrasi harus diawali angka dan underscore: %s", e.Name())
		}
		ver, err := strconv.Atoi(prefix)
		if err != nil || ver <= 0 {
			return fmt.Errorf("prefix versi migrasi tidak valid pada %s", e.Name())
		}
		migs = append(migs, migrationFile{version: ver, name: e.Name()})
	}

	sort.Slice(migs, func(i, j int) bool {
		if migs[i].version != migs[j].version {
			return migs[i].version < migs[j].version
		}
		return migs[i].name < migs[j].name
	})

	// Validasi kontiguitas nomor versi mulai dari 1 sebelum mengeksekusi SQL apa pun
	for i, m := range migs {
		expected := i + 1
		if m.version != expected {
			return fmt.Errorf("urutan versi migrasi tidak kontigu atau ganda pada %s (diharapkan %d, didapat %d)", m.name, expected, m.version)
		}
	}

	var current int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("baca user_version: %w", err)
	}

	for _, m := range migs {
		if m.version <= current {
			continue
		}
		body, err := fs.ReadFile(fsys, path.Join(dir, m.name))
		if err != nil {
			return fmt.Errorf("baca %s: %w", m.name, err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("mulai transaksi migrasi: %w", err)
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("jalankan %s: %w", m.name, err)
		}
		// PRAGMA tidak menerima parameter bind; versi sudah divalidasi sebagai integer positif berurutan
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", m.version)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("set user_version: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migrasi %s: %w", m.name, err)
		}
	}
	return nil
}

// nullString mengubah string kosong menjadi NULL.
func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

// nullInt64 mengubah nilai nol menjadi NULL.
func nullInt64(v int64) sql.NullInt64 {
	return sql.NullInt64{Int64: v, Valid: v > 0}
}
