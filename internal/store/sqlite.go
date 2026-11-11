// Package store menangani akses SQLite untuk metadata aplikasi (app.db).
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	// Driver SQLite pure Go agar binary bisa dibangun dengan CGO_ENABLED=0
	_ "modernc.org/sqlite"
)

// ErrNotFound dikembalikan saat baris yang dicari tidak ada.
var ErrNotFound = errors.New("not found")

// pragmas berisi tuning SQLite untuk RSS rendah dan terprediksi.
const pragmas = "?_pragma=journal_mode(WAL)" +
	"&_pragma=synchronous(NORMAL)" + // aman bersama WAL, fsync hanya saat checkpoint
	"&_pragma=busy_timeout(5000)" + // tunggu lock hingga 5 detik sebelum SQLITE_BUSY
	"&_pragma=foreign_keys(1)" + // wajib untuk whatsmeow sqlstore dan cascade app.db
	"&_pragma=cache_size(-2000)" + // page cache ~2 MB per koneksi
	"&_pragma=mmap_size(0)" + // matikan mmap agar RSS terprediksi
	"&_pragma=temp_store(FILE)" + // tabel sementara di disk, bukan RAM
	"&_pragma=journal_size_limit(4194304)" // potong WAL ke 4 MB setelah checkpoint

//go:embed migrations/*.sql
var migrationFS embed.FS

// OpenDB membuka SQLite dengan pool kecil karena cache_size berlaku per koneksi.
func OpenDB(ctx context.Context, path string, maxConns int) (*sql.DB, error) {
	dsn := "file:" + filepath.ToSlash(path) + pragmas
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxIdleTime(5 * time.Minute) // lepas cache koneksi yang lama menganggur
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping %s: %w", path, err)
	}
	return db, nil
}

// Store membungkus koneksi app.db.
type Store struct {
	DB *sql.DB
}

// Open membuka app.db lalu menjalankan migrasi yang belum diterapkan.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := OpenDB(ctx, path, 2)
	if err != nil {
		return nil, err
	}
	s := &Store{DB: db}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close melakukan checkpoint WAL lalu menutup koneksi.
func (s *Store) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = s.DB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return s.DB.Close()
}

// migrate menerapkan file migrasi berurutan dan mencatat versinya di user_version.
func (s *Store) migrate(ctx context.Context) error {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("baca migrasi: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	var current int
	if err := s.DB.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("baca user_version: %w", err)
	}

	for i, name := range names {
		version := i + 1
		if version <= current {
			continue
		}
		body, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("baca %s: %w", name, err)
		}
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("mulai transaksi migrasi: %w", err)
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("jalankan %s: %w", name, err)
		}
		// PRAGMA tidak menerima parameter bind, versi berasal dari indeks internal sehingga aman
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("set user_version: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migrasi %s: %w", name, err)
		}
	}
	return nil
}

// nullString mengubah string kosong menjadi NULL.
func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

