package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// TestBackupCLI memverifikasi waapi backup --out menghasilkan 3 file DB (app.db, journal.db, wa.db) yang valid dibuka SQLite.
func TestBackupCLI(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}

	st, err := store.Open(ctx, cfg.AppDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	j, err := journal.Open(ctx, cfg.JournalDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j))
	if err != nil {
		t.Fatal(err)
	}
	defer waMgr.Close()

	srv := control.NewServer(cfg, st, authMgr, waMgr, nil, control.WithJournal(j))
	srv.SetVersion(Version)
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen control socket gagal: %v", err)
	}
	httpSrv := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		errCh <- httpSrv.Serve(ln)
	}()
	defer func() {
		_ = httpSrv.Close()
		_ = ln.Close()
		<-errCh
	}()

	t.Setenv("WAAPI_DATA_DIR", tmp)
	outDir := filepath.Join(tmp, "exported-backup")

	stdout, err := captureStdout(t, func() error {
		return Execute([]string{"backup", "--out", outDir, "--json"})
	})
	if err != nil {
		t.Fatalf("waapi backup --out gagal: %v", err)
	}

	var res struct {
		Dir    string   `json:"dir"`
		OutDir string   `json:"out_dir"`
		Files  []string `json:"files"`
	}
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		t.Fatalf("output JSON backup tidak valid: %v (%s)", err, stdout)
	}
	if res.OutDir != outDir || len(res.Files) != 3 {
		t.Fatalf("hasil JSON backup tidak sesuai: %+v", res)
	}

	for _, name := range []string{"app.db", "journal.db", "wa.db"} {
		dbPath := filepath.Join(outDir, name)
		info, err := os.Stat(dbPath)
		if err != nil {
			t.Fatalf("file backup %s tidak ditemukan di --out: %v", dbPath, err)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			t.Fatalf("izin file backup %s diharapkan 0600, didapat %#o", name, info.Mode().Perm())
		}
		db, err := store.OpenDB(ctx, dbPath, store.DBOptions{MaxConns: 1, CacheKiB: 256})
		if err != nil {
			t.Fatalf("gagal membuka DB hasil backup %s: %v", name, err)
		}
		var one int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		_ = db.Close()
		if err != nil || one != 1 {
			t.Fatalf("query validasi DB %s gagal: err=%v one=%d", name, err, one)
		}
	}

	// Verifikasi pemanggilan tanpa --out mengembalikan path backups internal.
	textOut, err := captureStdout(t, func() error {
		return Execute([]string{"backup"})
	})
	if err != nil {
		t.Fatalf("waapi backup default gagal: %v", err)
	}
	if !strings.Contains(textOut, "backups") {
		t.Fatalf("output teks waapi backup harus menampilkan lokasi backups: %q", textOut)
	}
}
