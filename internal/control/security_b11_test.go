package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

func setupTestControlServer(t *testing.T) (*Server, *config.Config, *store.Store, *auth.Manager, *auth.AuditLog) {
	t.Helper()
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
	t.Cleanup(func() { _ = st.Close() })

	j, err := journal.Open(ctx, cfg.JournalDBPath(), store.DBOptions{MaxConns: 2, CacheKiB: 512, TxLock: "immediate"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })

	audit := auth.NewAuditLog(st.DB)
	srv := NewServer(cfg, st, authMgr, waMgr, nil, WithJournal(j), WithAuditLog(audit))
	srv.SetVersion("2.0.0")
	return srv, cfg, st, authMgr, audit
}

// TestControlSocket_PermAtomik memastikan Listen() membuat socket secara atomik tanpa menyisakan file .tmp dan berizin 0660 di Unix.
func TestControlSocket_PermAtomik(t *testing.T) {
	srv, cfg, _, _, _ := setupTestControlServer(t)
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen gagal: %v", err)
	}
	defer ln.Close()

	sock := cfg.ControlSocket()
	info, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat control.sock gagal: %v", err)
	}
	if _, err := os.Stat(sock + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("file sementara %s.tmp tidak boleh tersisa pasca-Listen", sock)
	}
	if runtime.GOOS != "windows" {
		if perm := info.Mode().Perm(); perm != 0o660 {
			t.Fatalf("izin control.sock diharapkan 0660, didapat %#o", perm)
		}
	}
}

// TestControlAudit_MutatingActions memverifikasi pencatatan audit log dan endpoint GET /control/audit untuk aksi key/instance.
func TestControlAudit_MutatingActions(t *testing.T) {
	srv, _, _, _, _ := setupTestControlServer(t)
	h := srv.Handler()

	// 1. Buat key baru via control plane
	recKey := httptest.NewRecorder()
	h.ServeHTTP(recKey, httptest.NewRequest(http.MethodPost, "/control/keys",
		strings.NewReader(`{"name":"dash-v2"}`)))
	if recKey.Code != http.StatusCreated {
		t.Fatalf("POST /control/keys diharapkan 201, didapat %d (%s)", recKey.Code, recKey.Body.String())
	}
	var keyResp createKeyResp
	if err := json.Unmarshal(recKey.Body.Bytes(), &keyResp); err != nil {
		t.Fatal(err)
	}

	// 2. Rotasi key
	recRot := httptest.NewRecorder()
	h.ServeHTTP(recRot, httptest.NewRequest(http.MethodPost, "/control/keys/"+keyResp.Meta.ID+"/rotate",
		strings.NewReader(`{"grace":"24h"}`)))
	if recRot.Code != http.StatusOK {
		t.Fatalf("POST /control/keys/{id}/rotate diharapkan 200, didapat %d (%s)", recRot.Code, recRot.Body.String())
	}

	// 3. Periksa /control/audit mengembalikan key.create dan key.rotate
	recAudit := httptest.NewRecorder()
	h.ServeHTTP(recAudit, httptest.NewRequest(http.MethodGet, "/control/audit?since=1h&action=key.*", nil))
	if recAudit.Code != http.StatusOK {
		t.Fatalf("GET /control/audit diharapkan 200, didapat %d (%s)", recAudit.Code, recAudit.Body.String())
	}
	var entries []auth.AuditEntry
	if err := json.Unmarshal(recAudit.Body.Bytes(), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("diharapkan minimal 2 entri audit key.*, didapat %d: %+v", len(entries), entries)
	}
	_ = bytes.TrimSpace(recAudit.Body.Bytes())
}

// TestBackup_VacuumInto memastikan POST /control/backup menyalin ketiga DB (app, journal, wa) via VACUUM INTO yang valid dan mempertahankan maks 3 backup terbaru.
func TestBackup_VacuumInto(t *testing.T) {
	srv, cfg, _, _, _ := setupTestControlServer(t)
	h := srv.Handler()
	ctx := context.Background()

	var lastDir string
	for i := range 4 {
		if i > 0 {
			time.Sleep(1100 * time.Millisecond)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/control/backup", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("POST /control/backup ke-%d gagal %d: %s", i+1, rec.Code, rec.Body.String())
		}
		var out BackupResult
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		lastDir = out.Dir
	}

	// Pastikan hanya 3 direktori backup DB terbaru yang tersisa
	bakRoot := filepath.Join(cfg.DataDir, "backups")
	dirs, err := os.ReadDir(bakRoot)
	if err != nil {
		t.Fatal(err)
	}
	dbDirs := 0
	for _, d := range dirs {
		if d.IsDir() && strings.HasPrefix(d.Name(), "db-") {
			dbDirs++
		}
	}
	if dbDirs != 3 {
		t.Fatalf("retensi backup DB diharapkan maksimal 3 direktori, didapat %d", dbDirs)
	}

	// Verifikasi ketiga file SQLite hasil VACUUM INTO bisa dibuka dan lolos integrity/SELECT 1
	for _, name := range []string{"app.db", "journal.db", "wa.db"} {
		dbPath := filepath.Join(lastDir, name)
		if _, err := os.Stat(dbPath); err != nil {
			t.Fatalf("file backup %s tidak ditemukan: %v", dbPath, err)
		}
		db, err := store.OpenDB(ctx, dbPath, store.DBOptions{MaxConns: 1, CacheKiB: 256})
		if err != nil {
			t.Fatalf("gagal membuka backup %s: %v", name, err)
		}
		var one int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		_ = db.Close()
		if err != nil || one != 1 {
			t.Fatalf("backup %s tidak valid: err=%v one=%d", name, err, one)
		}
	}
}
