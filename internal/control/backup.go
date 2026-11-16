package control

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const maxRetainedDBBackups = 3

// BackupResult merangkum direktori dan daftar file hasil backup VACUUM INTO.
type BackupResult struct {
	Dir   string   `json:"dir"`
	Files []string `json:"files"`
}

type backupDBTarget struct {
	name string
	db   *sql.DB
}

func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	if s.cfg == nil || s.store == nil || s.store.DB == nil {
		writeErr(w, http.StatusServiceUnavailable, errControlNotReady("backup database belum siap"))
		return
	}

	targets := []backupDBTarget{
		{name: "app.db", db: s.store.DB},
	}
	if s.journal != nil && s.journal.DB() != nil {
		targets = append(targets, backupDBTarget{name: "journal.db", db: s.journal.DB()})
	}
	if s.wa != nil && s.wa.WADB() != nil {
		targets = append(targets, backupDBTarget{name: "wa.db", db: s.wa.WADB()})
	}

	res, err := createDBBackups(r.Context(), s.cfg.DataDir, s.version, targets)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	s.recordAudit(r.Context(), "backup.create", filepath.Base(res.Dir), fmt.Sprintf("files=%d", len(res.Files)))
	writeJSON(w, http.StatusOK, res)
}

func createDBBackups(ctx context.Context, dataDir, version string, targets []backupDBTarget) (BackupResult, error) {
	backupsRoot := filepath.Join(dataDir, "backups")
	if err := os.MkdirAll(backupsRoot, 0o700); err != nil {
		return BackupResult{}, fmt.Errorf("buat direktori backups: %w", err)
	}

	cleanVer := strings.TrimPrefix(strings.TrimSpace(version), "v")
	if cleanVer == "" {
		cleanVer = "dev"
	}
	cleanVer = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, cleanVer)

	ts := time.Now().UTC().Format("20060102-150405")
	dirName := fmt.Sprintf("db-v%s-%s", cleanVer, ts)
	dstDir := filepath.Join(backupsRoot, dirName)
	for suffix := 1; ; suffix++ {
		if _, err := os.Stat(dstDir); errorsIsNotExist(err) {
			break
		}
		dstDir = filepath.Join(backupsRoot, fmt.Sprintf("%s-%d", dirName, suffix))
	}

	if err := os.MkdirAll(dstDir, 0o700); err != nil {
		return BackupResult{}, fmt.Errorf("buat direktori backup DB: %w", err)
	}

	files := make([]string, 0, len(targets))
	for _, t := range targets {
		if t.db == nil {
			continue
		}
		dstFile := filepath.Join(dstDir, t.name)
		escaped := strings.ReplaceAll(filepath.ToSlash(dstFile), "'", "''")
		if _, err := t.db.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s'", escaped)); err != nil {
			_ = os.RemoveAll(dstDir)
			return BackupResult{}, fmt.Errorf("vacuum backup %s: %w", t.name, err)
		}
		if err := os.Chmod(dstFile, 0o600); err != nil {
			_ = os.RemoveAll(dstDir)
			return BackupResult{}, fmt.Errorf("chmod backup %s: %w", t.name, err)
		}
		files = append(files, t.name)
	}

	pruneOldDBBackups(backupsRoot, maxRetainedDBBackups)
	return BackupResult{Dir: dstDir, Files: files}, nil
}

func pruneOldDBBackups(backupsRoot string, keep int) {
	entries, err := os.ReadDir(backupsRoot)
	if err != nil {
		return
	}
	type dirItem struct {
		path    string
		modTime int64
		name    string
	}
	var dbDirs []dirItem
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "db-") {
			continue
		}
		info, err := e.Info()
		var mt int64
		if err == nil {
			mt = info.ModTime().UnixNano()
		}
		dbDirs = append(dbDirs, dirItem{
			path:    filepath.Join(backupsRoot, e.Name()),
			modTime: mt,
			name:    e.Name(),
		})
	}
	if len(dbDirs) <= keep {
		return
	}
	// Urutkan terbaru lebih dulu berdasarkan nama/waktu modifikasi, sisakan keep teratas.
	sort.Slice(dbDirs, func(i, j int) bool {
		if dbDirs[i].modTime != dbDirs[j].modTime {
			return dbDirs[i].modTime > dbDirs[j].modTime
		}
		return dbDirs[i].name > dbDirs[j].name
	})
	for _, old := range dbDirs[keep:] {
		_ = os.RemoveAll(old.path)
	}
}

func errorsIsNotExist(err error) bool {
	return err != nil && os.IsNotExist(err)
}

type errControlNotReady string

func (e errControlNotReady) Error() string { return string(e) }
