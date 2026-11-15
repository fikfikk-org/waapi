// Package daemon merangkai seluruh komponen waapi dan menangani siklus hidup proses utama.
package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/httpapi"
	"github.com/fikfikk-org/waapi/internal/logx"
	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

var daemonBuildVersion = "dev"

// SetVersion mencatat string versi aplikasi untuk dilaporkan pada endpoint diagnostik daemon.
func SetVersion(v string) {
	if strings.TrimSpace(v) != "" {
		daemonBuildVersion = strings.TrimSpace(v)
	}
}

// BackupPreV2 membuat salinan app.db dan wa.db sebelum migrasi v2 jika app.db v1 (user_version 1 atau 2) terdeteksi.
func BackupPreV2(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return errors.New("konfigurasi tidak boleh nil")
	}
	appPath := cfg.AppDBPath()
	if _, err := os.Stat(appPath); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("periksa app.db pra-backup: %w", err)
	}

	appDB, err := store.OpenDB(ctx, appPath, store.DBOptions{MaxConns: 1, CacheKiB: 512})
	if err != nil {
		return fmt.Errorf("buka app.db untuk cek versi: %w", err)
	}
	var userVer int
	err = appDB.QueryRowContext(ctx, "PRAGMA user_version").Scan(&userVer)
	if err != nil {
		_ = appDB.Close()
		return fmt.Errorf("baca user_version app.db: %w", err)
	}
	if userVer != 1 && userVer != 2 {
		_ = appDB.Close()
		return nil
	}

	backupsRoot := filepath.Join(cfg.DataDir, "backups")
	if err := os.MkdirAll(backupsRoot, 0o700); err != nil {
		_ = appDB.Close()
		return fmt.Errorf("buat direktori backups: %w", err)
	}
	bakDir := filepath.Join(backupsRoot, fmt.Sprintf("pre-v2-%d", time.Now().Unix()))
	if err := os.MkdirAll(bakDir, 0o700); err != nil {
		_ = appDB.Close()
		return fmt.Errorf("buat direktori backup pra-v2: %w", err)
	}

	if err := vacuumIntoBackup(ctx, appDB, filepath.Join(bakDir, "app.db")); err != nil {
		_ = appDB.Close()
		_ = os.RemoveAll(bakDir)
		return fmt.Errorf("backup app.db: %w", err)
	}
	_ = appDB.Close()

	waPath := cfg.WADBPath()
	if _, err := os.Stat(waPath); err == nil {
		waDB, err := store.OpenDB(ctx, waPath, store.DBOptions{MaxConns: 1, CacheKiB: 512})
		if err != nil {
			_ = os.RemoveAll(bakDir)
			return fmt.Errorf("buka wa.db untuk backup: %w", err)
		}
		if err := vacuumIntoBackup(ctx, waDB, filepath.Join(bakDir, "wa.db")); err != nil {
			_ = waDB.Close()
			_ = os.RemoveAll(bakDir)
			return fmt.Errorf("backup wa.db: %w", err)
		}
		_ = waDB.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = os.RemoveAll(bakDir)
		return fmt.Errorf("periksa wa.db pra-backup: %w", err)
	}

	slog.Info("backup pra-v2 selesai dibuat", "dir", bakDir, "prev_version", userVer)
	return nil
}

func vacuumIntoBackup(ctx context.Context, db *sql.DB, dstPath string) error {
	escaped := strings.ReplaceAll(filepath.ToSlash(dstPath), "'", "''")
	// VACUUM INTO menghasilkan salinan SQLite konsisten tanpa menyalin file WAL terpisah
	if _, err := db.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s'", escaped)); err != nil {
		return err
	}
	if err := os.Chmod(dstPath, 0o600); err != nil {
		return fmt.Errorf("atur izin file backup: %w", err)
	}
	return nil
}

// Run menyalakan daemon waapi hingga ctx dibatalkan (SIGINT/SIGTERM), lalu melakukan graceful shutdown.
func Run(ctx context.Context, cfg *config.Config) error {
	cfg.Normalize()
	logx.Setup(cfg)
	cfg.ApplyMemoryLimit()
	if err := cfg.EnsureDirs(); err != nil {
		return err
	}
	wa.SweepStaleTemp(cfg.TmpDir(), time.Hour)

	if err := BackupPreV2(ctx, cfg); err != nil {
		return fmt.Errorf("backup pra-v2 gagal: %w", err)
	}

	st, err := store.Open(ctx, cfg.AppDBPath(), store.DBOptions{
		MaxConns: 2,
		CacheKiB: cfg.AppCacheKiB,
	})
	if err != nil {
		return fmt.Errorf("buka app.db: %w", err)
	}
	defer st.Close()

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		return fmt.Errorf("inisialisasi auth manager: %w", err)
	}
	go authMgr.FlushLoop(ctx)

	wh := webhook.NewDispatcher(st, func(ctx context.Context, instanceID string) (string, string, string, error) {
		inst, err := st.GetInstance(ctx, instanceID)
		if err != nil {
			return "", "", "", err
		}
		return inst.WebhookURL, inst.WebhookSecret, inst.WebhookEvents, nil
	}, cfg.WebhookWorkers)
	wh.Start(ctx)

	waMgr, err := wa.NewManager(ctx, cfg, st, wh)
	if err != nil {
		wh.Stop()
		return fmt.Errorf("inisialisasi wa manager: %w", err)
	}

	ctrlSrv := control.NewServer(cfg, st, authMgr, waMgr, wh)
	ctrlSrv.SetVersion(daemonBuildVersion)
	ctrlLn, err := ctrlSrv.Listen()
	if err != nil {
		_ = waMgr.Close()
		wh.Stop()
		return err
	}
	defer func() {
		_ = ctrlLn.Close()
		_ = os.Remove(cfg.ControlSocket())
	}()

	ctrlHTTP := &http.Server{
		Handler:           ctrlSrv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	rawPubLn, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		_ = waMgr.Close()
		wh.Stop()
		return fmt.Errorf("listen HTTP publik %s: %w", cfg.HTTPAddr, err)
	}
	pubLn := httpapi.LimitListener(rawPubLn, cfg.MaxConns)

	pubHTTP := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewHandler(authMgr, waMgr, httpapi.WithDB(st.DB), httpapi.WithGovernor(governor.AlwaysGreen)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       90 * time.Second, // cukup untuk unggah media 16-64 MB pada jaringan lambat
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    8 << 10, // batasi header maksimal 8 KB
	}

	var metricsHTTP *http.Server
	var metricsLn net.Listener
	if addr := strings.TrimSpace(cfg.MetricsAddr); addr != "" {
		if !isLoopbackListenAddr(addr) {
			slog.Warn("listener metrik Prometheus diikat ke alamat non-loopback", "addr", addr)
		}
		metricsLn, err = net.Listen("tcp", addr)
		if err != nil {
			_ = pubLn.Close()
			_ = waMgr.Close()
			wh.Stop()
			return fmt.Errorf("listen metrik %s: %w", addr, err)
		}
		metricsHTTP = &http.Server{
			Addr:              addr,
			Handler:           newMetricsHandler(cfg, st, waMgr),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
			MaxHeaderBytes:    4 << 10,
		}
	}

	errCh := make(chan error, 3)
	go func() {
		if err := ctrlHTTP.Serve(ctrlLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("control server: %w", err)
		}
	}()
	go func() {
		if err := pubHTTP.Serve(pubLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("public http server: %w", err)
		}
	}()
	if metricsHTTP != nil {
		go func() {
			if err := metricsHTTP.Serve(metricsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("metrics server: %w", err)
			}
		}()
	}

	// Jalankan auto-start instance secara berurutan di latar belakang
	go waMgr.AutoStartAll(ctx)

	slog.Info("daemon waapi aktif",
		"http", cfg.HTTPAddr,
		"control", cfg.ControlSocket(),
		"data_dir", cfg.DataDir,
		"metrics", cfg.MetricsAddr,
	)

	select {
	case <-ctx.Done():
		slog.Info("menerima sinyal berhenti, memulai graceful shutdown...")
	case err := <-errCh:
		return err
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if metricsHTTP != nil {
		_ = metricsHTTP.Shutdown(shutCtx)
	}
	_ = pubHTTP.Shutdown(shutCtx)
	_ = ctrlHTTP.Shutdown(shutCtx)
	_ = waMgr.Close()
	wh.Stop()
	_ = authMgr.FlushOnce(shutCtx)

	slog.Info("daemon waapi berhenti dengan bersih")
	return nil
}

// newMetricsHandler membangun HTTP handler terpisah yang hanya melayani /metrics.
func newMetricsHandler(cfg *config.Config, st *store.Store, waMgr *wa.Manager) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		dbStats := []obs.DBStats{
			obs.CollectDBStats(r.Context(), "app.db", st.DB, cfg.AppDBPath()),
		}
		if waDB := waMgr.WADB(); waDB != nil {
			dbStats = append(dbStats, obs.CollectDBStats(r.Context(), "wa.db", waDB, cfg.WADBPath()))
		}
		counts := waMgr.CountByStatus(r.Context())
		obs.RefreshDefaultMetrics(dbStats, counts)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := obs.Default.WritePrometheus(w); err != nil {
			slog.Warn("gagal menulis metrik prometheus", "err", err)
		}
	})
	return mux
}

func isLoopbackListenAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
