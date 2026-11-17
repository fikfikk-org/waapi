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
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/guardian"
	"github.com/fikfikk-org/waapi/internal/httpapi"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/logx"
	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/policy"
	"github.com/fikfikk-org/waapi/internal/sdnotify"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
	"go.mau.fi/whatsmeow/types"
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

	tc := webhook.NewTargetCache()
	if err := tc.Load(ctx, st); err != nil {
		return fmt.Errorf("muat target cache webhook: %w", err)
	}

	j, err := journal.Open(ctx, cfg.JournalDBPath(), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    cfg.JournalCacheKiB,
		IdleTimeout: 60 * time.Second,
		TxLock:      "immediate",
		Sync:        cfg.JournalSync,
	})
	if err != nil {
		return fmt.Errorf("buka journal.db: %w", err)
	}
	defer func() { _ = j.Close() }()

	if migrated, mErr := webhook.MigrateLegacyOutbox(ctx, st, j); mErr != nil {
		slog.Warn("migrasi webhook_outbox legacy tidak lengkap", "err", mErr)
	} else if migrated > 0 {
		slog.Info("migrasi webhook_outbox legacy ke journal selesai", "count", migrated)
	}

	dead := webhook.NewDeadStore(j.DB())
	gov := governor.New(governor.Config{
		BudgetBytes: int64(cfg.MemoryBudgetMiB) << 20,
	}, governor.NewProcSampler("", "", ""))
	gov.SetOnTransition(func(prev, next governor.Level) {
		slog.Warn("perubahan level tekanan memori governor", "from", prev.String(), "to", next.String())
	})

	whSvc := webhook.NewService(j, tc, dead, nil, nil, webhook.Options{
		Store:    st,
		Governor: gov,
	})

	instances, err := st.ListInstances(ctx)
	if err != nil {
		return fmt.Errorf("muat daftar instance awal: %w", err)
	}
	instanceIDs := make([]string, 0, len(instances))
	for i := range instances {
		instanceIDs = append(instanceIDs, instances[i].ID)
	}
	whSvc.Start(ctx, instanceIDs)

	wh := webhook.NewDispatcher(st, nil, 0)
	wh.SetTargetCache(tc)

	uploadStore := media.NewUploadStore(j.DB())
	pollStore := wa.NewPollStore(j.DB())
	mediaPipe := media.NewPipeline(media.Limits{
		MediaMaxBytes:    cfg.MediaMaxBytes,
		DocumentMaxBytes: cfg.DocumentMaxBytes,
		TmpDir:           cfg.TmpDir(),
		DropBehindOff:    strings.EqualFold(cfg.MediaDropbehind, "off"),
	})
	retryWaiter := media.NewRetryWaiter()
	ctxStore := ctxstore.New(j.DB(), ctxstore.NewStoreSettings(st))

	waMgr, err := wa.NewManager(ctx, cfg, st, wh,
		wa.WithJournal(j),
		wa.WithTargetCache(tc),
		wa.WithWebhookService(whSvc),
		wa.WithMedia(mediaPipe, uploadStore, retryWaiter),
		wa.WithPolls(pollStore),
		wa.WithContextStore(ctxStore),
	)
	if err != nil {
		whSvc.Stop()
		return fmt.Errorf("inisialisasi wa manager: %w", err)
	}

	gDeps := &guardianDaemonAdapter{st: st, waMgr: waMgr}
	guard, err := guardian.New(j.DB(), guardian.Options{
		Settings: gDeps,
		Contacts: gDeps,
		LIDs:     gDeps,
		Enqueuer: gDeps,
		Emitter:  gDeps,
	})
	if err != nil {
		_ = waMgr.Close()
		whSvc.Stop()
		return fmt.Errorf("inisialisasi account health guardian: %w", err)
	}
	guard.BootstrapShadow(ctx)
	guard.StartFlushLoop(ctx)

	waMgr.SetGroupGovernor(guard)
	waMgr.SetHealthHooks(
		guard.ObserveDelivered,
		guard.ObserveTempBan,
		func(id string) { guard.OnInstancePaired(context.Background(), id) },
		func(sCtx context.Context, id string) (*wa.HealthSummary, error) {
			sum, sErr := guard.Summary(sCtx, id)
			if sErr != nil {
				return nil, sErr
			}
			return &wa.HealthSummary{State: sum.State, Score: sum.Score}, nil
		},
	)
	waMgr.AddInboundObserver(guard.InboundObserver())

	auditLog := auth.NewAuditLog(st.DB)
	policyEnf := policy.New(authMgr, guard)

	outboxSvc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, waMgr, guard, policyEnf, outbox.Options{
		DefaultTTL:   cfg.OutboxTTL,
		MaxQueued:    cfg.OutboxMaxQueued,
		OnActivity:   waMgr.RecordActivity,
		ContextStore: ctxStore,
	})
	waMgr.AttachOutbox(outboxSvc, guard)
	waMgr.SetFreeOSMemoryFn(func() { gov.TriggerFreeOSMemory() })
	outboxSvc.StartLeaseRecovery(ctx)

	gov.RegisterAction(governor.Yellow, "shrink_memory", 60*time.Second, governor.ShrinkPoolsAction([]governor.DBPool{
		{Name: "app.db", DB: st.DB},
		{Name: "journal.db", DB: j.DB()},
		{Name: "wa.db", DB: waMgr.WADB()},
	}))
	gov.RegisterAction(governor.Red, "free_os_memory", 60*time.Second, func(context.Context) {
		gov.TriggerFreeOSMemory()
	})
	if cfg.AutoRecycle == "on" {
		gov.RegisterSustainedAction(governor.Red, 10*time.Minute, "auto_recycle", 30*time.Minute, func(actCtx context.Context) {
			_, _ = waMgr.RecycleNextEligible(actCtx)
		})
	}

	go j.RunPruner(ctx, journal.PrunePolicy{
		Retention:    cfg.JournalRetention,
		ReplayWindow: cfg.JournalReplayWindow,
		MaxRows:      cfg.JournalMaxRows,
		Interval:     5 * time.Minute,
		ContextTask: func(pCtx context.Context, now time.Time) {
			if _, cErr := ctxStore.Prune(pCtx, now); cErr != nil && ctx.Err() == nil {
				slog.Warn("pruner context store gagal", "err", cErr)
			}
		},
		HourlyTask: func(pCtx context.Context) {
			if _, pErr := outboxSvc.PruneOutbox(pCtx, time.Now(), 7*24*time.Hour, 1000); pErr != nil && ctx.Err() == nil {
				slog.Warn("pruner outbox gagal", "err", pErr)
			}
			if _, uErr := uploadStore.Prune(pCtx, time.Now()); uErr != nil && ctx.Err() == nil {
				slog.Warn("pruner uploads gagal", "err", uErr)
			}
			if gErr := guard.Prune(pCtx, time.Now()); gErr != nil && ctx.Err() == nil {
				slog.Warn("pruner guardian gagal", "err", gErr)
			}
			if _, aErr := auditLog.Prune(pCtx, time.Now().Add(-auth.AuditRetention)); aErr != nil && ctx.Err() == nil {
				slog.Warn("pruner audit_log gagal", "err", aErr)
			}
		},
	})

	ctrlSrv := control.NewServer(cfg, st, authMgr, waMgr, wh,
		control.WithJournal(j),
		control.WithWebhookService(whSvc, dead),
		control.WithOutbox(outboxSvc),
		control.WithHealthProvider(guard),
		control.WithGovernor(gov),
		control.WithAuditLog(auditLog),
		control.WithContextStore(ctxStore),
	)
	ctrlSrv.SetVersion(daemonBuildVersion)
	ctrlLn, err := ctrlSrv.Listen()
	if err != nil {
		outboxSvc.Stop()
		_ = waMgr.Close()
		guard.Stop()
		whSvc.Stop()
		return err
	}
	defer func() {
		_ = ctrlLn.Close()
		_ = os.Remove(cfg.ControlSocket())
	}()

	ctrlHTTP := &http.Server{
		Handler:           ctrlSrv.Handler(),
		ConnContext:       ctrlSrv.ConnContext,
		ReadHeaderTimeout: 5 * time.Second,
	}

	rawPubLn, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		outboxSvc.Stop()
		_ = waMgr.Close()
		guard.Stop()
		whSvc.Stop()
		return fmt.Errorf("listen HTTP publik %s: %w", cfg.HTTPAddr, err)
	}
	pubLn := httpapi.LimitListener(rawPubLn, cfg.MaxConns)

	pubHTTP := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewHandler(authMgr, waMgr, httpapi.WithDB(st.DB), httpapi.WithJournal(j), httpapi.WithOutbox(outboxSvc), httpapi.WithMedia(mediaPipe, uploadStore), httpapi.WithPolls(pollStore), httpapi.WithHealthDriver(guard), httpapi.WithGovernor(gov), httpapi.WithPolicyEnforcer(policyEnf), httpapi.WithAuditLog(auditLog), httpapi.WithContextStore(ctxStore), httpapi.WithSSEMax(cfg.SSEMax)),
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
			outboxSvc.Stop()
			_ = waMgr.Close()
			guard.Stop()
			whSvc.Stop()
			return fmt.Errorf("listen metrik %s: %w", addr, err)
		}
		metricsHTTP = &http.Server{
			Addr:              addr,
			Handler:           newMetricsHandler(cfg, st, waMgr, gov),
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

	// Beri tahu systemd bahwa listener siap melayani lalu mulai watchdog bila aktif
	if err := sdnotify.Ready(); err != nil {
		slog.Warn("gagal mengirim sd_notify READY=1", "err", err)
	}
	if wdInterval, ok := sdnotify.WatchdogInterval(); ok {
		go runWatchdogLoop(ctx, st.DB, wdInterval/2)
	}

	// Jalankan auto-start instance secara berurutan di latar belakang
	go waMgr.AutoStartAll(ctx)
	go gov.Run(ctx)
	waMgr.StartRecycleLoop(ctx)
	governor.ScheduleBootPageout(ctx, cfg.PageoutAfterBoot)

	slog.Info("daemon waapi aktif",
		"http", cfg.HTTPAddr,
		"control", cfg.ControlSocket(),
		"data_dir", cfg.DataDir,
		"metrics", cfg.MetricsAddr,
	)

	select {
	case <-ctx.Done():
		slog.Info("menerima sinyal berhenti, memulai graceful shutdown...")
		if err := sdnotify.Stopping(); err != nil {
			slog.Warn("gagal mengirim sd_notify STOPPING=1", "err", err)
		}
	case err := <-errCh:
		if err := sdnotify.Stopping(); err != nil {
			slog.Warn("gagal mengirim sd_notify STOPPING=1", "err", err)
		}
		return err
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if metricsHTTP != nil {
		_ = metricsHTTP.Shutdown(shutCtx)
	}
	_ = pubHTTP.Shutdown(shutCtx)
	_ = ctrlHTTP.Shutdown(shutCtx)
	outboxSvc.Stop()
	_ = waMgr.Close()
	guard.Stop()
	whSvc.Stop()
	wh.Stop()
	_ = authMgr.FlushOnce(shutCtx)
	_ = j.Close()

	slog.Info("daemon waapi berhenti dengan bersih")
	return nil
}

// runWatchdogLoop mengirim WATCHDOG=1 berkala hanya bila app.db merespons SELECT 1 dalam 500 ms.
func runWatchdogLoop(ctx context.Context, db *sql.DB, interval time.Duration) {
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			qCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			var ok int
			err := db.QueryRowContext(qCtx, "SELECT 1").Scan(&ok)
			cancel()
			if err != nil {
				slog.Warn("watchdog melewati heartbeat karena SELECT 1 app.db gagal", "err", err)
				continue
			}
			if err := sdnotify.Watchdog(); err != nil {
				slog.Warn("gagal mengirim sd_notify WATCHDOG=1", "err", err)
			}
		}
	}
}

// newMetricsHandler membangun HTTP handler terpisah yang hanya melayani /metrics.
func newMetricsHandler(cfg *config.Config, st *store.Store, waMgr *wa.Manager, gov governor.Source) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		dbStats := []obs.DBStats{
			obs.CollectDBStats(r.Context(), "app.db", st.DB, cfg.AppDBPath()),
		}
		if waDB := waMgr.WADB(); waDB != nil {
			dbStats = append(dbStats, obs.CollectDBStats(r.Context(), "wa.db", waDB, cfg.WADBPath()))
		}
		if j := waMgr.Journal(); j != nil && j.DB() != nil {
			dbStats = append(dbStats, obs.CollectDBStats(r.Context(), "journal.db", j.DB(), cfg.JournalDBPath()))
		}
		counts := waMgr.CountByStatus(r.Context())
		if gov != nil {
			obs.MetricMemoryGovernorLevel.Set(float64(gov.Level()))
		}
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

type guardianDaemonAdapter struct {
	st    *store.Store
	waMgr *wa.Manager
}

func (a *guardianDaemonAdapter) InstanceMeta(ctx context.Context, instanceID string) (store.Instance, bool) {
	meta, err := a.st.GetInstance(ctx, instanceID)
	if err != nil || meta == nil {
		return store.Instance{}, false
	}
	return *meta, true
}

func (a *guardianDaemonAdapter) ListInstances(ctx context.Context) ([]store.Instance, error) {
	return a.st.ListInstances(ctx)
}

func (a *guardianDaemonAdapter) SetShadowUntil(ctx context.Context, instanceID string, until int64) error {
	meta, err := a.st.GetInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	s := meta.Settings
	s.GuardianShadowUntil = until
	updated, err := a.st.UpdateInstance(ctx, instanceID, store.InstanceUpdate{Settings: &s})
	if err != nil {
		return err
	}
	if inst, ok := a.waMgr.Get(instanceID); ok {
		inst.SetSettings(updated.Settings)
	}
	return nil
}

func (a *guardianDaemonAdapter) IsSavedContact(ctx context.Context, instanceID string, target types.JID) bool {
	return a.waMgr.IsAddressBookContact(ctx, instanceID, target)
}

func (a *guardianDaemonAdapter) ResolvePN(ctx context.Context, instanceID string, target types.JID) (types.JID, bool) {
	return a.waMgr.ResolveLIDToPN(ctx, instanceID, target)
}

func (a *guardianDaemonAdapter) EnqueueSystemText(ctx context.Context, instanceID, to, text, idemKey string) error {
	return a.waMgr.EnqueueSystemText(ctx, instanceID, to, text, idemKey)
}

func (a *guardianDaemonAdapter) EmitHealth(ctx context.Context, instanceID string, payload guardian.HealthEventPayload) error {
	return a.waMgr.EmitHealthEvent(ctx, instanceID, payload)
}
