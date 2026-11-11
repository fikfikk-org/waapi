// Package daemon merangkai seluruh komponen waapi dan menangani siklus hidup proses utama.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/httpapi"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

// Run menyalakan daemon waapi hingga ctx dibatalkan (SIGINT/SIGTERM), lalu melakukan graceful shutdown.
func Run(ctx context.Context, cfg *config.Config) error {
	cfg.ApplyMemoryLimit()
	if err := cfg.EnsureDirs(); err != nil {
		return err
	}
	wa.SweepStaleTemp(cfg.TmpDir(), time.Hour)

	st, err := store.Open(ctx, cfg.AppDBPath())
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
		Handler:           httpapi.NewHandler(authMgr, waMgr),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       90 * time.Second, // cukup untuk unggah media 16-64 MB pada jaringan lambat
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    8 << 10, // batasi header maksimal 8 KB
	}

	errCh := make(chan error, 2)
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

	// Jalankan auto-start instance secara berurutan di latar belakang
	go waMgr.AutoStartAll(ctx)

	slog.Info("daemon waapi aktif",
		"http", cfg.HTTPAddr,
		"control", cfg.ControlSocket(),
		"data_dir", cfg.DataDir,
	)

	select {
	case <-ctx.Done():
		slog.Info("menerima sinyal berhenti, memulai graceful shutdown...")
	case err := <-errCh:
		return err
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_ = pubHTTP.Shutdown(shutCtx)
	_ = ctrlHTTP.Shutdown(shutCtx)
	_ = waMgr.Close()
	wh.Stop()
	_ = authMgr.FlushOnce(shutCtx)

	slog.Info("daemon waapi berhenti dengan bersih")
	return nil
}

