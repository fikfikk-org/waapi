// Package logx menyediakan konfigurasi logging slog terpusat dan adaptor logger untuk whatsmeow.
package logx

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	waLog "go.mau.fi/whatsmeow/util/log"
	"golang.org/x/time/rate"
)

// ParseLevel mengonversi string nama level log menjadi slog.Level.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Setup menginisialisasi default slog.Logger sesuai format dan level di konfigurasi.
func Setup(cfg *config.Config) *slog.Logger {
	if cfg == nil {
		cfg = &config.Config{}
	}
	cfg.Normalize()

	opts := &slog.HandlerOptions{
		Level: ParseLevel(cfg.LogLevel),
	}
	var handler slog.Handler
	if strings.EqualFold(cfg.LogFormat, "json") {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

type sharedWALogState struct {
	base        *slog.Logger
	min         slog.Level
	limiter     *rate.Limiter
	dropped     atomic.Int64
	summaryMu   sync.Mutex
	lastSummary time.Time
}

type waSlogLogger struct {
	shared *sharedWALogState
	module string
}

// WALogger membuat implementasi waLog.Logger di atas slog dengan filter level dan rate limit 20/s burst 50.
func WALogger(base *slog.Logger, module string, min slog.Level) waLog.Logger {
	if base == nil {
		base = slog.Default()
	}
	return &waSlogLogger{
		shared: &sharedWALogState{
			base:    base,
			min:     min,
			limiter: rate.NewLimiter(rate.Limit(20), 50),
		},
		module: module,
	}
}

func (l *waSlogLogger) Warnf(msg string, args ...any) {
	l.logf(slog.LevelWarn, msg, args...)
}

func (l *waSlogLogger) Errorf(msg string, args ...any) {
	l.logf(slog.LevelError, msg, args...)
}

func (l *waSlogLogger) Infof(msg string, args ...any) {
	l.logf(slog.LevelInfo, msg, args...)
}

func (l *waSlogLogger) Debugf(msg string, args ...any) {
	l.logf(slog.LevelDebug, msg, args...)
}

func (l *waSlogLogger) Sub(module string) waLog.Logger {
	nextMod := module
	if l.module != "" && module != "" {
		nextMod = l.module + "/" + module
	} else if l.module != "" {
		nextMod = l.module
	}
	return &waSlogLogger{
		shared: l.shared,
		module: nextMod,
	}
}

func (l *waSlogLogger) logf(level slog.Level, msg string, args ...any) {
	if level < l.shared.min || !l.shared.base.Enabled(context.Background(), level) {
		return
	}

	now := time.Now()
	if !l.shared.limiter.AllowN(now, 1) {
		l.shared.dropped.Add(1)
		l.maybeEmitSummary(now)
		return
	}
	if l.shared.dropped.Load() > 0 {
		l.maybeEmitSummary(now)
	}

	text := fmt.Sprintf(msg, args...)
	l.shared.base.Log(context.Background(), level, text, slog.String("module", l.module))
}

func (l *waSlogLogger) maybeEmitSummary(now time.Time) {
	l.shared.summaryMu.Lock()
	if !l.shared.lastSummary.IsZero() && now.Sub(l.shared.lastSummary) < 60*time.Second {
		l.shared.summaryMu.Unlock()
		return
	}
	n := l.shared.dropped.Swap(0)
	if n <= 0 {
		l.shared.summaryMu.Unlock()
		return
	}
	l.shared.lastSummary = now
	l.shared.summaryMu.Unlock()

	l.shared.base.Log(
		context.Background(),
		slog.LevelWarn,
		fmt.Sprintf("%d baris log whatsmeow dibuang karena rate limit", n),
		slog.String("module", l.module),
	)
}
