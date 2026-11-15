// Package config memuat konfigurasi daemon dan CLI dari environment variable.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const defaultUpdateBaseURL = "https://github.com/fikfikk-org/waapi/releases/download"

// Config menyimpan seluruh konfigurasi runtime wa-api.
type Config struct {
	DataDir          string // direktori data: app.db, journal.db, wa.db, control.sock, tmp/
	HTTPAddr         string // alamat listen HTTP publik
	LogLevel         string // debug | info | warn | error
	MemoryLimitBytes int64  // fallback soft memory limit jika GOMEMLIMIT tidak diset
	MaxConns         int    // batas koneksi HTTP publik konkuren
	MediaMaxBytes    int64  // batas ukuran media image/video/audio
	DocumentMaxBytes int64  // batas ukuran media document
	WebhookWorkers   int    // jumlah worker pengirim webhook

	Profile             Profile       // micro | lean | classic
	LogFormat           string        // text | json
	WALogLevel          string        // debug | info | warn | error
	MetricsAddr         string        // alamat listen metrik Prometheus (kosong = mati)
	KeepaliveMaxFail    time.Duration // batas gagal keepalive sebelum reconnect
	TCPUserTimeout      time.Duration // timeout TCP_USER_TIMEOUT websocket
	JournalRetention    time.Duration // umur maksimal event belum terkirim di journal
	JournalReplayWindow time.Duration // umur event sudah terkirim di journal
	JournalMaxRows      int           // batas baris journal per instance
	JournalSync         string        // normal | full
	SSEMax              int           // batas koneksi SSE konkuren
	OutboxTTL           time.Duration // umur maksimal pesan di outbox
	OutboxMaxQueued     int           // batas antrian pesan keluar per instance
	MediaDropbehind     string        // on | off
	UpdateBaseURL       string        // base URL unduhan rilis untuk self-update
	MemoryBudgetMiB     int           // budget anon memory governor
	AutoRecycle         string        // off | on
	PageoutAfterBoot    string        // off | cold | pageout
	MCP                 string        // on | off
	MCPAllowedOrigins   string        // daftar Origin yang diizinkan untuk MCP (CSV)
	AIBaseURL           string        // base URL endpoint AI OpenAI-compatible (kosong = mati)
	AIAPIKey            string        // API key provider AI
	AIModel             string        // nama model default AI
	AITimeout           time.Duration // timeout panggilan AI
	AIMaxConcurrency    int           // maksimal panggilan AI konkuren

	AppCacheKiB     int // ukuran page cache app.db dalam KiB
	JournalCacheKiB int // ukuran page cache journal.db dalam KiB
	WACacheKiB      int // ukuran page cache wa.db dalam KiB
}

// Load membaca environment variable WAAPI_* dan mengisi nilai default sesuai profil.
func Load() (*Config, error) {
	profile, err := ParseProfile(env("WAAPI_PROFILE", string(ProfileLean)))
	if err != nil {
		return nil, err
	}
	pv := ProfileDefaults(profile)

	dataDir := env("WAAPI_DATA_DIR", defaultDataDir())
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve WAAPI_DATA_DIR: %w", err)
	}

	logLevel, err := envEnum("WAAPI_LOG_LEVEL", "info", "debug", "info", "warn", "error")
	if err != nil {
		return nil, err
	}
	logFormat, err := envEnum("WAAPI_LOG_FORMAT", "text", "text", "json")
	if err != nil {
		return nil, err
	}
	waLogLevel, err := envEnum("WAAPI_WA_LOG_LEVEL", "warn", "debug", "info", "warn", "error")
	if err != nil {
		return nil, err
	}

	memMiB, err := envInt("WAAPI_MEMORY_LIMIT_MIB", pv.MemoryLimitMiB)
	if err != nil {
		return nil, err
	}
	maxConns, err := envInt("WAAPI_MAX_CONNS", 64)
	if err != nil {
		return nil, err
	}
	mediaMiB, err := envInt("WAAPI_MEDIA_MAX_MIB", 16)
	if err != nil {
		return nil, err
	}
	docMiB, err := envInt("WAAPI_DOCUMENT_MAX_MIB", 64)
	if err != nil {
		return nil, err
	}
	workers, err := envInt("WAAPI_WEBHOOK_WORKERS", 4)
	if err != nil {
		return nil, err
	}

	keepaliveMaxFail, err := envDuration("WAAPI_KEEPALIVE_MAX_FAIL", 75*time.Second)
	if err != nil {
		return nil, err
	}
	tcpUserTimeout, err := envDuration("WAAPI_TCP_USER_TIMEOUT", 45*time.Second)
	if err != nil {
		return nil, err
	}
	journalRetention, err := envDuration("WAAPI_JOURNAL_RETENTION", 72*time.Hour)
	if err != nil {
		return nil, err
	}
	journalReplayWindow, err := envDuration("WAAPI_JOURNAL_REPLAY_WINDOW", time.Hour)
	if err != nil {
		return nil, err
	}
	journalMaxRows, err := envInt("WAAPI_JOURNAL_MAX_ROWS", 50000)
	if err != nil {
		return nil, err
	}
	journalSync, err := envEnum("WAAPI_JOURNAL_SYNC", "normal", "normal", "full")
	if err != nil {
		return nil, err
	}
	sseMax, err := envInt("WAAPI_SSE_MAX", pv.SSEMax)
	if err != nil {
		return nil, err
	}
	outboxTTL, err := envDuration("WAAPI_OUTBOX_TTL", 24*time.Hour)
	if err != nil {
		return nil, err
	}
	outboxMaxQueued, err := envInt("WAAPI_OUTBOX_MAX_QUEUED", 1000)
	if err != nil {
		return nil, err
	}
	mediaDropbehind, err := envEnum("WAAPI_MEDIA_DROPBEHIND", "on", "on", "off")
	if err != nil {
		return nil, err
	}
	memBudgetMiB, err := envInt("WAAPI_MEMORY_BUDGET_MIB", pv.MemoryBudgetMiB)
	if err != nil {
		return nil, err
	}
	autoRecycle, err := envEnum("WAAPI_AUTO_RECYCLE", "off", "off", "on")
	if err != nil {
		return nil, err
	}
	pageoutAfterBoot, err := envEnum("WAAPI_PAGEOUT_AFTER_BOOT", "off", "off", "cold", "pageout")
	if err != nil {
		return nil, err
	}
	mcpMode, err := envEnum("WAAPI_MCP", "on", "on", "off")
	if err != nil {
		return nil, err
	}
	aiTimeout, err := envDuration("WAAPI_AI_TIMEOUT", 20*time.Second)
	if err != nil {
		return nil, err
	}
	aiMaxConcurrency, err := envInt("WAAPI_AI_MAX_CONCURRENCY", 2)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		DataDir:             abs,
		HTTPAddr:            env("WAAPI_HTTP_ADDR", "127.0.0.1:8080"),
		LogLevel:            logLevel,
		MemoryLimitBytes:    int64(memMiB) << 20,
		MaxConns:            maxConns,
		MediaMaxBytes:       int64(mediaMiB) << 20,
		DocumentMaxBytes:    int64(docMiB) << 20,
		WebhookWorkers:      workers,
		Profile:             profile,
		LogFormat:           logFormat,
		WALogLevel:          waLogLevel,
		MetricsAddr:         strings.TrimSpace(os.Getenv("WAAPI_METRICS_ADDR")),
		KeepaliveMaxFail:    keepaliveMaxFail,
		TCPUserTimeout:      tcpUserTimeout,
		JournalRetention:    journalRetention,
		JournalReplayWindow: journalReplayWindow,
		JournalMaxRows:      journalMaxRows,
		JournalSync:         journalSync,
		SSEMax:              sseMax,
		OutboxTTL:           outboxTTL,
		OutboxMaxQueued:     outboxMaxQueued,
		MediaDropbehind:     mediaDropbehind,
		UpdateBaseURL:       strings.TrimRight(env("WAAPI_UPDATE_BASE_URL", defaultUpdateBaseURL), "/"),
		MemoryBudgetMiB:     memBudgetMiB,
		AutoRecycle:         autoRecycle,
		PageoutAfterBoot:    pageoutAfterBoot,
		MCP:                 mcpMode,
		MCPAllowedOrigins:   strings.TrimSpace(os.Getenv("WAAPI_MCP_ALLOWED_ORIGINS")),
		AIBaseURL:           strings.TrimSpace(os.Getenv("WAAPI_AI_BASE_URL")),
		AIAPIKey:            strings.TrimSpace(os.Getenv("WAAPI_AI_API_KEY")),
		AIModel:             strings.TrimSpace(os.Getenv("WAAPI_AI_MODEL")),
		AITimeout:           aiTimeout,
		AIMaxConcurrency:    aiMaxConcurrency,
		AppCacheKiB:         pv.AppCacheKiB,
		JournalCacheKiB:     pv.JournalCacheKiB,
		WACacheKiB:          pv.WACacheKiB,
	}
	cfg.Normalize()
	return cfg, nil
}

// Normalize mengisi nilai bawaan berbasis profil untuk field Config yang masih bernilai nol.
func (c *Config) Normalize() {
	if c == nil {
		return
	}
	if c.Profile == "" {
		c.Profile = ProfileLean
	}
	pv := ProfileDefaults(c.Profile)

	if c.DataDir == "" {
		c.DataDir = defaultDataDir()
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = "127.0.0.1:8080"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.LogFormat == "" {
		c.LogFormat = "text"
	}
	if c.WALogLevel == "" {
		c.WALogLevel = "warn"
	}
	if c.MemoryLimitBytes <= 0 {
		c.MemoryLimitBytes = int64(pv.MemoryLimitMiB) << 20
	}
	if c.MaxConns <= 0 {
		c.MaxConns = 64
	}
	if c.MediaMaxBytes <= 0 {
		c.MediaMaxBytes = 16 << 20
	}
	if c.DocumentMaxBytes <= 0 {
		c.DocumentMaxBytes = 64 << 20
	}
	if c.WebhookWorkers <= 0 {
		c.WebhookWorkers = 4
	}
	if c.KeepaliveMaxFail <= 0 {
		c.KeepaliveMaxFail = 75 * time.Second
	}
	if c.TCPUserTimeout <= 0 {
		c.TCPUserTimeout = 45 * time.Second
	}
	if c.JournalRetention <= 0 {
		c.JournalRetention = 72 * time.Hour
	}
	if c.JournalReplayWindow <= 0 {
		c.JournalReplayWindow = time.Hour
	}
	if c.JournalMaxRows <= 0 {
		c.JournalMaxRows = 50000
	}
	if c.JournalSync == "" {
		c.JournalSync = "normal"
	}
	if c.SSEMax <= 0 {
		c.SSEMax = pv.SSEMax
	}
	if c.OutboxTTL <= 0 {
		c.OutboxTTL = 24 * time.Hour
	}
	if c.OutboxMaxQueued <= 0 {
		c.OutboxMaxQueued = 1000
	}
	if c.MediaDropbehind == "" {
		c.MediaDropbehind = "on"
	}
	if c.UpdateBaseURL == "" {
		c.UpdateBaseURL = defaultUpdateBaseURL
	}
	if c.MemoryBudgetMiB <= 0 {
		c.MemoryBudgetMiB = pv.MemoryBudgetMiB
	}
	if c.AutoRecycle == "" {
		c.AutoRecycle = "off"
	}
	if c.PageoutAfterBoot == "" {
		c.PageoutAfterBoot = "off"
	}
	if c.MCP == "" {
		c.MCP = "on"
	}
	if c.AITimeout <= 0 {
		c.AITimeout = 20 * time.Second
	}
	if c.AIMaxConcurrency <= 0 {
		c.AIMaxConcurrency = 2
	}
	if c.AppCacheKiB <= 0 {
		c.AppCacheKiB = pv.AppCacheKiB
	}
	if c.JournalCacheKiB <= 0 {
		c.JournalCacheKiB = pv.JournalCacheKiB
	}
	if c.WACacheKiB <= 0 {
		c.WACacheKiB = pv.WACacheKiB
	}
}

// ApplyMemoryLimit memasang soft memory limit hanya jika GOMEMLIMIT tidak diset dari env.
func (c *Config) ApplyMemoryLimit() {
	if os.Getenv("GOMEMLIMIT") == "" && c.MemoryLimitBytes > 0 {
		debug.SetMemoryLimit(c.MemoryLimitBytes)
	}
}

// ControlSocket mengembalikan path unix socket control plane.
func (c *Config) ControlSocket() string { return filepath.Join(c.DataDir, "control.sock") }

// AppDBPath mengembalikan path database metadata.
func (c *Config) AppDBPath() string { return filepath.Join(c.DataDir, "app.db") }

// JournalDBPath mengembalikan path database journal event.
func (c *Config) JournalDBPath() string { return filepath.Join(c.DataDir, "journal.db") }

// WADBPath mengembalikan path database crypto store WhatsApp.
func (c *Config) WADBPath() string { return filepath.Join(c.DataDir, "wa.db") }

// TmpDir mengembalikan direktori file media sementara.
func (c *Config) TmpDir() string { return filepath.Join(c.DataDir, "tmp") }

// EnsureDirs membuat direktori data dan tmp dengan permission ketat.
func (c *Config) EnsureDirs() error {
	if err := os.MkdirAll(c.DataDir, 0o700); err != nil {
		return fmt.Errorf("buat data dir: %w", err)
	}
	if err := os.MkdirAll(c.TmpDir(), 0o700); err != nil {
		return fmt.Errorf("buat tmp dir: %w", err)
	}
	return nil
}

// defaultDataDir memilih lokasi data default yang masuk akal per OS.
func defaultDataDir() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".wa-api")
	}
	return ".wa-api"
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s harus bilangan bulat positif, didapat %q", key, v)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s harus durasi positif yang valid, didapat %q", key, v)
	}
	return d, nil
}

func envEnum(key, def string, allowed ...string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def, nil
	}
	for _, a := range allowed {
		if v == a {
			return v, nil
		}
	}
	return "", fmt.Errorf("%s harus salah satu dari (%s), didapat %q", key, strings.Join(allowed, "|"), v)
}
