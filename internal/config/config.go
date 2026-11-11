// Package config memuat konfigurasi daemon dan CLI dari environment variable.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

// Config menyimpan seluruh konfigurasi runtime wa-api.
type Config struct {
	DataDir          string // direktori data: app.db, wa.db, control.sock, tmp/
	HTTPAddr         string // alamat listen HTTP publik
	LogLevel         string // debug | info | warn | error
	MemoryLimitBytes int64  // fallback soft memory limit jika GOMEMLIMIT tidak diset
	MaxConns         int    // batas koneksi HTTP publik konkuren
	MediaMaxBytes    int64  // batas ukuran media image/video/audio
	DocumentMaxBytes int64  // batas ukuran media document
	WebhookWorkers   int    // jumlah worker pengirim webhook
}

// Load membaca environment variable WAAPI_* dan mengisi nilai default.
func Load() (*Config, error) {
	dataDir := env("WAAPI_DATA_DIR", defaultDataDir())
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve WAAPI_DATA_DIR: %w", err)
	}

	memMiB, err := envInt("WAAPI_MEMORY_LIMIT_MIB", 80)
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

	cfg := &Config{
		DataDir:          abs,
		HTTPAddr:         env("WAAPI_HTTP_ADDR", "127.0.0.1:8080"),
		LogLevel:         strings.ToLower(env("WAAPI_LOG_LEVEL", "info")),
		MemoryLimitBytes: int64(memMiB) << 20,
		MaxConns:         maxConns,
		MediaMaxBytes:    int64(mediaMiB) << 20,
		DocumentMaxBytes: int64(docMiB) << 20,
		WebhookWorkers:   workers,
	}
	return cfg, nil
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

