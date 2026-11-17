package config

import (
	"strings"
	"testing"
	"time"
)

// TestConfig_NormalizeNilaiNol memastikan Normalize mengisi default profil lean untuk field bernilai nol.
func TestConfig_NormalizeNilaiNol(t *testing.T) {
	cfg := Config{}
	cfg.Normalize()

	if cfg.OutboxMaxQueued != 1000 {
		t.Fatalf("OutboxMaxQueued diharapkan 1000, didapat %d", cfg.OutboxMaxQueued)
	}
	if cfg.SSEMax != 8 {
		t.Fatalf("SSEMax diharapkan 8, didapat %d", cfg.SSEMax)
	}
	if cfg.Profile != ProfileLean {
		t.Fatalf("Profile diharapkan %q, didapat %q", ProfileLean, cfg.Profile)
	}
}

// TestConfig_DefaultPerProfil memastikan WAAPI_PROFILE=micro mengisi default cache dan batas sesuai profil.
func TestConfig_DefaultPerProfil(t *testing.T) {
	t.Setenv("WAAPI_PROFILE", "micro")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load dengan profil micro gagal: %v", err)
	}
	if cfg.SSEMax != 4 {
		t.Fatalf("SSEMax profil micro diharapkan 4, didapat %d", cfg.SSEMax)
	}
	if cfg.AppCacheKiB != 256 || cfg.JournalCacheKiB != 512 || cfg.WACacheKiB != 1024 {
		t.Fatalf("cache profil micro tidak sesuai: app=%d journal=%d wa=%d", cfg.AppCacheKiB, cfg.JournalCacheKiB, cfg.WACacheKiB)
	}
}

// TestConfig_DurasiTidakValid memastikan durasi env yang rusak menghasilkan error yang menyebut nama variabelnya.
func TestConfig_DurasiTidakValid(t *testing.T) {
	t.Setenv("WAAPI_OUTBOX_TTL", "abc")

	_, err := Load()
	if err == nil {
		t.Fatal("Load seharusnya gagal saat WAAPI_OUTBOX_TTL tidak valid")
	}
	if !strings.Contains(err.Error(), "WAAPI_OUTBOX_TTL") {
		t.Fatalf("pesan error seharusnya menyebut WAAPI_OUTBOX_TTL, didapat: %v", err)
	}
}

// TestConfig_AIDefaultDanNormalisasi memastikan env WAAPI_AI_* memiliki default aman serta pembersihan trailing slash Base URL.
func TestConfig_AIDefaultDanNormalisasi(t *testing.T) {
	t.Setenv("WAAPI_AI_BASE_URL", " http://127.0.0.1:11434/v1/ ")
	t.Setenv("WAAPI_AI_API_KEY", "sk-rahasia")
	t.Setenv("WAAPI_AI_MODEL", " qwen2.5:7b ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load dengan env WAAPI_AI_* gagal: %v", err)
	}
	if cfg.AIBaseURL != "http://127.0.0.1:11434/v1" {
		t.Fatalf("AIBaseURL diharapkan tanpa trailing slash, didapat %q", cfg.AIBaseURL)
	}
	if cfg.AIAPIKey != "sk-rahasia" || cfg.AIModel != "qwen2.5:7b" {
		t.Fatalf("AIAPIKey/AIModel tidak sesuai: key=%q model=%q", cfg.AIAPIKey, cfg.AIModel)
	}
	if cfg.AITimeout != 20*time.Second || cfg.AIMaxConcurrency != 2 {
		t.Fatalf("default AI tidak sesuai: timeout=%v conc=%d", cfg.AITimeout, cfg.AIMaxConcurrency)
	}

	var zero Config
	zero.Normalize()
	if zero.AITimeout != 20*time.Second || zero.AIMaxConcurrency != 2 || zero.AIBaseURL != "" {
		t.Fatalf("Normalize nilai nol AI tidak sesuai: %+v", zero)
	}
}
