package config

import (
	"strings"
	"testing"
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
