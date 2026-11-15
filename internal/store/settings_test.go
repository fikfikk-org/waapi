package store

import (
	"encoding/json"
	"testing"
	"time"
)

// TestInstanceSettings_DefaultValidateMerge memastikan nilai default, validasi, penolakan field asing, dan merge parsial.
func TestInstanceSettings_DefaultValidateMerge(t *testing.T) {
	def := Defaults()
	if def.Timezone != "Asia/Jakarta" {
		t.Fatalf("timezone default diharapkan Asia/Jakarta, didapat %q", def.Timezone)
	}
	if def.JournalReceipts != "dm" {
		t.Fatalf("journal_receipts default diharapkan dm, didapat %q", def.JournalReceipts)
	}
	if def.WebhookLanes != 4 {
		t.Fatalf("webhook_lanes default diharapkan 4, didapat %d", def.WebhookLanes)
	}
	if def.WebhookFormat != "waapi" {
		t.Fatalf("webhook_format default diharapkan waapi, didapat %q", def.WebhookFormat)
	}
	if def.WebhookMaxAttempts != 12 {
		t.Fatalf("webhook_max_attempts default diharapkan 12, didapat %d", def.WebhookMaxAttempts)
	}
	if def.ContextStore {
		t.Fatal("context_store default diharapkan false")
	}
	if def.ContextPerChat != 50 {
		t.Fatalf("context_per_chat default diharapkan 50, didapat %d", def.ContextPerChat)
	}
	if def.ContextMaxAge != Duration(168*time.Hour) {
		t.Fatalf("context_max_age default diharapkan 168h, didapat %v", def.ContextMaxAge)
	}
	if def.GuardianProfile != "standard" {
		t.Fatalf("guardian_profile default diharapkan standard, didapat %q", def.GuardianProfile)
	}
	if def.HooksMaxAge != Duration(120*time.Second) {
		t.Fatalf("hooks_max_age default diharapkan 120s, didapat %v", def.HooksMaxAge)
	}
	if def.TakeoverPause != Duration(30*time.Minute) {
		t.Fatalf("takeover_pause default diharapkan 30m, didapat %v", def.TakeoverPause)
	}
	if err := def.Validate(); err != nil {
		t.Fatalf("Defaults() harus valid: %v", err)
	}

	// webhook_lanes=0 harus ditolak
	if _, err := def.Merge(json.RawMessage(`{"webhook_lanes":0}`)); err == nil {
		t.Fatal("webhook_lanes=0 seharusnya mengembalikan error")
	}

	// Field asing harus ditolak
	if _, err := def.Merge(json.RawMessage(`{"field_tidak_dikenal":true}`)); err == nil {
		t.Fatal("field asing seharusnya ditolak oleh Merge")
	}

	// Merge parsial harus mempertahankan field lainnya
	merged, err := def.Merge(json.RawMessage(`{"guardian_profile":"strict","webhook_lanes":6,"context_store":true}`))
	if err != nil {
		t.Fatalf("Merge parsial valid gagal: %v", err)
	}
	if merged.GuardianProfile != "strict" || merged.WebhookLanes != 6 || !merged.ContextStore {
		t.Fatalf("hasil merge tidak sesuai: %+v", merged)
	}
	if merged.Timezone != def.Timezone || merged.ContextPerChat != def.ContextPerChat || merged.WebhookMaxAttempts != def.WebhookMaxAttempts {
		t.Fatalf("field yang tidak di-patch berubah secara tidak terduga: %+v", merged)
	}
	if def.GuardianProfile != "standard" {
		t.Fatal("Merge tidak boleh memutasi struct asal")
	}
}
