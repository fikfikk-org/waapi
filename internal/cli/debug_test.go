package cli

import (
	"context"
	"testing"
)

// TestDebugDanDoctor_ValidasiArgumen memastikan validasi argumen perintah doctor dan debug pprof menolak input tidak valid.
func TestDebugDanDoctor_ValidasiArgumen(t *testing.T) {
	if err := runDoctor(context.Background(), nil, []string{"--unknown"}); err == nil {
		t.Error("runDoctor seharusnya menolak flag tidak dikenal")
	}
	if err := runDebug(context.Background(), nil, []string{"unknown"}); err == nil {
		t.Error("runDebug seharusnya menolak subperintah selain pprof")
	}
	if err := runDebugPprof(context.Background(), nil, []string{"allocs"}); err == nil {
		t.Error("runDebugPprof seharusnya menolak nama profil di luar kontrak")
	}
	if err := runDebugPprof(context.Background(), nil, []string{"profile", "--seconds", "0"}); err == nil {
		t.Error("runDebugPprof seharusnya menolak --seconds non-positif")
	}
}
