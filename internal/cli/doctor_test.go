package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/fikfikk-org/waapi/internal/obs"
)

// TestDoctor_GovernorDanRekomendasiTHP memverifikasi output teks & JSON doctor mencantumkan level Governor, rekomendasi THP always, dan metrik Prometheus.
func TestDoctor_GovernorDanRekomendasiTHP(t *testing.T) {
	obs.MetricMemoryGovernorLevel.Set(2)
	prom := new(bytes.Buffer)
	if err := obs.Default.WritePrometheus(prom); err != nil {
		t.Fatalf("WritePrometheus gagal: %v", err)
	}
	if !strings.Contains(prom.String(), "waapi_memory_governor_level 2") {
		t.Fatalf("metrik Prometheus harus berisi waapi_memory_governor_level 2, didapat:\n%s", prom.String())
	}

	rep := obs.DeepReport{
		Doctor: obs.DoctorReport{
			PID:           1234,
			UptimeSec:     60,
			RSSBytes:      18 << 20,
			GovernorLevel: "yellow",
			InstanceCount: map[string]int{},
			Env:           map[string]string{},
		},
		GovernorLevel: "yellow",
		THP:           "always",
		Build: obs.BuildInfo{
			Go:      "go1.27.4",
			Version: "v2.0.0",
		},
	}

	// Pastikan marshal JSON menyertakan field governor_level agar mudah dibaca jq (Lampiran A.1)
	rawJSON, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rawJSON), `"governor_level":"yellow"`) {
		t.Fatalf("JSON DeepReport harus menyertakan governor_level, didapat: %s", string(rawJSON))
	}

	out, err := captureStdout(t, func() error {
		printDeepDoctor(rep)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(out), "governor") || !strings.Contains(strings.ToLower(out), "yellow") {
		t.Fatalf("output printDeepDoctor harus menampilkan level Governor, didapat:\n%s", out)
	}
	if !strings.Contains(out, "GODEBUG=disablethp=1") {
		t.Fatalf("output printDeepDoctor saat THP always harus menyarankan GODEBUG=disablethp=1, didapat:\n%s", out)
	}
}
