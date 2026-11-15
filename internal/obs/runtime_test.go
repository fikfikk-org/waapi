package obs

import (
	"runtime/metrics"
	"testing"
)

// TestRuntimeMetrics_NamaDikenal memastikan seluruh nama metrik Go runtime yang dibutuhkan dikenali oleh runtime/metrics.All.
func TestRuntimeMetrics_NamaDikenal(t *testing.T) {
	all := make(map[string]bool)
	for _, d := range metrics.All() {
		all[d.Name] = true
	}

	for _, name := range requiredRuntimeMetrics {
		if !all[name] {
			t.Errorf("metrik runtime %q tidak ditemukan di runtime/metrics.All()", name)
		}
	}

	stats := ReadRuntime()
	if stats.Goroutines == 0 {
		t.Errorf("Goroutines seharusnya > 0")
	}
	if stats.Total == 0 {
		t.Errorf("Total bytes runtime seharusnya > 0")
	}
}
