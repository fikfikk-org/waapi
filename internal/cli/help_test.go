package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestHelp_Golden memverifikasi output waapi --help identik dengan golden file dan versi default adalah 2.0.0.
func TestHelp_Golden(t *testing.T) {
	if Version != "2.0.0" {
		t.Fatalf("Version default diharapkan 2.0.0, didapat %q", Version)
	}

	verOut, err := captureStdout(t, func() error {
		return Execute([]string{"version"})
	})
	if err != nil {
		t.Fatalf("Execute version gagal: %v", err)
	}
	if strings.TrimSpace(verOut) != "waapi v2.0.0" {
		t.Fatalf("output waapi version tidak sesuai: %q", verOut)
	}

	out, err := captureStdout(t, func() error {
		return Execute([]string{"--help"})
	})
	if err != nil {
		t.Fatalf("Execute --help gagal: %v", err)
	}

	rawGolden, err := os.ReadFile(filepath.Join("testdata", "help.golden"))
	if err != nil {
		t.Fatalf("gagal membaca testdata/help.golden: %v", err)
	}

	normGot := strings.ReplaceAll(out, "\r\n", "\n")
	normWant := strings.ReplaceAll(string(rawGolden), "\r\n", "\n")
	if normGot != normWant {
		t.Fatalf("output waapi --help tidak cocok dengan testdata/help.golden:\n--- GOT ---\n%s\n--- WANT ---\n%s", normGot, normWant)
	}
}
