package obs

import "testing"

// TestReadTHPMode memastikan token di dalam kurung siku [madvise] diekstrak dengan benar.
func TestReadTHPMode(t *testing.T) {
	got := ReadTHPMode("testdata/thp_enabled")
	if got != "madvise" {
		t.Fatalf("diharapkan 'madvise', didapat %q", got)
	}
}
