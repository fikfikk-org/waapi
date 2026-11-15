package cli

import "testing"

// TestParseEventsArgs memverifikasi urutan flag fleksibel sebelum maupun sesudah argumen posisi instance ID.
func TestParseEventsArgs(t *testing.T) {
	opts, err := parseEventsArgs([]string{"sales", "--types", "message,receipt", "--after", "42", "--limit", "50", "--follow", "--json"})
	if err != nil {
		t.Fatalf("parseEventsArgs gagal: %v", err)
	}
	if opts.id != "sales" || opts.types != "message,receipt" || opts.after != 42 || opts.limit != 50 || !opts.follow || !opts.asJSON {
		t.Fatalf("hasil parsing tidak sesuai: %+v", opts)
	}

	opts2, err := parseEventsArgs([]string{"--types", "message", "--after=10", "cs"})
	if err != nil {
		t.Fatalf("parseEventsArgs dengan flag di depan gagal: %v", err)
	}
	if opts2.id != "cs" || opts2.types != "message" || opts2.after != 10 {
		t.Fatalf("hasil parsing urutan terbalik tidak sesuai: %+v", opts2)
	}

	if _, err := parseEventsArgs([]string{"--follow"}); err == nil {
		t.Fatal("diharapkan error ketika instance ID tidak diberikan")
	}
}
