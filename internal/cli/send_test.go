package cli

import "testing"

// TestParseSendArgs memastikan urutan argumen posisi maupun flag --wait/--idem dikenali dengan benar.
func TestParseSendArgs(t *testing.T) {
	opts, err := parseSendArgs([]string{"sales", "6281234567890", "Halo dunia", "--wait", "--idem", "key-1", "--typing"})
	if err != nil {
		t.Fatalf("parseSendArgs gagal: %v", err)
	}
	if opts.id != "sales" || opts.to != "6281234567890" || opts.text != "Halo dunia" || !opts.wait || opts.idem != "key-1" || !opts.typing {
		t.Fatalf("hasil parse tidak sesuai: %+v", opts)
	}

	opts2, err := parseSendArgs([]string{"--idem=k2", "--wait", "sales", "6281234567890", "Pesan"})
	if err != nil {
		t.Fatalf("parseSendArgs urutan flag depan gagal: %v", err)
	}
	if opts2.idem != "k2" || !opts2.wait || opts2.id != "sales" {
		t.Fatalf("hasil parse urutan depan tidak sesuai: %+v", opts2)
	}

	if _, err := parseSendArgs([]string{"sales", "6281234567890"}); err == nil {
		t.Fatal("argumen text yang kurang harus mengembalikan error")
	}
}
