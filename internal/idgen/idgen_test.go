package idgen

import (
	"strings"
	"testing"
	"time"
)

// TestIDGen_UnikDanUrutWaktu memastikan ID Crockford base32 unik untuk 10.000 pembuatan dan terurut leksikografis mengikuti waktu.
func TestIDGen_UnikDanUrutWaktu(t *testing.T) {
	const count = 10000
	seen := make(map[string]struct{}, count)
	prev := ""

	for i := range count {
		id := New("msg_")
		if !strings.HasPrefix(id, "msg_") {
			t.Fatalf("ID pada indeks %d tidak memiliki prefix msg_: %q", i, id)
		}
		raw := strings.TrimPrefix(id, "msg_")
		if len(raw) != 26 {
			t.Fatalf("panjang bagian ULID harus 26 karakter, didapat %d (%q)", len(raw), id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("ID duplikat pada iterasi %d: %q", i, id)
		}
		seen[id] = struct{}{}
		if prev != "" && id <= prev {
			t.Fatalf("urutan leksikografis harus naik: prev=%q cur=%q (iter=%d)", prev, id, i)
		}
		prev = id
	}

	// Uji urutan lintas milidetik yang berbeda secara deterministik.
	t1 := time.UnixMilli(1794360100100)
	t2 := time.UnixMilli(1794360100101)
	id1 := NewAt("msg_", t1)
	id2 := NewAt("msg_", t2)
	if id1 >= id2 {
		t.Fatalf("ID waktu lebih baru harus lebih besar secara leksikografis: id1=%q id2=%q", id1, id2)
	}
}
