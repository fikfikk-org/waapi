package wa

import (
	"testing"
	"testing/synctest"
	"time"
)

// TestLRU_EvictionDanTTL memverifikasi eviction urutan paling jarang dipakai (LRU) dan kedaluwarsa TTL di dalam synctest.
func TestLRU_EvictionDanTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := NewLRU[string, int](2, 5*time.Minute)

		cache.Set("a", 1)
		cache.Set("b", 2)

		// Akses "a" agar "b" menjadi kandidat eviction berikutnya.
		if got, ok := cache.Get("a"); !ok || got != 1 {
			t.Fatalf("diharapkan key a=1 ada di cache, didapat ok=%v val=%d", ok, got)
		}

		// Tambah "c" melebihi kapasitas 2 -> "b" harus ter-evict.
		cache.Set("c", 3)
		if _, ok := cache.Get("b"); ok {
			t.Fatal("key b seharusnya sudah ter-evict oleh kebijakan LRU")
		}
		if got, ok := cache.Get("a"); !ok || got != 1 {
			t.Fatalf("key a seharusnya masih ada, didapat ok=%v val=%d", ok, got)
		}
		if got, ok := cache.Get("c"); !ok || got != 3 {
			t.Fatalf("key c seharusnya ada, didapat ok=%v val=%d", ok, got)
		}
		if cache.Len() != 2 {
			t.Fatalf("panjang cache diharapkan 2, didapat %d", cache.Len())
		}

		// Sebelum TTL 5 menit habis, entri tetap tersedia.
		time.Sleep(4*time.Minute + 59*time.Second)
		synctest.Wait()
		if got, ok := cache.Get("a"); !ok || got != 1 {
			t.Fatalf("entri sebelum batas TTL seharusnya belum kedaluwarsa, ok=%v val=%d", ok, got)
		}

		// Setelah melewati TTL 5 menit, entri harus kedaluwarsa dan dihapus saat diakses.
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if _, ok := cache.Get("a"); ok {
			t.Fatal("key a seharusnya sudah kedaluwarsa melewati TTL")
		}
		if _, ok := cache.Get("c"); ok {
			t.Fatal("key c seharusnya sudah kedaluwarsa melewati TTL")
		}
		if cache.Len() != 0 {
			t.Fatalf("cache setelah TTL kedaluwarsa harus kosong saat diakses, didapat %d", cache.Len())
		}
	})
}
