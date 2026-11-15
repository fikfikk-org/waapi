// Package idgen menghasilkan pengenal unik terurut waktu menggunakan encoding Crockford Base32 (ULID 26 karakter).
package idgen

import (
	"crypto/rand"
	"sync"
	"time"
)

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var (
	mu       sync.Mutex
	lastMS   int64
	lastRand [10]byte
)

// New menghasilkan ID baru berformat "<prefix><26-char Crockford ULID>" memakai waktu sekarang.
func New(prefix string) string {
	return NewAt(prefix, time.Now())
}

// NewAt menghasilkan ID unik berformat "<prefix><26-char Crockford ULID>" yang monoton untuk waktu t.
func NewAt(prefix string, t time.Time) string {
	ms := t.UnixMilli()
	if ms < 0 {
		ms = 0
	}

	mu.Lock()
	if ms > lastMS {
		if _, err := rand.Read(lastRand[:]); err != nil {
			// Fallback deterministik tetap menjaga keunikan bila sumber entropi gagal sementara.
			for i := range lastRand {
				lastRand[i]++
			}
		}
		lastMS = ms
	} else {
		// Pertahankan urutan leksikografis ketat ketika beberapa ID dibuat di milidetik yang sama atau jam mundur.
		ms = lastMS
		if !inc80(&lastRand) {
			lastMS++
			ms = lastMS
			_, _ = rand.Read(lastRand[:])
		}
	}
	rnd := lastRand
	mu.Unlock()

	var buf [26]byte
	u := uint64(ms)
	buf[0] = crockford[(u>>45)&0x1F]
	buf[1] = crockford[(u>>40)&0x1F]
	buf[2] = crockford[(u>>35)&0x1F]
	buf[3] = crockford[(u>>30)&0x1F]
	buf[4] = crockford[(u>>25)&0x1F]
	buf[5] = crockford[(u>>20)&0x1F]
	buf[6] = crockford[(u>>15)&0x1F]
	buf[7] = crockford[(u>>10)&0x1F]
	buf[8] = crockford[(u>>5)&0x1F]
	buf[9] = crockford[u&0x1F]

	encode40(buf[10:18], rnd[0:5])
	encode40(buf[18:26], rnd[5:10])

	if prefix == "" {
		return string(buf[:])
	}
	return prefix + string(buf[:])
}

func encode40(dst, src []byte) {
	v := uint64(src[0])<<32 | uint64(src[1])<<24 | uint64(src[2])<<16 | uint64(src[3])<<8 | uint64(src[4])
	dst[0] = crockford[(v>>35)&0x1F]
	dst[1] = crockford[(v>>30)&0x1F]
	dst[2] = crockford[(v>>25)&0x1F]
	dst[3] = crockford[(v>>20)&0x1F]
	dst[4] = crockford[(v>>15)&0x1F]
	dst[5] = crockford[(v>>10)&0x1F]
	dst[6] = crockford[(v>>5)&0x1F]
	dst[7] = crockford[v&0x1F]
}

func inc80(b *[10]byte) bool {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] != 0 {
			return true
		}
	}
	return false
}
