package wa

import (
	"container/list"
	"sync"
	"time"
)

type lruEntry[K comparable, V any] struct {
	key       K
	val       V
	expiresAt time.Time
}

// LRU adalah cache in-memory berkapasitas tetap dengan kebijakan Least Recently Used dan masa berlaku TTL.
type LRU[K comparable, V any] struct {
	mu    sync.Mutex
	cap   int
	ttl   time.Duration
	ll    *list.List
	items map[K]*list.Element
}

// NewLRU membuat cache LRU berkapasitas capacity dan masa berlaku ttl (<= 0 berarti tanpa batas waktu).
func NewLRU[K comparable, V any](capacity int, ttl time.Duration) *LRU[K, V] {
	return &LRU[K, V]{
		cap:   capacity,
		ttl:   ttl,
		ll:    list.New(),
		items: make(map[K]*list.Element),
	}
}

func (c *LRU[K, V]) ensureInitLocked() {
	if c.ll == nil {
		c.ll = list.New()
	}
	if c.items == nil {
		c.items = make(map[K]*list.Element)
	}
}

func (c *LRU[K, V]) expiredLocked(e *lruEntry[K, V], now time.Time) bool {
	return c.ttl > 0 && !now.Before(e.expiresAt)
}

func (c *LRU[K, V]) removeElemLocked(elem *list.Element) {
	if elem == nil {
		return
	}
	entry := elem.Value.(*lruEntry[K, V])
	delete(c.items, entry.key)
	c.ll.Remove(elem)
}

func (c *LRU[K, V]) setLocked(key K, val V, now time.Time) {
	if c.cap <= 0 {
		return
	}
	c.ensureInitLocked()

	var exp time.Time
	if c.ttl > 0 {
		exp = now.Add(c.ttl)
	}

	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lruEntry[K, V])
		entry.val = val
		entry.expiresAt = exp
		c.ll.MoveToFront(elem)
		return
	}

	for c.ll.Len() >= c.cap {
		c.removeElemLocked(c.ll.Back())
	}
	elem := c.ll.PushFront(&lruEntry[K, V]{
		key:       key,
		val:       val,
		expiresAt: exp,
	})
	c.items[key] = elem
}

// Get mengambil nilai berdasarkan kunci jika masih ada dan belum melewati TTL sekaligus memperbarui posisi LRU.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	var zero V
	if c == nil || c.cap <= 0 {
		return zero, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items == nil || c.ll == nil {
		return zero, false
	}

	elem, ok := c.items[key]
	if !ok {
		return zero, false
	}
	entry := elem.Value.(*lruEntry[K, V])
	if c.expiredLocked(entry, time.Now()) {
		c.removeElemLocked(elem)
		return zero, false
	}
	c.ll.MoveToFront(elem)
	return entry.val, true
}

// Set menyimpan atau memperbarui nilai pada kunci dengan masa berlaku baru dan mengeluarkan entri terlama bila penuh.
func (c *LRU[K, V]) Set(key K, val V) {
	if c == nil || c.cap <= 0 {
		return
	}
	c.mu.Lock()
	c.setLocked(key, val, time.Now())
	c.mu.Unlock()
}

// ClaimIfExpired mengklaim kunci secara atomik bila belum ada atau sudah kedaluwarsa, lalu mengembalikan true jika berhasil diklaim.
func (c *LRU[K, V]) ClaimIfExpired(key K, val V) bool {
	if c == nil || c.cap <= 0 {
		return true
	}

	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitLocked()

	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lruEntry[K, V])
		if !c.expiredLocked(entry, now) {
			c.ll.MoveToFront(elem)
			return false
		}
		c.removeElemLocked(elem)
	}
	c.setLocked(key, val, now)
	return true
}

// Delete menghapus satu kunci dari cache apabila ada.
func (c *LRU[K, V]) Delete(key K) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.items != nil && c.ll != nil {
		if elem, ok := c.items[key]; ok {
			c.removeElemLocked(elem)
		}
	}
	c.mu.Unlock()
}

// Clear mengosongkan seluruh isi cache.
func (c *LRU[K, V]) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.ll != nil {
		c.ll.Init()
	}
	c.items = make(map[K]*list.Element)
	c.mu.Unlock()
}

// Len mengembalikan jumlah entri yang saat ini tersimpan di cache.
func (c *LRU[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	n := 0
	if c.ll != nil {
		n = c.ll.Len()
	}
	c.mu.Unlock()
	return n
}
