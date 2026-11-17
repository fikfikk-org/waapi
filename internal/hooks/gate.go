package hooks

import (
	"container/list"
	"context"
	"log/slog"
	"sync"
	"time"
)

type suppressKey struct {
	instanceID string
	seq        int64
}

type gateInstanceState struct {
	canSuppress bool
	cursor      int64
	notify      chan struct{}
}

// Gate mengimplementasikan webhook.Gate untuk menahan sementara pengiriman webhook bila instance memiliki aksi suppress_webhook.
type Gate struct {
	runs       RunStore
	now        func() time.Time
	mu         sync.Mutex
	instances  map[string]*gateInstanceState
	suppressed *boundedSet[suppressKey]
}

// NewGate membuat Webhook Gate baru yang opsional terhubung ke RunStore persisten.
func NewGate(runs RunStore, nowFn func() time.Time) *Gate {
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Gate{
		runs:       runs,
		now:        nowFn,
		instances:  make(map[string]*gateInstanceState),
		suppressed: newBoundedSet[suppressKey](SuppressLRUCap),
	}
}

func (g *Gate) ensureStateLocked(instanceID string) *gateInstanceState {
	st := g.instances[instanceID]
	if st == nil {
		st = &gateInstanceState{
			notify: make(chan struct{}),
		}
		g.instances[instanceID] = st
	}
	return st
}

// ConfigureInstance memperbarui status apakah instance berpotensi men-suppress event serta cursor awal hooks.
func (g *Gate) ConfigureInstance(instanceID string, canSuppress bool, cursor int64) {
	if g == nil || instanceID == "" {
		return
	}
	g.mu.Lock()
	st := g.ensureStateLocked(instanceID)
	st.canSuppress = canSuppress
	if cursor > st.cursor {
		st.cursor = cursor
	}
	close(st.notify)
	st.notify = make(chan struct{})
	g.mu.Unlock()
}

// AdvanceCursor memajukan posisi seq yang telah selesai dievaluasi hooks dan membangunkan deliverer webhook.
func (g *Gate) AdvanceCursor(instanceID string, seq int64) {
	if g == nil || instanceID == "" {
		return
	}
	g.mu.Lock()
	st := g.ensureStateLocked(instanceID)
	if seq > st.cursor {
		st.cursor = seq
	}
	close(st.notify)
	st.notify = make(chan struct{})
	g.mu.Unlock()
}

// MarkSuppressed mencatat dalam memori bahwa (instanceID, seq) di-suppress oleh hooks.
func (g *Gate) MarkSuppressed(instanceID string, seq int64) {
	if g == nil || instanceID == "" || seq <= 0 {
		return
	}
	g.mu.Lock()
	g.suppressed.Add(suppressKey{instanceID: instanceID, seq: seq})
	g.mu.Unlock()
}

// IsSuppressed memeriksa status suppressed di cache memori maupun di tabel hook_runs SQLite (tahan restart).
func (g *Gate) IsSuppressed(ctx context.Context, instanceID string, seq int64) bool {
	if g == nil || instanceID == "" || seq <= 0 {
		return false
	}
	g.mu.Lock()
	hit := g.suppressed.Contains(suppressKey{instanceID: instanceID, seq: seq})
	g.mu.Unlock()
	if hit {
		return true
	}
	if g.runs != nil {
		sup, err := g.runs.IsSeqSuppressed(ctx, instanceID, seq)
		if err == nil && sup {
			g.MarkSuppressed(instanceID, seq)
			return true
		}
	}
	return false
}

// Wait memenuhi interface webhook.Gate: langsung deliver=true bila tanpa rule suppress, atau menunggu cursor hooks <= 10 detik (fail-open).
func (g *Gate) Wait(ctx context.Context, instanceID string, seq int64) (bool, error) {
	if g == nil || instanceID == "" || seq <= 0 {
		return true, nil
	}

	var timer *time.Timer
	for {
		g.mu.Lock()
		st := g.instances[instanceID]
		if st == nil || !st.canSuppress {
			g.mu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			return true, nil
		}
		if st.cursor >= seq {
			g.mu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			return !g.IsSuppressed(ctx, instanceID, seq), nil
		}
		waitCh := st.notify
		g.mu.Unlock()

		if timer == nil {
			timer = time.NewTimer(GateWaitTimeout)
		}

		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-waitCh:
			continue
		case <-timer.C:
			slog.Warn("webhook gate timeout menunggu hooks, fail-open mengirim event",
				"instance", instanceID,
				"seq", seq,
			)
			return true, nil
		}
	}
}

// boundedSet adalah set LRU berukuran tetap berbasis container/list + map agar konsumsi memori selalu konstan.
type boundedSet[K comparable] struct {
	cap   int
	ll    *list.List
	items map[K]*list.Element
}

func newBoundedSet[K comparable](capLimit int) *boundedSet[K] {
	return &boundedSet[K]{
		cap:   capLimit,
		ll:    list.New(),
		items: make(map[K]*list.Element),
	}
}

func (s *boundedSet[K]) Add(k K) {
	if el, ok := s.items[k]; ok {
		s.ll.MoveToFront(el)
		return
	}
	el := s.ll.PushFront(k)
	s.items[k] = el
	for s.ll.Len() > s.cap {
		back := s.ll.Back()
		if back == nil {
			break
		}
		s.ll.Remove(back)
		delete(s.items, back.Value.(K))
	}
}

func (s *boundedSet[K]) Contains(k K) bool {
	if el, ok := s.items[k]; ok {
		s.ll.MoveToFront(el)
		return true
	}
	return false
}
