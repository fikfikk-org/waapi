package media

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	defaultRetryTimeout = 30 * time.Second
	maxRetryWaiters     = 16
)

var (
	// ErrMediaGone dikembalikan ketika media tidak lagi tersedia setelah upaya retry ke pengirim gagal atau habis waktu.
	ErrMediaGone = errors.New("media sudah tidak tersedia pada CDN maupun pengirim (410)")
	// ErrRetryWaitersFull dikembalikan jika jumlah penunggu media retry aktif telah mencapai batas 16.
	ErrRetryWaitersFull = errors.New("kapasitas penunggu media retry sedang penuh")
)

// RetryReceiptSender mengirim permintaan unggah ulang media terenkripsi kepada pengirim pesan asli.
type RetryReceiptSender func(ctx context.Context, info *types.MessageInfo, mediaKey []byte) error

type retrySlot struct {
	ch   chan *events.MediaRetry
	refs int
}

// RetryWaiter memasangkan pengiriman SendMediaRetryReceipt dengan kedatangan event *events.MediaRetry secara aman dari race.
type RetryWaiter struct {
	mu      sync.Mutex
	waiters map[string]*retrySlot
	timeout time.Duration
}

// NewRetryWaiter membuat RetryWaiter baru dengan batas maksimal 16 penunggu dan timeout 30 detik.
func NewRetryWaiter() *RetryWaiter {
	return &RetryWaiter{
		waiters: make(map[string]*retrySlot),
		timeout: defaultRetryTimeout,
	}
}

// Reserve memesan slot penunggu sebelum mengirim SendMediaRetryReceipt agar event cepat tidak terlewat.
func (w *RetryWaiter) Reserve(msgID string) (func(), error) {
	if w == nil || msgID == "" {
		return func() {}, ErrMediaGone
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	slot, exists := w.waiters[msgID]
	if !exists {
		if len(w.waiters) >= maxRetryWaiters {
			return nil, ErrRetryWaitersFull
		}
		slot = &retrySlot{ch: make(chan *events.MediaRetry, 1)}
		w.waiters[msgID] = slot
	}
	slot.refs++

	var once sync.Once
	return func() {
		once.Do(func() {
			w.release(msgID)
		})
	}, nil
}

func (w *RetryWaiter) release(msgID string) {
	w.mu.Lock()
	if slot, ok := w.waiters[msgID]; ok {
		slot.refs--
		if slot.refs <= 0 {
			delete(w.waiters, msgID)
		}
	}
	w.mu.Unlock()
}

// Await menunggu kedatangan *events.MediaRetry untuk msgID hingga maksimal 30 detik atau pembatalan ctx.
func (w *RetryWaiter) Await(ctx context.Context, msgID string) (*events.MediaRetry, error) {
	if w == nil || msgID == "" {
		return nil, ErrMediaGone
	}

	w.mu.Lock()
	slot, ok := w.waiters[msgID]
	ownsReserve := false
	if !ok {
		if len(w.waiters) >= maxRetryWaiters {
			w.mu.Unlock()
			return nil, ErrRetryWaitersFull
		}
		slot = &retrySlot{ch: make(chan *events.MediaRetry, 1), refs: 1}
		w.waiters[msgID] = slot
		ownsReserve = true
	}
	w.mu.Unlock()

	if ownsReserve {
		defer w.release(msgID)
	}

	timeout := w.timeout
	if timeout <= 0 {
		timeout = defaultRetryTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case evt := <-slot.ch:
		return evt, nil
	case <-timer.C:
		return nil, ErrMediaGone
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Deliver menyalurkan notifikasi MediaRetry dari whatsmeow ke penunggu yang sedang menunggu msgID terkait.
func (w *RetryWaiter) Deliver(evt *events.MediaRetry) {
	if w == nil || evt == nil || evt.MessageID == "" {
		return
	}
	w.mu.Lock()
	slot := w.waiters[string(evt.MessageID)]
	w.mu.Unlock()
	if slot == nil {
		return
	}
	select {
	case slot.ch <- evt:
	default:
	}
}
