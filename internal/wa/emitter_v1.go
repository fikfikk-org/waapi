package wa

import (
	"context"

	"github.com/fikfikk-org/waapi/internal/webhook"
)

// Emitter mencatat dan meneruskan event non-pesan milik sebuah instance WhatsApp.
type Emitter interface {
	// Emit mencatat event non-pesan (connection, health, call, ...) milik sebuah instance.
	Emit(ctx context.Context, instanceID, event string, data any) error
}

// emitterV1 dipertahankan sebagai stub tanpa-efek bila daemon dijalankan tanpa journal.
type emitterV1 struct {
	wh *webhook.Dispatcher
}

func newEmitterV1(wh *webhook.Dispatcher) Emitter {
	return &emitterV1{wh: wh}
}

func (e *emitterV1) Emit(context.Context, string, string, any) error {
	return nil
}
