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

// emitterV1 adalah implementasi sementara Emitter yang meneruskan langsung ke webhook.Dispatcher.
type emitterV1 struct {
	wh *webhook.Dispatcher
}

func newEmitterV1(wh *webhook.Dispatcher) Emitter {
	return &emitterV1{wh: wh}
}

func (e *emitterV1) Emit(_ context.Context, instanceID, event string, data any) error {
	if e == nil || e.wh == nil {
		return nil
	}
	e.wh.Enqueue(instanceID, event, data)
	return nil
}
