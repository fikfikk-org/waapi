package wa

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

// emitterJournal mencatat event non-pesan (connection, pairing, health, call, ...) ke journal.db
// sambil mempertahankan dual-write sementara ke webhook.Dispatcher v1 sampai Batch 4.
type emitterJournal struct {
	j   *journal.Journal
	mgr *Manager
	wh  *webhook.Dispatcher
}

func newEmitterJournal(j *journal.Journal, mgr *Manager, wh *webhook.Dispatcher) Emitter {
	return &emitterJournal{j: j, mgr: mgr, wh: wh}
}

func (e *emitterJournal) Emit(ctx context.Context, instanceID, event string, data any) error {
	if e == nil {
		return nil
	}
	now := time.Now()
	var payload []byte
	var err error
	if data != nil {
		payload, err = json.Marshal(data)
		if err != nil {
			return err
		}
	} else {
		payload = []byte("{}")
	}

	var ev *journal.Event
	if e.j != nil {
		dedupe := fmt.Sprintf("sys:%s:%s:%d", instanceID, event, now.UnixNano())
		ev = &journal.Event{
			InstanceID: instanceID,
			Type:       event,
			DedupeKey:  dedupe,
			TS:         now.Unix(),
			ReceivedAt: now.UnixMilli(),
			Payload:    payload,
		}
		var hooks []journal.TxHook
		if e.mgr != nil {
			hooks = e.mgr.collectHooks(ctx, ev, data)
		}
		res, err := e.j.Append(ctx, ev, hooks...)
		if err != nil {
			return err
		}
		ev.Seq = res.Seq
	}

	if e.wh != nil {
		if ev != nil && ev.Seq > 0 {
			e.wh.EnqueueEvent(*ev)
		} else {
			e.wh.Enqueue(instanceID, event, data)
		}
	}
	return nil
}
