package wa

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
)

// emitterJournal mencatat event non-pesan (connection, pairing, health, call, ...) langsung ke journal.db.
type emitterJournal struct {
	j   *journal.Journal
	mgr *Manager
}

func newEmitterJournal(j *journal.Journal, mgr *Manager) Emitter {
	return &emitterJournal{j: j, mgr: mgr}
}

func (e *emitterJournal) Emit(ctx context.Context, instanceID, event string, data any) error {
	if e == nil || e.j == nil {
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

	dedupe := fmt.Sprintf("sys:%s:%s:%d", instanceID, event, now.UnixNano())
	ev := &journal.Event{
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
	if _, err := e.j.Append(ctx, ev, hooks...); err != nil {
		return err
	}
	return nil
}
