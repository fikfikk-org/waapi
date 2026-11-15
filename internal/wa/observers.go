package wa

import (
	"context"
	"database/sql"
	"strings"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type observerCtxKey int

const ctxKeyObserverInstance observerCtxKey = 1

func withObserverInstanceID(ctx context.Context, instanceID string) context.Context {
	return context.WithValue(ctx, ctxKeyObserverInstance, instanceID)
}

func observerInstanceID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyObserverInstance).(string)
	return v
}

// NewReceiptObserver membuat InboundObserver yang memajukan status outbox saat receipt diterima sebelum ack ke WhatsApp.
func NewReceiptObserver(svc *outbox.Service) InboundObserver {
	return func(ctx context.Context, ev *journal.Event, raw any) journal.TxHook {
		if svc == nil {
			return nil
		}
		rec, ok := raw.(*events.Receipt)
		if !ok || rec == nil || rec.IsFromMe || len(rec.MessageIDs) == 0 {
			return nil
		}

		rtype := strings.ToLower(string(rec.Type))
		switch rtype {
		case "", "delivered", "read", "played":
		default:
			return nil
		}

		instanceID := ""
		if ev != nil {
			instanceID = ev.InstanceID
		} else {
			instanceID = observerInstanceID(ctx)
		}
		if instanceID == "" {
			return nil
		}

		waIDs := make([]string, 0, len(rec.MessageIDs))
		for _, id := range rec.MessageIDs {
			if id != "" {
				waIDs = append(waIDs, string(id))
			}
		}
		if len(waIDs) == 0 {
			return nil
		}

		group := rec.IsGroup || rec.Chat.Server == types.GroupServer
		return func(txCtx context.Context, tx *sql.Tx, _ int64) error {
			_, err := svc.MarkReceiptTx(txCtx, tx, instanceID, waIDs, rtype, group)
			return err
		}
	}
}
