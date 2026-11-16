package wa

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	callReplyCacheCap = 1024
	callReplyCacheTTL = 6 * time.Hour
)

type callClient interface {
	RejectCall(ctx context.Context, callFrom types.JID, callID string) error
}

type callOutbox interface {
	Enqueue(ctx context.Context, req outbox.EnqueueRequest) (*outbox.Job, bool, error)
}

type callJournal interface {
	Append(ctx context.Context, ev *journal.Event, hooks ...journal.TxHook) (journal.AppendResult, error)
	Notify(instanceID string)
}

type callRejectHandler struct {
	replyLRU *LRU[string, struct{}]
}

func newCallRejectHandler() *callRejectHandler {
	return &callRejectHandler{
		replyLRU: NewLRU[string, struct{}](callReplyCacheCap, callReplyCacheTTL),
	}
}

func (h *callRejectHandler) processCallOffer(
	ctx context.Context,
	instanceID string,
	evt *events.CallOffer,
	offline bool,
	settings store.InstanceSettings,
	cli callClient,
	ob callOutbox,
	jrn callJournal,
	lid LIDResolver,
) {
	// Panggilan saat sinkronisasi offline, panggilan grup, atau saat fitur nonaktif wajib dilewati.
	if h == nil || evt == nil || cli == nil || offline || !settings.AutoRejectCalls {
		return
	}
	if !evt.GroupJID.IsEmpty() || evt.From.IsEmpty() || evt.CallID == "" {
		return
	}

	if err := cli.RejectCall(ctx, evt.From, evt.CallID); err != nil {
		slog.Warn("gagal menolak panggilan masuk secara otomatis", "instance", instanceID, "call_id", evt.CallID, "err", err)
		return
	}

	nc := NormalizeContext{
		Now:     time.Now(),
		Offline: offline,
		LID:     lid,
	}
	if jrn != nil {
		if rejectedEv, ok := buildCallJournalEvent(ctx, instanceID, evt.BasicCallMeta, "rejected", "", nc); ok {
			if _, err := jrn.Append(ctx, rejectedEv); err != nil {
				slog.Warn("gagal menyimpan event call rejected ke journal", "instance", instanceID, "call_id", evt.CallID, "err", err)
			} else {
				jrn.Notify(instanceID)
			}
		}
	}

	replyMsg := strings.TrimSpace(settings.CallRejectMessage)
	if replyMsg == "" || ob == nil {
		return
	}

	callerTarget := canonicalCallerTarget(ctx, evt.BasicCallMeta, lid)
	if callerTarget == "" {
		return
	}

	// Klaim kuota balasan 1x per 6 jam per penelepon (berbasis JID kanonik PN bila tersedia).
	if !h.replyLRU.ClaimIfExpired(callerTarget, struct{}{}) {
		return
	}

	_, _, err := ob.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: instanceID,
		IdemKey:    "call:" + evt.CallID,
		Spec: outbox.Spec{
			Type: "text",
			To:   callerTarget,
			Text: replyMsg,
		},
		Origin: outbox.Origin{Kind: "system"},
	})
	if err != nil && !errors.Is(err, outbox.ErrIdempotencyMismatch) {
		h.replyLRU.Delete(callerTarget)
		slog.Warn("gagal mengantrekan pesan penolakan panggilan ke outbox", "instance", instanceID, "call_id", evt.CallID, "err", err)
	}
}

func canonicalCallerTarget(ctx context.Context, meta types.BasicCallMeta, lid LIDResolver) string {
	if pn := resolveCallFromPN(ctx, meta, lid); pn != "" {
		return pn
	}
	if !meta.From.IsEmpty() {
		return meta.From.ToNonAD().String()
	}
	if !meta.CallCreator.IsEmpty() {
		return meta.CallCreator.ToNonAD().String()
	}
	return ""
}

func (i *Instance) ensureCallHandler() *callRejectHandler {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.calls == nil {
		i.calls = newCallRejectHandler()
	}
	return i.calls
}

func (i *Instance) triggerCallAutoReject(evt *events.CallOffer, offline bool) {
	if i == nil || evt == nil || offline || !evt.GroupJID.IsEmpty() || evt.From.IsEmpty() || evt.CallID == "" {
		return
	}
	settings := i.cachedSettings()
	if !settings.AutoRejectCalls {
		return
	}
	var ccli callClient = i.callCli
	if ccli == nil {
		cli := i.getClient()
		if cli == nil {
			return
		}
		ccli = cli
	}

	var ob callOutbox
	var jrn callJournal
	if i.mgr != nil {
		if i.mgr.outbox != nil {
			ob = i.mgr.outbox
		}
		if i.mgr.journal != nil {
			jrn = i.mgr.journal
		}
	}
	lid := i.lidResolver()
	handler := i.ensureCallHandler()
	parent := i.baseCtx
	if parent == nil {
		parent = context.Background()
	}

	go func() {
		ctx, cancel := context.WithTimeout(parent, 15*time.Second)
		defer cancel()
		handler.processCallOffer(ctx, i.id, evt, offline, settings, ccli, ob, jrn, lid)
	}()
}
