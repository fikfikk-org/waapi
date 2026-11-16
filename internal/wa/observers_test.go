package wa

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type stubExecForObs struct{}

func (stubExecForObs) Ready(string) bool                                        { return true }
func (stubExecForObs) NewMessageID(string) (string, error)                      { return "3EB0OBS00001", nil }
func (stubExecForObs) Execute(context.Context, *outbox.Job) (outbox.ExecResult, error) {
	return outbox.ExecResult{}, nil
}

// TestReceiptObserver_GroupNonJournal memastikan receipt grup saat journal_receipts=dm tetap memajukan status outbox di transaksi journal.
func TestReceiptObserver_GroupNonJournal(t *testing.T) {
	_, j, mgr, inst := openInstanceTestEnv(t, nil, nil)
	ctx := context.Background()

	st := outbox.NewSQLiteStore(j.DB())
	outSvc := outbox.NewService(st, j, stubExecForObs{}, nil, nil, outbox.Options{})
	mgr.AttachOutbox(outSvc, nil)

	job, _, err := outSvc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: inst.id,
		Spec:       outbox.Spec{Type: "text", To: "1203630123456789@g.us", Text: "grup"},
	})
	if err != nil {
		t.Fatalf("Enqueue gagal: %v", err)
	}
	if _, err := st.MarkSent(ctx, job.ID, time.Now(), time.Now()); err != nil {
		t.Fatalf("MarkSent gagal: %v", err)
	}

	// Default JournalReceipts = dm -> NormalizeEvent mengembalikan ok=false untuk receipt grup, namun observer tetap wajib jalan.
	ok := inst.handleEventV2(&events.Receipt{
		MessageSource: types.MessageSource{
			Chat:     types.NewJID("1203630123456789", types.GroupServer),
			Sender:   types.NewJID("628111222333", types.DefaultUserServer),
			IsGroup:  true,
			IsFromMe: false,
		},
		MessageIDs: []types.MessageID{types.MessageID(job.WAID)},
		Timestamp:  time.Now(),
		Type:       types.ReceiptTypeDelivered,
	})
	if !ok {
		t.Fatal("handleEventV2 harus mengembalikan true")
	}

	got, err := outSvc.Get(ctx, inst.id, job.ID)
	if err != nil {
		t.Fatalf("Get gagal: %v", err)
	}
	if got.Status != outbox.StatusDelivered || got.RcptDelivered != 1 {
		t.Fatalf("status outbox grup seharusnya naik ke delivered: %+v", got)
	}
}

// TestReceiptObserver_DeliveredCallbackPostCommit memastikan onDelivered hanya dipanggil setelah transaksi sukses commit (bukan saat rollback).
func TestReceiptObserver_DeliveredCallbackPostCommit(t *testing.T) {
	_, j, mgr, inst := openInstanceTestEnv(t, nil, nil)
	ctx := context.Background()

	var deliveredTotal int
	mgr.SetHealthHooks(func(_ string, n int) { deliveredTotal += n }, nil, nil, nil)

	st := outbox.NewSQLiteStore(j.DB())
	outSvc := outbox.NewService(st, j, stubExecForObs{}, nil, nil, outbox.Options{})
	mgr.AttachOutbox(outSvc, nil)

	job, _, err := outSvc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: inst.id,
		Spec:       outbox.Spec{Type: "text", To: "628111222333@s.whatsapp.net", Text: "dm"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkSent(ctx, job.ID, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}

	raw := &events.Receipt{
		MessageSource: types.MessageSource{
			Chat:   types.NewJID("628111222333", types.DefaultUserServer),
			Sender: types.NewJID("628111222333", types.DefaultUserServer),
		},
		MessageIDs: []types.MessageID{types.MessageID(job.WAID)},
		Timestamp:  time.Now(),
		Type:       types.ReceiptTypeDelivered,
	}

	// Simulasikan transaksi rollback -> onDelivered tidak boleh berjalan.
	hook := NewReceiptObserverWithCallback(outSvc, func(_ string, n int) { deliveredTotal += n })(ctx, &journal.Event{InstanceID: inst.id}, raw)
	_ = j.RunTx(ctx, hook, func(context.Context, *sql.Tx, int64) error { return errors.New("paksa rollback") })
	if deliveredTotal != 0 {
		t.Fatalf("onDelivered seharusnya tidak dipanggil saat transaksi rollback, didapat %d", deliveredTotal)
	}

	// Kirim receipt lewat handleEventV2 hingga commit sukses -> onDelivered terpanggil tepat 1.
	if ok := inst.handleEventV2(raw); !ok {
		t.Fatal("handleEventV2 receipt gagal")
	}
	if deliveredTotal != 1 {
		t.Fatalf("onDelivered diharapkan terpanggil +1 setelah commit, didapat %d", deliveredTotal)
	}
}
