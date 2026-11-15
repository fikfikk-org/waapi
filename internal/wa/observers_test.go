package wa

import (
	"context"
	"testing"
	"time"

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
