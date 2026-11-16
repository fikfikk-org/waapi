package wa

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type fakeCallClient struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeCallClient) RejectCall(_ context.Context, callFrom types.JID, callID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, callFrom.String()+"|"+callID)
	return nil
}

func (f *fakeCallClient) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type fakeCallOutbox struct {
	mu   sync.Mutex
	reqs []outbox.EnqueueRequest
}

func (f *fakeCallOutbox) Enqueue(_ context.Context, req outbox.EnqueueRequest) (*outbox.Job, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return &outbox.Job{ID: req.IdemKey}, false, nil
}

func (f *fakeCallOutbox) all() []outbox.EnqueueRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]outbox.EnqueueRequest, len(f.reqs))
	copy(out, f.reqs)
	return out
}

type fakeCallJournal struct {
	mu       sync.Mutex
	events   []*journal.Event
	notified []string
}

func (f *fakeCallJournal) Append(_ context.Context, ev *journal.Event, _ ...journal.TxHook) (journal.AppendResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return journal.AppendResult{Seq: int64(len(f.events))}, nil
}

func (f *fakeCallJournal) Notify(instanceID string) {
	f.mu.Lock()
	f.notified = append(f.notified, instanceID)
	f.mu.Unlock()
}

func (f *fakeCallJournal) all() []*journal.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*journal.Event, len(f.events))
	copy(out, f.events)
	return out
}

// TestCalls_AutoRejectSekaliPer6Jam memastikan 2 panggilan dalam 1 jam memicu 2 reject & 1 pesan, lalu pesan kedua setelah 6 jam, serta guard offline/grup.
func TestCalls_AutoRejectSekaliPer6Jam(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		cli := &fakeCallClient{}
		ob := &fakeCallOutbox{}
		jrn := &fakeCallJournal{}
		h := newCallRejectHandler()

		callerLID := types.NewJID("777888999", types.HiddenUserServer)
		callerPN := types.NewJID("6281234567890", types.DefaultUserServer)
		lidMap := mapLIDResolver{callerLID.String(): callerPN}
		settings := store.Defaults()
		settings.AutoRejectCalls = true
		settings.CallRejectMessage = "Maaf, nomor ini tidak menerima panggilan suara/video."

		// Panggilan 1 (memakai LID yang teresolusi ke PN) -> reject + event rejected + 1 pesan outbox.
		h.processCallOffer(ctx, "sales", &events.CallOffer{
			BasicCallMeta: types.BasicCallMeta{
				From:        callerLID,
				CallCreator: callerLID,
				CallID:      "C1",
				Timestamp:   time.Now(),
			},
		}, false, settings, cli, ob, jrn, lidMap)
		synctest.Wait()

		if cli.count() != 1 {
			t.Fatalf("diharapkan 1 RejectCall pada panggilan pertama, didapat %d", cli.count())
		}
		msgs := ob.all()
		if len(msgs) != 1 {
			t.Fatalf("diharapkan 1 pesan balasan outbox, didapat %d", len(msgs))
		}
		if msgs[0].Origin.Kind != "system" || msgs[0].IdemKey != "call:C1" || msgs[0].Spec.To != callerPN.String() {
			t.Fatalf("parameter outbox panggilan pertama tidak sesuai: %+v", msgs[0])
		}
		evs := jrn.all()
		if len(evs) != 1 || evs[0].Type != "call" || evs[0].DedupeKey != "c:C1:rejected" {
			t.Fatalf("journal rejected pertama tidak sesuai: %+v", evs)
		}
		var payload CallEventV2
		if err := json.Unmarshal(evs[0].Payload, &payload); err != nil || payload.Action != "rejected" {
			t.Fatalf("payload call rejected tidak sesuai: %v (%+v)", err, payload)
		}

		// 1 jam kemudian, pemanggil yang sama menelepon lagi lewat PN -> tetap reject, tapi pesan TIDAK dikirim ulang (jendela 6 jam).
		time.Sleep(1 * time.Hour)
		synctest.Wait()
		h.processCallOffer(ctx, "sales", &events.CallOffer{
			BasicCallMeta: types.BasicCallMeta{
				From:      callerPN,
				CallID:    "C2",
				Timestamp: time.Now(),
			},
		}, false, settings, cli, ob, jrn, lidMap)
		synctest.Wait()

		if cli.count() != 2 {
			t.Fatalf("diharapkan 2 RejectCall setelah panggilan kedua, didapat %d", cli.count())
		}
		if len(ob.all()) != 1 {
			t.Fatalf("pesan balasan ke pemanggil yang sama dalam 1 jam harus tetap 1, didapat %d", len(ob.all()))
		}

		// Lewati 5 jam + 1 detik (total > 6 jam dari panggilan pertama) -> pesan kedua BOLEH dikirim.
		time.Sleep(5*time.Hour + time.Second)
		synctest.Wait()
		h.processCallOffer(ctx, "sales", &events.CallOffer{
			BasicCallMeta: types.BasicCallMeta{
				From:      callerPN,
				CallID:    "C3",
				Timestamp: time.Now(),
			},
		}, false, settings, cli, ob, jrn, lidMap)
		synctest.Wait()

		if cli.count() != 3 {
			t.Fatalf("diharapkan 3 RejectCall setelah 6 jam, didapat %d", cli.count())
		}
		msgs = ob.all()
		if len(msgs) != 2 || msgs[1].IdemKey != "call:C3" {
			t.Fatalf("diharapkan pesan kedua dikirim setelah 6 jam, didapat %+v", msgs)
		}

		// Guard offline sync dan panggilan grup wajib dilewati (tanpa reject & tanpa pesan).
		h.processCallOffer(ctx, "sales", &events.CallOffer{
			BasicCallMeta: types.BasicCallMeta{From: callerPN, CallID: "C4-OFFLINE", Timestamp: time.Now()},
		}, true, settings, cli, ob, jrn, lidMap)
		h.processCallOffer(ctx, "sales", &events.CallOffer{
			BasicCallMeta: types.BasicCallMeta{
				From:      callerPN,
				GroupJID:  types.NewJID("12036301122334455", types.GroupServer),
				CallID:    "C5-GROUP",
				Timestamp: time.Now(),
			},
		}, false, settings, cli, ob, jrn, lidMap)
		synctest.Wait()

		if cli.count() != 3 || len(ob.all()) != 2 {
			t.Fatalf("offline sync dan panggilan grup harus dilewati: rejects=%d msgs=%d", cli.count(), len(ob.all()))
		}
	})
}
