package wa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// TestPollVote_PemetaanHash memastikan suara polling dipetakan lewat SHA-256 nama opsi dan suara terbaru menimpa suara sebelumnya.
func TestPollVote_PemetaanHash(t *testing.T) {
	ctx := context.Background()
	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{MaxConns: 1})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	ps := NewPollStore(j.DB())
	const (
		instID = "sales"
		chat   = "120363012345@g.us"
		pollID = "POLL-1"
		voter  = "628123456789@s.whatsapp.net"
	)

	if err := ps.SavePoll(ctx, Poll{
		InstanceID: instID,
		Chat:       chat,
		WAID:       pollID,
		Creator:    "628000000001@s.whatsapp.net",
		Name:       "Hari rapat?",
		Options:    []string{"Senin", "Selasa"},
		Selectable: 1,
		CreatedAt:  1700000000,
	}); err != nil {
		t.Fatalf("SavePoll gagal: %v", err)
	}

	hSelasa := sha256.Sum256([]byte("Selasa"))
	ev1 := &journal.Event{
		InstanceID: instID,
		Type:       "poll_vote",
		Chat:       chat,
		Sender:     voter,
		WAID:       "VOTE-1",
		DedupeKey:  "pv:" + chat + ":" + voter + ":VOTE-1",
		TS:         1700000010,
		ReceivedAt: 1700000010000,
	}
	if _, err := ps.RecordVoteAndAppend(ctx, j, ev1, pollID, voter, [][]byte{hSelasa[:]}, nil); err != nil {
		t.Fatalf("vote pertama gagal: %v", err)
	}

	tally, err := ps.GetTally(ctx, instID, chat, pollID)
	if err != nil {
		t.Fatalf("GetTally gagal: %v", err)
	}
	if tally.TotalVoters != 1 || tally.Options[0].Votes != 0 || tally.Options[1].Votes != 1 || len(tally.Options[1].Voters) != 1 {
		t.Fatalf("tally vote pertama tidak sesuai: %+v", tally)
	}

	// Suara kedua memilih Senin -> harus menggantikan pilihan Selasa sebelumnya.
	hSenin := sha256.Sum256([]byte("Senin"))
	var payloadJSON []byte
	ev2 := &journal.Event{
		InstanceID: instID,
		Type:       "poll_vote",
		Chat:       chat,
		Sender:     voter,
		WAID:       "VOTE-2",
		DedupeKey:  "pv:" + chat + ":" + voter + ":VOTE-2",
		TS:         1700000020,
		ReceivedAt: 1700000020000,
	}
	if _, err := ps.RecordVoteAndAppend(ctx, j, ev2, pollID, voter, [][]byte{hSenin[:]}, &payloadJSON); err != nil {
		t.Fatalf("vote kedua gagal: %v", err)
	}

	var p PollVotePayload
	if err := json.Unmarshal(payloadJSON, &p); err != nil {
		t.Fatalf("decode PollVotePayload gagal: %v", err)
	}
	if !p.PollKnown || len(p.Selected) != 1 || p.Selected[0] != "Senin" {
		t.Fatalf("payload poll_vote tidak sesuai: %+v", p)
	}

	tally2, err := ps.GetTally(ctx, instID, chat, pollID)
	if err != nil {
		t.Fatalf("GetTally kedua gagal: %v", err)
	}
	if tally2.TotalVoters != 1 || tally2.Options[0].Votes != 1 || tally2.Options[1].Votes != 0 {
		t.Fatalf("tally setelah suara kedua harus pindah ke Senin: %+v", tally2)
	}
}

// TestPollVote_PollTidakDikenal memastikan vote untuk polling yang tidak dikenal tetap di-journal dengan poll_known=false dan hash hex.
func TestPollVote_PollTidakDikenal(t *testing.T) {
	ctx := context.Background()
	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{MaxConns: 1})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	ps := NewPollStore(j.DB())
	rawHash := sha256.Sum256([]byte("OpsiEntah"))
	var outPayload []byte
	ev := &journal.Event{
		InstanceID: "sales",
		Type:       "poll_vote",
		Chat:       "628123456789@s.whatsapp.net",
		Sender:     "628123456789@s.whatsapp.net",
		WAID:       "VOTE-UNK",
		DedupeKey:  "pv:unk:1",
		TS:         1700000050,
		ReceivedAt: 1700000050000,
	}
	// Simulasi simpan ke events untuk pollID yang tidak ada di tabel polls.
	res, err := ps.RecordVoteAndAppend(ctx, j, ev, "POLL-UNKNOWN", "628123456789@s.whatsapp.net", [][]byte{rawHash[:]}, &outPayload)
	if err != nil {
		t.Fatalf("RecordVoteAndAppend gagal: %v", err)
	}
	if res.Seq <= 0 {
		t.Fatalf("seq event poll_vote harus > 0, didapat %d", res.Seq)
	}

	var vp PollVotePayload
	if err := json.Unmarshal(outPayload, &vp); err != nil {
		t.Fatalf("unmarshal PollVotePayload gagal: %v", err)
	}
	if vp.PollKnown || len(vp.Selected) != 0 || len(vp.SelectedHashes) != 1 || vp.SelectedHashes[0] != hex.EncodeToString(rawHash[:]) {
		t.Fatalf("payload polling tak dikenal tidak sesuai: %+v", vp)
	}
}

// TestPollVote_PollKeluarTersimpanSebelumKirim memastikan baris polling keluar tersimpan di tabel polls meski pengiriman WhatsApp gagal setelahnya.
func TestPollVote_PollKeluarTersimpanSebelumKirim(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 1})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	ps := NewPollStore(j.DB())
	mgr, err := NewManager(ctx, cfg, st, nil, WithJournal(j), WithPolls(ps))
	if err != nil {
		t.Fatalf("NewManager gagal: %v", err)
	}
	defer mgr.Close()

	_, err = mgr.Create(ctx, "sales", 60, false)
	if err != nil {
		t.Fatalf("mgr.Create gagal: %v", err)
	}

	sendErr := errors.New("simulasi gagal jaringan setelah persist poll")
	job := &outbox.Job{
		ID:         "msg_testpoll",
		InstanceID: "sales",
		WAID:       "3EB0POLLOUT01",
		Spec: outbox.Spec{
			Type: "poll",
			To:   "628123456789@s.whatsapp.net",
			Poll: &outbox.PollSpec{
				Name:       "Menu makan siang?",
				Options:    []string{"Soto", "Bakso"},
				Selectable: 1,
			},
		},
	}

	deps := &fakeComposeDeps{ownJID: types.NewJID("628000000001", types.DefaultUserServer)}
	_, execErr := mgr.executeWithSender(ctx, job, deps, func(context.Context, types.JID, *waE2E.Message, ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		return whatsmeow.SendResponse{}, sendErr
	})
	if !errors.Is(execErr, sendErr) {
		t.Fatalf("diharapkan error sendErr (%v), didapat: %v", sendErr, execErr)
	}

	tally, err := ps.GetTally(ctx, "sales", "628123456789@s.whatsapp.net", "3EB0POLLOUT01")
	if err != nil {
		t.Fatalf("baris polls wajib tetap ada meskipun SendMessage gagal: %v", err)
	}
	if tally.Name != "Menu makan siang?" || len(tally.Options) != 2 {
		t.Fatalf("isi polling tersimpan tidak sesuai: %+v", tally)
	}
}

// TestPollUpdate_TidakMasukMessageUnknown memastikan PollUpdateMessage diabaikan oleh normalizer message standar dan PollCreationMessage masuk tersimpan via TxHook.
func TestPollUpdate_TidakMasukMessageUnknown(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, _ := store.Open(ctx, filepath.Join(tmp, "app.db"))
	defer st.Close()
	j, _ := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 1})
	defer j.Close()

	ps := NewPollStore(j.DB())
	mgr, _ := NewManager(ctx, cfg, st, nil, WithJournal(j), WithPolls(ps))
	defer mgr.Close()
	_, _ = mgr.Create(ctx, "sales", 60, false)
	inst, _ := mgr.Get("sales")

	// PollUpdateMessage tidak boleh menghasilkan event=message (type=unknown).
	if _, ok := NormalizeEvent(ctx, "sales", &events.Message{
		Info: types.MessageInfo{ID: "UPD1", MessageSource: types.MessageSource{Chat: types.NewJID("628111111111", types.DefaultUserServer)}},
		Message: &waE2E.Message{
			PollUpdateMessage: &waE2E.PollUpdateMessage{},
		},
	}, NormalizeContext{}); ok {
		t.Fatal("PollUpdateMessage tidak boleh dinormalisasi sebagai event message unknown")
	}

	// PollCreationMessage masuk harus menyimpan definisi ke tabel polls lewat TxHook atomik.
	pollIn := &events.Message{
		Info: types.MessageInfo{
			ID: "POLLIN-1",
			MessageSource: types.MessageSource{
				Chat:   types.NewJID("120363012345", types.GroupServer),
				Sender: types.NewJID("628123456789", types.DefaultUserServer),
			},
		},
		Message: &waE2E.Message{
			PollCreationMessageV3: &waE2E.PollCreationMessage{
				Name:                   proto.String("Pilih hari?"),
				SelectableOptionsCount: proto.Uint32(1),
				Options: []*waE2E.PollCreationMessage_Option{
					{OptionName: proto.String("Rabu")},
					{OptionName: proto.String("Kamis")},
				},
			},
		},
	}
	if !inst.handleEventV2(pollIn) {
		t.Fatal("handleEventV2 gagal memproses PollCreationMessage masuk")
	}
	tally, err := ps.GetTally(ctx, "sales", "120363012345@g.us", "POLLIN-1")
	if err != nil || tally.Name != "Pilih hari?" || len(tally.Options) != 2 {
		t.Fatalf("polling masuk harus tersimpan via TxHook: tally=%+v err=%v", tally, err)
	}
}
