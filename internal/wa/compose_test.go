package wa

import (
	"context"
	"strings"
	"testing"

	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type fakeComposeDeps struct {
	ownJID       types.JID
	quotedByKey  map[string]QuotedInfo
	pollCalls    []struct{ name string; opts []string; sel int }
	reactCalls   []struct{ chat, sender types.JID; id types.MessageID; emoji string }
	editCalls    []struct{ chat types.JID; id types.MessageID; content *waE2E.Message }
	revokeCalls  []struct{ chat, sender types.JID; id types.MessageID }
	mediaUpload  media.Uploaded
	mediaErr     error
	mediaCalls   int
	lastMediaRef outbox.MediaRef
}

func (f *fakeComposeDeps) BuildPollCreation(name string, optionNames []string, selectableOptionCount int) *waE2E.Message {
	f.pollCalls = append(f.pollCalls, struct {
		name string
		opts []string
		sel  int
	}{name: name, opts: append([]string(nil), optionNames...), sel: selectableOptionCount})
	var opts []*waE2E.PollCreationMessage_Option
	for _, o := range optionNames {
		opts = append(opts, &waE2E.PollCreationMessage_Option{OptionName: proto.String(o)})
	}
	return &waE2E.Message{
		PollCreationMessage: &waE2E.PollCreationMessage{
			Name:                   proto.String(name),
			Options:                opts,
			SelectableOptionsCount: proto.Uint32(uint32(selectableOptionCount)),
		},
	}
}

func (f *fakeComposeDeps) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	f.reactCalls = append(f.reactCalls, struct {
		chat, sender types.JID
		id           types.MessageID
		emoji        string
	}{chat: chat, sender: sender, id: id, emoji: reaction})
	return &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{
			Text: proto.String(reaction),
		},
	}
}

func (f *fakeComposeDeps) BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message {
	f.editCalls = append(f.editCalls, struct {
		chat    types.JID
		id      types.MessageID
		content *waE2E.Message
	}{chat: chat, id: id, content: newContent})
	return &waE2E.Message{
		EditedMessage: &waE2E.FutureProofMessage{Message: newContent},
	}
}

func (f *fakeComposeDeps) BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message {
	f.revokeCalls = append(f.revokeCalls, struct {
		chat, sender types.JID
		id           types.MessageID
	}{chat: chat, sender: sender, id: id})
	return &waE2E.Message{
		ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_REVOKE.Enum(),
		},
	}
}

func (f *fakeComposeDeps) OwnJID() types.JID {
	return f.ownJID
}

func (f *fakeComposeDeps) Media(_ context.Context, ref outbox.MediaRef, _ string) (media.Uploaded, error) {
	f.mediaCalls++
	f.lastMediaRef = ref
	return f.mediaUpload, f.mediaErr
}

func (f *fakeComposeDeps) Quoted(_ context.Context, chat, waID string) (QuotedInfo, bool) {
	if f.quotedByKey == nil {
		return QuotedInfo{}, false
	}
	qi, ok := f.quotedByKey[chat+"|"+waID]
	return qi, ok
}

// TestCompose_TextPolosReplyMention memverifikasi perbedaan Conversation dan ExtendedTextMessage untuk teks polos, reply, dan mention.
func TestCompose_TextPolosReplyMention(t *testing.T) {
	ctx := context.Background()
	deps := &fakeComposeDeps{ownJID: types.NewJID("628000000001", types.DefaultUserServer)}

	kasus := []struct {
		nama      string
		spec      outbox.Spec
		wantConv  bool
		wantText  string
		wantStzID string
		wantMents int
		wantErr   bool
	}{
		{
			nama:     "text-polos",
			spec:     outbox.Spec{Type: "text", To: "628123456789", Text: "Halo dunia"},
			wantConv: true,
			wantText: "Halo dunia",
		},
		{
			nama: "text-reply",
			spec: outbox.Spec{
				Type:    "text",
				To:      "628123456789",
				Text:    "Balasan",
				ReplyTo: &outbox.ReplyRef{ID: "MSG-ASLI", Sender: "628123456789@s.whatsapp.net"},
			},
			wantText:  "Balasan",
			wantStzID: "MSG-ASLI",
		},
		{
			nama: "text-mention",
			spec: outbox.Spec{
				Type:     "text",
				To:       "120363012345@g.us",
				Text:     "Hai @628111111111",
				Mentions: []string{"628111111111@s.whatsapp.net"},
			},
			wantText:  "Hai @628111111111",
			wantMents: 1,
		},
		{
			nama:    "text-lebih-60000-byte",
			spec:    outbox.Spec{Type: "text", To: "628123456789", Text: strings.Repeat("a", 60001)},
			wantErr: true,
		},
	}

	for _, tc := range kasus {
		t.Run(tc.nama, func(t *testing.T) {
			msg, err := Compose(ctx, deps, tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatal("diharapkan error untuk teks melebihi batas")
				}
				return
			}
			if err != nil {
				t.Fatalf("Compose gagal: %v", err)
			}
			if tc.wantConv {
				if msg.GetConversation() != tc.wantText || msg.GetExtendedTextMessage() != nil {
					t.Fatalf("pesan polos harus memakai Conversation, didapat: %+v", msg)
				}
				return
			}
			ext := msg.GetExtendedTextMessage()
			if ext == nil || ext.GetText() != tc.wantText {
				t.Fatalf("ExtendedTextMessage tidak sesuai: %+v", msg)
			}
			ci := ext.GetContextInfo()
			if ci == nil || ci.GetStanzaID() != tc.wantStzID || len(ci.GetMentionedJID()) != tc.wantMents {
				t.Fatalf("ContextInfo tidak sesuai: %+v", ci)
			}
		})
	}
}

// TestCompose_ReplyGrupMengisiParticipant memastikan reply di grup mengisi Participant dan QuotedMessage dari lookup Quoted serta menjaga nil bila kutipan non-teks.
func TestCompose_ReplyGrupMengisiParticipant(t *testing.T) {
	ctx := context.Background()
	grpChat := "1203630123456789@g.us"
	deps := &fakeComposeDeps{
		ownJID: types.NewJID("628000000001", types.DefaultUserServer),
		quotedByKey: map[string]QuotedInfo{
			grpChat + "|WAMID-1": {
				Sender:  "628111222333@s.whatsapp.net",
				Message: &waE2E.Message{Conversation: proto.String("Pesan asli grup")},
			},
			grpChat + "|WAMID-STIKER": {
				Sender:  "628444555666@s.whatsapp.net",
				Message: nil,
			},
		},
	}

	msg, err := Compose(ctx, deps, outbox.Spec{
		Type:    "text",
		To:      grpChat,
		Text:    "Setuju!",
		ReplyTo: &outbox.ReplyRef{ID: "WAMID-1"},
	})
	if err != nil {
		t.Fatalf("Compose reply grup gagal: %v", err)
	}
	ci := msg.GetExtendedTextMessage().GetContextInfo()
	if ci == nil || ci.GetStanzaID() != "WAMID-1" || ci.GetParticipant() != "628111222333@s.whatsapp.net" {
		t.Fatalf("StanzaID atau Participant tidak terisi benar: %+v", ci)
	}
	if ci.GetQuotedMessage().GetConversation() != "Pesan asli grup" {
		t.Fatalf("QuotedMessage seharusnya berisi teks kutipan, didapat: %+v", ci.GetQuotedMessage())
	}

	msgNonText, err := Compose(ctx, deps, outbox.Spec{
		Type:    "text",
		To:      grpChat,
		Text:    "Lucu stikernya",
		ReplyTo: &outbox.ReplyRef{ID: "WAMID-STIKER"},
	})
	if err != nil {
		t.Fatalf("Compose reply stiker gagal: %v", err)
	}
	ci2 := msgNonText.GetExtendedTextMessage().GetContextInfo()
	if ci2 == nil || ci2.GetParticipant() != "628444555666@s.whatsapp.net" || ci2.GetQuotedMessage() != nil {
		t.Fatalf("kutipan non-teks harus mengisi Participant tetapi QuotedMessage nil, didapat: %+v", ci2)
	}
}

// TestCompose_LocationValidasi memverifikasi validasi rentang koordinat dan penyusunan LocationMessage.
func TestCompose_LocationValidasi(t *testing.T) {
	ctx := context.Background()
	deps := &fakeComposeDeps{}

	kasus := []struct {
		nama    string
		loc     *outbox.Location
		wantErr bool
	}{
		{nama: "valid", loc: &outbox.Location{Lat: -6.2, Lng: 106.8, Name: "Monas", Address: "Jakarta"}},
		{nama: "lat-91-ditolak", loc: &outbox.Location{Lat: 91, Lng: 106.8}, wantErr: true},
		{nama: "lat-min-91-ditolak", loc: &outbox.Location{Lat: -91, Lng: 0}, wantErr: true},
		{nama: "lng-181-ditolak", loc: &outbox.Location{Lat: 0, Lng: 181}, wantErr: true},
	}

	for _, tc := range kasus {
		t.Run(tc.nama, func(t *testing.T) {
			msg, err := Compose(ctx, deps, outbox.Spec{
				Type:     "location",
				To:       "628123456789",
				Location: tc.loc,
			})
			if tc.wantErr {
				if err == nil {
					t.Fatal("diharapkan error validasi koordinat")
				}
				return
			}
			if err != nil {
				t.Fatalf("Compose location gagal: %v", err)
			}
			lm := msg.GetLocationMessage()
			if lm == nil || lm.GetDegreesLatitude() != -6.2 || lm.GetDegreesLongitude() != 106.8 ||
				lm.GetName() != "Monas" || lm.GetAddress() != "Jakarta" {
				t.Fatalf("LocationMessage tidak sesuai: %+v", lm)
			}
		})
	}
}

// TestCompose_ContactVCardEscape memastikan escape RFC 6350 pada vCard dan perbedaan 1 vs banyak kontak berjalan tepat.
func TestCompose_ContactVCardEscape(t *testing.T) {
	ctx := context.Background()
	deps := &fakeComposeDeps{}

	msg, err := Compose(ctx, deps, outbox.Spec{
		Type: "contact",
		To:   "628123456789",
		Contacts: []outbox.Contact{
			{
				Name:   "Budi, S.Kom; CTO",
				Phones: []string{"+62 812-3456-7890", "628111111111"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Compose contact tunggal gagal: %v", err)
	}
	cm := msg.GetContactMessage()
	if cm == nil || cm.GetDisplayName() != "Budi, S.Kom; CTO" {
		t.Fatalf("ContactMessage tidak sesuai: %+v", msg)
	}
	vc := cm.GetVcard()
	if !strings.Contains(vc, `FN:Budi\, S.Kom\; CTO`) ||
		!strings.Contains(vc, "TEL;type=CELL;waid=6281234567890:+6281234567890") ||
		!strings.Contains(vc, "TEL;type=CELL;waid=628111111111:+628111111111") {
		t.Fatalf("format vCard RFC 6350 tidak sesuai:\n%s", vc)
	}

	// Kontak > 1 memakai ContactsArrayMessage.
	msgArr, err := Compose(ctx, deps, outbox.Spec{
		Type: "contact",
		To:   "628123456789",
		Contacts: []outbox.Contact{
			{Name: "Satu", Phones: []string{"628111111111"}},
			{Name: "Dua", Phones: []string{"628222222222"}},
		},
	})
	if err != nil {
		t.Fatalf("Compose contacts array gagal: %v", err)
	}
	if ca := msgArr.GetContactsArrayMessage(); ca == nil || len(ca.GetContacts()) != 2 {
		t.Fatalf("ContactsArrayMessage tidak sesuai: %+v", msgArr)
	}

	// Kontak > 10 ditolak.
	tooMany := make([]outbox.Contact, 11)
	for i := range tooMany {
		tooMany[i] = outbox.Contact{Name: "Kontak", Phones: []string{"628111111111"}}
	}
	if _, err := Compose(ctx, deps, outbox.Spec{Type: "contact", To: "628123456789", Contacts: tooMany}); err == nil {
		t.Fatal("diharapkan error untuk > 10 kontak")
	}
}

// TestCompose_PollValidasiDanBuilder memverifikasi validasi poll dan argumen yang diteruskan ke BuildPollCreation.
func TestCompose_PollValidasiDanBuilder(t *testing.T) {
	ctx := context.Background()
	deps := &fakeComposeDeps{}

	kasusErr := []struct {
		nama string
		poll *outbox.PollSpec
	}{
		{nama: "opsi-kurang-dari-2", poll: &outbox.PollSpec{Name: "Q", Options: []string{"A"}, Selectable: 1}},
		{nama: "opsi-lebih-dari-12", poll: &outbox.PollSpec{Name: "Q", Options: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"}, Selectable: 1}},
		{nama: "opsi-duplikat", poll: &outbox.PollSpec{Name: "Q", Options: []string{"Senin", "Senin"}, Selectable: 1}},
		{nama: "selectable-nol", poll: &outbox.PollSpec{Name: "Q", Options: []string{"A", "B"}, Selectable: 0}},
		{nama: "selectable-berlebih", poll: &outbox.PollSpec{Name: "Q", Options: []string{"A", "B"}, Selectable: 3}},
		{nama: "nama-terlalu-panjang", poll: &outbox.PollSpec{Name: strings.Repeat("x", 256), Options: []string{"A", "B"}, Selectable: 1}},
	}
	for _, tc := range kasusErr {
		t.Run(tc.nama, func(t *testing.T) {
			if _, err := Compose(ctx, deps, outbox.Spec{Type: "poll", To: "628123456789", Poll: tc.poll}); err == nil {
				t.Fatalf("kasus %s seharusnya ditolak", tc.nama)
			}
		})
	}

	msg, err := Compose(ctx, deps, outbox.Spec{
		Type: "poll",
		To:   "120363012345@g.us",
		Poll: &outbox.PollSpec{
			Name:       "Pilih jadwal?",
			Options:    []string{"Senin", "Selasa"},
			Selectable: 1,
		},
	})
	if err != nil {
		t.Fatalf("Compose poll valid gagal: %v", err)
	}
	if len(deps.pollCalls) != 1 || deps.pollCalls[0].name != "Pilih jadwal?" || deps.pollCalls[0].sel != 1 || msg.GetPollCreationMessage() == nil {
		t.Fatalf("pemanggilan BuildPollCreation tidak sesuai: calls=%+v msg=%+v", deps.pollCalls, msg)
	}
}

// TestCompose_ReactionEditRevoke memverifikasi pemanggilan builder reaksi, edit, dan revoke termasuk resolusi sender default.
func TestCompose_ReactionEditRevoke(t *testing.T) {
	ctx := context.Background()
	own := types.NewJID("628000000001", types.DefaultUserServer)
	grp := "120363012345@g.us"
	deps := &fakeComposeDeps{
		ownJID: own,
		quotedByKey: map[string]QuotedInfo{
			grp + "|TARGET-LOOKUP": {Sender: "628999888777@s.whatsapp.net"},
		},
	}

	// Reaction memakai sender hasil Quoted saat sender dikosongkan.
	_, err := Compose(ctx, deps, outbox.Spec{
		Type:     "reaction",
		To:       grp,
		Reaction: &outbox.Reaction{MessageID: "TARGET-LOOKUP", Emoji: "🔥"},
	})
	if err != nil {
		t.Fatalf("Compose reaction lookup gagal: %v", err)
	}
	if len(deps.reactCalls) != 1 || deps.reactCalls[0].sender.User != "628999888777" || deps.reactCalls[0].emoji != "🔥" {
		t.Fatalf("BuildReaction tidak sesuai: %+v", deps.reactCalls)
	}

	// Reaction hapus (emoji kosong) saat cache Quoted tidak ada -> fallback ke OwnJID.
	_, err = Compose(ctx, deps, outbox.Spec{
		Type:     "reaction",
		To:       "628123456789",
		Reaction: &outbox.Reaction{MessageID: "TARGET-LAMA", Emoji: ""},
	})
	if err != nil {
		t.Fatalf("Compose reaction fallback gagal: %v", err)
	}
	if len(deps.reactCalls) != 2 || deps.reactCalls[1].sender.User != own.User || deps.reactCalls[1].emoji != "" {
		t.Fatalf("BuildReaction fallback tidak sesuai: %+v", deps.reactCalls)
	}

	// Edit pesan.
	_, err = Compose(ctx, deps, outbox.Spec{
		Type: "edit",
		To:   "628123456789",
		Edit: &outbox.Edit{MessageID: "EDIT-1", Text: "Teks revisi"},
	})
	if err != nil {
		t.Fatalf("Compose edit gagal: %v", err)
	}
	if len(deps.editCalls) != 1 || deps.editCalls[0].id != "EDIT-1" || deps.editCalls[0].content.GetConversation() != "Teks revisi" {
		t.Fatalf("BuildEdit tidak sesuai: %+v", deps.editCalls)
	}

	// Revoke tanpa sender wajib memakai OwnJID.
	_, err = Compose(ctx, deps, outbox.Spec{
		Type:   "revoke",
		To:     grp,
		Revoke: &outbox.Revoke{MessageID: "REV-OWN"},
	})
	if err != nil {
		t.Fatalf("Compose revoke tanpa sender gagal: %v", err)
	}
	if len(deps.revokeCalls) != 1 || deps.revokeCalls[0].sender.User != own.User || deps.revokeCalls[0].id != "REV-OWN" {
		t.Fatalf("BuildRevoke tanpa sender tidak sesuai: %+v", deps.revokeCalls)
	}

	// Revoke dengan sender (mis. admin grup).
	_, err = Compose(ctx, deps, outbox.Spec{
		Type:   "revoke",
		To:     grp,
		Revoke: &outbox.Revoke{MessageID: "REV-ADM", Sender: "628555444333@s.whatsapp.net"},
	})
	if err != nil {
		t.Fatalf("Compose revoke admin gagal: %v", err)
	}
	if len(deps.revokeCalls) != 2 || deps.revokeCalls[1].sender.User != "628555444333" {
		t.Fatalf("BuildRevoke admin tidak sesuai: %+v", deps.revokeCalls)
	}
}
