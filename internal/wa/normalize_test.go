package wa

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type mapLIDResolver map[string]types.JID

func (m mapLIDResolver) PNForLID(_ context.Context, lid types.JID) (types.JID, bool) {
	pn, ok := m[lid.ToNonAD().String()]
	return pn, ok
}

// TestNormalize_Tabel memverifikasi normalisasi beragam tipe pesan, alamat LID, truncation, receipt, dan undecryptable ke kontrak MessageV2.
func TestNormalize_Tabel(t *testing.T) {
	pnChat := types.NewJID("6281234567890", types.DefaultUserServer)
	lidUser := types.NewJID("123456789012345", types.HiddenUserServer)
	grpJID := types.NewJID("12036301122334455", types.GroupServer)
	newsletterJID := types.NewJID("12036309988776655", types.NewsletterServer)
	bcastJID := types.NewJID("1700000000", types.BroadcastServer)
	ts := time.Unix(1794360100, 0)
	now := time.Unix(1794360101, 500_000_000)

	cases := []struct {
		name      string
		raw       any
		nc        NormalizeContext
		wantOK    bool
		wantType  string // Event.Type
		checkFunc func(t *testing.T, ev *journal.Event)
	}{
		{
			name: "text biasa",
			raw: &events.Message{
				Info: types.MessageInfo{
					ID:        "TXT1",
					PushName:  "Budi",
					Timestamp: ts,
					MessageSource: types.MessageSource{
						Chat:           pnChat,
						Sender:         pnChat,
						AddressingMode: types.AddressingModePN,
					},
				},
				Message: &waE2E.Message{Conversation: proto.String("Halo, stok masih ada?")},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatal(err)
				}
				if p.ID != "TXT1" || p.Type != "text" || p.Text != "Halo, stok masih ada?" || p.ChatType != "dm" || p.IsGroup {
					t.Fatalf("hasil MessageV2 tidak sesuai: %+v", p)
				}
				if p.Truncated || p.PushName != "Budi" || p.AddressingMode != "pn" {
					t.Fatalf("metadata tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "extended text dengan mention quote dan forwarded",
			raw: &events.Message{
				Info: types.MessageInfo{
					ID:        "EXT1",
					Timestamp: ts,
					MessageSource: types.MessageSource{
						Chat:   pnChat,
						Sender: pnChat,
					},
				},
				Message: &waE2E.Message{
					ExtendedTextMessage: &waE2E.ExtendedTextMessage{
						Text: proto.String("Cek @6281111111111"),
						ContextInfo: &waE2E.ContextInfo{
							MentionedJID: []string{"6281111111111@s.whatsapp.net"},
							StanzaID:     proto.String("3EB0AA11"),
							Participant:  proto.String("6281111111111@s.whatsapp.net"),
							IsForwarded:  proto.Bool(true),
							QuotedMessage: &waE2E.Message{
								Conversation: proto.String(strings.Repeat("Q", 1500)),
							},
						},
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatal(err)
				}
				if p.Type != "text" || len(p.Mentions) != 1 || p.Mentions[0] != "6281111111111@s.whatsapp.net" {
					t.Fatalf("mentions tidak sesuai: %+v", p)
				}
				if p.Quoted == nil || p.Quoted.ID != "3EB0AA11" || p.Quoted.Type != "text" || len(p.Quoted.Text) > 1024 {
					t.Fatalf("quoted tidak sesuai atau tidak dipotong 1 KiB: %+v", p.Quoted)
				}
				if !p.Flags.Forwarded {
					t.Fatal("flags.forwarded seharusnya true")
				}
			},
		},
		{
			name: "image dengan caption dan dimensi",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "IMG1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ImageMessage: &waE2E.ImageMessage{
						Mimetype:   proto.String("image/jpeg"),
						Caption:    proto.String("Foto produk"),
						DirectPath: proto.String("/v/img1"),
						FileLength: proto.Uint64(2048),
						Width:      proto.Uint32(1080),
						Height:     proto.Uint32(720),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "image" || p.Text != "Foto produk" || p.Media == nil || p.Media.Width != 1080 || p.Media.Height != 720 {
					t.Fatalf("payload image tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "video dengan seconds",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "VID1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					VideoMessage: &waE2E.VideoMessage{
						Mimetype: proto.String("video/mp4"),
						Caption:  proto.String("Demo"),
						Seconds:  proto.Uint32(19),
						Width:    proto.Uint32(720),
						Height:   proto.Uint32(1280),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "video" || p.Media == nil || p.Media.Seconds != 19 {
					t.Fatalf("payload video tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "voice note audio ptt true",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "PTT1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					AudioMessage: &waE2E.AudioMessage{
						Mimetype: proto.String("audio/ogg; codecs=opus"),
						Seconds:  proto.Uint32(7),
						PTT:      proto.Bool(true),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "audio" || p.Media == nil || !p.Media.PTT || p.Media.Seconds != 7 {
					t.Fatalf("voice note harus type=audio dan media.ptt=true: %+v", p)
				}
			},
		},
		{
			name: "document dengan filename",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "DOC1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					DocumentMessage: &waE2E.DocumentMessage{
						Mimetype: proto.String("application/pdf"),
						FileName: proto.String("invoice.pdf"),
						Caption:  proto.String("Tagihan"),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "document" || p.Media == nil || p.Media.Filename != "invoice.pdf" || p.Text != "Tagihan" {
					t.Fatalf("document tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "sticker",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "STK1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					StickerMessage: &waE2E.StickerMessage{
						Mimetype: proto.String("image/webp"),
						Width:    proto.Uint32(512),
						Height:   proto.Uint32(512),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "sticker" || p.Media == nil || p.Media.Width != 512 {
					t.Fatalf("sticker tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "location",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "LOC1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					LocationMessage: &waE2E.LocationMessage{
						DegreesLatitude:  proto.Float64(-6.2),
						DegreesLongitude: proto.Float64(106.8),
						Name:             proto.String("Monas"),
						Address:          proto.String("Jakarta"),
						URL:              proto.String("https://maps.example"),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "location" || p.Location == nil || p.Location.Name != "Monas" || p.Location.Lat != -6.2 {
					t.Fatalf("location tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "live location",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "LLOC1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					LiveLocationMessage: &waE2E.LiveLocationMessage{
						DegreesLatitude:  proto.Float64(-7.25),
						DegreesLongitude: proto.Float64(112.75),
						Caption:          proto.String("Kurir menuju lokasi"),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "live_location" || p.Location == nil || p.Text != "Kurir menuju lokasi" {
					t.Fatalf("live_location tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "contact tunggal",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "CON1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ContactMessage: &waE2E.ContactMessage{
						DisplayName: proto.String("CS Toko"),
						Vcard:       proto.String("BEGIN:VCARD\nFN:CS Toko\nEND:VCARD"),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "contact" || len(p.Contacts) != 1 || p.Contacts[0].DisplayName != "CS Toko" {
					t.Fatalf("contact tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "contacts array",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "CON2", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ContactsArrayMessage: &waE2E.ContactsArrayMessage{
						Contacts: []*waE2E.ContactMessage{
							{DisplayName: proto.String("Satu"), Vcard: proto.String("BEGIN:VCARD\nEND:VCARD")},
							{DisplayName: proto.String("Dua"), Vcard: proto.String("BEGIN:VCARD\nEND:VCARD")},
						},
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "contacts" || len(p.Contacts) != 2 {
					t.Fatalf("contacts array tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "poll creation v3",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "POL1", Timestamp: ts, MessageSource: types.MessageSource{Chat: grpJID, Sender: pnChat, IsGroup: true}},
				Message: &waE2E.Message{
					PollCreationMessageV3: &waE2E.PollCreationMessage{
						Name: proto.String("Jadwal rapat?"),
						Options: []*waE2E.PollCreationMessage_Option{
							{OptionName: proto.String("Senin")},
							{OptionName: proto.String("Selasa")},
						},
						SelectableOptionsCount: proto.Uint32(1),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "poll" || p.Poll == nil || p.Poll.Name != "Jadwal rapat?" || len(p.Poll.Options) != 2 || p.Poll.Selectable != 1 {
					t.Fatalf("poll tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "reaksi tambah",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "REA1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ReactionMessage: &waE2E.ReactionMessage{
						Key: &waCommon.MessageKey{
							ID:          proto.String("ORIG1"),
							Participant: proto.String("6281234567890@s.whatsapp.net"),
						},
						Text: proto.String("👍"),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "reaction" || p.Reaction == nil || p.Reaction.TargetID != "ORIG1" || p.Reaction.Emoji != "👍" || p.Reaction.Removed {
					t.Fatalf("reaksi tambah tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "reaksi hapus",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "REA2", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ReactionMessage: &waE2E.ReactionMessage{
						Key: &waCommon.MessageKey{
							ID:        proto.String("ORIG1"),
							RemoteJID: proto.String("6281234567890@s.whatsapp.net"),
						},
						Text: proto.String(""),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "reaction" || p.Reaction == nil || !p.Reaction.Removed || p.Reaction.Emoji != "" {
					t.Fatalf("reaksi hapus harus removed=true: %+v", p)
				}
			},
		},
		{
			name: "edit via ProtocolMessage MESSAGE_EDIT",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "EDT1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ProtocolMessage: &waE2E.ProtocolMessage{
						Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
						Key:           &waCommon.MessageKey{ID: proto.String("ORIG-EDT")},
						EditedMessage: &waE2E.Message{Conversation: proto.String("Teks revisi")},
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "edit" || p.Edit == nil || p.Edit.TargetID != "ORIG-EDT" || p.Edit.Text != "Teks revisi" {
					t.Fatalf("edit payload tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "revoke via ProtocolMessage REVOKE",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "REV1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ProtocolMessage: &waE2E.ProtocolMessage{
						Type: waE2E.ProtocolMessage_REVOKE.Enum(),
						Key:  &waCommon.MessageKey{ID: proto.String("ORIG-REV")},
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.Type != "revoke" || p.Revoke == nil || p.Revoke.TargetID != "ORIG-REV" {
					t.Fatalf("revoke payload tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "protocol message lain diabaikan",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "PRT1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ProtocolMessage: &waE2E.ProtocolMessage{
						Type: waE2E.ProtocolMessage_EPHEMERAL_SETTING.Enum(),
					},
				},
			},
			wantOK: false,
		},
		{
			name: "sender key distribution message diabaikan",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "SKD1", Timestamp: ts, MessageSource: types.MessageSource{Chat: grpJID, Sender: pnChat, IsGroup: true}},
				Message: &waE2E.Message{
					SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{GroupID: proto.String(grpJID.String())},
				},
			},
			wantOK: false,
		},
		{
			name: "grup metadata chat_type group dan is_group true",
			raw: &events.Message{
				Info: types.MessageInfo{
					ID:        "GRP1",
					Timestamp: ts,
					MessageSource: types.MessageSource{
						Chat:    grpJID,
						Sender:  pnChat,
						IsGroup: true,
					},
				},
				Message: &waE2E.Message{Conversation: proto.String("Halo grup")},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.ChatType != "group" || !p.IsGroup || p.Chat != grpJID.String() {
					t.Fatalf("grup tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "status broadcast",
			raw: &events.Message{
				Info: types.MessageInfo{
					ID:        "STS1",
					Timestamp: ts,
					MessageSource: types.MessageSource{
						Chat:   types.StatusBroadcastJID,
						Sender: pnChat,
					},
				},
				Message: &waE2E.Message{Conversation: proto.String("Status baru")},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.ChatType != "status" || p.IsGroup {
					t.Fatalf("status broadcast tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "broadcast list dan newsletter",
			raw: &events.Message{
				Info: types.MessageInfo{
					ID:        "NWL1",
					Timestamp: ts,
					MessageSource: types.MessageSource{
						Chat:   newsletterJID,
						Sender: pnChat,
					},
				},
				Message: &waE2E.Message{Conversation: proto.String("Berita")},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.ChatType != "newsletter" {
					t.Fatalf("chat_type newsletter tidak sesuai: %s", p.ChatType)
				}
			},
		},
		{
			name: "broadcast chat_type broadcast dan is_group true",
			raw: &events.Message{
				Info: types.MessageInfo{
					ID:        "BCT1",
					Timestamp: ts,
					MessageSource: types.MessageSource{
						Chat:    bcastJID,
						Sender:  pnChat,
						IsGroup: true,
					},
				},
				Message: &waE2E.Message{Conversation: proto.String("Broadcast")},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.ChatType != "broadcast" || !p.IsGroup {
					t.Fatalf("broadcast tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "LID dengan SenderAlt",
			raw: &events.Message{
				Info: types.MessageInfo{
					ID:        "LID1",
					Timestamp: ts,
					MessageSource: types.MessageSource{
						Chat:           lidUser,
						Sender:         lidUser,
						SenderAlt:      pnChat,
						AddressingMode: types.AddressingModeLID,
					},
				},
				Message: &waE2E.Message{Conversation: proto.String("Dari LID")},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.From != lidUser.String() || p.SenderPN != pnChat.String() || p.SenderLID != lidUser.String() || p.Chat != pnChat.String() || p.ChatRaw != lidUser.String() {
					t.Fatalf("resolusi LID + SenderAlt tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "LID via resolver tanpa SenderAlt",
			raw: &events.Message{
				Info: types.MessageInfo{
					ID:        "LID2",
					Timestamp: ts,
					MessageSource: types.MessageSource{
						Chat:           lidUser,
						Sender:         lidUser,
						AddressingMode: types.AddressingModeLID,
					},
				},
				Message: &waE2E.Message{Conversation: proto.String("Via store LID")},
			},
			nc: NormalizeContext{
				Now: now,
				LID: mapLIDResolver{lidUser.String(): pnChat},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.SenderPN != pnChat.String() || p.Chat != pnChat.String() || p.ChatRaw != lidUser.String() {
					t.Fatalf("resolusi LID via resolver tidak sesuai: %+v", p)
				}
			},
		},
		{
			name: "flag offline ephemeral view_once",
			raw: &events.Message{
				Info:        types.MessageInfo{ID: "OFF1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				IsEphemeral: true,
				IsViewOnce:  true,
				Message:     &waE2E.Message{Conversation: proto.String("Pesan offline")},
			},
			nc:       NormalizeContext{Now: now, Offline: true},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				if ev.Flags&journal.FlagOffline == 0 {
					t.Fatal("ev.Flags harus memiliki FlagOffline")
				}
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if !p.Flags.Offline || !p.Flags.Ephemeral || !p.Flags.ViewOnce {
					t.Fatalf("flags tidak sesuai: %+v", p.Flags)
				}
			},
		},
		{
			name: "teks > 16 KiB dipotong tetap simpan id dan truncated true",
			raw: &events.Message{
				Info:    types.MessageInfo{ID: "BIGTXT1", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{Conversation: proto.String(strings.Repeat("A", 25000))},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				if ev.Flags&journal.FlagTruncated == 0 {
					t.Fatal("ev.Flags harus memiliki FlagTruncated")
				}
				if len(ev.Payload) > journal.MaxPayloadBytes {
					t.Fatalf("panjang payload %d melebihi 64 KiB", len(ev.Payload))
				}
				var p MessageV2
				_ = json.Unmarshal(ev.Payload, &p)
				if p.ID != "BIGTXT1" || !p.Truncated || len(p.Text) > 16384 {
					t.Fatalf("truncation tidak sesuai: id=%q, truncated=%v, len=%d", p.ID, p.Truncated, len(p.Text))
				}
			},
		},
		{
			name: "receipt DM diterima dan receipt grup dilewati saat dm",
			raw: &events.Receipt{
				MessageSource: types.MessageSource{Chat: grpJID, Sender: pnChat, IsGroup: true},
				MessageIDs:    []types.MessageID{"M1"},
				Timestamp:     ts,
				Type:          types.ReceiptTypeRead,
			},
			nc:     NormalizeContext{Now: now, Receipts: "dm"},
			wantOK: false,
		},
		{
			name: "undecryptable message",
			raw: &events.UndecryptableMessage{
				Info:          types.MessageInfo{ID: "UND1", Timestamp: ts, MessageSource: types.MessageSource{Chat: lidUser, Sender: lidUser, SenderAlt: pnChat}},
				IsUnavailable: true,
			},
			wantOK:   true,
			wantType: "undecryptable",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				expectedDedupe := "u:" + lidUser.String() + ":" + lidUser.String() + ":UND1"
				if ev.DedupeKey != expectedDedupe {
					t.Fatalf("dedupe_key undecryptable diharapkan %q, didapat %q", expectedDedupe, ev.DedupeKey)
				}
			},
		},
		{
			name: "vcard > 64 KiB dipadatkan agar payload <= 64 KiB dan id tetap ada",
			raw: &events.Message{
				Info: types.MessageInfo{ID: "VCARD-BIG", Timestamp: ts, MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat}},
				Message: &waE2E.Message{
					ContactMessage: &waE2E.ContactMessage{
						DisplayName: proto.String("Kontak Besar"),
						Vcard:       proto.String("BEGIN:VCARD\nNOTE:" + strings.Repeat("V", 80*1024) + "\nEND:VCARD"),
					},
				},
			},
			wantOK:   true,
			wantType: "message",
			checkFunc: func(t *testing.T, ev *journal.Event) {
				if ev.Flags&journal.FlagTruncated == 0 {
					t.Fatal("ev.Flags harus memiliki FlagTruncated saat vCard dipotong")
				}
				var p MessageV2
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatal(err)
				}
				if p.ID != "VCARD-BIG" || !p.Truncated {
					t.Fatalf("id dan truncated harus tetap terjaga: %+v", p)
				}
			},
		},
		{
			name: "receipt grup diterima saat mode all",
			raw: &events.Receipt{
				MessageSource: types.MessageSource{Chat: grpJID, Sender: pnChat, IsGroup: true},
				MessageIDs:    []types.MessageID{"R-GRP-1"},
				Timestamp:     ts,
				Type:          types.ReceiptTypeRead,
			},
			nc:       NormalizeContext{Now: now, Receipts: "all"},
			wantOK:   true,
			wantType: "receipt",
		},
		{
			name: "receipt DM diabaikan saat mode none",
			raw: &events.Receipt{
				MessageSource: types.MessageSource{Chat: pnChat, Sender: pnChat},
				MessageIDs:    []types.MessageID{"R-DM-NONE"},
				Timestamp:     ts,
				Type:          types.ReceiptTypeDelivered,
			},
			nc:     NormalizeContext{Now: now, Receipts: "none"},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nc := tc.nc
			if nc.Now.IsZero() {
				nc.Now = now
			}
			ev, ok := NormalizeEvent(context.Background(), "sales", tc.raw, nc)
			if ok != tc.wantOK {
				t.Fatalf("ok diharapkan %v, didapat %v", tc.wantOK, ok)
			}
			if !ok {
				return
			}
			if ev.Type != tc.wantType {
				t.Fatalf("ev.Type diharapkan %q, didapat %q", tc.wantType, ev.Type)
			}
			if len(ev.Payload) > journal.MaxPayloadBytes {
				t.Fatalf("ukuran payload %d melebihi 64 KiB", len(ev.Payload))
			}
			if tc.checkFunc != nil {
				tc.checkFunc(t, ev)
			}
		})
	}
}

// TestNormalize_DedupeMentahChatKanonik memastikan dedupe_key memakai alamat mentah sementara Event.Chat memakai chat kanonik PN.
func TestNormalize_DedupeMentahChatKanonik(t *testing.T) {
	lid := types.NewJID("123456789012345", types.HiddenUserServer)
	pn := types.NewJID("6281234567890", types.DefaultUserServer)

	raw := &events.Message{
		Info: types.MessageInfo{
			ID:        "3EB0F00D",
			Timestamp: time.Unix(1794360100, 0),
			MessageSource: types.MessageSource{
				Chat:           lid,
				Sender:         lid,
				SenderAlt:      pn,
				AddressingMode: types.AddressingModeLID,
			},
		},
		Message: &waE2E.Message{Conversation: proto.String("uji kanonik")},
	}

	ev, ok := NormalizeEvent(context.Background(), "sales", raw, NormalizeContext{Now: time.Now()})
	if !ok {
		t.Fatal("NormalizeEvent harus mengembalikan ok=true")
	}
	if ev.Chat != pn.String() {
		t.Fatalf("Event.Chat kanonik diharapkan %q, didapat %q", pn.String(), ev.Chat)
	}
	wantDedupe := "m:" + lid.String() + ":" + lid.String() + ":3EB0F00D"
	if ev.DedupeKey != wantDedupe {
		t.Fatalf("DedupeKey mentah diharapkan %q, didapat %q", wantDedupe, ev.DedupeKey)
	}

	var p MessageV2
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.Chat != pn.String() || p.ChatRaw != lid.String() {
		t.Fatalf("payload chat/chat_raw diharapkan (%q, %q), didapat (%q, %q)", pn.String(), lid.String(), p.Chat, p.ChatRaw)
	}
}
