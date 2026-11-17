package wa

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// ComposeDeps adalah subset *whatsmeow.Client dan lookup lokal yang dibutuhkan Compose agar tetap murni dan mudah diuji.
type ComposeDeps interface {
	BuildPollCreation(name string, optionNames []string, selectableOptionCount int) *waE2E.Message
	BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message
	BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message
	BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message
	OwnJID() types.JID
	Media(ctx context.Context, ref outbox.MediaRef, kind string) (media.Uploaded, error)
	Quoted(ctx context.Context, chat, waID string) (QuotedInfo, bool)
}

// QuotedInfo menyimpan pengirim asli dan representasi protobuf minimal dari pesan yang dikutip.
type QuotedInfo struct {
	Sender  string
	Message *waE2E.Message // minimal: Conversation berisi teks kutipan
}

// Compose menyusun *waE2E.Message secara murni terhadap ComposeDeps untuk seluruh tipe pesan v2.
func Compose(ctx context.Context, d ComposeDeps, spec outbox.Spec) (*waE2E.Message, error) {
	if d == nil {
		return nil, &outbox.SendError{Transient: false, Code: "invalid_spec", Err: errors.New("compose dependencies belum tersedia")}
	}
	toJID, err := jid.ParseRecipient(spec.To)
	if err != nil {
		return nil, toSendError(err)
	}

	msgType := strings.ToLower(strings.TrimSpace(spec.Type))
	switch msgType {
	case "text":
		if err := outbox.ValidateTextBody(spec.Text); err != nil {
			return nil, invalidSpecErr(err)
		}
		ci := resolveContextInfo(ctx, d, toJID.String(), spec)
		if ci == nil {
			return &waE2E.Message{Conversation: proto.String(spec.Text)}, nil
		}
		return &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text:        proto.String(spec.Text),
				ContextInfo: ci,
			},
		}, nil

	case "image", "video", "audio", "document", "sticker":
		if spec.Media == nil {
			return nil, invalidSpecErr(fmt.Errorf("%w: objek media wajib diisi", outbox.ErrInvalidSpec))
		}
		upKind := msgType
		if msgType == "audio" && spec.Media.PTT {
			upKind = string(media.KindPTT)
		}
		up, mErr := d.Media(ctx, *spec.Media, upKind)
		if mErr != nil {
			return nil, mErr
		}
		rec := media.UploadRecord{
			Kind:     media.Kind(upKind),
			Mimetype: up.Meta.Mimetype,
			Size:     up.Meta.Size,
			Resp:     up.Resp,
			Width:    up.Meta.Width,
			Height:   up.Meta.Height,
			Seconds:  up.Meta.Seconds,
			PTT:      up.Meta.PTT,
			Filename: up.Meta.Filename,
		}
		ci := resolveContextInfo(ctx, d, toJID.String(), spec)
		return buildMediaE2EFromRecord(spec, rec, ci)

	case "location":
		if err := outbox.ValidateLocation(spec.Location); err != nil {
			return nil, invalidSpecErr(err)
		}
		ci := resolveContextInfo(ctx, d, toJID.String(), spec)
		return &waE2E.Message{
			LocationMessage: &waE2E.LocationMessage{
				DegreesLatitude:  proto.Float64(spec.Location.Lat),
				DegreesLongitude: proto.Float64(spec.Location.Lng),
				Name:             optString(spec.Location.Name),
				Address:          optString(spec.Location.Address),
				ContextInfo:      ci,
			},
		}, nil

	case "contact":
		if err := outbox.ValidateContacts(spec.Contacts); err != nil {
			return nil, invalidSpecErr(err)
		}
		ci := resolveContextInfo(ctx, d, toJID.String(), spec)
		if len(spec.Contacts) == 1 {
			c := spec.Contacts[0]
			vc := buildContactVCard(c)
			return &waE2E.Message{
				ContactMessage: &waE2E.ContactMessage{
					DisplayName: proto.String(c.Name),
					Vcard:       proto.String(vc),
					ContextInfo: ci,
				},
			}, nil
		}
		items := make([]*waE2E.ContactMessage, 0, len(spec.Contacts))
		for _, c := range spec.Contacts {
			items = append(items, &waE2E.ContactMessage{
				DisplayName: proto.String(c.Name),
				Vcard:       proto.String(buildContactVCard(c)),
			})
		}
		return &waE2E.Message{
			ContactsArrayMessage: &waE2E.ContactsArrayMessage{
				DisplayName: proto.String(fmt.Sprintf("%s dan %d lainnya", spec.Contacts[0].Name, len(spec.Contacts)-1)),
				Contacts:    items,
				ContextInfo: ci,
			},
		}, nil

	case "poll":
		if err := outbox.ValidatePoll(spec.Poll); err != nil {
			return nil, invalidSpecErr(err)
		}
		opts := make([]string, len(spec.Poll.Options))
		for idx, o := range spec.Poll.Options {
			opts[idx] = strings.TrimSpace(o)
		}
		return d.BuildPollCreation(strings.TrimSpace(spec.Poll.Name), opts, spec.Poll.Selectable), nil

	case "reaction":
		if err := outbox.ValidateReaction(spec.Reaction); err != nil {
			return nil, invalidSpecErr(err)
		}
		senderJID := d.OwnJID()
		if rawSender := strings.TrimSpace(spec.Reaction.Sender); rawSender != "" {
			parsed, err := jid.ParseRecipient(rawSender)
			if err != nil {
				return nil, toSendError(err)
			}
			senderJID = parsed
		} else if qi, ok := d.Quoted(ctx, toJID.String(), spec.Reaction.MessageID); ok && strings.TrimSpace(qi.Sender) != "" {
			if parsed, err := jid.ParseRecipient(qi.Sender); err == nil {
				senderJID = parsed
			}
		}
		return d.BuildReaction(toJID, senderJID, types.MessageID(spec.Reaction.MessageID), spec.Reaction.Emoji), nil

	case "edit":
		if err := outbox.ValidateEdit(spec.Edit); err != nil {
			return nil, invalidSpecErr(err)
		}
		return d.BuildEdit(toJID, types.MessageID(spec.Edit.MessageID), &waE2E.Message{
			Conversation: proto.String(spec.Edit.Text),
		}), nil

	case "revoke":
		if err := outbox.ValidateRevoke(spec.Revoke); err != nil {
			return nil, invalidSpecErr(err)
		}
		senderJID := d.OwnJID()
		if rawSender := strings.TrimSpace(spec.Revoke.Sender); rawSender != "" {
			parsed, err := jid.ParseRecipient(rawSender)
			if err != nil {
				return nil, toSendError(err)
			}
			senderJID = parsed
		}
		return d.BuildRevoke(toJID, senderJID, types.MessageID(spec.Revoke.MessageID)), nil

	default:
		return nil, invalidSpecErr(fmt.Errorf("%w: tipe pesan %q tidak didukung", outbox.ErrInvalidSpec, spec.Type))
	}
}

func invalidSpecErr(err error) error {
	return &outbox.SendError{Transient: false, Code: "invalid_spec", Err: err}
}

func resolveContextInfo(ctx context.Context, d ComposeDeps, chat string, spec outbox.Spec) *waE2E.ContextInfo {
	hasReply := spec.ReplyTo != nil && strings.TrimSpace(spec.ReplyTo.ID) != ""
	hasMentions := len(spec.Mentions) > 0
	if !hasReply && !hasMentions {
		return nil
	}

	ci := &waE2E.ContextInfo{}
	if hasReply {
		ci.StanzaID = proto.String(spec.ReplyTo.ID)
		participant := strings.TrimSpace(spec.ReplyTo.Sender)
		if qi, ok := d.Quoted(ctx, chat, spec.ReplyTo.ID); ok {
			if participant == "" && strings.TrimSpace(qi.Sender) != "" {
				participant = strings.TrimSpace(qi.Sender)
			}
			if qi.Message != nil {
				ci.QuotedMessage = qi.Message
			}
		}
		if participant != "" {
			ci.Participant = proto.String(participant)
		}
	}
	if hasMentions {
		ci.MentionedJID = spec.Mentions
	}
	return ci
}

func buildMediaE2EFromRecord(spec outbox.Spec, rec media.UploadRecord, ci *waE2E.ContextInfo) (*waE2E.Message, error) {
	var mSpec outbox.MediaRef
	if spec.Media != nil {
		mSpec = *spec.Media
	}

	var thumb []byte
	if mSpec.Thumbnail != "" {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(mSpec.Thumbnail))
		if err != nil {
			return nil, invalidSpecErr(fmt.Errorf("thumbnail base64 tidak valid: %w", err))
		}
		if len(raw) > 16<<10 {
			return nil, invalidSpecErr(errors.New("ukuran thumbnail melebihi batas 16 KiB"))
		}
		thumb = raw
	}

	fileLen := rec.Resp.FileLength
	if fileLen == 0 && rec.Size > 0 {
		fileLen = uint64(rec.Size)
	}

	switch strings.ToLower(strings.TrimSpace(spec.Type)) {
	case "image":
		return &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				URL:           proto.String(rec.Resp.URL),
				DirectPath:    proto.String(rec.Resp.DirectPath),
				MediaKey:      rec.Resp.MediaKey,
				Mimetype:      proto.String(rec.Mimetype),
				FileEncSHA256: rec.Resp.FileEncSHA256,
				FileSHA256:    rec.Resp.FileSHA256,
				FileLength:    proto.Uint64(fileLen),
				Width:         optUint32(rec.Width),
				Height:        optUint32(rec.Height),
				Caption:       optString(mSpec.Caption),
				JPEGThumbnail: thumb,
				ContextInfo:   ci,
			},
		}, nil

	case "video":
		secs := mSpec.Seconds
		if secs == 0 {
			secs = rec.Seconds
		}
		return &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				URL:              proto.String(rec.Resp.URL),
				DirectPath:       proto.String(rec.Resp.DirectPath),
				MediaKey:         rec.Resp.MediaKey,
				Mimetype:         proto.String(rec.Mimetype),
				FileEncSHA256:    rec.Resp.FileEncSHA256,
				FileSHA256:       rec.Resp.FileSHA256,
				FileLength:       proto.Uint64(fileLen),
				Seconds:          optUint32(secs),
				GifPlayback:      optBool(mSpec.GIF),
				StreamingSidecar: rec.Resp.StreamingSidecar,
				Width:            optUint32(rec.Width),
				Height:           optUint32(rec.Height),
				Caption:          optString(mSpec.Caption),
				JPEGThumbnail:    thumb,
				ContextInfo:      ci,
			},
		}, nil

	case "audio":
		secs := mSpec.Seconds
		if secs == 0 {
			secs = rec.Seconds
		}
		ptt := mSpec.PTT || rec.PTT
		return &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				URL:           proto.String(rec.Resp.URL),
				DirectPath:    proto.String(rec.Resp.DirectPath),
				MediaKey:      rec.Resp.MediaKey,
				Mimetype:      proto.String(rec.Mimetype),
				FileEncSHA256: rec.Resp.FileEncSHA256,
				FileSHA256:    rec.Resp.FileSHA256,
				FileLength:    proto.Uint64(fileLen),
				Seconds:       optUint32(secs),
				PTT:           optBool(ptt),
				ContextInfo:   ci,
			},
		}, nil

	case "document":
		name := media.SanitizeFilename(mSpec.Filename)
		if name == "" {
			name = media.SanitizeFilename(rec.Filename)
		}
		if name == "" {
			name = "attachment"
		}
		return &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				URL:           proto.String(rec.Resp.URL),
				DirectPath:    proto.String(rec.Resp.DirectPath),
				MediaKey:      rec.Resp.MediaKey,
				Mimetype:      proto.String(rec.Mimetype),
				FileEncSHA256: rec.Resp.FileEncSHA256,
				FileSHA256:    rec.Resp.FileSHA256,
				FileLength:    proto.Uint64(fileLen),
				FileName:      proto.String(name),
				Title:         proto.String(name),
				Caption:       optString(mSpec.Caption),
				ContextInfo:   ci,
			},
		}, nil

	case "sticker":
		return &waE2E.Message{
			StickerMessage: &waE2E.StickerMessage{
				URL:           proto.String(rec.Resp.URL),
				DirectPath:    proto.String(rec.Resp.DirectPath),
				MediaKey:      rec.Resp.MediaKey,
				Mimetype:      proto.String("image/webp"),
				FileEncSHA256: rec.Resp.FileEncSHA256,
				FileSHA256:    rec.Resp.FileSHA256,
				FileLength:    proto.Uint64(fileLen),
				Width:         optUint32(rec.Width),
				Height:        optUint32(rec.Height),
				ContextInfo:   ci,
			},
		}, nil

	default:
		return nil, invalidSpecErr(outbox.ErrInvalidSpec)
	}
}

func buildContactVCard(c outbox.Contact) string {
	if strings.TrimSpace(c.VCard) != "" {
		return c.VCard
	}
	var b strings.Builder
	b.WriteString("BEGIN:VCARD\nVERSION:3.0\n")
	b.WriteString("FN:")
	b.WriteString(escapeVCardValue(c.Name))
	b.WriteByte('\n')
	for _, rawPhone := range c.Phones {
		digits := jid.DigitsOnly(rawPhone)
		if digits == "" {
			continue
		}
		fmt.Fprintf(&b, "TEL;type=CELL;waid=%s:+%s\n", digits, digits)
	}
	b.WriteString("END:VCARD")
	return b.String()
}

// escapeVCardValue menerapkan aturan escape RFC 6350 pada properti vCard.
func escapeVCardValue(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\r\n", `\n`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\r", `\n`)
	s = strings.ReplaceAll(s, ",", `\,`)
	s = strings.ReplaceAll(s, ";", `\;`)
	return s
}

// resolveQuotedFromStores membaca pesan rujukan dari context store lebih dulu, lalu fallback ke journal.
func resolveQuotedFromStores(ctx context.Context, mgr *Manager, instanceID string, ownJID types.JID, chat, waID string) (QuotedInfo, bool) {
	if mgr == nil || waID == "" {
		return QuotedInfo{}, false
	}
	if mgr.ctxStore != nil {
		if sender, text, fromMe, ok := mgr.ctxStore.LookupQuoted(ctx, instanceID, chat, waID); ok {
			if fromMe && !ownJID.IsEmpty() {
				sender = ownJID.String()
			}
			var quotedMsg *waE2E.Message
			if text != "" {
				quotedMsg = &waE2E.Message{Conversation: proto.String(text)}
			}
			return QuotedInfo{
				Sender:  sender,
				Message: quotedMsg,
			}, true
		}
	}
	if mgr.journal == nil {
		return QuotedInfo{}, false
	}
	ev, err := mgr.journal.FindByWAID(ctx, instanceID, chat, waID)
	if err != nil || ev == nil {
		return QuotedInfo{}, false
	}

	var msg MessageV2
	if err := json.Unmarshal(ev.Payload, &msg); err != nil {
		return QuotedInfo{}, false
	}

	sender := ev.Sender
	if msg.IsFromMe {
		if !ownJID.IsEmpty() {
			sender = ownJID.String()
		}
	} else if sender == "" {
		sender = msg.From
	}

	var quotedMsg *waE2E.Message
	if msg.Text != "" {
		quotedMsg = &waE2E.Message{Conversation: proto.String(msg.Text)}
	}
	return QuotedInfo{
		Sender:  sender,
		Message: quotedMsg,
	}, true
}
