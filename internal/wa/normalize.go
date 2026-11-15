package wa

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fikfikk-org/waapi/internal/journal"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	// maxTextBytes membatasi teks pesan utama di payload journal agar hemat memori dan penyimpanan.
	maxTextBytes = 16 * 1024
	// maxQuotedTextBytes membatasi cuplikan teks pesan yang dikutip.
	maxQuotedTextBytes = 1024
	// maxVcardBytes membatasi ukuran awal setiap vCard saat pemadatan payload > 64 KiB.
	maxVcardBytes = 8 * 1024
)

// NormalizeContext membawa konteks lokal yang dibutuhkan saat menormalkan event mentah whatsmeow.
type NormalizeContext struct {
	Now      time.Time
	Offline  bool
	Receipts string // "dm" | "all" | "none"
	LID      LIDResolver
}

// NormalizeEvent mengubah event mentah whatsmeow yang perlu dijurnal menjadi *journal.Event.
// Mengembalikan (nil, false) jika event diabaikan atau tidak termasuk jenis yang dijurnal.
func NormalizeEvent(ctx context.Context, instanceID string, raw any, nc NormalizeContext) (*journal.Event, bool) {
	if nc.Now.IsZero() {
		nc.Now = time.Now()
	}

	switch e := raw.(type) {
	case *events.Message:
		return normalizeMessageEvent(ctx, instanceID, e, nc)
	case *events.Receipt:
		return normalizeReceiptEvent(ctx, instanceID, e, nc)
	case *events.UndecryptableMessage:
		return normalizeUndecryptableEvent(ctx, instanceID, e, nc)
	default:
		return nil, false
	}
}

type resolvedAddr struct {
	chatCanon      string
	chatRaw        string
	chatType       string
	isGroup        bool
	senderRaw      string
	senderPN       string
	senderLID      string
	addressingMode string
}

// resolveAddresses memetakan alamat mentah pengirim/chat ke JID kanonik PN jika tersedia tanpa I/O jaringan.
func resolveAddresses(ctx context.Context, src types.MessageSource, resolver LIDResolver) resolvedAddr {
	chatRaw := src.Chat
	senderRaw := src.Sender
	isGroup := src.IsGroup || chatRaw.Server == types.GroupServer
	cType := detectChatType(chatRaw, isGroup)
	if cType == "status" {
		// Status WhatsApp memakai server broadcast, tetapi bukan percakapan grup.
		isGroup = false
	}

	var senderPN, senderLID string
	switch senderRaw.Server {
	case types.HiddenUserServer:
		senderLID = senderRaw.ToNonAD().String()
		if !src.SenderAlt.IsEmpty() && src.SenderAlt.Server == types.DefaultUserServer {
			senderPN = src.SenderAlt.ToNonAD().String()
		} else if resolver != nil {
			if pn, ok := resolver.PNForLID(ctx, senderRaw.ToNonAD()); ok && !pn.IsEmpty() {
				senderPN = pn.ToNonAD().String()
			}
		}
	case types.DefaultUserServer:
		senderPN = senderRaw.ToNonAD().String()
		if !src.SenderAlt.IsEmpty() && src.SenderAlt.Server == types.HiddenUserServer {
			senderLID = src.SenderAlt.ToNonAD().String()
		}
	}

	chatRawStr := chatRaw.String()
	chatCanon := chatRawStr
	if chatRaw.Server == types.HiddenUserServer {
		if !isGroup && chatRaw.User == senderRaw.User && senderPN != "" {
			chatCanon = senderPN
		} else if !src.RecipientAlt.IsEmpty() && src.RecipientAlt.Server == types.DefaultUserServer {
			chatCanon = src.RecipientAlt.ToNonAD().String()
		} else if resolver != nil {
			if pn, ok := resolver.PNForLID(ctx, chatRaw.ToNonAD()); ok && !pn.IsEmpty() {
				chatCanon = pn.ToNonAD().String()
			}
		}
	}

	outChatRaw := ""
	if chatCanon != chatRawStr || chatRaw.Server == types.HiddenUserServer {
		outChatRaw = chatRawStr
	}

	return resolvedAddr{
		chatCanon:      chatCanon,
		chatRaw:        outChatRaw,
		chatType:       cType,
		isGroup:        isGroup,
		senderRaw:      senderRaw.String(),
		senderPN:       senderPN,
		senderLID:      senderLID,
		addressingMode: string(src.AddressingMode),
	}
}

func detectChatType(chat types.JID, isGroup bool) string {
	if chat == types.StatusBroadcastJID || (chat.Server == types.BroadcastServer && chat.User == "status") {
		return "status"
	}
	switch chat.Server {
	case types.NewsletterServer:
		return "newsletter"
	case types.BroadcastServer:
		return "broadcast"
	case types.GroupServer:
		return "group"
	default:
		if isGroup {
			return "group"
		}
		return "dm"
	}
}

func normalizeMessageEvent(ctx context.Context, instanceID string, e *events.Message, nc NormalizeContext) (*journal.Event, bool) {
	if e == nil || e.Message == nil {
		return nil, false
	}

	msg, isEph, isVO := unwrapE2EMessage(e.Message, e.IsEphemeral, e.IsViewOnce || e.IsViewOnceV2 || e.IsViewOnceV2Extension)
	if msg == nil {
		return nil, false
	}

	// Abaikan sinyal distribusi kunci grup dan protocol message selain edit/revoke.
	if pm := msg.GetProtocolMessage(); pm != nil {
		pType := pm.GetType()
		if pType != waE2E.ProtocolMessage_MESSAGE_EDIT && pType != waE2E.ProtocolMessage_REVOKE {
			return nil, false
		}
	} else if msg.GetSenderKeyDistributionMessage() != nil && !hasContentOutsideSenderKey(msg) {
		return nil, false
	}

	addr := resolveAddresses(ctx, e.Info.MessageSource, nc.LID)
	ts := eventUnixTS(e.Info.Timestamp, nc.Now)

	p := MessageV2{
		ID:             e.Info.ID,
		From:           addr.senderRaw,
		SenderPN:       addr.senderPN,
		SenderLID:      addr.senderLID,
		Chat:           addr.chatCanon,
		ChatRaw:        addr.chatRaw,
		ChatType:       addr.chatType,
		IsGroup:        addr.isGroup,
		IsFromMe:       e.Info.IsFromMe,
		PushName:       e.Info.PushName,
		AddressingMode: addr.addressingMode,
		Timestamp:      ts,
		Flags: MsgFlags{
			Offline:   nc.Offline,
			Ephemeral: isEph,
			ViewOnce:  isVO,
		},
	}

	var ctxInfo *waE2E.ContextInfo
	p.Type, p.Text, ctxInfo = extractMessageBody(msg, &p)
	if ctxInfo != nil {
		applyContextInfo(ctxInfo, &p)
	}

	// Potong teks utama > 16 KiB secara aman pada batas UTF-8.
	if trimmed, cut := truncateUTF8(p.Text, maxTextBytes); cut {
		p.Text = trimmed
		p.Truncated = true
	}

	payload, err := fitMessagePayload(&p)
	if err != nil {
		return nil, false
	}

	var flags uint32
	if nc.Offline {
		flags |= journal.FlagOffline
	}
	if p.Truncated {
		flags |= journal.FlagTruncated
	}

	// Dedupe memakai alamat mentah wire WhatsApp sebelum resolusi kanonik LID.
	dedupeKey := "m:" + e.Info.Chat.String() + ":" + addr.senderRaw + ":" + e.Info.ID
	return &journal.Event{
		InstanceID: instanceID,
		Type:       "message",
		Chat:       addr.chatCanon,
		Sender:     addr.senderRaw,
		WAID:       e.Info.ID,
		DedupeKey:  dedupeKey,
		TS:         ts,
		ReceivedAt: nc.Now.UnixMilli(),
		Flags:      flags,
		Payload:    payload,
	}, true
}

func unwrapE2EMessage(msg *waE2E.Message, isEph, isVO bool) (*waE2E.Message, bool, bool) {
	cur := msg
	for i := 0; i < 6 && cur != nil; i++ {
		switch {
		case cur.GetEphemeralMessage().GetMessage() != nil:
			isEph = true
			cur = cur.GetEphemeralMessage().GetMessage()
		case cur.GetViewOnceMessage().GetMessage() != nil:
			isVO = true
			cur = cur.GetViewOnceMessage().GetMessage()
		case cur.GetViewOnceMessageV2().GetMessage() != nil:
			isVO = true
			cur = cur.GetViewOnceMessageV2().GetMessage()
		case cur.GetViewOnceMessageV2Extension().GetMessage() != nil:
			isVO = true
			cur = cur.GetViewOnceMessageV2Extension().GetMessage()
		case cur.GetDocumentWithCaptionMessage().GetMessage() != nil:
			cur = cur.GetDocumentWithCaptionMessage().GetMessage()
		case cur.GetDeviceSentMessage().GetMessage() != nil:
			cur = cur.GetDeviceSentMessage().GetMessage()
		case cur.GetEditedMessage().GetMessage() != nil:
			cur = cur.GetEditedMessage().GetMessage()
		default:
			return cur, isEph, isVO
		}
	}
	return cur, isEph, isVO
}

func hasContentOutsideSenderKey(msg *waE2E.Message) bool {
	return msg.GetConversation() != "" ||
		msg.GetExtendedTextMessage() != nil ||
		msg.GetImageMessage() != nil ||
		msg.GetVideoMessage() != nil ||
		msg.GetAudioMessage() != nil ||
		msg.GetDocumentMessage() != nil ||
		msg.GetStickerMessage() != nil ||
		msg.GetLocationMessage() != nil ||
		msg.GetLiveLocationMessage() != nil ||
		msg.GetContactMessage() != nil ||
		msg.GetContactsArrayMessage() != nil ||
		msg.GetPollCreationMessage() != nil ||
		msg.GetPollCreationMessageV2() != nil ||
		msg.GetPollCreationMessageV3() != nil ||
		msg.GetReactionMessage() != nil ||
		msg.GetProtocolMessage() != nil
}

func extractMessageBody(msg *waE2E.Message, p *MessageV2) (msgType, text string, ctxInfo *waE2E.ContextInfo) {
	switch {
	case msg.Conversation != nil:
		return "text", msg.GetConversation(), nil
	case msg.GetExtendedTextMessage() != nil:
		ext := msg.GetExtendedTextMessage()
		return "text", ext.GetText(), ext.GetContextInfo()
	case msg.GetImageMessage() != nil:
		img := msg.GetImageMessage()
		md := newMediaDescriptor("image", img.GetMimetype(), img.GetDirectPath(), img.GetMediaKey(), img.GetFileEncSHA256(), img.GetFileSHA256(), img.GetFileLength())
		md.Width = img.GetWidth()
		md.Height = img.GetHeight()
		p.Media = md
		return "image", img.GetCaption(), img.GetContextInfo()
	case msg.GetVideoMessage() != nil:
		vid := msg.GetVideoMessage()
		md := newMediaDescriptor("video", vid.GetMimetype(), vid.GetDirectPath(), vid.GetMediaKey(), vid.GetFileEncSHA256(), vid.GetFileSHA256(), vid.GetFileLength())
		md.Width = vid.GetWidth()
		md.Height = vid.GetHeight()
		md.Seconds = vid.GetSeconds()
		p.Media = md
		return "video", vid.GetCaption(), vid.GetContextInfo()
	case msg.GetAudioMessage() != nil:
		aud := msg.GetAudioMessage()
		md := newMediaDescriptor("audio", aud.GetMimetype(), aud.GetDirectPath(), aud.GetMediaKey(), aud.GetFileEncSHA256(), aud.GetFileSHA256(), aud.GetFileLength())
		md.Seconds = aud.GetSeconds()
		md.PTT = aud.GetPTT()
		p.Media = md
		return "audio", "", aud.GetContextInfo()
	case msg.GetDocumentMessage() != nil:
		doc := msg.GetDocumentMessage()
		md := newMediaDescriptor("document", doc.GetMimetype(), doc.GetDirectPath(), doc.GetMediaKey(), doc.GetFileEncSHA256(), doc.GetFileSHA256(), doc.GetFileLength())
		md.Filename = doc.GetFileName()
		p.Media = md
		return "document", doc.GetCaption(), doc.GetContextInfo()
	case msg.GetStickerMessage() != nil:
		stk := msg.GetStickerMessage()
		md := newMediaDescriptor("sticker", stk.GetMimetype(), stk.GetDirectPath(), stk.GetMediaKey(), stk.GetFileEncSHA256(), stk.GetFileSHA256(), stk.GetFileLength())
		md.Width = stk.GetWidth()
		md.Height = stk.GetHeight()
		p.Media = md
		return "sticker", "", stk.GetContextInfo()
	case msg.GetLocationMessage() != nil:
		loc := msg.GetLocationMessage()
		p.Location = &LocationInfo{
			Lat:     loc.GetDegreesLatitude(),
			Lon:     loc.GetDegreesLongitude(),
			Name:    loc.GetName(),
			Address: loc.GetAddress(),
			URL:     loc.GetURL(),
		}
		return "location", "", loc.GetContextInfo()
	case msg.GetLiveLocationMessage() != nil:
		loc := msg.GetLiveLocationMessage()
		p.Location = &LocationInfo{
			Lat: loc.GetDegreesLatitude(),
			Lon: loc.GetDegreesLongitude(),
		}
		return "live_location", loc.GetCaption(), loc.GetContextInfo()
	case msg.GetContactMessage() != nil:
		c := msg.GetContactMessage()
		p.Contacts = []ContactInfo{{DisplayName: c.GetDisplayName(), Vcard: c.GetVcard()}}
		return "contact", c.GetDisplayName(), c.GetContextInfo()
	case msg.GetContactsArrayMessage() != nil:
		ca := msg.GetContactsArrayMessage()
		contacts := make([]ContactInfo, 0, len(ca.GetContacts()))
		for _, c := range ca.GetContacts() {
			if c == nil {
				continue
			}
			contacts = append(contacts, ContactInfo{DisplayName: c.GetDisplayName(), Vcard: c.GetVcard()})
		}
		p.Contacts = contacts
		return "contacts", ca.GetDisplayName(), ca.GetContextInfo()
	case msg.GetPollCreationMessage() != nil || msg.GetPollCreationMessageV2() != nil || msg.GetPollCreationMessageV3() != nil || msg.GetPollCreationMessageV5() != nil || msg.GetPollCreationMessageV6() != nil:
		poll := firstNonNilPoll(
			msg.GetPollCreationMessage(),
			msg.GetPollCreationMessageV2(),
			msg.GetPollCreationMessageV3(),
			msg.GetPollCreationMessageV5(),
			msg.GetPollCreationMessageV6(),
		)
		opts := make([]string, 0, len(poll.GetOptions()))
		for _, o := range poll.GetOptions() {
			if o != nil {
				opts = append(opts, o.GetOptionName())
			}
		}
		p.Poll = &PollInfo{
			Name:       poll.GetName(),
			Options:    opts,
			Selectable: poll.GetSelectableOptionsCount(),
		}
		return "poll", poll.GetName(), poll.GetContextInfo()
	case msg.GetReactionMessage() != nil:
		rx := msg.GetReactionMessage()
		emoji := rx.GetText()
		p.Reaction = &ReactionInfo{
			TargetID:    rx.GetKey().GetID(),
			Emoji:       emoji,
			Removed:     emoji == "",
			Participant: rx.GetKey().GetParticipant(),
		}
		return "reaction", emoji, nil
	case msg.GetProtocolMessage() != nil:
		pm := msg.GetProtocolMessage()
		switch pm.GetType() {
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			editedText := ""
			if em := pm.GetEditedMessage(); em != nil {
				editedText = extractSummaryText(em)
			}
			p.Edit = &EditInfo{
				TargetID: pm.GetKey().GetID(),
				Text:     editedText,
			}
			return "edit", editedText, nil
		case waE2E.ProtocolMessage_REVOKE:
			p.Revoke = &RevokeInfo{
				TargetID: pm.GetKey().GetID(),
			}
			return "revoke", "", nil
		}
	}
	return "unknown", "", nil
}

func firstNonNilPoll(polls ...*waE2E.PollCreationMessage) *waE2E.PollCreationMessage {
	for _, p := range polls {
		if p != nil {
			return p
		}
	}
	return nil
}

func newMediaDescriptor(mediaType, mime, directPath string, mediaKey, encSHA, sha []byte, fileLen uint64) *MediaDescriptor {
	return &MediaDescriptor{
		Mimetype:      mime,
		DirectPath:    directPath,
		MediaKey:      encodeBase64(mediaKey),
		FileEncSHA256: encodeBase64(encSHA),
		FileSHA256:    encodeBase64(sha),
		FileLength:    fileLen,
		MediaType:     mediaType,
	}
}

func encodeBase64(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

func applyContextInfo(ci *waE2E.ContextInfo, p *MessageV2) {
	if len(ci.GetMentionedJID()) > 0 {
		p.Mentions = append([]string(nil), ci.GetMentionedJID()...)
	}
	if ci.GetIsForwarded() {
		p.Flags.Forwarded = true
	}
	if ci.GetQuotedMessage() != nil || ci.GetStanzaID() != "" {
		qType, qText := "unknown", ""
		if qm := ci.GetQuotedMessage(); qm != nil {
			qType, qText = extractQuotedSummary(qm)
			if trimmed, cut := truncateUTF8(qText, maxQuotedTextBytes); cut {
				qText = trimmed
				p.Truncated = true
			}
		}
		p.Quoted = &QuotedInfo{
			ID:          ci.GetStanzaID(),
			Participant: ci.GetParticipant(),
			Type:        qType,
			Text:        qText,
		}
	}
}

func extractSummaryText(msg *waE2E.Message) string {
	_, txt := extractQuotedSummary(msg)
	return txt
}

func extractQuotedSummary(msg *waE2E.Message) (string, string) {
	unwrapped, _, _ := unwrapE2EMessage(msg, false, false)
	if unwrapped == nil {
		return "unknown", ""
	}
	var tmp MessageV2
	t, txt, _ := extractMessageBody(unwrapped, &tmp)
	return t, txt
}

// fitMessagePayload menjaga ukuran JSON pesan di bawah 64 KiB tanpa pernah menghapus ID/chat/from/type/timestamp.
func fitMessagePayload(p *MessageV2) ([]byte, error) {
	payload, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if len(payload) <= journal.MaxPayloadBytes {
		return payload, nil
	}

	p.Truncated = true
	// Tahap 1: pangkas vCard setiap kontak hingga maksimal 8 KiB.
	for i := range p.Contacts {
		if trimmed, cut := truncateUTF8(p.Contacts[i].Vcard, maxVcardBytes); cut {
			p.Contacts[i].Vcard = trimmed
		}
	}
	if payload, err = json.Marshal(p); err != nil || len(payload) <= journal.MaxPayloadBytes {
		return payload, err
	}

	// Tahap 2: kosongkan vCard atau pangkas array kontak jika masih melebihi batas.
	for i := range p.Contacts {
		p.Contacts[i].Vcard = ""
	}
	if len(p.Contacts) > 32 {
		p.Contacts = p.Contacts[:32]
	}
	if payload, err = json.Marshal(p); err != nil || len(payload) <= journal.MaxPayloadBytes {
		return payload, err
	}

	// Tahap 3: kosongkan teks kutipan, kurangi mentions, dan perkecil teks utama.
	if p.Quoted != nil {
		p.Quoted.Text = ""
	}
	if len(p.Mentions) > 32 {
		p.Mentions = p.Mentions[:32]
	}
	if p.Edit != nil {
		if trimmed, cut := truncateUTF8(p.Edit.Text, maxTextBytes/2); cut {
			p.Edit.Text = trimmed
		}
	}
	for limit := len(p.Text) / 2; limit >= 256; limit /= 2 {
		p.Text, _ = truncateUTF8(p.Text, limit)
		payload, err = json.Marshal(p)
		if err != nil || len(payload) <= journal.MaxPayloadBytes {
			return payload, err
		}
	}

	p.Text = ""
	p.Contacts = nil
	p.Mentions = nil
	p.Quoted = nil
	if p.Poll != nil && len(p.Poll.Options) > 16 {
		p.Poll.Options = p.Poll.Options[:16]
	}
	return json.Marshal(p)
}

func normalizeReceiptEvent(ctx context.Context, instanceID string, e *events.Receipt, nc NormalizeContext) (*journal.Event, bool) {
	if e == nil {
		return nil, false
	}
	addr := resolveAddresses(ctx, e.MessageSource, nc.LID)
	mode := nc.Receipts
	if mode == "" {
		mode = "dm"
	}
	switch mode {
	case "none":
		return nil, false
	case "dm":
		if addr.chatType != "dm" {
			return nil, false
		}
	case "all":
	default:
		if addr.chatType != "dm" {
			return nil, false
		}
	}

	ids := make([]string, 0, len(e.MessageIDs))
	for _, id := range e.MessageIDs {
		ids = append(ids, string(id))
	}
	ts := eventUnixTS(e.Timestamp, nc.Now)

	body := ReceiptV2{
		Type:       string(e.Type),
		Chat:       addr.chatCanon,
		ChatRaw:    addr.chatRaw,
		Sender:     addr.senderRaw,
		SenderPN:   addr.senderPN,
		MessageIDs: ids,
		Timestamp:  ts,
		IsGroup:    addr.isGroup,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, false
	}
	var flags uint32
	if len(payload) > journal.MaxPayloadBytes && len(body.MessageIDs) > 32 {
		body.MessageIDs = body.MessageIDs[:32]
		flags |= journal.FlagTruncated
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, false
		}
	}
	if nc.Offline {
		flags |= journal.FlagOffline
	}

	waID := ""
	keyIDPart := ""
	if len(ids) > 0 {
		waID = ids[0]
		keyIDPart = strings.Join(ids, ",")
		if len(keyIDPart) > 128 {
			keyIDPart = keyIDPart[:128]
		}
	} else {
		keyIDPart = "ts:" + e.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	dedupeKey := "r:" + e.Chat.String() + ":" + addr.senderRaw + ":" + string(e.Type) + ":" + keyIDPart

	return &journal.Event{
		InstanceID: instanceID,
		Type:       "receipt",
		Chat:       addr.chatCanon,
		Sender:     addr.senderRaw,
		WAID:       waID,
		DedupeKey:  dedupeKey,
		TS:         ts,
		ReceivedAt: nc.Now.UnixMilli(),
		Flags:      flags,
		Payload:    payload,
	}, true
}

func normalizeUndecryptableEvent(ctx context.Context, instanceID string, e *events.UndecryptableMessage, nc NormalizeContext) (*journal.Event, bool) {
	if e == nil {
		return nil, false
	}
	addr := resolveAddresses(ctx, e.Info.MessageSource, nc.LID)
	ts := eventUnixTS(e.Info.Timestamp, nc.Now)
	body := UndecryptableV2{
		ID:              e.Info.ID,
		Chat:            addr.chatCanon,
		ChatRaw:         addr.chatRaw,
		From:            addr.senderRaw,
		SenderPN:        addr.senderPN,
		Timestamp:       ts,
		IsUnavailable:   e.IsUnavailable,
		DecryptFailMode: string(e.DecryptFailMode),
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, false
	}
	var flags uint32
	if nc.Offline {
		flags |= journal.FlagOffline
	}

	dedupeKey := "u:" + e.Info.Chat.String() + ":" + addr.senderRaw + ":" + e.Info.ID
	return &journal.Event{
		InstanceID: instanceID,
		Type:       "undecryptable",
		Chat:       addr.chatCanon,
		Sender:     addr.senderRaw,
		WAID:       e.Info.ID,
		DedupeKey:  dedupeKey,
		TS:         ts,
		ReceivedAt: nc.Now.UnixMilli(),
		Flags:      flags,
		Payload:    payload,
	}, true
}

func eventUnixTS(t, fallback time.Time) int64 {
	if t.IsZero() {
		return fallback.Unix()
	}
	return t.Unix()
}

// truncateUTF8 memotong string pada batas maksimal byte tanpa memutus rune UTF-8 di tengah.
func truncateUTF8(s string, maxBytes int) (string, bool) {
	if maxBytes <= 0 {
		if s == "" {
			return "", false
		}
		return "", true
	}
	if len(s) <= maxBytes {
		return s, false
	}
	n := maxBytes
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	if n > 0 && !utf8.ValidString(s[:n]) {
		// Mundur satu rune jika byte valid terakhir ternyata multibyte yang terpotong.
		for n > 0 && n > maxBytes-4 && !utf8.ValidString(s[:n]) {
			n--
		}
	}
	return s[:n], true
}
