package wa

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/netguard"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// Ready memeriksa apakah instance terdaftar dan saat ini tersambung ke WhatsApp.
func (m *Manager) Ready(instanceID string) bool {
	if m == nil {
		return false
	}
	inst, ok := m.Get(instanceID)
	if !ok {
		return false
	}
	return inst.IsConnected()
}

// NewMessageID menghasilkan ID pesan WhatsApp berformat 3EB0... untuk instance yang sudah di-pairing walau sedang offline.
func (m *Manager) NewMessageID(instanceID string) (string, error) {
	if m == nil {
		return "", outbox.ErrNotPaired
	}
	inst, ok := m.Get(instanceID)
	if !ok {
		return "", outbox.ErrNotPaired
	}

	cli := inst.getClient()
	if cli != nil && cli.Store != nil && cli.Store.ID != nil {
		return string(cli.GenerateMessageID()), nil
	}

	rawJID := strings.TrimSpace(inst.getJID())
	if rawJID == "" {
		return "", outbox.ErrNotPaired
	}
	own, err := types.ParseJID(rawJID)
	if err != nil || own.User == "" {
		return "", outbox.ErrNotPaired
	}
	return generateOfflineMessageID(own.User), nil
}

func generateOfflineMessageID(ownUser string) string {
	data := make([]byte, 8, 8+len(ownUser)+5+16)
	binary.BigEndian.PutUint64(data, uint64(time.Now().Unix()))
	data = append(data, ownUser...)
	data = append(data, "@c.us"...)
	var rnd [16]byte
	_, _ = rand.Read(rnd[:])
	data = append(data, rnd[:]...)
	sum := sha256.Sum256(data)
	return whatsmeow.WebMessageIDPrefix + strings.ToUpper(hex.EncodeToString(sum[:9]))
}

// Execute mengeksekusi pengiriman satu job outbox ke WhatsApp memakai WAID deterministik.
func (m *Manager) Execute(ctx context.Context, job *outbox.Job) (outbox.ExecResult, error) {
	if m == nil || job == nil {
		return outbox.ExecResult{}, toSendError(whatsmeow.ErrNotConnected)
	}
	inst, ok := m.Get(job.InstanceID)
	if !ok {
		return outbox.ExecResult{}, toSendError(whatsmeow.ErrNotConnected)
	}
	cli := inst.getClient()
	if cli == nil || !cli.IsConnected() {
		return outbox.ExecResult{}, toSendError(whatsmeow.ErrNotConnected)
	}

	deps := &realComposeDeps{
		mgr:        m,
		inst:       inst,
		cli:        cli,
		instanceID: job.InstanceID,
	}
	return m.executeWithSender(ctx, job, deps, cli.SendMessage)
}

type sendMessageFn func(ctx context.Context, to types.JID, message *waE2E.Message, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error)

func (m *Manager) executeWithSender(ctx context.Context, job *outbox.Job, deps ComposeDeps, sendFn sendMessageFn) (outbox.ExecResult, error) {
	if m == nil || job == nil {
		return outbox.ExecResult{}, toSendError(whatsmeow.ErrNotConnected)
	}
	toJID, err := jid.ParseRecipient(job.Spec.To)
	if err != nil {
		return outbox.ExecResult{}, toSendError(err)
	}

	msg, err := Compose(ctx, deps, job.Spec)
	if err != nil {
		return outbox.ExecResult{}, err
	}

	// Simpan definisi polling keluar ke journal.db SEBELUM SendMessage agar vote masuk selalu punya acuan opsi.
	if strings.EqualFold(strings.TrimSpace(job.Spec.Type), "poll") && m.pollStore != nil && job.Spec.Poll != nil {
		creator := ""
		if own := deps.OwnJID(); !own.IsEmpty() {
			creator = own.ToNonAD().String()
		}
		opts := make([]string, len(job.Spec.Poll.Options))
		for idx, o := range job.Spec.Poll.Options {
			opts[idx] = strings.TrimSpace(o)
		}
		if pErr := m.pollStore.SavePoll(ctx, Poll{
			InstanceID: job.InstanceID,
			Chat:       toJID.String(),
			WAID:       job.WAID,
			Creator:    creator,
			Name:       strings.TrimSpace(job.Spec.Poll.Name),
			Options:    opts,
			Selectable: job.Spec.Poll.Selectable,
			CreatedAt:  time.Now().Unix(),
		}); pErr != nil {
			return outbox.ExecResult{}, &outbox.SendError{Transient: true, Code: "send_failed", Err: pErr}
		}
	}

	if job.Spec.Typing {
		if rc, ok := deps.(*realComposeDeps); ok && rc.cli != nil {
			typingLen := len(job.Spec.Text)
			if typingLen == 0 && job.Spec.Media != nil {
				typingLen = len(job.Spec.Media.Caption)
			}
			if err := sendTypingCadence(ctx, rc.cli, toJID, typingLen); err != nil {
				return outbox.ExecResult{}, toSendError(err)
			}
		}
	}

	resp, err := sendFn(ctx, toJID, msg, whatsmeow.SendRequestExtra{
		ID: types.MessageID(job.WAID),
	})
	if err != nil {
		return outbox.ExecResult{}, toSendError(err)
	}
	if rc, ok := deps.(*realComposeDeps); ok && rc.inst != nil {
		rc.inst.recordActivity(time.Now())
	}
	return outbox.ExecResult{ServerTS: resp.Timestamp}, nil
}

type realComposeDeps struct {
	mgr        *Manager
	inst       *Instance
	cli        *whatsmeow.Client
	instanceID string
}

func (d *realComposeDeps) BuildPollCreation(name string, optionNames []string, selectableOptionCount int) *waE2E.Message {
	return d.cli.BuildPollCreation(name, optionNames, selectableOptionCount)
}

func (d *realComposeDeps) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	return d.cli.BuildReaction(chat, sender, id, reaction)
}

func (d *realComposeDeps) BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message {
	return d.cli.BuildEdit(chat, id, newContent)
}

func (d *realComposeDeps) BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message {
	return d.cli.BuildRevoke(chat, sender, id)
}

func (d *realComposeDeps) OwnJID() types.JID {
	if d.cli != nil && d.cli.Store != nil && d.cli.Store.ID != nil {
		return d.cli.Store.ID.ToNonAD()
	}
	if d.inst != nil {
		if raw := strings.TrimSpace(d.inst.getJID()); raw != "" {
			if parsed, err := types.ParseJID(raw); err == nil {
				return parsed.ToNonAD()
			}
		}
	}
	return types.EmptyJID
}

func (d *realComposeDeps) Media(ctx context.Context, ref outbox.MediaRef, kind string) (media.Uploaded, error) {
	if ref.UploadID != "" {
		if d.mgr.uploadStore == nil {
			return media.Uploaded{}, &outbox.SendError{Transient: false, Code: "media_expired", Err: outbox.ErrMediaExpired}
		}
		rec, err := d.mgr.uploadStore.Get(ctx, d.instanceID, ref.UploadID, time.Now())
		if err != nil {
			if errors.Is(err, media.ErrUploadExpired) || errors.Is(err, media.ErrUploadNotFound) {
				return media.Uploaded{}, &outbox.SendError{Transient: false, Code: "media_expired", Err: outbox.ErrMediaExpired}
			}
			return media.Uploaded{}, &outbox.SendError{Transient: true, Code: "send_failed", Err: err}
		}
		if !IsUploadKindCompatible(rec.Kind, canonicalKindForMessageType(kind)) {
			return media.Uploaded{}, &outbox.SendError{Transient: false, Code: "invalid_spec", Err: outbox.ErrInvalidSpec}
		}
		return media.Uploaded{
			Resp: rec.Resp,
			Meta: media.Meta{
				Mimetype: rec.Mimetype,
				Size:     rec.Size,
				Width:    rec.Width,
				Height:   rec.Height,
				Seconds:  rec.Seconds,
				PTT:      rec.PTT,
				Filename: rec.Filename,
			},
		}, nil
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	stream, err := fetchSafeMediaStream(fetchCtx, d.mgr.mediaHTTPClient(), ref.URL)
	if err != nil {
		return media.Uploaded{}, classifyMediaFetchError(err)
	}
	defer stream.Close()

	k := media.Kind(kind)
	if k == "audio" && ref.PTT {
		k = media.KindPTT
	}
	uploaded, err := d.mgr.ensureMediaPipeline().Upload(fetchCtx, d.cli, stream, k, media.Hint{
		Filename: ref.Filename,
		Seconds:  ref.Seconds,
	})
	if err != nil {
		return media.Uploaded{}, classifyMediaUploadError(err)
	}
	return uploaded, nil
}

func (d *realComposeDeps) Quoted(ctx context.Context, chat, waID string) (QuotedInfo, bool) {
	if d.mgr == nil || d.mgr.journal == nil || waID == "" {
		return QuotedInfo{}, false
	}
	ev, err := d.mgr.journal.FindByWAID(ctx, d.instanceID, chat, waID)
	if err != nil || ev == nil {
		return QuotedInfo{}, false
	}

	var msg MessageV2
	if err := json.Unmarshal(ev.Payload, &msg); err != nil {
		return QuotedInfo{}, false
	}

	sender := ev.Sender
	if msg.IsFromMe {
		if own := d.OwnJID(); !own.IsEmpty() {
			sender = own.String()
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

func canonicalKindForMessageType(kind string) string {
	if kind == string(media.KindPTT) {
		return "audio"
	}
	return kind
}

// IsUploadKindCompatible memeriksa kecocokan kind descriptor upload terhadap type pesan outbox.
func IsUploadKindCompatible(uploadKind media.Kind, msgType string) bool {
	if msgType == "audio" && uploadKind == media.KindPTT {
		return true
	}
	return string(uploadKind) == msgType
}

func classifyMediaFetchError(err error) error {
	if netguard.IsPermanentSSRF(err) {
		return &outbox.SendError{Transient: false, Code: "ssrf_blocked", Err: err}
	}
	var hErr httpStatusError
	if errors.As(err, &hErr) {
		return &outbox.SendError{
			Transient: hErr.transient(),
			Code:      "media_fetch_failed",
			Err:       err,
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && !dnsErr.Temporary() {
		return &outbox.SendError{Transient: true, Code: "media_fetch_failed", Err: err}
	}
	return &outbox.SendError{Transient: true, Code: "media_fetch_failed", Err: err}
}

func classifyMediaUploadError(err error) error {
	switch {
	case errors.Is(err, media.ErrMediaTooLarge),
		errors.Is(err, media.ErrUnsupportedMedia),
		errors.Is(err, media.ErrPTTRequiresOggOpus),
		errors.Is(err, media.ErrStickerRequiresWebP),
		errors.Is(err, media.ErrInvalidKind):
		return &outbox.SendError{Transient: false, Code: "invalid_media", Err: err}
	case errors.Is(err, whatsmeow.ErrNotConnected),
		errors.Is(err, whatsmeow.ErrNotLoggedIn):
		return toSendError(err)
	default:
		return &outbox.SendError{Transient: true, Code: "media_fetch_failed", Err: err}
	}
}

func composeTextMessage(spec outbox.Spec) *waE2E.Message {
	hasReply := spec.ReplyTo != nil && strings.TrimSpace(spec.ReplyTo.ID) != ""
	hasMentions := len(spec.Mentions) > 0
	if !hasReply && !hasMentions {
		return &waE2E.Message{Conversation: proto.String(spec.Text)}
	}
	ci := &waE2E.ContextInfo{}
	if hasReply {
		ci.StanzaID = proto.String(spec.ReplyTo.ID)
		if spec.ReplyTo.Sender != "" {
			ci.Participant = proto.String(spec.ReplyTo.Sender)
		}
	}
	if hasMentions {
		ci.MentionedJID = spec.Mentions
	}
	return &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text:        proto.String(spec.Text),
			ContextInfo: ci,
		},
	}
}

func composeMediaMessage(spec outbox.Spec, rec media.UploadRecord) (*waE2E.Message, error) {
	var ci *waE2E.ContextInfo
	hasReply := spec.ReplyTo != nil && strings.TrimSpace(spec.ReplyTo.ID) != ""
	hasMentions := len(spec.Mentions) > 0
	if hasReply || hasMentions {
		ci = &waE2E.ContextInfo{}
		if hasReply {
			ci.StanzaID = proto.String(spec.ReplyTo.ID)
			if spec.ReplyTo.Sender != "" {
				ci.Participant = proto.String(spec.ReplyTo.Sender)
			}
		}
		if hasMentions {
			ci.MentionedJID = spec.Mentions
		}
	}
	return buildMediaE2EFromRecord(spec, rec, ci)
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}

func optUint32(v uint32) *uint32 {
	if v == 0 {
		return nil
	}
	return proto.Uint32(v)
}

func optBool(v bool) *bool {
	if !v {
		return nil
	}
	return proto.Bool(true)
}

func sendTypingCadence(ctx context.Context, cli *whatsmeow.Client, to types.JID, textLen int) error {
	_ = cli.SendChatPresence(ctx, to, types.ChatPresenceComposing, types.ChatPresenceMediaText)
	typingWait := time.Duration(min(max(textLen*25, 400), 2500)) * time.Millisecond
	timer := time.NewTimer(typingWait)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
	}
	_ = cli.SendChatPresence(ctx, to, types.ChatPresencePaused, types.ChatPresenceMediaText)
	return nil
}
