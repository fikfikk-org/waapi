package wa

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
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

// Execute mengeksekusi pengiriman satu job outbox (text maupun media) ke WhatsApp memakai WAID deterministik.
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

	toJID, err := jid.ParseRecipient(job.Spec.To)
	if err != nil {
		return outbox.ExecResult{}, toSendError(err)
	}

	var msg *waE2E.Message
	switch job.Spec.Type {
	case "text":
		msg = composeTextMessage(job.Spec)
	case "image", "video", "audio", "document", "sticker":
		rec, mErr := m.resolveMediaRecord(ctx, cli, job)
		if mErr != nil {
			return outbox.ExecResult{}, mErr
		}
		var cErr error
		msg, cErr = composeMediaMessage(job.Spec, rec)
		if cErr != nil {
			return outbox.ExecResult{}, cErr
		}
	default:
		return outbox.ExecResult{}, &outbox.SendError{
			Transient: false,
			Code:      "invalid_spec",
			Err:       outbox.ErrInvalidSpec,
		}
	}

	if job.Spec.Typing {
		typingLen := len(job.Spec.Text)
		if typingLen == 0 && job.Spec.Media != nil {
			typingLen = len(job.Spec.Media.Caption)
		}
		if err := sendTypingCadence(ctx, cli, toJID, typingLen); err != nil {
			return outbox.ExecResult{}, toSendError(err)
		}
	}

	resp, err := cli.SendMessage(ctx, toJID, msg, whatsmeow.SendRequestExtra{
		ID: types.MessageID(job.WAID),
	})
	if err != nil {
		return outbox.ExecResult{}, toSendError(err)
	}
	return outbox.ExecResult{ServerTS: resp.Timestamp}, nil
}

func (m *Manager) resolveMediaRecord(ctx context.Context, cli *whatsmeow.Client, job *outbox.Job) (media.UploadRecord, error) {
	if job.Spec.Media == nil {
		return media.UploadRecord{}, &outbox.SendError{Transient: false, Code: "invalid_spec", Err: outbox.ErrInvalidSpec}
	}
	ref := job.Spec.Media
	if ref.UploadID != "" {
		if m.uploadStore == nil {
			return media.UploadRecord{}, &outbox.SendError{Transient: false, Code: "media_expired", Err: outbox.ErrMediaExpired}
		}
		rec, err := m.uploadStore.Get(ctx, job.InstanceID, ref.UploadID, time.Now())
		if err != nil {
			if errors.Is(err, media.ErrUploadExpired) || errors.Is(err, media.ErrUploadNotFound) {
				return media.UploadRecord{}, &outbox.SendError{Transient: false, Code: "media_expired", Err: outbox.ErrMediaExpired}
			}
			return media.UploadRecord{}, &outbox.SendError{Transient: true, Code: "send_failed", Err: err}
		}
		if !IsUploadKindCompatible(rec.Kind, job.Spec.Type) {
			return media.UploadRecord{}, &outbox.SendError{Transient: false, Code: "invalid_spec", Err: outbox.ErrInvalidSpec}
		}
		return *rec, nil
	}

	// Unduh dari media.url saat eksekusi lalu alirkan langsung ke pipeline upload satu-pass.
	fetchCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	stream, err := fetchSafeMediaStream(fetchCtx, m.mediaHTTPClient(), ref.URL)
	if err != nil {
		return media.UploadRecord{}, classifyMediaFetchError(err)
	}
	defer stream.Close()

	kind := media.Kind(job.Spec.Type)
	if job.Spec.Type == "audio" && ref.PTT {
		kind = media.KindPTT
	}
	uploaded, err := m.ensureMediaPipeline().Upload(fetchCtx, cli, stream, kind, media.Hint{
		Filename: ref.Filename,
		Seconds:  ref.Seconds,
	})
	if err != nil {
		return media.UploadRecord{}, classifyMediaUploadError(err)
	}

	return media.UploadRecord{
		InstanceID: job.InstanceID,
		Kind:       kind,
		Mimetype:   uploaded.Meta.Mimetype,
		Size:       uploaded.Meta.Size,
		Resp:       uploaded.Resp,
		Width:      uploaded.Meta.Width,
		Height:     uploaded.Meta.Height,
		Seconds:    uploaded.Meta.Seconds,
		PTT:        uploaded.Meta.PTT,
		Filename:   uploaded.Meta.Filename,
	}, nil
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

func buildContextInfo(spec outbox.Spec) *waE2E.ContextInfo {
	hasReply := spec.ReplyTo != nil && strings.TrimSpace(spec.ReplyTo.ID) != ""
	hasMentions := len(spec.Mentions) > 0
	if !hasReply && !hasMentions {
		return nil
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
	return ci
}

func composeTextMessage(spec outbox.Spec) *waE2E.Message {
	ci := buildContextInfo(spec)
	if ci == nil {
		return &waE2E.Message{
			Conversation: proto.String(spec.Text),
		}
	}
	return &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text:        proto.String(spec.Text),
			ContextInfo: ci,
		},
	}
}

func composeMediaMessage(spec outbox.Spec, rec media.UploadRecord) (*waE2E.Message, error) {
	ci := buildContextInfo(spec)
	var mSpec outbox.MediaRef
	if spec.Media != nil {
		mSpec = *spec.Media
	}

	var thumb []byte
	if mSpec.Thumbnail != "" {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(mSpec.Thumbnail))
		if err != nil {
			return nil, &outbox.SendError{Transient: false, Code: "invalid_spec", Err: fmt.Errorf("thumbnail base64 tidak valid: %w", err)}
		}
		if len(raw) > 16<<10 {
			return nil, &outbox.SendError{Transient: false, Code: "invalid_spec", Err: errors.New("ukuran thumbnail melebihi batas 16 KiB")}
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
		return nil, &outbox.SendError{Transient: false, Code: "invalid_spec", Err: outbox.ErrInvalidSpec}
	}
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
