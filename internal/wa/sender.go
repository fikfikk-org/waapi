package wa

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// ErrQueueFull dikembalikan jika antrian kirim (256 pesan) pada sebuah instance sedang penuh.
var ErrQueueFull = errors.New("antrian kirim instance penuh")

// ErrNotConnected dikembalikan saat instance belum tersambung ke WhatsApp.
var ErrNotConnected = errors.New("instance sedang tidak tersambung")

// SendResult berisi ID pesan dan timestamp dari server WhatsApp.
type SendResult struct {
	MessageID string `json:"message_id"`
	To        string `json:"to"`
	Timestamp int64  `json:"timestamp"`
}

type sendRequest struct {
	ctx    context.Context
	to     types.JID
	msg    *waE2E.Message
	typing bool
	textLen int
	reply  chan sendOutcome
}

type sendOutcome struct {
	res SendResult
	err error
}

// EnqueueText memasukkan pesan teks ke dalam antrian ber-rate-limit milik instance.
func (i *Instance) EnqueueText(ctx context.Context, toRaw, text, replyTo string, typing bool) (SendResult, error) {
	toJID, err := ParseRecipient(toRaw)
	if err != nil {
		return SendResult{}, err
	}
	var msg *waE2E.Message
	if replyTo != "" {
		msg = &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String(text),
				ContextInfo: &waE2E.ContextInfo{
					StanzaID: proto.String(replyTo),
				},
			},
		}
	} else {
		msg = &waE2E.Message{
			Conversation: proto.String(text),
		}
	}
	return i.enqueueMessage(ctx, toJID, msg, typing, len(text))
}

// EnqueueMedia mengunggah stream media ke CDN WhatsApp lalu menjadwalkan pengiriman pesannya.
func (i *Instance) EnqueueMedia(ctx context.Context, in MediaInput) (SendResult, error) {
	toJID, err := ParseRecipient(in.To)
	if err != nil {
		return SendResult{}, err
	}
	up, detectedMime, _, err := i.UploadStream(ctx, in.Reader, in.MediaType)
	if err != nil {
		return SendResult{}, err
	}
	mime := in.MimeType
	if mime == "" {
		mime = detectedMime
	}
	msg := buildMediaE2EMessage(in.MediaType, mime, in.Caption, in.Filename, up)
	return i.enqueueMessage(ctx, toJID, msg, false, len(in.Caption))
}

func (i *Instance) enqueueMessage(ctx context.Context, to types.JID, msg *waE2E.Message, typing bool, textLen int) (SendResult, error) {
	if !i.IsConnected() {
		return SendResult{}, ErrNotConnected
	}
	req := sendRequest{
		ctx:     ctx,
		to:      to,
		msg:     msg,
		typing:  typing,
		textLen: textLen,
		reply:   make(chan sendOutcome, 1),
	}
	select {
	case i.sendCh <- req:
	default:
		return SendResult{}, ErrQueueFull
	}

	select {
	case <-ctx.Done():
		return SendResult{}, ctx.Err()
	case out := <-req.reply:
		return out.res, out.err
	}
}

// senderLoop memproses antrian pesan secara serial (1 goroutine per instance) dengan rate limit + jitter.
func (i *Instance) senderLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-i.sendCh:
			if !ok {
				return
			}
			res, err := i.processSend(ctx, req)
			req.reply <- sendOutcome{res: res, err: err}
		}
	}
}

func (i *Instance) processSend(loopCtx context.Context, req sendRequest) (SendResult, error) {
	if err := req.ctx.Err(); err != nil {
		return SendResult{}, err
	}
	// Tunggu token dari rate limiter instance
	if err := i.getLimiter().Wait(req.ctx); err != nil {
		return SendResult{}, err
	}

	// Jitter acak 300-1500 ms agar pola pengiriman tidak periodik
	jitter := time.Duration(300+rand.IntN(1200)) * time.Millisecond
	select {
	case <-req.ctx.Done():
		return SendResult{}, req.ctx.Err()
	case <-loopCtx.Done():
		return SendResult{}, loopCtx.Err()
	case <-time.After(jitter):
	}

	cli := i.getClient()
	if cli == nil || !cli.IsConnected() {
		return SendResult{}, ErrNotConnected
	}

	if req.typing {
		_ = cli.SendChatPresence(req.ctx, req.to, types.ChatPresenceComposing, types.ChatPresenceMediaText)
		typingWait := time.Duration(min(max(req.textLen*25, 400), 2500)) * time.Millisecond
		select {
		case <-req.ctx.Done():
			return SendResult{}, req.ctx.Err()
		case <-time.After(typingWait):
		}
		_ = cli.SendChatPresence(req.ctx, req.to, types.ChatPresencePaused, types.ChatPresenceMediaText)
	}

	sendCtx, cancel := context.WithTimeout(req.ctx, 30*time.Second)
	defer cancel()
	resp, err := cli.SendMessage(sendCtx, req.to, req.msg)
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{
		MessageID: string(resp.ID),
		To:        req.to.ToNonAD().String(),
		Timestamp: resp.Timestamp.Unix(),
	}, nil
}

