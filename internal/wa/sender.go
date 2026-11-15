package wa

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

// ErrQueueFull dikembalikan jika antrian kirim pada sebuah instance sedang penuh.
var ErrQueueFull = errors.New("antrian kirim instance penuh")

// ErrNotConnected dikembalikan saat instance belum tersambung ke WhatsApp.
var ErrNotConnected = errors.New("instance sedang tidak tersambung")

// SendResult berisi ID pesan dan timestamp dari server WhatsApp untuk respons kompatibilitas v1.
type SendResult struct {
	MessageID string `json:"message_id"`
	To        string `json:"to"`
	Timestamp int64  `json:"timestamp"`
}

// EnqueueText mengirim pesan teks secara langsung atau lewat durable outbox bila tersedia.
func (i *Instance) EnqueueText(ctx context.Context, toRaw, text, replyTo string, typing bool) (SendResult, error) {
	if i.mgr != nil && i.mgr.outbox != nil {
		if !i.IsConnected() {
			return SendResult{}, ErrNotConnected
		}
		var reply *outbox.ReplyRef
		if replyTo != "" {
			reply = &outbox.ReplyRef{ID: replyTo}
		}
		job, _, err := i.mgr.outbox.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: i.id,
			Spec: outbox.Spec{
				Type:    "text",
				To:      toRaw,
				Text:    text,
				ReplyTo: reply,
				Typing:  typing,
			},
			Origin: outbox.Origin{Kind: "v1"},
		})
		if err != nil {
			return SendResult{}, err
		}
		waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		sent, err := i.mgr.outbox.Wait(waitCtx, i.id, job.ID, outbox.StatusSent)
		if err != nil {
			return SendResult{}, err
		}
		return SendResult{
			MessageID: sent.WAID,
			To:        sent.ToJID,
			Timestamp: sent.SentAt / 1000,
		}, nil
	}

	toJID, err := ParseRecipient(toRaw)
	if err != nil {
		return SendResult{}, err
	}
	var reply *outbox.ReplyRef
	if replyTo != "" {
		reply = &outbox.ReplyRef{ID: replyTo}
	}
	msg := composeTextMessage(outbox.Spec{Text: text, ReplyTo: reply})
	return i.sendDirect(ctx, toJID, msg, typing, len(text))
}

// EnqueueMedia mengunggah stream media ke CDN WhatsApp lalu mengirimkannya langsung dengan rate limit dan jitter v1 (dipindah ke outbox di Batch 6).
func (i *Instance) EnqueueMedia(ctx context.Context, in MediaInput) (SendResult, error) {
	if !i.IsConnected() {
		return SendResult{}, ErrNotConnected
	}
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
	return i.sendDirect(ctx, toJID, msg, false, len(in.Caption))
}

func (i *Instance) sendDirect(ctx context.Context, to types.JID, msg *waE2E.Message, typing bool, textLen int) (SendResult, error) {
	if !i.IsConnected() {
		return SendResult{}, ErrNotConnected
	}
	if err := i.getLimiter().Wait(ctx); err != nil {
		return SendResult{}, err
	}

	// Jitter acak 300-1500 ms (paritas v1) sebelum menghubungi server WhatsApp.
	jitter := time.Duration(300+rand.IntN(1201)) * time.Millisecond
	timer := time.NewTimer(jitter)
	select {
	case <-ctx.Done():
		timer.Stop()
		return SendResult{}, ctx.Err()
	case <-timer.C:
	}

	cli := i.getClient()
	if cli == nil || !cli.IsConnected() {
		return SendResult{}, ErrNotConnected
	}

	if typing {
		if err := sendTypingCadence(ctx, cli, to, textLen); err != nil {
			return SendResult{}, err
		}
	}

	sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := cli.SendMessage(sendCtx, to, msg)
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{
		MessageID: string(resp.ID),
		To:        to.ToNonAD().String(),
		Timestamp: resp.Timestamp.Unix(),
	}, nil
}
