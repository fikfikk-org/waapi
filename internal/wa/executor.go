package wa

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/jid"
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

// Execute mengeksekusi pengiriman satu job outbox ke WhatsApp memakai WAID yang telah ditetapkan sebelumnya.
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
	if job.Spec.Type != "text" {
		return outbox.ExecResult{}, &outbox.SendError{
			Transient: false,
			Code:      "invalid_spec",
			Err:       outbox.ErrInvalidSpec,
		}
	}

	if job.Spec.Typing {
		if err := sendTypingCadence(ctx, cli, toJID, len(job.Spec.Text)); err != nil {
			return outbox.ExecResult{}, toSendError(err)
		}
	}

	msg := composeTextMessage(job.Spec)
	resp, err := cli.SendMessage(ctx, toJID, msg, whatsmeow.SendRequestExtra{
		ID: types.MessageID(job.WAID),
	})
	if err != nil {
		return outbox.ExecResult{}, toSendError(err)
	}
	return outbox.ExecResult{ServerTS: resp.Timestamp}, nil
}

func composeTextMessage(spec outbox.Spec) *waE2E.Message {
	hasReply := spec.ReplyTo != nil && strings.TrimSpace(spec.ReplyTo.ID) != ""
	hasMentions := len(spec.Mentions) > 0
	if !hasReply && !hasMentions {
		return &waE2E.Message{
			Conversation: proto.String(spec.Text),
		}
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
