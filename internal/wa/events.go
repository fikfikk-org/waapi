package wa

import (
	"encoding/base64"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

// MediaDescriptor berisi metadata untuk mengunduh media masuk secara on-demand tanpa menyimpannya di RAM/disk.
type MediaDescriptor struct {
	Mimetype      string `json:"mimetype"`
	DirectPath    string `json:"direct_path"`
	MediaKey      string `json:"media_key"`
	FileEncSHA256 string `json:"file_enc_sha256"`
	FileSHA256    string `json:"file_sha256"`
	FileLength    uint64 `json:"file_length"`
	MediaType     string `json:"media_type"` // image | video | audio | document | sticker
	Filename      string `json:"filename,omitempty"`
}

// MessagePayload adalah bentuk ringkas pesan masuk yang dikirim ke webhook.
type MessagePayload struct {
	ID       string           `json:"id"`
	From     string           `json:"from"`
	Chat     string           `json:"chat"`
	IsGroup  bool             `json:"is_group"`
	FromMe   bool             `json:"from_me"`
	PushName string           `json:"push_name,omitempty"`
	Type     string           `json:"type"`
	Text     string           `json:"text,omitempty"`
	Media    *MediaDescriptor `json:"media,omitempty"`
}

// ReceiptPayload merangkum tanda terima pesan (delivered/read/played).
type ReceiptPayload struct {
	MessageIDs []string `json:"message_ids"`
	Chat       string   `json:"chat"`
	Sender     string   `json:"sender"`
	Type       string   `json:"type"`
	Timestamp  int64    `json:"timestamp"`
}

// extractMessage mengubah *events.Message whatsmeow menjadi MessagePayload ringan tanpa raw protobuf.
func extractMessage(evt *events.Message) *MessagePayload {
	if evt == nil || evt.Message == nil {
		return nil
	}
	msg := evt.Message
	out := &MessagePayload{
		ID:       string(evt.Info.ID),
		From:     evt.Info.Sender.ToNonAD().String(),
		Chat:     evt.Info.Chat.ToNonAD().String(),
		IsGroup:  evt.Info.IsGroup,
		FromMe:   evt.Info.IsFromMe,
		PushName: evt.Info.PushName,
		Type:     "unknown",
	}

	switch {
	case msg.GetConversation() != "":
		out.Type = "text"
		out.Text = msg.GetConversation()
	case msg.GetExtendedTextMessage() != nil:
		out.Type = "text"
		out.Text = msg.GetExtendedTextMessage().GetText()
	case msg.GetImageMessage() != nil:
		im := msg.GetImageMessage()
		out.Type = "image"
		out.Text = im.GetCaption()
		out.Media = buildMedia("image", im.GetMimetype(), "", im.GetDirectPath(), im.GetMediaKey(), im.GetFileEncSHA256(), im.GetFileSHA256(), im.GetFileLength())
	case msg.GetVideoMessage() != nil:
		vm := msg.GetVideoMessage()
		out.Type = "video"
		out.Text = vm.GetCaption()
		out.Media = buildMedia("video", vm.GetMimetype(), "", vm.GetDirectPath(), vm.GetMediaKey(), vm.GetFileEncSHA256(), vm.GetFileSHA256(), vm.GetFileLength())
	case msg.GetAudioMessage() != nil:
		am := msg.GetAudioMessage()
		out.Type = "audio"
		out.Media = buildMedia("audio", am.GetMimetype(), "", am.GetDirectPath(), am.GetMediaKey(), am.GetFileEncSHA256(), am.GetFileSHA256(), am.GetFileLength())
	case msg.GetDocumentMessage() != nil:
		dm := msg.GetDocumentMessage()
		out.Type = "document"
		out.Text = dm.GetCaption()
		out.Media = buildMedia("document", dm.GetMimetype(), dm.GetFileName(), dm.GetDirectPath(), dm.GetMediaKey(), dm.GetFileEncSHA256(), dm.GetFileSHA256(), dm.GetFileLength())
	case msg.GetStickerMessage() != nil:
		sm := msg.GetStickerMessage()
		out.Type = "sticker"
		out.Media = buildMedia("sticker", sm.GetMimetype(), "", sm.GetDirectPath(), sm.GetMediaKey(), sm.GetFileEncSHA256(), sm.GetFileSHA256(), sm.GetFileLength())
	case msg.GetReactionMessage() != nil:
		out.Type = "reaction"
		out.Text = msg.GetReactionMessage().GetText()
	default:
		// Abaikan pesan sistem/protocol message kosong agar tidak membanjiri webhook
		if isProtocolOnly(msg) {
			return nil
		}
	}

	if len(out.Text) > 16384 {
		out.Text = out.Text[:16384]
	}
	return out
}

func isProtocolOnly(m *waE2E.Message) bool {
	return m.GetProtocolMessage() != nil || m.GetSenderKeyDistributionMessage() != nil
}

func buildMedia(kind, mime, filename, directPath string, mediaKey, encHash, sha []byte, length uint64) *MediaDescriptor {
	return &MediaDescriptor{
		Mimetype:      mime,
		DirectPath:    directPath,
		MediaKey:      base64.StdEncoding.EncodeToString(mediaKey),
		FileEncSHA256: base64.StdEncoding.EncodeToString(encHash),
		FileSHA256:    base64.StdEncoding.EncodeToString(sha),
		FileLength:    length,
		MediaType:     kind,
		Filename:      filename,
	}
}

