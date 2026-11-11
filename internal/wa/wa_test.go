package wa

import (
	"net"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// TestParseRecipient memastikan normalisasi nomor telepon dan JID berjalan akurat.
func TestParseRecipient(t *testing.T) {
	jid, err := ParseRecipient("+62 812-3456-7890")
	if err != nil {
		t.Fatalf("ParseRecipient gagal: %v", err)
	}
	if jid.User != "6281234567890" || jid.Server != types.DefaultUserServer {
		t.Fatalf("JID tidak sesuai: %+v", jid)
	}

	grp, err := ParseRecipient("1203630123456789@g.us")
	if err != nil || grp.Server != types.GroupServer {
		t.Fatalf("group JID gagal diparse: %+v, err=%v", grp, err)
	}

	if _, err := ParseRecipient("123"); err == nil {
		t.Fatal("diharapkan error untuk nomor yang terlalu pendek")
	}
}

// TestIsBlockedIP memastikan anti-SSRF menolak IP privat dan mengizinkan IP publik.
func TestIsBlockedIP(t *testing.T) {
	blocked := []string{"127.0.0.1", "10.0.0.1", "192.168.1.10", "172.16.5.4", "169.254.169.254", "::1"}
	for _, s := range blocked {
		if !isBlockedIP(net.ParseIP(s)) {
			t.Fatalf("IP %s seharusnya diblokir", s)
		}
	}
	if isBlockedIP(net.ParseIP("1.1.1.1")) {
		t.Fatal("IP publik 1.1.1.1 tidak boleh diblokir")
	}
}

// TestExtractMessage memastikan konversi pesan masuk ke payload ringkas berjalan sesuai aturan.
func TestExtractMessage(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			ID: "MSG123",
			MessageSource: types.MessageSource{
				Chat:   types.NewJID("628123456789", types.DefaultUserServer),
				Sender: types.NewJID("628123456789", types.DefaultUserServer),
			},
			PushName: "Budi",
		},
		Message: &waE2E.Message{
			Conversation: proto.String("Halo dunia"),
		},
	}
	p := extractMessage(evt)
	if p == nil || p.Type != "text" || p.Text != "Halo dunia" {
		t.Fatalf("payload pesan teks tidak sesuai: %+v", p)
	}

	// Protocol message murni harus di-drop (nil)
	protoEvt := &events.Message{
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{},
		},
	}
	if extractMessage(protoEvt) != nil {
		t.Fatal("protocol message seharusnya di-drop")
	}
}

