package jid

import (
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

// TestParseRecipient memverifikasi normalisasi nomor telepon, grup, LID, dan penolakan input tidak valid.
func TestParseRecipient(t *testing.T) {
	pn, err := ParseRecipient("+62 812-3456-7890")
	if err != nil {
		t.Fatalf("ParseRecipient PN gagal: %v", err)
	}
	if pn.User != "6281234567890" || pn.Server != types.DefaultUserServer {
		t.Fatalf("hasil PN tidak sesuai: %+v", pn)
	}

	grp, err := ParseRecipient("1203630123456789@g.us")
	if err != nil || grp.Server != types.GroupServer {
		t.Fatalf("hasil group JID tidak sesuai: %+v, err=%v", grp, err)
	}

	lid, err := ParseRecipient("123456789012345@lid")
	if err != nil || lid.Server != types.HiddenUserServer {
		t.Fatalf("hasil LID JID tidak sesuai: %+v, err=%v", lid, err)
	}

	if got := DigitsOnly("+62 (812) 345-678"); got != "62812345678" {
		t.Fatalf("DigitsOnly tidak sesuai: %q", got)
	}

	for _, bad := range []string{"", "123", "abc", "@s.whatsapp.net"} {
		_, err := ParseRecipient(bad)
		if !errors.Is(err, ErrInvalidRecipient) {
			t.Fatalf("input %q harus mengembalikan ErrInvalidRecipient, didapat %v", bad, err)
		}
	}
}
