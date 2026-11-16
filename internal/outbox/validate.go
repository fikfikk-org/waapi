package outbox

import (
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/fikfikk-org/waapi/internal/jid"
)

const maxTextMessageBytes = 60000

// ValidateTextBody memastikan teks pesan atau edit non-kosong dan maksimal 60.000 byte.
func ValidateTextBody(text string) error {
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("%w: field text wajib diisi", ErrInvalidSpec)
	}
	if len(text) > maxTextMessageBytes {
		return fmt.Errorf("%w: panjang teks melebihi batas %d byte", ErrInvalidSpec, maxTextMessageBytes)
	}
	return nil
}

// ValidateLocation memvalidasi koordinat lokasi berada dalam rentang lintang [-90, 90] dan bujur [-180, 180].
func ValidateLocation(loc *Location) error {
	if loc == nil {
		return fmt.Errorf("%w: objek location wajib diisi", ErrInvalidSpec)
	}
	if math.IsNaN(loc.Lat) || math.IsInf(loc.Lat, 0) || loc.Lat < -90 || loc.Lat > 90 {
		return fmt.Errorf("%w: lat harus berada di antara -90 dan 90", ErrInvalidSpec)
	}
	if math.IsNaN(loc.Lng) || math.IsInf(loc.Lng, 0) || loc.Lng < -180 || loc.Lng > 180 {
		return fmt.Errorf("%w: lng harus berada di antara -180 dan 180", ErrInvalidSpec)
	}
	return nil
}

// ValidateContacts memvalidasi daftar 1–10 kontak beserta vCard atau nomor telepon masing-masing.
func ValidateContacts(contacts []Contact) error {
	if len(contacts) < 1 || len(contacts) > 10 {
		return fmt.Errorf("%w: jumlah kontak harus 1 sampai 10", ErrInvalidSpec)
	}
	for idx, c := range contacts {
		if strings.TrimSpace(c.Name) == "" {
			return fmt.Errorf("%w: contacts[%d].name wajib diisi", ErrInvalidSpec, idx)
		}
		if strings.TrimSpace(c.VCard) != "" {
			continue
		}
		if len(c.Phones) == 0 {
			return fmt.Errorf("%w: contacts[%d] wajib mengisi vcard atau minimal satu nomor telepon", ErrInvalidSpec, idx)
		}
		for _, p := range c.Phones {
			digits := jid.DigitsOnly(p)
			if len(digits) < 7 || len(digits) > 16 {
				return fmt.Errorf("%w: contacts[%d] memiliki nomor telepon tidak valid", ErrInvalidSpec, idx)
			}
		}
	}
	return nil
}

// ValidatePoll memvalidasi judul polling, keunikan serta panjang 2–12 opsi, dan nilai selectable.
func ValidatePoll(p *PollSpec) error {
	if p == nil {
		return fmt.Errorf("%w: objek poll wajib diisi", ErrInvalidSpec)
	}
	name := strings.TrimSpace(p.Name)
	if name == "" {
		return fmt.Errorf("%w: nama polling wajib diisi", ErrInvalidSpec)
	}
	if utf8.RuneCountInString(name) > 255 {
		return fmt.Errorf("%w: nama polling maksimal 255 karakter", ErrInvalidSpec)
	}
	if len(p.Options) < 2 || len(p.Options) > 12 {
		return fmt.Errorf("%w: opsi polling harus berjumlah 2 sampai 12", ErrInvalidSpec)
	}
	seen := make(map[string]struct{}, len(p.Options))
	for idx, opt := range p.Options {
		clean := strings.TrimSpace(opt)
		if clean == "" {
			return fmt.Errorf("%w: opsi polling ke-%d tidak boleh kosong", ErrInvalidSpec, idx)
		}
		if utf8.RuneCountInString(clean) > 100 {
			return fmt.Errorf("%w: opsi polling ke-%d maksimal 100 karakter", ErrInvalidSpec, idx)
		}
		if _, dup := seen[clean]; dup {
			return fmt.Errorf("%w: opsi polling %q duplikat", ErrInvalidSpec, clean)
		}
		seen[clean] = struct{}{}
	}
	if p.Selectable < 1 || p.Selectable > len(p.Options) {
		return fmt.Errorf("%w: selectable harus berada di antara 1 dan %d", ErrInvalidSpec, len(p.Options))
	}
	return nil
}

// ValidateReaction memastikan target message_id reaksi tersedia dan sender (bila diisi) valid.
func ValidateReaction(rx *Reaction) error {
	if rx == nil {
		return fmt.Errorf("%w: objek reaction wajib diisi", ErrInvalidSpec)
	}
	if strings.TrimSpace(rx.MessageID) == "" {
		return fmt.Errorf("%w: reaction.message_id wajib diisi", ErrInvalidSpec)
	}
	if strings.TrimSpace(rx.Sender) != "" {
		if _, err := jid.ParseRecipient(rx.Sender); err != nil {
			return fmt.Errorf("%w: reaction.sender tidak valid", ErrInvalidSpec)
		}
	}
	return nil
}

// ValidateEdit memastikan target message_id edit tersedia dan teks revisi memenuhi batas panjang.
func ValidateEdit(ed *Edit) error {
	if ed == nil {
		return fmt.Errorf("%w: objek edit wajib diisi", ErrInvalidSpec)
	}
	if strings.TrimSpace(ed.MessageID) == "" {
		return fmt.Errorf("%w: edit.message_id wajib diisi", ErrInvalidSpec)
	}
	return ValidateTextBody(ed.Text)
}

// ValidateRevoke memastikan target message_id revoke tersedia dan sender (bila diisi) valid.
func ValidateRevoke(rv *Revoke) error {
	if rv == nil {
		return fmt.Errorf("%w: objek revoke wajib diisi", ErrInvalidSpec)
	}
	if strings.TrimSpace(rv.MessageID) == "" {
		return fmt.Errorf("%w: revoke.message_id wajib diisi", ErrInvalidSpec)
	}
	if strings.TrimSpace(rv.Sender) != "" {
		if _, err := jid.ParseRecipient(rv.Sender); err != nil {
			return fmt.Errorf("%w: revoke.sender tidak valid", ErrInvalidSpec)
		}
	}
	return nil
}
