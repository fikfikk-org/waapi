// Package jid memvalidasi dan menormalkan nomor telepon serta JID penerima WhatsApp tanpa dependensi internal.
package jid

import (
	"errors"
	"strings"
	"unicode"

	"go.mau.fi/whatsmeow/types"
)

// ErrInvalidRecipient dikembalikan jika format nomor tujuan atau JID penerima tidak valid.
var ErrInvalidRecipient = errors.New("nomor atau JID tujuan tidak valid")

// ParseRecipient mengubah string nomor telepon atau JID menjadi types.JID kanonik WhatsApp.
func ParseRecipient(raw string) (types.JID, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return types.JID{}, ErrInvalidRecipient
	}
	if strings.Contains(s, "@") {
		parsed, err := types.ParseJID(s)
		if err != nil || parsed.User == "" {
			return types.JID{}, ErrInvalidRecipient
		}
		return parsed, nil
	}
	digits := DigitsOnly(s)
	if len(digits) < 7 || len(digits) > 16 {
		return types.JID{}, ErrInvalidRecipient
	}
	return types.NewJID(digits, types.DefaultUserServer), nil
}

// DigitsOnly menyaring string sehingga hanya karakter angka Unicode yang dipertahankan.
func DigitsOnly(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}
