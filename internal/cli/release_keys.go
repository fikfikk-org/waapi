package cli

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// releasePublicKeys memuat maksimal 2 public key Ed25519 Base64 (mendukung rotasi kunci rilis).
// Dibiarkan kosong secara default (fail-closed) sampai maintainer menanamkan public key resmi hasil tools/sign -gen.
var releasePublicKeys = []string{}

func verifyReleaseSignature(payload []byte, sigText string, encodedKeys []string) error {
	if len(encodedKeys) == 0 {
		return errors.New("verifikasi rilis gagal: belum ada public key Ed25519 tertanam di binary (gunakan --insecure-no-verify hanya untuk rilis lama v1)")
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(sigText))
	if err != nil {
		return fmt.Errorf("decode tanda tangan Base64: %w", err)
	}
	if len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("ukuran tanda tangan Ed25519 tidak valid: %d byte", len(sig))
	}

	for _, rawPub := range encodedKeys {
		trimmed := strings.TrimSpace(rawPub)
		if trimmed == "" {
			continue
		}
		pubBytes, pErr := base64.StdEncoding.DecodeString(trimmed)
		if pErr != nil || len(pubBytes) != ed25519.PublicKeySize {
			continue
		}
		if ed25519.Verify(ed25519.PublicKey(pubBytes), payload, sig) {
			return nil
		}
	}
	return errors.New("tanda tangan Ed25519 checksums.txt tidak cocok dengan public key rilis mana pun")
}
