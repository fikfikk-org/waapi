package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSignTool_GenDanSign memastikan utilitas tools/sign dapat membuat pasangan kunci Ed25519 base64 serta menandatangani checksums.txt secara sah.
func TestSignTool_GenDanSign(t *testing.T) {
	seedB64, pubB64, err := generateKeyPair()
	if err != nil {
		t.Fatalf("generateKeyPair gagal: %v", err)
	}

	sumPath := filepath.Join(t.TempDir(), "checksums.txt")
	msg := []byte("abc123  waapi-linux-amd64\n")
	if err := os.WriteFile(sumPath, msg, 0o600); err != nil {
		t.Fatal(err)
	}

	sigB64, err := signFileWithSeedBase64(sumPath, seedB64)
	if err != nil {
		t.Fatalf("signFileWithSeedBase64 gagal: %v", err)
	}

	pubBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(pubB64))
	if err != nil {
		t.Fatal(err)
	}
	sigBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(sigB64))
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(ed25519.PublicKey(pubBytes), msg, sigBytes) {
		t.Fatal("tanda tangan Ed25519 hasil tools/sign tidak valid terhadap public key-nya")
	}
}
