// Program sign menghasilkan pasangan kunci Ed25519 rilis (-gen) dan menandatangani checksums.txt menggunakan seed Base64 dari environment variable.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "sign error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("sign", flag.ContinueOnError)
	gen := fs.Bool("gen", false, "buat pasangan kunci Ed25519 baru")
	keyEnv := fs.String("key-env", "WAAPI_RELEASE_ED25519_SEED", "nama environment variable berisi seed Ed25519 Base64")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *gen {
		seedB64, pubB64, err := generateKeyPair()
		if err != nil {
			return err
		}
		fmt.Printf("Simpan seed sebagai GitHub Secret %s (jangan commit ke git):\n", *keyEnv)
		fmt.Printf("WAAPI_RELEASE_ED25519_SEED=%s\n\n", seedB64)
		fmt.Println("Commit public key berikut ke internal/cli/release_keys.go:")
		fmt.Printf("WAAPI_RELEASE_ED25519_PUB=%s\n", pubB64)
		return nil
	}

	if fs.NArg() != 1 {
		return errors.New("penggunaan: go run ./tools/sign [-gen] [-key-env VAR] <file>")
	}
	seedB64 := strings.TrimSpace(os.Getenv(*keyEnv))
	if seedB64 == "" {
		return fmt.Errorf("environment variable %s kosong", *keyEnv)
	}
	sigB64, err := signFileWithSeedBase64(fs.Arg(0), seedB64)
	if err != nil {
		return err
	}
	fmt.Println(sigB64)
	return nil
}

func generateKeyPair() (seedB64, pubB64 string, err error) {
	var seed [ed25519.SeedSize]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return "", "", fmt.Errorf("buat seed Ed25519 acak: %w", err)
	}
	priv := ed25519.NewKeyFromSeed(seed[:])
	pub := priv.Public().(ed25519.PublicKey)
	return base64.StdEncoding.EncodeToString(seed[:]), base64.StdEncoding.EncodeToString(pub), nil
}

func signFileWithSeedBase64(path, seedB64 string) (string, error) {
	seed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(seedB64))
	if err != nil {
		return "", fmt.Errorf("decode seed Base64: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return "", fmt.Errorf("panjang seed Ed25519 harus %d byte, didapat %d byte", ed25519.SeedSize, len(seed))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("baca file %s: %w", path, err)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	sig := ed25519.Sign(priv, data)
	return base64.StdEncoding.EncodeToString(sig), nil
}
