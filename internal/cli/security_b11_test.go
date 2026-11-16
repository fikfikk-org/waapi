package cli

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/store"
)

// TestVerifyRelease memverifikasi alur fail-closed waapi update (tanda tangan Ed25519 valid, checksum diubah, .sig hilang, dan --insecure-no-verify).
func TestVerifyRelease(t *testing.T) {
	assetName, err := releaseAssetName()
	if err != nil {
		t.Skipf("arsitektur host %s tidak punya release asset standar", assetName)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	origKeys := releasePublicKeys
	releasePublicKeys = []string{base64.StdEncoding.EncodeToString(pub)}
	t.Cleanup(func() { releasePublicKeys = origKeys })

	payload := []byte("waapi-binary-v2.0.0-signed-payload")
	sum := sha256.Sum256(payload)
	checksumsText := fmt.Sprintf("%s  %s\n", hex.EncodeToString(sum[:]), assetName)
	sigB64 := base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(checksumsText)))

	var mode string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/"+assetName):
			_, _ = w.Write(payload)
		case strings.HasSuffix(r.URL.Path, "/checksums.txt"):
			if mode == "tampered_checksum" {
				_, _ = w.Write([]byte("deadbeef  " + assetName + "\n"))
				return
			}
			_, _ = w.Write([]byte(checksumsText))
		case strings.HasSuffix(r.URL.Path, "/checksums.txt.sig"):
			if mode == "missing_sig" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(sigB64 + "\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	t.Setenv("WAAPI_UPDATE_BASE_URL", ts.URL)

	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	targetBin := filepath.Join(tmp, "waapi-bin")
	if err := os.WriteFile(targetBin, []byte("old-binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	origResolve := resolveBinaryPathFn
	resolveBinaryPathFn = func() (string, error) { return targetBin, nil }
	t.Cleanup(func() { resolveBinaryPathFn = origResolve })

	ctx := context.Background()

	// 1. Tanda tangan & checksum sah -> update sukses
	mode = "valid"
	if err := installReleaseVersionWithOptions(ctx, cfg, "v2.0.0", false); err != nil {
		t.Fatalf("update dengan tanda tangan valid seharusnya lulus, didapat %v", err)
	}
	gotBin, _ := os.ReadFile(targetBin)
	if string(gotBin) != string(payload) {
		t.Fatalf("binary tidak diperbarui dengan payload baru")
	}

	// Kembalikan binary lama untuk uji kegagalan fail-closed
	_ = os.WriteFile(targetBin, []byte("old-binary"), 0o755)

	// 2. Checksums diubah (tanda tangan tidak cocok) -> gagal fail-closed
	mode = "tampered_checksum"
	if err := installReleaseVersionWithOptions(ctx, cfg, "v2.0.0", false); err == nil {
		t.Fatal("update dengan checksum diubah seharusnya gagal fail-closed")
	}
	gotBin, _ = os.ReadFile(targetBin)
	if string(gotBin) != "old-binary" {
		t.Fatal("binary lama tidak boleh berubah saat verifikasi tanda tangan gagal")
	}

	// 3. File .sig hilang -> gagal fail-closed
	mode = "missing_sig"
	if err := installReleaseVersionWithOptions(ctx, cfg, "v2.0.0", false); err == nil {
		t.Fatal("update tanpa .sig seharusnya gagal fail-closed")
	}

	// 4. File .sig hilang namun --insecure-no-verify aktif -> lulus dengan peringatan
	if err := installReleaseVersionWithOptions(ctx, cfg, "v2.0.0", true); err != nil {
		t.Fatalf("--insecure-no-verify seharusnya meloloskan update meski tanpa .sig, didapat %v", err)
	}
	gotBin, _ = os.ReadFile(targetBin)
	if string(gotBin) != string(payload) {
		t.Fatal("binary seharusnya diperbarui saat --insecure-no-verify")
	}
}

type fakeSecControl struct {
	lastMethod string
	lastPath   string
	lastBody   any
	keyResp    struct {
		Key  string       `json:"key"`
		Meta store.APIKey `json:"meta"`
	}
	auditResp []auth.AuditEntry
}

func (f *fakeSecControl) DoJSON(_ context.Context, method, path string, in, out any) error {
	f.lastMethod = method
	f.lastPath = path
	f.lastBody = in
	switch p := out.(type) {
	case *store.APIKey:
		*p = f.keyResp.Meta
	case *[]auth.AuditEntry:
		*p = f.auditResp
	default:
		if ptr, ok := out.(*struct {
			Key  string       `json:"key"`
			Meta store.APIKey `json:"meta"`
		}); ok {
			*ptr = f.keyResp
		}
	}
	return nil
}

// TestCLIKeyDanAudit memastikan perintah CLI key v2 (--scopes/--expires/--rate/--policy, rotate, show) dan waapi audit memanggil endpoint kontrol tanpa membuka DB langsung.
func TestCLIKeyDanAudit(t *testing.T) {
	tmp := t.TempDir()
	polPath := filepath.Join(tmp, "policy.json")
	if err := os.WriteFile(polPath, []byte(`{"first_contact":"deny","max_sends_per_hour":10}`), 0o600); err != nil {
		t.Fatal(err)
	}

	fc := &fakeSecControl{}
	fc.keyResp.Key = "wak_newtestkey"
	fc.keyResp.Meta = store.APIKey{
		ID:         "k_123456",
		Name:       "bot",
		Prefix:     "wak_newt",
		Scopes:     "instances:read,messages:send",
		RatePerSec: 12,
		CreatedAt:  1700000000,
	}
	fc.auditResp = []auth.AuditEntry{
		{ID: 1, At: 1700000000, Actor: "control:uid=1000", Action: "key.create", Target: "k_123456"},
	}

	ctx := context.Background()
	out, err := captureStdout(t, func() error {
		return executeKeyAdd(ctx, fc, []string{
			"--name", "bot",
			"--instance", "sales",
			"--scopes", "instances:read,messages:send",
			"--expires", "24h",
			"--rate", "12",
			"--policy", polPath,
		})
	})
	if err != nil {
		t.Fatalf("executeKeyAdd gagal: %v", err)
	}
	if fc.lastMethod != http.MethodPost || fc.lastPath != "/control/keys" || !strings.Contains(out, "instances:read,messages:send") {
		t.Fatalf("executeKeyAdd tidak sesuai: method=%s path=%s out=%s", fc.lastMethod, fc.lastPath, out)
	}

	if err := executeKeyRotate(ctx, fc, []string{"k_123456", "--grace", "12h"}); err != nil {
		t.Fatalf("executeKeyRotate gagal: %v", err)
	}
	if fc.lastMethod != http.MethodPost || fc.lastPath != "/control/keys/k_123456/rotate" {
		t.Fatalf("executeKeyRotate path salah: %s %s", fc.lastMethod, fc.lastPath)
	}

	if err := executeKeyShow(ctx, fc, []string{"k_123456"}); err != nil {
		t.Fatalf("executeKeyShow gagal: %v", err)
	}
	if fc.lastMethod != http.MethodGet || fc.lastPath != "/control/keys/k_123456" {
		t.Fatalf("executeKeyShow path salah: %s %s", fc.lastMethod, fc.lastPath)
	}

	if err := executeAudit(ctx, fc, []string{"--since", "24h", "--action", "key.*"}); err != nil {
		t.Fatalf("executeAudit gagal: %v", err)
	}
	if fc.lastMethod != http.MethodGet || !strings.HasPrefix(fc.lastPath, "/control/audit") {
		t.Fatalf("executeAudit path salah: %s %s", fc.lastMethod, fc.lastPath)
	}
}
