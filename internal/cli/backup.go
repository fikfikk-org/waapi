package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
)

type backupCLIOutput struct {
	Dir    string   `json:"dir"`
	OutDir string   `json:"out_dir,omitempty"`
	Files  []string `json:"files"`
}

// runBackup mengeksekusi POST /control/backup (VACUUM INTO app.db, journal.db, wa.db) serta menyalin hasilnya ke --out bila diberikan.
func runBackup(ctx context.Context, ctrl *control.Client, args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	outDir := fs.String("out", "", "direktori tujuan untuk menyalin hasil snapshot SQLite (izin 0600)")
	asJSON := fs.Bool("json", false, "keluarkan hasil backup dalam format JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	var res control.BackupResult
	if err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/backup", nil, &res); err != nil {
		return err
	}

	out := backupCLIOutput{
		Dir:   res.Dir,
		Files: res.Files,
	}

	targetDir := strings.TrimSpace(*outDir)
	if targetDir != "" {
		absTarget, err := filepath.Abs(targetDir)
		if err != nil {
			return fmt.Errorf("resolusi direktori tujuan backup %q: %w", targetDir, err)
		}
		if err := os.MkdirAll(absTarget, 0o700); err != nil {
			return fmt.Errorf("buat direktori tujuan backup %s: %w", absTarget, err)
		}
		for _, name := range res.Files {
			src := filepath.Join(res.Dir, name)
			dst := filepath.Join(absTarget, name)
			if err := copyBackupFile(src, dst); err != nil {
				return err
			}
		}
		out.OutDir = targetDir
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	fmt.Printf("✔ Snapshot backup SQLite selesai (%d file):\n", len(out.Files))
	fmt.Printf("  Snapshot internal : %s\n", out.Dir)
	if out.OutDir != "" {
		fmt.Printf("  Disalin ke        : %s\n", out.OutDir)
	}
	fmt.Printf("  Berkas            : %s\n", strings.Join(out.Files, ", "))
	return nil
}

// copyBackupFile menyalin satu file backup secara atomik menggunakan temp file berizin 0600.
func copyBackupFile(srcPath, dstPath string) error {
	if strings.TrimSpace(srcPath) == "" || strings.TrimSpace(dstPath) == "" {
		return errors.New("path sumber dan tujuan backup wajib terisi")
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("buka sumber backup %s: %w", srcPath, err)
	}
	defer func() { _ = src.Close() }()

	tmpPath := dstPath + ".tmp"
	dst, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("buat file backup sementara %s: %w", tmpPath, err)
	}

	cleanup := func() {
		_ = dst.Close()
		_ = os.Remove(tmpPath)
	}

	if _, err := io.Copy(dst, src); err != nil {
		cleanup()
		return fmt.Errorf("salin backup ke %s: %w", dstPath, err)
	}
	if err := dst.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync backup %s: %w", dstPath, err)
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("tutup backup %s: %w", dstPath, err)
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("set izin 0600 pada %s: %w", dstPath, err)
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("pindahkan backup ke %s: %w", dstPath, err)
	}
	return nil
}
