package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/control"
)

const (
	githubRepo       = "fikfikk-org/waapi"
	maxChecksumsSize = 1 << 20 // 1 MiB
	maxSigSize       = 4 << 10 // 4 KiB
)

var resolveBinaryPathFn = resolveBinaryPath

type githubRelease struct {
	TagName     string `json:"tag_name"`
	Name        string `json:"name"`
	PublishedAt string `json:"published_at"`
	Prerelease  bool   `json:"prerelease"`
}

// runUpdateCommand menangani waapi update [--check|--list|--force|--insecure-no-verify] [latest|vX.Y.Z].
func runUpdateCommand(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	checkOnly := fs.Bool("check", false, "hanya periksa apakah ada versi terbaru")
	listOnly := fs.Bool("list", false, "tampilkan daftar versi di GitHub & backup lokal")
	force := fs.Bool("force", false, "paksa pasang ulang walau versi sudah sama")
	insecureNoVerify := fs.Bool("insecure-no-verify", false, "lewati verifikasi tanda tangan Ed25519 (khusus rilis v1 tanpa .sig)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *listOnly {
		return runListVersions(ctx, cfg)
	}
	if *checkOnly {
		return runCheckUpdate(ctx)
	}

	target := "latest"
	if fs.NArg() > 0 {
		target = strings.TrimSpace(fs.Arg(0))
	}
	if target == "" || strings.EqualFold(target, "latest") {
		rel, err := fetchLatestRelease(ctx)
		if err != nil {
			return err
		}
		target = rel.TagName
	}
	if !strings.HasPrefix(target, "v") {
		target = "v" + target
	}

	currentTag := "v" + strings.TrimPrefix(Version, "v")
	if target == currentTag && !*force {
		fmt.Printf("✔ waapi sudah berada di versi %s (gunakan --force untuk pasang ulang).\n", currentTag)
		return nil
	}

	return installReleaseVersionWithOptions(ctx, cfg, target, *insecureNoVerify)
}

// runCheckUpdate memeriksa versi terbaru dari GitHub Releases dan membandingkannya dengan binary saat ini.
func runCheckUpdate(ctx context.Context) error {
	currentTag := "v" + strings.TrimPrefix(Version, "v")
	fmt.Printf("Versi terpasang : %s\n", currentTag)

	rel, err := fetchLatestRelease(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Versi terbaru   : %s (rilis: %s)\n", rel.TagName, rel.PublishedAt)

	if rel.TagName == currentTag {
		fmt.Println("✔ Anda sudah menggunakan versi terbaru.")
	} else {
		fmt.Printf("⬆ Update tersedia! Jalankan: sudo waapi update (atau sudo waapi update %s)\n", rel.TagName)
	}
	return nil
}

// runListVersions menampilkan daftar rilis di GitHub dan daftar backup lokal yang siap di-rollback.
func runListVersions(ctx context.Context, cfg *config.Config) error {
	currentTag := "v" + strings.TrimPrefix(Version, "v")
	fmt.Printf("Versi aktif saat ini: %s\n\n", currentTag)

	backups := listLocalBackups(cfg)
	fmt.Println("Backup lokal (siap rollback instan tanpa download):")
	if len(backups) == 0 {
		fmt.Println("  (belum ada backup lokal)")
	} else {
		for _, b := range backups {
			fmt.Printf("  - %s\n", b)
		}
	}

	fmt.Println("\nRilis tersedia di GitHub:")
	rels, err := fetchReleases(ctx, 10)
	if err != nil {
		fmt.Printf("  (gagal mengambil daftar rilis GitHub: %v)\n", err)
		return nil
	}
	if len(rels) == 0 {
		fmt.Println("  (belum ada rilis di GitHub)")
		return nil
	}
	for _, r := range rels {
		mark := " "
		if r.TagName == currentTag {
			mark = "*"
		}
		fmt.Printf(" %s %-10s  %s\n", mark, r.TagName, r.PublishedAt)
	}
	return nil
}

// runRollbackCommand mengembalikan binary ke versi sebelumnya atau ke versi spesifik.
func runRollbackCommand(ctx context.Context, cfg *config.Config, args []string) error {
	backupDir := filepath.Join(cfg.DataDir, "backups")
	var targetTag string
	if len(args) > 0 {
		targetTag = strings.TrimSpace(args[0])
		if targetTag != "" && !strings.HasPrefix(targetTag, "v") {
			targetTag = "v" + targetTag
		}
	}

	exePath, err := resolveBinaryPathFn()
	if err != nil {
		return err
	}

	// Jika tanpa argumen, cari backup lokal terbaru
	if targetTag == "" {
		backups := listLocalBackups(cfg)
		if len(backups) == 0 {
			return errors.New("belum ada backup lokal. Sebutkan versi tujuan, misal: sudo waapi rollback v1.0.0")
		}
		targetTag = backups[0]
	}

	localBackup := filepath.Join(backupDir, "waapi-"+targetTag)
	if st, err := os.Stat(localBackup); err == nil && !st.IsDir() {
		fmt.Printf("Mengembalikan binary dari backup lokal %s...\n", localBackup)
		if err := backupCurrentBinary(cfg, exePath); err != nil {
			return err
		}
		if err := replaceBinaryFile(localBackup, exePath); err != nil {
			return err
		}
		restartSystemdIfActive(ctx)
		fmt.Printf("✔ Rollback ke %s selesai.\n", targetTag)
		return nil
	}

	// Jika tidak ada di backup lokal, unduh versi tersebut dari GitHub Releases
	fmt.Printf("Backup lokal %s tidak ditemukan, mengunduh dari GitHub Releases...\n", targetTag)
	return installReleaseVersion(ctx, cfg, targetTag)
}

// installReleaseVersion mengunduh binary versi targetTag secara fail-closed dengan verifikasi tanda tangan Ed25519.
func installReleaseVersion(ctx context.Context, cfg *config.Config, targetTag string) error {
	return installReleaseVersionWithOptions(ctx, cfg, targetTag, false)
}

func updateBaseURL(cfg *config.Config) string {
	if envURL := strings.TrimRight(strings.TrimSpace(os.Getenv("WAAPI_UPDATE_BASE_URL")), "/"); envURL != "" {
		return envURL
	}
	if cfg != nil && strings.TrimSpace(cfg.UpdateBaseURL) != "" {
		return strings.TrimRight(strings.TrimSpace(cfg.UpdateBaseURL), "/")
	}
	return fmt.Sprintf("https://github.com/%s/releases/download", githubRepo)
}

func installReleaseVersionWithOptions(ctx context.Context, cfg *config.Config, targetTag string, insecureNoVerify bool) error {
	assetName, err := releaseAssetName()
	if err != nil {
		return err
	}
	exePath, err := resolveBinaryPathFn()
	if err != nil {
		return err
	}

	baseURL := updateBaseURL(cfg) + "/" + targetTag
	binURL := baseURL + "/" + assetName
	sumURL := baseURL + "/checksums.txt"
	sigURL := baseURL + "/checksums.txt.sig"

	dlCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	// 1. Unduh dan verifikasi tanda tangan checksums.txt secara fail-closed sebelum menyentuh binary.
	sumBytes, sumErr := downloadBytesLimit(dlCtx, sumURL, maxChecksumsSize)
	if !insecureNoVerify {
		if sumErr != nil {
			return fmt.Errorf("unduh checksums.txt gagal (fail-closed): %w", sumErr)
		}
		sigBytes, sigErr := downloadBytesLimit(dlCtx, sigURL, maxSigSize)
		if sigErr != nil {
			return fmt.Errorf("unduh checksums.txt.sig gagal (fail-closed): %w", sigErr)
		}
		if err := verifyReleaseSignature(sumBytes, string(sigBytes), releasePublicKeys); err != nil {
			return err
		}
		fmt.Println("✔ Tanda tangan Ed25519 checksums.txt terverifikasi")
	} else {
		fmt.Fprintln(os.Stderr, "PERINGATAN: --insecure-no-verify aktif; verifikasi tanda tangan Ed25519 dilewati.")
	}

	var expectedHash string
	if sumErr == nil && len(sumBytes) > 0 {
		expectedHash = parseChecksumFromBytes(sumBytes, assetName)
	}
	if !insecureNoVerify && expectedHash == "" {
		return fmt.Errorf("checksum SHA-256 untuk aset %s tidak ditemukan di checksums.txt", assetName)
	}

	fmt.Printf("Mengunduh %s (%s)...\n", targetTag, assetName)
	tmpFile, err := os.CreateTemp(filepath.Dir(exePath), ".waapi-update-*")
	if err != nil {
		return fmt.Errorf("gagal membuat file sementara di %s (jalankan dengan sudo): %w", filepath.Dir(exePath), err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}()

	hasher := sha256.New()
	if err := downloadToWriter(dlCtx, binURL, io.MultiWriter(tmpFile, hasher)); err != nil {
		return fmt.Errorf("unduh binary %s gagal: %w", targetTag, err)
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	gotHash := hex.EncodeToString(hasher.Sum(nil))

	if expectedHash != "" {
		if !strings.EqualFold(gotHash, expectedHash) {
			return fmt.Errorf("checksum SHA-256 tidak cocok! diharapkan %s, didapat %s", expectedHash, gotHash)
		}
		fmt.Printf("✔ Checksum SHA-256 terverifikasi (%s)\n", gotHash[:min(12, len(gotHash))])
	}

	// 2. Jika daemon aktif, backup ketiga database (app, journal, wa) lewat control plane tanpa membuka DB langsung.
	if err := backupDatabaseViaControl(ctx, cfg); err != nil {
		return fmt.Errorf("backup database sebelum update gagal: %w", err)
	}

	if err := os.Chmod(tmpPath, 0o755); err != nil {
		return err
	}

	if err := backupCurrentBinary(cfg, exePath); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, exePath); err != nil {
		return fmt.Errorf("ganti binary %s: %w", exePath, err)
	}

	restartSystemdIfActive(ctx)
	fmt.Printf("✔ Berhasil memperbarui waapi ke %s.\n", targetTag)
	return nil
}

func backupDatabaseViaControl(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	sock := cfg.ControlSocket()
	if _, err := os.Stat(sock); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	bCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var res control.BackupResult
	err := control.NewClient(sock).DoJSON(bCtx, http.MethodPost, "/control/backup", nil, &res)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			// Daemon tidak berjalan: lewati backup DB (CLI tidak boleh membuka DB langsung).
			return nil
		}
		return err
	}
	if res.Dir != "" {
		fmt.Printf("✔ Backup database disimpan ke %s (%s)\n", res.Dir, strings.Join(res.Files, ", "))
	}
	return nil
}

func downloadBytesLimit(ctx context.Context, url string, maxBytes int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "waapi-updater")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d untuk %s", resp.StatusCode, url)
	}
	limited := io.LimitReader(resp.Body, maxBytes+1)
	b, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxBytes {
		return nil, fmt.Errorf("ukuran respons %s melebihi batas %d byte", url, maxBytes)
	}
	return b, nil
}

func parseChecksumFromBytes(raw []byte, assetName string) string {
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && strings.TrimPrefix(fields[1], "*") == assetName {
			return fields[0]
		}
	}
	return ""
}

// backupCurrentBinary menyalin binary yang sedang terpasang ke direktori backups sebelum ditimpa.
func backupCurrentBinary(cfg *config.Config, exePath string) error {
	st, err := os.Stat(exePath)
	if err != nil || st.IsDir() {
		return nil
	}
	backupDir := filepath.Join(cfg.DataDir, "backups")
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		return fmt.Errorf("buat folder backup: %w", err)
	}
	currentTag := "v" + strings.TrimPrefix(Version, "v")
	dstPath := filepath.Join(backupDir, "waapi-"+currentTag)

	if err := copyFile(exePath, dstPath, 0o755); err != nil {
		return fmt.Errorf("backup binary lama ke %s: %w", dstPath, err)
	}
	fmt.Printf("✔ Binary lama (%s) disimpan ke %s\n", currentTag, dstPath)
	return nil
}

func replaceBinaryFile(src, dst string) error {
	tmp := dst + ".tmp"
	if err := copyFile(src, tmp, 0o755); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func listLocalBackups(cfg *config.Config) []string {
	backupDir := filepath.Join(cfg.DataDir, "backups")
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return nil
	}
	var tags []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "waapi-v") {
			continue
		}
		tags = append(tags, strings.TrimPrefix(e.Name(), "waapi-"))
	}
	sort.Sort(sort.Reverse(sort.StringSlice(tags)))
	return tags
}

func resolveBinaryPath() (string, error) {
	if _, err := os.Stat("/usr/local/bin/waapi"); err == nil {
		return "/usr/local/bin/waapi", nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("tentukan lokasi binary: %w", err)
	}
	return filepath.EvalSymlinks(exe)
}

func releaseAssetName() (string, error) {
	switch runtime.GOARCH {
	case "amd64", "arm64":
		return "waapi-linux-" + runtime.GOARCH, nil
	default:
		return "", fmt.Errorf("arsitektur %s belum didukung untuk binary prebuilt", runtime.GOARCH)
	}
}

func fetchLatestRelease(ctx context.Context) (*githubRelease, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", githubRepo)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "waapi-updater")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hubungi GitHub API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.New("belum ada GitHub Release yang dipublikasikan di repository")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("GitHub API mengembalikan HTTP %d", resp.StatusCode)
	}

	var rel githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return nil, err
	}
	return &rel, nil
}

func fetchReleases(ctx context.Context, limit int) ([]githubRelease, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	url := fmt.Sprintf("https://api.github.com/repos/%s/releases?per_page=%d", githubRepo, limit)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "waapi-updater")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var out []githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func downloadToWriter(ctx context.Context, url string, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "waapi-updater")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server mengembalikan HTTP %d untuk %s", resp.StatusCode, url)
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

func restartSystemdIfActive(ctx context.Context) {
	if runtime.GOOS != "linux" {
		return
	}
	if err := exec.CommandContext(ctx, "systemctl", "is-active", "--quiet", "waapi").Run(); err == nil {
		fmt.Println("Me-restart service systemd waapi...")
		_ = exec.CommandContext(ctx, "systemctl", "restart", "waapi").Run()
	}
}
