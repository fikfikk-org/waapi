package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/daemon"
)

// Version diisi saat build melalui -ldflags "-X github.com/fikfikk-org/waapi/internal/cli.Version=..."
var Version = "1.0.0"

// Execute mengeksekusi argumen baris perintah waapi.
func Execute(args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		printHelp()
		return nil
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := args[0]
	rest := args[1:]

	if cmd == "serve" {
		return daemon.Run(ctx, cfg)
	}
	if cmd == "version" || cmd == "-v" || cmd == "--version" {
		fmt.Printf("waapi v%s\n", Version)
		return nil
	}

	ctrl := control.NewClient(cfg.ControlSocket())

	switch cmd {
	case "status", "doctor":
		return runDoctor(ctx, ctrl)
	case "list", "ls":
		return runInstanceList(ctx, ctrl)
	case "add", "create":
		return runInstanceCreate(ctx, ctrl, rest)
	case "show", "info":
		return runInstanceStatus(ctx, ctrl, rest)
	case "edit":
		return runInstanceEdit(ctx, ctrl, rest)
	case "pair":
		return runInstancePair(ctx, ctrl, rest)
	case "restart":
		return runInstanceRestart(ctx, ctrl, rest)
	case "stop":
		return runInstanceStop(ctx, ctrl, rest)
	case "logout":
		return runInstanceLogout(ctx, ctrl, rest)
	case "rm", "delete":
		return runInstanceDelete(ctx, ctrl, rest)
	case "instance":
		return runInstanceSubcommand(ctx, ctrl, rest)
	case "key":
		return runKeySubcommand(ctx, ctrl, rest)
	case "webhook":
		return runWebhookSubcommand(ctx, ctrl, rest)
	case "logs":
		return runLogs(ctx, rest)
	case "update":
		return runUpdate(ctx)
	case "uninstall":
		return runUninstall(ctx, cfg, rest)
	default:
		return fmt.Errorf("perintah tidak dikenal: %q (jalankan 'waapi --help' untuk daftar perintah)", cmd)
	}
}

func isHelp(arg string) bool {
	return arg == "help" || arg == "-h" || arg == "--help"
}

func runLogs(ctx context.Context, args []string) error {
	jArgs := []string{"-u", "waapi", "-f", "-n", "100"}
	if len(args) > 0 && args[0] != "" {
		jArgs = append(jArgs, "-g", args[0])
	}
	c := exec.CommandContext(ctx, "journalctl", jArgs...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Stdin = os.Stdin
	return c.Run()
}

// runUpdate membangun ulang atau menjalankan installer lalu me-restart service systemd waapi.
func runUpdate(ctx context.Context) error {
	if _, err := os.Stat("deploy/install.sh"); err == nil {
		c := exec.CommandContext(ctx, "bash", "deploy/install.sh")
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		c.Stdin = os.Stdin
		return c.Run()
	}
	c := exec.CommandContext(ctx, "systemctl", "restart", "waapi")
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("restart service waapi gagal (jalankan dengan sudo): %w", err)
	}
	fmt.Println("✔ Service waapi berhasil dimulai ulang.")
	return nil
}

// runUninstall menghentikan service systemd, menghapus binary/unit, dan opsional menghapus data (--purge).
func runUninstall(ctx context.Context, cfg *config.Config, args []string) error {
	purge := false
	for _, a := range args {
		if a == "--purge" {
			purge = true
		}
	}
	_ = exec.CommandContext(ctx, "systemctl", "disable", "--now", "waapi").Run()
	_ = os.Remove("/etc/systemd/system/waapi.service")
	_ = os.Remove("/etc/profile.d/waapi.sh")
	_ = exec.CommandContext(ctx, "systemctl", "daemon-reload").Run()
	_ = os.Remove("/usr/local/bin/waapi")
	if purge {
		if err := os.RemoveAll(cfg.DataDir); err != nil {
			return fmt.Errorf("hapus data dir %s: %w", cfg.DataDir, err)
		}
		fmt.Printf("✔ waapi beserta seluruh data (%s) telah dihapus.\n", cfg.DataDir)
		return nil
	}
	fmt.Printf("✔ Service dan binary waapi dihapus (data tetap tersimpan di %s).\n", cfg.DataDir)
	return nil
}

func printHelp() {
	fmt.Print(`waapi — Private WhatsApp API Gateway (Headless, < 100 MB RAM)

PENGGUNAAN:
  waapi <perintah> [argumen]

PERINTAH UTAMA (INSTANCE):
  waapi list                                   Daftar semua instance WhatsApp
  waapi add <id> [--rate 20] [--no-autostart]  Buat instance baru
  waapi show <id>                              Detail status satu instance
  waapi edit <id> [opsi]                       Ubah rate limit / webhook / autostart instance
  waapi pair <id> [--phone 62812xxx]           Tautkan nomor via QR ASCII atau 8-digit kode
  waapi restart <id>                           Sambungkan ulang instance
  waapi stop <id>                              Hentikan koneksi instance dan matikan autostart
  waapi logout <id>                            Unlink perangkat dari WhatsApp
  waapi rm <id> [--yes]                        Logout dan hapus instance beserta datanya

MANAJEMEN API KEY:
  waapi key list                               Daftar seluruh API key
  waapi key add --name <nama> [--instance <id>] Buat API key baru (ditampilkan sekali)
  waapi key rm <key-id>                        Cabut (revoke) API key

MANAJEMEN WEBHOOK:
  waapi webhook set <id> <url> [--events ...]  Pasang URL webhook pada instance
  waapi webhook unset <id>                     Hapus URL webhook dari instance
  waapi webhook test <id>                      Kirim event uji coba ke webhook

DAEMON & DIAGNOSTIK:
  waapi status | waapi doctor                  Cek pemakaian RAM (RSS/Heap), DB, & kesehatan daemon
  waapi logs [<id>]                            Pantau log service systemd (journalctl)
  waapi update                                 Pasang ulang binary terbaru & restart service waapi
  waapi uninstall [--purge]                    Hapus service & binary (--purge untuk hapus data)
  waapi serve                                  Jalankan background daemon (dipakai oleh systemd)
  waapi version                                Tampilkan versi binary

OPSI EDIT INSTANCE (waapi edit <id>):
  --rate <n>              Batas kirim pesan per menit (contoh: --rate 30)
  --webhook <url>         URL tujuan webhook (kosongkan "" untuk menghapus)
  --events <csv>          Filter event: message,receipt,connection
  --autostart <bool>      true atau false
  --rotate-secret         Putar ulang secret HMAC webhook
`)
}

