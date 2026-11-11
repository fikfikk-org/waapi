package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/wa"
)

func runInstanceSubcommand(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		printHelp()
		return nil
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list", "ls":
		return runInstanceList(ctx, ctrl)
	case "create", "add":
		return runInstanceCreate(ctx, ctrl, rest)
	case "status", "show":
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
	case "delete", "rm":
		return runInstanceDelete(ctx, ctrl, rest)
	default:
		return fmt.Errorf("subcommand instance tidak dikenal: %q", sub)
	}
}

func runInstanceList(ctx context.Context, ctrl *control.Client) error {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var list []wa.InstanceStatus
	if err := ctrl.DoJSON(reqCtx, http.MethodGet, "/control/instances", nil, &list); err != nil {
		return err
	}
	if len(list) == 0 {
		fmt.Println("Belum ada instance. Buat dengan: waapi add <id>")
		return nil
	}

	tw := newTabWriter(os.Stdout)
	fmt.Fprintln(tw, "ID\tSTATUS\tJID\tRATE/MIN\tQUEUE\tWEBHOOK")
	for _, it := range list {
		jid := it.JID
		if jid == "" {
			jid = "-"
		}
		wh := it.WebhookURL
		if wh == "" {
			wh = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\n",
			it.ID, it.Status, jid, it.RatePerMin, it.QueueDepth, wh)
	}
	return tw.Flush()
}

func runInstanceCreate(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi add <id> [--rate 20] [--no-autostart]")
	}
	id := args[0]
	fs := flag.NewFlagSet("add", flag.ContinueOnError)
	rate := fs.Int("rate", 20, "batas kirim pesan per menit")
	noAuto := fs.Bool("no-autostart", false, "jangan nyalakan otomatis saat daemon start")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var st wa.InstanceStatus
	err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/instances", map[string]any{
		"id":           id,
		"rate_per_min": *rate,
		"auto_start":   !*noAuto,
	}, &st)
	if err != nil {
		return err
	}
	fmt.Printf("✔ Instance %q berhasil dibuat (rate: %d/menit). Lanjutkan dengan: waapi pair %s\n", st.ID, st.RatePerMin, st.ID)
	return nil
}

func runInstanceStatus(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi show <id>")
	}
	id := args[0]
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var st wa.InstanceStatus
	if err := ctrl.DoJSON(reqCtx, http.MethodGet, "/control/instances/"+id, nil, &st); err != nil {
		return err
	}
	printInstanceDetail(st)
	return nil
}

func runInstanceEdit(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi edit <id> [--rate N] [--webhook URL] [--events CSV] [--autostart true|false] [--rotate-secret]")
	}
	id := args[0]
	fs := flag.NewFlagSet("edit", flag.ContinueOnError)
	rate := fs.Int("rate", 0, "batas kirim per menit")
	webhookURL := fs.String("webhook", "", "url webhook (isi \"\" untuk menghapus)")
	eventsCSV := fs.String("events", "", "daftar event dipisah koma")
	autoStart := fs.String("autostart", "", "true atau false")
	rotateSecret := fs.Bool("rotate-secret", false, "putar ulang secret HMAC webhook")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	setFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true
	})

	payload := map[string]any{}
	if setFlags["rate"] {
		if *rate <= 0 {
			return errors.New("--rate harus lebih besar dari 0")
		}
		payload["rate_per_min"] = *rate
	}
	if setFlags["webhook"] {
		payload["webhook_url"] = strings.TrimSpace(*webhookURL)
	}
	if setFlags["events"] {
		payload["webhook_events"] = strings.TrimSpace(*eventsCSV)
	}
	if setFlags["autostart"] {
		val := strings.EqualFold(*autoStart, "true") || *autoStart == "1"
		payload["auto_start"] = val
	}
	if *rotateSecret {
		payload["rotate_secret"] = true
	}
	if len(payload) == 0 {
		return errors.New("tidak ada perubahan yang diberikan (gunakan --rate, --webhook, --events, --autostart, atau --rotate-secret)")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var st wa.InstanceStatus
	if err := ctrl.DoJSON(reqCtx, http.MethodPatch, "/control/instances/"+id, payload, &st); err != nil {
		return err
	}
	fmt.Printf("✔ Instance %q berhasil diperbarui.\n\n", st.ID)
	printInstanceDetail(st)
	return nil
}

func runInstancePair(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi pair <id> [--phone 62812xxxx]")
	}
	id := args[0]
	fs := flag.NewFlagSet("pair", flag.ContinueOnError)
	phone := fs.String("phone", "", "nomor telepon untuk 8-digit pairing code")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	resp, err := ctrl.OpenPairStream(ctx, id, *phone)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if *phone == "" {
		fmt.Println("Menunggu QR... scan dari WhatsApp > Perangkat tertaut > Tautkan perangkat")
	} else {
		fmt.Printf("Meminta kode pairing 8 digit untuk nomor %s...\n", *phone)
	}

	lastQR := ""
	err = control.ReadPairStream(resp, func(snap wa.PairSnapshot) error {
		switch snap.State {
		case wa.PairQRRefreshed:
			if snap.QRRaw != nil && *snap.QRRaw != lastQR {
				lastQR = *snap.QRRaw
				fmt.Println()
				renderQR(*snap.QRRaw)
				fmt.Println("Menunggu scan QR... (tekan Ctrl+C untuk membatalkan)")
			}
		case wa.PairCodeGenerated:
			if snap.PairingCode != nil {
				fmt.Printf("\nMasukkan kode berikut di HP (Perangkat tertaut > Tautkan dengan nomor telepon):\n\n    %s\n\n", *snap.PairingCode)
			}
		case wa.PairConnected:
			jid := ""
			if snap.JID != nil {
				jid = *snap.JID
			}
			fmt.Printf("✔ Terhubung sebagai %s\n", jid)
		case wa.PairTimeout:
			return errors.New("waktu pairing habis")
		case wa.PairError:
			msg := "pairing gagal"
			if snap.Error != nil {
				msg = *snap.Error
			}
			return errors.New(msg)
		}
		return nil
	})
	if ctx.Err() != nil {
		fmt.Println("\nPairing dibatalkan.")
		return nil
	}
	return err
}

func runInstanceRestart(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi restart <id>")
	}
	id := args[0]
	reqCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var st wa.InstanceStatus
	if err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/instances/"+id+"/restart", nil, &st); err != nil {
		return err
	}
	fmt.Printf("✔ Instance %q dimulai ulang (status: %s)\n", st.ID, st.Status)
	return nil
}

func runInstanceStop(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi stop <id>")
	}
	id := args[0]
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var st wa.InstanceStatus
	if err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/instances/"+id+"/stop", nil, &st); err != nil {
		return err
	}
	fmt.Printf("✔ Instance %q dihentikan (status: %s)\n", st.ID, st.Status)
	return nil
}

func runInstanceLogout(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi logout <id>")
	}
	id := args[0]
	reqCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var st wa.InstanceStatus
	if err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/instances/"+id+"/logout", nil, &st); err != nil {
		return err
	}
	fmt.Printf("✔ Instance %q berhasil logout dari WhatsApp\n", st.ID)
	return nil
}

func runInstanceDelete(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi rm <id> [--yes]")
	}
	id := args[0]
	fs := flag.NewFlagSet("rm", flag.ContinueOnError)
	yes := fs.Bool("yes", false, "lewati konfirmasi")
	force := fs.Bool("force", false, "alias --yes")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	if !*yes && !*force {
		fmt.Printf("Hapus instance %q beserta sesi WhatsApp dan API key terkait? [y/N]: ", id)
		sc := bufio.NewScanner(os.Stdin)
		if !sc.Scan() || !strings.EqualFold(strings.TrimSpace(sc.Text()), "y") {
			fmt.Println("Dibatalkan.")
			return nil
		}
	}

	reqCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	if err := ctrl.DoJSON(reqCtx, http.MethodDelete, "/control/instances/"+id, nil, nil); err != nil {
		return err
	}
	fmt.Printf("✔ Instance %q berhasil dihapus.\n", id)
	return nil
}

func printInstanceDetail(st wa.InstanceStatus) {
	jid := st.JID
	if jid == "" {
		jid = "-"
	}
	wh := st.WebhookURL
	if wh == "" {
		wh = "-"
	}
	fmt.Printf("ID              %s\n", st.ID)
	fmt.Printf("Status          %s\n", st.Status)
	fmt.Printf("JID             %s\n", jid)
	fmt.Printf("Push Name       %s\n", st.PushName)
	fmt.Printf("Rate Limit      %d pesan/menit\n", st.RatePerMin)
	fmt.Printf("Queue Depth     %d\n", st.QueueDepth)
	fmt.Printf("Auto Start      %v\n", st.AutoStart)
	fmt.Printf("Webhook URL     %s\n", wh)
	fmt.Printf("Webhook Events  %s\n", st.WebhookEvents)
	fmt.Printf("Connected Since %s\n", formatTime(st.ConnectedSince))
}

