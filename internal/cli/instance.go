package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/store"
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

type stringSliceFlag []string

func (f *stringSliceFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringSliceFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

// parseEditSetFlags mengonversi argumen --set key=value menjadi patch JSON bertipe untuk InstanceSettings.
func parseEditSetFlags(items []string) (json.RawMessage, error) {
	patch := make(map[string]any, len(items))
	for _, raw := range items {
		k, v, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("format --set harus key=value, didapat %q", raw)
		}
		key := strings.TrimSpace(k)
		if key == "" {
			return nil, fmt.Errorf("nama field pada --set %q tidak boleh kosong", raw)
		}
		kind, known := store.SettingsFieldKind(key)
		if !known {
			return nil, fmt.Errorf("field settings tidak dikenal: %q", key)
		}

		trimmed := strings.TrimSpace(v)
		switch kind {
		case "bool":
			b, err := strconv.ParseBool(trimmed)
			if err != nil {
				return nil, fmt.Errorf("nilai %s harus boolean (true/false), didapat %q", key, v)
			}
			patch[key] = b
		case "int":
			n, err := strconv.ParseInt(trimmed, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("nilai %s harus bilangan bulat, didapat %q", key, v)
			}
			patch[key] = n
		case "duration":
			d, err := time.ParseDuration(trimmed)
			if err != nil || d <= 0 {
				return nil, fmt.Errorf("nilai %s harus durasi positif valid (mis. 120s, 24h), didapat %q", key, v)
			}
			patch[key] = trimmed
		case "string":
			patch[key] = v
		case "list":
			if trimmed == "" {
				patch[key] = []string{}
				continue
			}
			parts := strings.Split(v, ",")
			list := make([]string, 0, len(parts))
			for _, p := range parts {
				if item := strings.TrimSpace(p); item != "" {
					list = append(list, item)
				}
			}
			patch[key] = list
		}
	}

	b, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("encode patch settings: %w", err)
	}
	return json.RawMessage(b), nil
}

func runInstanceEdit(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi edit <id> [--rate N] [--webhook URL] [--events CSV] [--autostart true|false] [--rotate-secret] [--set key=value]")
	}
	id := args[0]
	fs := flag.NewFlagSet("edit", flag.ContinueOnError)
	rate := fs.Int("rate", 0, "batas kirim per menit")
	webhookURL := fs.String("webhook", "", "url webhook (isi \"\" untuk menghapus)")
	eventsCSV := fs.String("events", "", "daftar event dipisah koma")
	autoStart := fs.String("autostart", "", "true atau false")
	rotateSecret := fs.Bool("rotate-secret", false, "putar ulang secret HMAC webhook")
	var setList stringSliceFlag
	fs.Var(&setList, "set", "ubah settings instance dalam format key=value (boleh berulang)")
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
	if len(setList) > 0 {
		rawSettings, err := parseEditSetFlags(setList)
		if err != nil {
			return err
		}
		payload["settings"] = rawSettings
	}
	if len(payload) == 0 {
		return errors.New("tidak ada perubahan yang diberikan (gunakan --rate, --webhook, --events, --autostart, --rotate-secret, atau --set)")
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
	if nonDef := nonDefaultSettings(st.Settings); len(nonDef) > 0 {
		fmt.Println("Settings        (non-default)")
		for _, kv := range nonDef {
			fmt.Printf("  %-22s %s\n", kv[0], kv[1])
		}
	}
}

func nonDefaultSettings(s store.InstanceSettings) [][2]string {
	if reflect.ValueOf(s).IsZero() {
		return nil
	}
	defMap, err := settingsToMap(store.Defaults())
	if err != nil {
		return nil
	}
	curMap, err := settingsToMap(s)
	if err != nil {
		return nil
	}
	keys := make([]string, 0, len(curMap))
	for k := range curMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var diff [][2]string
	for _, k := range keys {
		curVal := curMap[k]
		defVal := defMap[k]
		if reflect.DeepEqual(curVal, defVal) {
			continue
		}
		diff = append(diff, [2]string{k, formatSettingValue(curVal)})
	}
	return diff
}

func settingsToMap(s store.InstanceSettings) (map[string]any, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func formatSettingValue(v any) string {
	switch t := v.(type) {
	case []any:
		parts := make([]string, 0, len(t))
		for _, item := range t {
			parts = append(parts, fmt.Sprint(item))
		}
		return strings.Join(parts, ",")
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

