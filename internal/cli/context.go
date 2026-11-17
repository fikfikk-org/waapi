package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
)

type contextCaller interface {
	DoJSON(ctx context.Context, method, path string, in, out any) error
}

func runContext(ctx context.Context, ctrl *control.Client, args []string) error {
	return executeContext(ctx, ctrl, args)
}

func executeContext(ctx context.Context, ctrl contextCaller, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("penggunaan: waapi context <id> on|off [--purge]|purge [--chat jid]")
	}
	id := strings.TrimSpace(args[0])
	action := strings.ToLower(strings.TrimSpace(args[1]))
	if id == "" {
		return fmt.Errorf("id instance tidak boleh kosong")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var resp map[string]any
	switch action {
	case "on":
		fs := flag.NewFlagSet("context on", flag.ContinueOnError)
		asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}
		path := fmt.Sprintf("/control/instances/%s/context", url.PathEscape(id))
		if err := ctrl.DoJSON(reqCtx, http.MethodPost, path, map[string]any{"enabled": true}, &resp); err != nil {
			return err
		}
		if *asJSON {
			return encodeContextJSON(resp)
		}
		fmt.Printf("✔ Context store untuk %s diaktifkan.\n", id)
		return nil

	case "off":
		fs := flag.NewFlagSet("context off", flag.ContinueOnError)
		purge := fs.Bool("purge", false, "hapus seluruh pesan dan ringkasan chat instance")
		asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}
		path := fmt.Sprintf("/control/instances/%s/context", url.PathEscape(id))
		if err := ctrl.DoJSON(reqCtx, http.MethodPost, path, map[string]any{"enabled": false, "purge": *purge}, &resp); err != nil {
			return err
		}
		if *asJSON {
			return encodeContextJSON(resp)
		}
		if *purge {
			fmt.Printf("✔ Context store untuk %s dimatikan dan seluruh datanya dibersihkan.\n", id)
		} else {
			fmt.Printf("✔ Context store untuk %s dimatikan.\n", id)
		}
		return nil

	case "purge":
		fs := flag.NewFlagSet("context purge", flag.ContinueOnError)
		chat := fs.String("chat", "", "hapus hanya riwayat satu JID chat tertentu")
		asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}
		path := fmt.Sprintf("/control/instances/%s/context/purge", url.PathEscape(id))
		if err := ctrl.DoJSON(reqCtx, http.MethodPost, path, map[string]any{"chat": strings.TrimSpace(*chat)}, &resp); err != nil {
			return err
		}
		if *asJSON {
			return encodeContextJSON(resp)
		}
		if strings.TrimSpace(*chat) != "" {
			fmt.Printf("✔ Data context store chat %s (%s) dibersihkan.\n", strings.TrimSpace(*chat), id)
		} else {
			fmt.Printf("✔ Seluruh data context store instance %s dibersihkan.\n", id)
		}
		return nil

	default:
		return fmt.Errorf("aksi context tidak dikenal %q (gunakan: on, off, atau purge)", action)
	}
}

func encodeContextJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
