package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/fikfikk-org/waapi/internal/control"
)

type sendCLIOptions struct {
	id     string
	to     string
	text   string
	wait   bool
	idem   string
	typing bool
	asJSON bool
}

type cliSendPayload struct {
	To      string `json:"to"`
	Text    string `json:"text"`
	Typing  bool   `json:"typing,omitempty"`
	Wait    string `json:"wait,omitempty"`
	IdemKey string `json:"idem_key,omitempty"`
}

type cliSendResult struct {
	ID          string `json:"id"`
	WAMessageID string `json:"wa_message_id"`
	Status      string `json:"status"`
	To          string `json:"to"`
	NotBefore   int64  `json:"not_before"`
	SentAt      int64  `json:"sent_at,omitempty"`
}

func runSend(ctx context.Context, ctrl *control.Client, args []string) error {
	opts, err := parseSendArgs(args)
	if err != nil {
		return err
	}

	waitMode := "none"
	if opts.wait {
		waitMode = "sent"
	}

	var resp cliSendResult
	path := "/control/instances/" + url.PathEscape(opts.id) + "/messages"
	if err := ctrl.DoJSON(ctx, http.MethodPost, path, cliSendPayload{
		To:      opts.to,
		Text:    opts.text,
		Typing:  opts.typing,
		Wait:    waitMode,
		IdemKey: opts.idem,
	}, &resp); err != nil {
		return err
	}

	if opts.asJSON {
		raw, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(raw))
		return nil
	}

	fmt.Printf("✔ Pesan [%s] -> %s\n", resp.Status, resp.To)
	fmt.Printf("  ID       : %s\n", resp.ID)
	fmt.Printf("  WA ID    : %s\n", resp.WAMessageID)
	if resp.SentAt > 0 {
		fmt.Printf("  Sent At  : %d\n", resp.SentAt)
	}
	return nil
}

func parseSendArgs(args []string) (sendCLIOptions, error) {
	var opts sendCLIOptions
	pos := make([]string, 0, 3)
	flagArgs := make([]string, 0, len(args))

	// Pisahkan argumen posisi <id> <to> <text> dari flag agar urutan flag fleksibel.
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") || a == "-" {
			if len(pos) < 3 {
				pos = append(pos, a)
				continue
			}
			flagArgs = append(flagArgs, a)
			continue
		}
		flagArgs = append(flagArgs, a)
		if !strings.Contains(a, "=") && sendFlagNeedsValue(a) && i+1 < len(args) {
			i++
			flagArgs = append(flagArgs, args[i])
		}
	}

	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.BoolVar(&opts.wait, "wait", false, "tunggu sampai pesan terkirim ke server WhatsApp")
	fs.StringVar(&opts.idem, "idem", "", "idempotency key opsional")
	fs.BoolVar(&opts.typing, "typing", false, "kirim indikator mengetik sebelum pesan")
	fs.BoolVar(&opts.asJSON, "json", false, "cetak respons dalam format JSON")

	if err := fs.Parse(flagArgs); err != nil {
		return opts, err
	}
	for _, rem := range fs.Args() {
		if len(pos) < 3 {
			pos = append(pos, rem)
		} else {
			return opts, fmt.Errorf("argumen berlebih: %q", rem)
		}
	}
	if len(pos) < 3 {
		return opts, errors.New("penggunaan: waapi send <id> <to> <text> [--wait] [--idem KEY] [--typing] [--json]")
	}
	opts.id = strings.TrimSpace(pos[0])
	opts.to = strings.TrimSpace(pos[1])
	opts.text = pos[2]
	if opts.id == "" || opts.to == "" || strings.TrimSpace(opts.text) == "" {
		return opts, errors.New("penggunaan: waapi send <id> <to> <text> [--wait] [--idem KEY] [--typing] [--json]")
	}
	return opts, nil
}

func sendFlagNeedsValue(arg string) bool {
	return strings.TrimLeft(arg, "-") == "idem"
}
