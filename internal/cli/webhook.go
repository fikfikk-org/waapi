package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/wa"
)

func runWebhookSubcommand(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Println("Penggunaan: waapi webhook <set|unset|test> <instance-id> [url]")
		return nil
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "set":
		return runWebhookSet(ctx, ctrl, rest)
	case "unset", "rm":
		return runWebhookUnset(ctx, ctrl, rest)
	case "test":
		return runWebhookTest(ctx, ctrl, rest)
	default:
		return fmt.Errorf("subcommand webhook tidak dikenal: %q", sub)
	}
}

func runWebhookSet(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) < 2 {
		return errors.New("penggunaan: waapi webhook set <instance-id> <url> [--events message,receipt,connection]")
	}
	id := args[0]
	url := args[1]
	fs := flag.NewFlagSet("webhook set", flag.ContinueOnError)
	events := fs.String("events", "message,receipt,connection", "daftar event dipisah koma")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var st wa.InstanceStatus
	err := ctrl.DoJSON(reqCtx, http.MethodPatch, "/control/instances/"+id, map[string]any{
		"webhook_url":    url,
		"webhook_events": *events,
	}, &st)
	if err != nil {
		return err
	}
	fmt.Printf("✔ Webhook untuk instance %q diset ke %s (events: %s)\n", st.ID, st.WebhookURL, st.WebhookEvents)
	return nil
}

func runWebhookUnset(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi webhook unset <instance-id>")
	}
	id := args[0]
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	empty := ""
	var st wa.InstanceStatus
	err := ctrl.DoJSON(reqCtx, http.MethodPatch, "/control/instances/"+id, map[string]any{
		"webhook_url": empty,
	}, &st)
	if err != nil {
		return err
	}
	fmt.Printf("✔ Webhook untuk instance %q telah dihapus.\n", st.ID)
	return nil
}

func runWebhookTest(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi webhook test <instance-id>")
	}
	id := args[0]
	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var res struct {
		Status     string `json:"status"`
		HTTPStatus int    `json:"http_status"`
	}
	if err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/instances/"+id+"/webhook/test", nil, &res); err != nil {
		return err
	}
	fmt.Printf("✔ Webhook test terkirim (HTTP %d)\n", res.HTTPStatus)
	return nil
}

