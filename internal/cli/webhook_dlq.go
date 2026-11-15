package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

func runWebhookDLQ(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) < 2 || isHelp(args[0]) {
		return errors.New("penggunaan: waapi webhook dlq <instance-id> <ls|replay|purge> [opsi]")
	}
	id := args[0]
	action := args[1]
	rest := args[2:]

	switch action {
	case "ls", "list":
		return runWebhookDLQList(ctx, ctrl, id, rest)
	case "replay":
		return runWebhookDLQReplay(ctx, ctrl, id, rest)
	case "purge":
		return runWebhookDLQPurge(ctx, ctrl, id)
	default:
		return fmt.Errorf("aksi DLQ tidak dikenal: %q (gunakan ls, replay, atau purge)", action)
	}
}

func runWebhookDLQList(ctx context.Context, ctrl *control.Client, id string, args []string) error {
	fs := flag.NewFlagSet("webhook dlq ls", flag.ContinueOnError)
	limit := fs.Int("limit", 100, "jumlah maksimum entri DLQ")
	if err := fs.Parse(args); err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	path := "/control/instances/" + url.PathEscape(id) + "/webhook/dlq?limit=" + strconv.Itoa(*limit)
	var items []webhook.DeadEntry
	if err := ctrl.DoJSON(reqCtx, http.MethodGet, path, nil, &items); err != nil {
		return err
	}
	if len(items) == 0 {
		fmt.Printf("(DLQ kosong untuk instance %q)\n", id)
		return nil
	}
	for _, it := range items {
		ts := time.UnixMilli(it.DeadAt).Format(time.RFC3339)
		fmt.Printf("seq=%-6d attempts=%-2d status=%-3d type=%-12s dead_at=%s err=%s\n",
			it.Seq, it.Attempts, it.LastStatus, it.EventType, ts, it.LastError)
	}
	return nil
}

func runWebhookDLQReplay(ctx context.Context, ctrl *control.Client, id string, args []string) error {
	fs := flag.NewFlagSet("webhook dlq replay", flag.ContinueOnError)
	all := fs.Bool("all", false, "kirim ulang seluruh entri DLQ")
	seq := fs.Int64("seq", 0, "kirim ulang satu entri dengan seq tertentu")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*all && *seq <= 0 {
		return errors.New("tentukan --all atau --seq <N> untuk replay DLQ")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	body := map[string]any{
		"all": *all,
		"seq": *seq,
	}
	var res webhook.ReplayResult
	path := "/control/instances/" + url.PathEscape(id) + "/webhook/dlq/replay"
	if err := ctrl.DoJSON(reqCtx, http.MethodPost, path, body, &res); err != nil {
		return err
	}
	fmt.Printf("✔ Replay DLQ %q selesai: berhasil=%d, gagal=%d\n", id, res.Replayed, res.Failed)
	return nil
}

func runWebhookDLQPurge(ctx context.Context, ctrl *control.Client, id string) error {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var res struct {
		Purged int64 `json:"purged"`
	}
	path := "/control/instances/" + url.PathEscape(id) + "/webhook/dlq"
	if err := ctrl.DoJSON(reqCtx, http.MethodDelete, path, nil, &res); err != nil {
		return err
	}
	fmt.Printf("✔ %d entri DLQ untuk instance %q telah dihapus.\n", res.Purged, id)
	return nil
}
