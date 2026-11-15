package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
)

type eventsCLIOptions struct {
	id     string
	after  int64
	types  string
	limit  int
	follow bool
	asJSON bool
}

type eventsPullPayload struct {
	Events    []json.RawMessage `json:"events"`
	NextAfter int64             `json:"next_after"`
	HasMore   bool              `json:"has_more"`
}

func runEvents(ctx context.Context, ctrl *control.Client, args []string) error {
	opts, err := parseEventsArgs(args)
	if err != nil {
		return err
	}

	after := opts.after
	for {
		waitSec := 0
		if opts.follow {
			waitSec = 25
		}
		path := buildControlEventsPath(opts.id, after, opts.limit, opts.types, waitSec)
		var resp eventsPullPayload
		if err := ctrl.DoJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
			if opts.follow && ctx.Err() != nil {
				return nil
			}
			return err
		}

		for _, raw := range resp.Events {
			printCLIEvent(raw, opts.asJSON)
		}
		if resp.NextAfter > after {
			after = resp.NextAfter
		}
		if !opts.follow {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func parseEventsArgs(args []string) (eventsCLIOptions, error) {
	var opts eventsCLIOptions
	var id string
	flagArgs := make([]string, 0, len(args))

	// Pisahkan argumen posisi <id> dari flag supaya urutan `sales --types message` dan `--types message sales` sama-sama valid.
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") || a == "-" {
			if id == "" {
				id = a
				continue
			}
			flagArgs = append(flagArgs, a)
			continue
		}
		flagArgs = append(flagArgs, a)
		if !strings.Contains(a, "=") && flagNeedsValue(a) && i+1 < len(args) {
			i++
			flagArgs = append(flagArgs, args[i])
		}
	}

	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	fs.Int64Var(&opts.after, "after", 0, "ambil event setelah seq")
	fs.StringVar(&opts.types, "types", "", "filter jenis event dipisah koma (mis. message,receipt)")
	fs.IntVar(&opts.limit, "limit", 100, "jumlah maksimum event per halaman")
	fs.BoolVar(&opts.follow, "follow", false, "ikuti event masuk secara berkelanjutan")
	fs.BoolVar(&opts.asJSON, "json", false, "cetak output NDJSON mentah")

	if err := fs.Parse(flagArgs); err != nil {
		return opts, err
	}
	if id == "" && fs.NArg() > 0 {
		id = fs.Arg(0)
	}
	if id == "" {
		return opts, errors.New("penggunaan: waapi events <id> [--after N] [--types csv] [--limit N] [--follow] [--json]")
	}
	opts.id = id
	if opts.limit <= 0 {
		opts.limit = 100
	}
	return opts, nil
}

func flagNeedsValue(arg string) bool {
	name := strings.TrimLeft(arg, "-")
	switch name {
	case "after", "types", "limit":
		return true
	default:
		return false
	}
}

func buildControlEventsPath(id string, after int64, limit int, types string, waitSec int) string {
	q := url.Values{}
	if after > 0 {
		q.Set("after", strconv.FormatInt(after, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if strings.TrimSpace(types) != "" {
		q.Set("types", strings.TrimSpace(types))
	}
	if waitSec > 0 {
		q.Set("wait", strconv.Itoa(waitSec))
	}
	return "/control/instances/" + url.PathEscape(id) + "/events?" + q.Encode()
}

func printCLIEvent(raw json.RawMessage, asJSON bool) {
	if asJSON {
		fmt.Println(string(raw))
		return
	}
	var env struct {
		ID        string          `json:"id"`
		Event     string          `json:"event"`
		Timestamp int64           `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		fmt.Println(string(raw))
		return
	}
	var summary struct {
		Chat string `json:"chat"`
		Type string `json:"type"`
		Text string `json:"text"`
		WAID string `json:"id"`
	}
	_ = json.Unmarshal(env.Data, &summary)
	tsStr := time.Unix(env.Timestamp, 0).Format(time.RFC3339)
	kind := env.Event
	if summary.Type != "" && summary.Type != env.Event {
		kind += "/" + summary.Type
	}
	fmt.Printf("[%s] %s %-18s chat=%-28s %s\n", env.ID, tsStr, kind, summary.Chat, summary.Text)
}
