package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
)

type hookCaller interface {
	DoJSON(ctx context.Context, method, path string, in, out any) error
}

func runHook(ctx context.Context, ctrl *control.Client, args []string) error {
	return executeHook(ctx, ctrl, args)
}

func executeHook(ctx context.Context, ctrl hookCaller, args []string) error {
	if len(args) < 1 {
		return errors.New("penggunaan: waapi hook <ls|add|rm|enable|disable|logs|test> <id> [opsi]")
	}
	sub := strings.ToLower(strings.TrimSpace(args[0]))
	rest := args[1:]

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	switch sub {
	case "ls", "list":
		return runHookList(reqCtx, ctrl, rest)
	case "add":
		return runHookAdd(reqCtx, ctrl, rest)
	case "rm", "delete":
		return runHookRemove(reqCtx, ctrl, rest)
	case "enable":
		return runHookToggle(reqCtx, ctrl, rest, true)
	case "disable":
		return runHookToggle(reqCtx, ctrl, rest, false)
	case "logs":
		return runHookLogs(reqCtx, ctrl, rest)
	case "test":
		return runHookTest(reqCtx, ctrl, rest)
	default:
		return fmt.Errorf("subperintah hook tidak dikenal %q (gunakan: ls, add, rm, enable, disable, logs, test)", sub)
	}
}

func runHookList(ctx context.Context, ctrl hookCaller, args []string) error {
	id, flagArgs, err := takePositionalAndFlags(args, 1)
	if err != nil || id[0] == "" {
		return errors.New("penggunaan: waapi hook ls <id> [--json]")
	}
	fs := flag.NewFlagSet("hook ls", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	var resp struct {
		Hooks []struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			Enabled  bool   `json:"enabled"`
			Priority int    `json:"priority"`
			Trusted  bool   `json:"trusted"`
		} `json:"hooks"`
	}
	path := fmt.Sprintf("/control/instances/%s/hooks", url.PathEscape(id[0]))
	if err := ctrl.DoJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printHookJSON(resp)
	}
	if len(resp.Hooks) == 0 {
		fmt.Println("(belum ada rule hooks)")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tPRIO\tENABLED\tTRUSTED\tNAME")
	for _, h := range resp.Hooks {
		fmt.Fprintf(tw, "%s\t%d\t%t\t%t\t%s\n", h.ID, h.Priority, h.Enabled, h.Trusted, h.Name)
	}
	return tw.Flush()
}

func runHookAdd(ctx context.Context, ctrl hookCaller, args []string) error {
	pos, flagArgs, err := takePositionalAndFlags(args, 1)
	if err != nil || pos[0] == "" {
		return errors.New("penggunaan: waapi hook add <id> --file rule.json [--json]")
	}
	fs := flag.NewFlagSet("hook add", flag.ContinueOnError)
	filePath := fs.String("file", "", "path file JSON rule hook")
	asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if strings.TrimSpace(*filePath) == "" {
		return errors.New("--file wajib diisi")
	}
	raw, err := os.ReadFile(*filePath)
	if err != nil {
		return fmt.Errorf("baca file rule %s: %w", *filePath, err)
	}

	var resp map[string]any
	path := fmt.Sprintf("/control/instances/%s/hooks", url.PathEscape(pos[0]))
	if err := ctrl.DoJSON(ctx, http.MethodPost, path, json.RawMessage(raw), &resp); err != nil {
		return err
	}
	if *asJSON {
		return printHookJSON(resp)
	}
	fmt.Printf("✔ Rule hook %s dipasang pada %s.\n", stringOrDash(resp["id"]), pos[0])
	return nil
}

func runHookRemove(ctx context.Context, ctrl hookCaller, args []string) error {
	pos, flagArgs, err := takePositionalAndFlags(args, 2)
	if err != nil {
		return errors.New("penggunaan: waapi hook rm <id> <hook_id> [--json]")
	}
	fs := flag.NewFlagSet("hook rm", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	var resp map[string]any
	path := fmt.Sprintf("/control/instances/%s/hooks/%s", url.PathEscape(pos[0]), url.PathEscape(pos[1]))
	if err := ctrl.DoJSON(ctx, http.MethodDelete, path, nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printHookJSON(resp)
	}
	fmt.Printf("✔ Rule hook %s dihapus dari %s.\n", pos[1], pos[0])
	return nil
}

func runHookToggle(ctx context.Context, ctrl hookCaller, args []string, enabled bool) error {
	pos, flagArgs, err := takePositionalAndFlags(args, 2)
	if err != nil {
		return errors.New("penggunaan: waapi hook enable|disable <id> <hook_id> [--json]")
	}
	fs := flag.NewFlagSet("hook toggle", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	act := "disable"
	if enabled {
		act = "enable"
	}
	var resp map[string]any
	path := fmt.Sprintf("/control/instances/%s/hooks/%s/%s", url.PathEscape(pos[0]), url.PathEscape(pos[1]), act)
	if err := ctrl.DoJSON(ctx, http.MethodPost, path, nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printHookJSON(resp)
	}
	fmt.Printf("✔ Rule hook %s pada %s sekarang %sd.\n", pos[1], pos[0], act)
	return nil
}

func runHookLogs(ctx context.Context, ctrl hookCaller, args []string) error {
	pos, flagArgs, err := takePositionalAndFlags(args, 1)
	if err != nil {
		return errors.New("penggunaan: waapi hook logs <id> [--hook x] [--since 1h] [--limit 50] [--json]")
	}
	fs := flag.NewFlagSet("hook logs", flag.ContinueOnError)
	hookID := fs.String("hook", "", "filter ID hook tertentu")
	since := fs.String("since", "", "rentang waktu ke belakang (mis. 1h, 30m)")
	limit := fs.Int("limit", 0, "jumlah maksimal baris log")
	asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	q := url.Values{}
	if strings.TrimSpace(*hookID) != "" {
		q.Set("hook", strings.TrimSpace(*hookID))
	}
	if strings.TrimSpace(*since) != "" {
		q.Set("since", strings.TrimSpace(*since))
	}
	if *limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limit))
	}
	path := fmt.Sprintf("/control/instances/%s/hooks/logs", url.PathEscape(pos[0]))
	if qs := q.Encode(); qs != "" {
		path += "?" + qs
	}

	var resp struct {
		Logs []struct {
			Seq    int64  `json:"seq"`
			HookID string `json:"hook_id"`
			Status string `json:"status"`
			Detail string `json:"detail"`
			At     int64  `json:"at"`
		} `json:"logs"`
	}
	if err := ctrl.DoJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printHookJSON(resp)
	}
	if len(resp.Logs) == 0 {
		fmt.Println("(belum ada log eksekusi hooks)")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "SEQ\tTIME\tHOOK\tSTATUS\tDETAIL")
	for _, l := range resp.Logs {
		ts := time.UnixMilli(l.At).Format(time.RFC3339)
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", l.Seq, ts, l.HookID, l.Status, l.Detail)
	}
	return tw.Flush()
}

func runHookTest(ctx context.Context, ctrl hookCaller, args []string) error {
	pos, flagArgs, err := takePositionalAndFlags(args, 1)
	if err != nil {
		return errors.New("penggunaan: waapi hook test <id> (--file rule.json | --hook id) --event event.json [--json]")
	}
	fs := flag.NewFlagSet("hook test", flag.ContinueOnError)
	ruleFile := fs.String("file", "", "path file JSON rule hook")
	hookID := fs.String("hook", "", "ID hook tersimpan untuk disimulasikan")
	eventFile := fs.String("event", "", "path file JSON envelope event")
	asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if strings.TrimSpace(*eventFile) == "" {
		return errors.New("--event wajib diisi")
	}
	eventRaw, err := os.ReadFile(*eventFile)
	if err != nil {
		return fmt.Errorf("baca file event %s: %w", *eventFile, err)
	}

	payload := map[string]any{
		"event": json.RawMessage(eventRaw),
	}
	if strings.TrimSpace(*ruleFile) != "" {
		ruleRaw, err := os.ReadFile(*ruleFile)
		if err != nil {
			return fmt.Errorf("baca file rule %s: %w", *ruleFile, err)
		}
		payload["rule"] = json.RawMessage(ruleRaw)
	} else if strings.TrimSpace(*hookID) != "" {
		payload["hook_id"] = strings.TrimSpace(*hookID)
	} else {
		return errors.New("wajib menentukan --file rule.json atau --hook <id>")
	}

	var resp map[string]any
	path := fmt.Sprintf("/control/instances/%s/hooks/test", url.PathEscape(pos[0]))
	if err := ctrl.DoJSON(ctx, http.MethodPost, path, payload, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printHookJSON(resp)
	}
	fmt.Printf("Matched        : %v\n", resp["matched"])
	if reason := stringOrEmpty(resp["skipped_reason"]); reason != "" {
		fmt.Printf("Skipped Reason : %s\n", reason)
	}
	if acts, ok := resp["actions"].([]any); ok {
		fmt.Printf("Actions (%d):\n", len(acts))
		for i, a := range acts {
			fmt.Printf("  %d. %v\n", i+1, a)
		}
	}
	return nil
}

func stringOrDash(v any) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return "-"
}

func stringOrEmpty(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func takePositionalAndFlags(args []string, need int) (pos []string, flags []string, err error) {
	pos = make([]string, 0, need)
	flags = make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") && len(pos) < need {
			pos = append(pos, a)
			continue
		}
		flags = append(flags, a)
		if strings.HasPrefix(a, "-") && !strings.Contains(a, "=") && a != "--json" && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	if len(pos) < need {
		return nil, nil, errors.New("argumen posisi tidak mencukupi")
	}
	return pos, flags, nil
}

func printHookJSON(v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(raw))
	return nil
}
