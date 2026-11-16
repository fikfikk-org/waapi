package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/control"
)

type auditCaller interface {
	DoJSON(ctx context.Context, method, path string, in, out any) error
}

func runAudit(ctx context.Context, ctrl *control.Client, args []string) error {
	return executeAudit(ctx, ctrl, args)
}

func executeAudit(ctx context.Context, ctrl auditCaller, args []string) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	since := fs.String("since", "24h", "rentang waktu log mundur dari sekarang (mis. 1h, 24h)")
	action := fs.String("action", "", "filter pola action (mis. key.*, settings.update)")
	limit := fs.Int("limit", 100, "maksimal entri audit ditampilkan")
	asJSON := fs.Bool("json", false, "keluarkan dalam format JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	q := url.Values{}
	if strings.TrimSpace(*since) != "" {
		q.Set("since", strings.TrimSpace(*since))
	}
	if strings.TrimSpace(*action) != "" {
		q.Set("action", strings.TrimSpace(*action))
	}
	if *limit > 0 {
		q.Set("limit", strconv.Itoa(*limit))
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var entries []auth.AuditEntry
	if err := ctrl.DoJSON(reqCtx, http.MethodGet, "/control/audit?"+q.Encode(), nil, &entries); err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(entries)
	}
	if len(entries) == 0 {
		fmt.Println("Tidak ada entri audit pada rentang filter tersebut.")
		return nil
	}

	tw := newTabWriter(os.Stdout)
	fmt.Fprintln(tw, "ID\tTIME\tACTOR\tACTION\tTARGET\tDETAIL")
	for _, e := range entries {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n",
			e.ID, formatTime(e.At), e.Actor, e.Action, e.Target, e.Detail)
	}
	return tw.Flush()
}
