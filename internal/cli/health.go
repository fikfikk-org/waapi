package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/guardian"
)

type jsonCaller interface {
	DoJSON(ctx context.Context, method, path string, in, out any) error
}

func runHealth(ctx context.Context, ctrl *control.Client, args []string) error {
	return executeHealth(ctx, ctrl, args)
}

func executeHealth(ctx context.Context, caller jsonCaller, args []string) error {
	var (
		id     string
		asJSON bool
	)
	for _, a := range args {
		if a == "--json" {
			asJSON = true
			continue
		}
		if strings.HasPrefix(a, "-") {
			return fmt.Errorf("opsi tidak dikenal untuk health: %q (gunakan --json)", a)
		}
		if id == "" {
			id = strings.TrimSpace(a)
			continue
		}
		return errors.New("penggunaan: waapi health <id> [--json]")
	}
	if id == "" {
		return errors.New("penggunaan: waapi health <id> [--json]")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var rep guardian.HealthReport
	path := "/control/instances/" + url.PathEscape(id) + "/health"
	if err := caller.DoJSON(reqCtx, http.MethodGet, path, nil, &rep); err != nil {
		return err
	}

	if asJSON {
		raw, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(raw))
		return nil
	}

	fmt.Printf("Instance : %s\n", rep.Instance)
	fmt.Printf("State    : %s (skor: %d/100, profil: %s, shadow: %v)\n", rep.State, rep.Score, rep.Profile, rep.Shadow)
	fmt.Printf("Kuota    : harian %d/%d sisa | first-contact %d/%d sisa | warm-up %.2fx\n",
		rep.Limits.DailyRemaining, rep.Limits.DailyCap,
		rep.Limits.FirstContactRemaining, rep.Limits.FirstContactCap,
		rep.Limits.WarmupFactor)
	fmt.Printf("24 Jam   : terkirim %d (first-contact %d) | delivered %d | replied %d\n",
		rep.Counters.Sent24h, rep.Counters.FirstContact24h, rep.Counters.Delivered24h, rep.Counters.Replied24h)
	if rep.Ban.Active {
		fmt.Printf("Ban      : AKTIF (kode %d)\n", rep.Ban.LastCode)
	}
	if len(rep.Factors) > 0 {
		tw := newTabWriter(os.Stdout)
		fmt.Fprintln(tw, "\nFAKTOR\tDAMPAK\tKETERANGAN")
		for _, f := range rep.Factors {
			fmt.Fprintf(tw, "%s\t%+d\t%s\n", f.Code, f.Impact, f.Detail)
		}
		return tw.Flush()
	}
	return nil
}
