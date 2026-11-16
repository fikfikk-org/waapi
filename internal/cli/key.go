package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/store"
)

type keyControlCaller interface {
	DoJSON(ctx context.Context, method, path string, in, out any) error
}

func runKeySubcommand(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Println("Penggunaan: waapi key <list|add|show|rotate|rm>")
		return nil
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list", "ls":
		return runKeyList(ctx, ctrl)
	case "add", "create":
		return executeKeyAdd(ctx, ctrl, rest)
	case "show", "info":
		return executeKeyShow(ctx, ctrl, rest)
	case "rotate":
		return executeKeyRotate(ctx, ctrl, rest)
	case "rm", "revoke", "delete":
		return runKeyRevoke(ctx, ctrl, rest)
	default:
		return fmt.Errorf("subcommand key tidak dikenal: %q", sub)
	}
}

func runKeyList(ctx context.Context, ctrl keyControlCaller) error {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var keys []store.APIKey
	if err := ctrl.DoJSON(reqCtx, http.MethodGet, "/control/keys", nil, &keys); err != nil {
		return err
	}
	if len(keys) == 0 {
		fmt.Println("Belum ada API key. Buat dengan: waapi key add --name backend")
		return nil
	}

	tw := newTabWriter(os.Stdout)
	fmt.Fprintln(tw, "ID\tNAME\tPREFIX\tINSTANCE\tSCOPES\tEXPIRES\tRATE\tSTATUS")
	for _, k := range keys {
		inst := "*"
		if k.InstanceID != "" {
			inst = k.InstanceID
		}
		scopes := k.Scopes
		if scopes == "" {
			scopes = "*"
		}
		exp := "never"
		if k.ExpiresAt > 0 {
			exp = formatTime(k.ExpiresAt)
		}
		rateStr := "50/s"
		if k.RatePerSec > 0 {
			rateStr = fmt.Sprintf("%d/s", k.RatePerSec)
		}
		status := "active"
		if k.RevokedAt != 0 {
			status = "revoked"
		} else if k.ExpiresAt > 0 && time.Now().Unix() >= k.ExpiresAt {
			status = "expired"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			k.ID, k.Name, k.Prefix, inst, scopes, exp, rateStr, status)
	}
	return tw.Flush()
}

func executeKeyAdd(ctx context.Context, ctrl keyControlCaller, args []string) error {
	fs := flag.NewFlagSet("key add", flag.ContinueOnError)
	name := fs.String("name", "", "nama deskriptif API key")
	inst := fs.String("instance", "", "batasi akses hanya ke instance tertentu (opsional)")
	scopes := fs.String("scopes", "", "daftar scope dipisah koma atau '*' (kosong = default §4.1.5)")
	expires := fs.String("expires", "", "masa berlaku key (mis. 720h, 24h)")
	rateRPS := fs.Int("rate", 0, "batas request per detik per key (0 = default 50)")
	policyFile := fs.String("policy", "", "path file JSON KeyPolicy (opsional)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*name) == "" {
		return errors.New("penggunaan: waapi key add --name <nama> [--instance <id>] [--scopes ...] [--expires 720h] [--rate 50] [--policy file.json]")
	}

	payload := map[string]any{
		"name":        strings.TrimSpace(*name),
		"instance_id": strings.TrimSpace(*inst),
	}
	if strings.TrimSpace(*scopes) != "" {
		payload["scopes"] = strings.TrimSpace(*scopes)
	}
	if strings.TrimSpace(*expires) != "" {
		payload["expires_in"] = strings.TrimSpace(*expires)
	}
	if *rateRPS > 0 {
		payload["rate_per_sec"] = *rateRPS
	}
	if p := strings.TrimSpace(*policyFile); p != "" {
		rawPol, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("baca file policy %s: %w", p, err)
		}
		pol, err := auth.ParseKeyPolicy(string(rawPol))
		if err != nil {
			return fmt.Errorf("isi policy %s tidak valid: %w", p, err)
		}
		payload["policy"] = pol
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var resp struct {
		Key  string       `json:"key"`
		Meta store.APIKey `json:"meta"`
	}
	if err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/keys", payload, &resp); err != nil {
		return err
	}

	printCreatedKeySummary(resp.Key, resp.Meta, "")
	return nil
}

func executeKeyRotate(ctx context.Context, ctrl keyControlCaller, args []string) error {
	id, flagArgs := extractPositionalAndFlags(args)
	fs := flag.NewFlagSet("key rotate", flag.ContinueOnError)
	grace := fs.String("grace", "24h", "masa tenggang key lama tetap berlaku sebelum expired")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if id == "" && fs.NArg() > 0 {
		id = fs.Arg(0)
	}
	if id == "" {
		return errors.New("penggunaan: waapi key rotate <key-id> [--grace 24h]")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var resp struct {
		Key  string       `json:"key"`
		Meta store.APIKey `json:"meta"`
	}
	if err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/keys/"+id+"/rotate", map[string]string{
		"grace": strings.TrimSpace(*grace),
	}, &resp); err != nil {
		return err
	}

	printCreatedKeySummary(resp.Key, resp.Meta, fmt.Sprintf("Key lama %s akan kedaluwarsa setelah grace %s", id, *grace))
	return nil
}

func executeKeyShow(ctx context.Context, ctrl keyControlCaller, args []string) error {
	if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
		return errors.New("penggunaan: waapi key show <key-id>")
	}
	id := strings.TrimSpace(args[0])

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var k store.APIKey
	if err := ctrl.DoJSON(reqCtx, http.MethodGet, "/control/keys/"+id, nil, &k); err != nil {
		return err
	}

	inst := "* (semua instance)"
	if k.InstanceID != "" {
		inst = k.InstanceID
	}
	scopes := k.Scopes
	if scopes == "" {
		scopes = "*"
	}
	exp := "never"
	if k.ExpiresAt > 0 {
		exp = formatTime(k.ExpiresAt)
	}
	rps := 50
	if k.RatePerSec > 0 {
		rps = k.RatePerSec
	}
	pol := k.PolicyJSON
	if pol == "" {
		pol = "{}"
	}
	fmt.Printf("ID        %s\n", k.ID)
	fmt.Printf("Name      %s\n", k.Name)
	fmt.Printf("Prefix    %s\n", k.Prefix)
	fmt.Printf("Instance  %s\n", inst)
	fmt.Printf("Scopes    %s\n", scopes)
	fmt.Printf("Expires   %s\n", exp)
	fmt.Printf("Rate      %d req/s\n", rps)
	fmt.Printf("Policy    %s\n", pol)
	fmt.Printf("LastUsed  %s\n", formatTime(k.LastUsedAt))
	return nil
}

func printCreatedKeySummary(rawKey string, meta store.APIKey, note string) {
	instScope := "* (semua instance)"
	if meta.InstanceID != "" {
		instScope = "instance: " + meta.InstanceID
	}
	scopes := meta.Scopes
	if scopes == "" {
		scopes = auth.DefaultScopes().String()
	}
	fmt.Println("✔ API key dibuat (simpan sekarang, tidak akan ditampilkan lagi)")
	if note != "" {
		fmt.Println("  " + note)
	}
	fmt.Println()
	fmt.Printf("  ID        %s\n", meta.ID)
	fmt.Printf("  Name      %s\n", meta.Name)
	fmt.Printf("  Instance  %s\n", instScope)
	fmt.Printf("  Scopes    %s\n", scopes)
	if meta.ExpiresAt > 0 {
		fmt.Printf("  Expires   %s\n", formatTime(meta.ExpiresAt))
	}
	if meta.RatePerSec > 0 {
		fmt.Printf("  Rate      %d req/s\n", meta.RatePerSec)
	}
	fmt.Printf("  Key       %s\n", rawKey)
}

func extractPositionalAndFlags(args []string) (pos string, flags []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") && pos == "" {
			pos = a
			continue
		}
		flags = append(flags, a)
		if strings.HasPrefix(a, "-") && !strings.Contains(a, "=") && i+1 < len(args) {
			flags = append(flags, args[i+1])
			i++
		}
	}
	return pos, flags
}

func runKeyRevoke(ctx context.Context, ctrl keyControlCaller, args []string) error {
	if len(args) == 0 {
		return errors.New("penggunaan: waapi key rm <key-id>")
	}
	id := args[0]
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := ctrl.DoJSON(reqCtx, http.MethodDelete, "/control/keys/"+id, nil, nil); err != nil {
		return err
	}
	fmt.Printf("✔ API key %q telah dicabut (revoked).\n", id)
	return nil
}
