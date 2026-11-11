package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/store"
)

func runKeySubcommand(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Println("Penggunaan: waapi key <list|add|rm>")
		return nil
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list", "ls":
		return runKeyList(ctx, ctrl)
	case "add", "create":
		return runKeyCreate(ctx, ctrl, rest)
	case "rm", "revoke", "delete":
		return runKeyRevoke(ctx, ctrl, rest)
	default:
		return fmt.Errorf("subcommand key tidak dikenal: %q", sub)
	}
}

func runKeyList(ctx context.Context, ctrl *control.Client) error {
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
	fmt.Fprintln(tw, "ID\tNAME\tPREFIX\tSCOPE\tLAST USED\tSTATUS")
	for _, k := range keys {
		scope := "*"
		if k.InstanceID != "" {
			scope = k.InstanceID
		}
		status := "active"
		if k.RevokedAt != 0 {
			status = "revoked"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			k.ID, k.Name, k.Prefix, scope, formatTime(k.LastUsedAt), status)
	}
	return tw.Flush()
}

func runKeyCreate(ctx context.Context, ctrl *control.Client, args []string) error {
	fs := flag.NewFlagSet("key add", flag.ContinueOnError)
	name := fs.String("name", "", "nama deskriptif API key")
	inst := fs.String("instance", "", "batasi akses hanya ke instance tertentu (opsional)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("penggunaan: waapi key add --name <nama> [--instance <id>]")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var resp struct {
		Key  string       `json:"key"`
		Meta store.APIKey `json:"meta"`
	}
	err := ctrl.DoJSON(reqCtx, http.MethodPost, "/control/keys", map[string]string{
		"name":        *name,
		"instance_id": *inst,
	}, &resp)
	if err != nil {
		return err
	}

	scope := "* (semua instance)"
	if resp.Meta.InstanceID != "" {
		scope = "instance: " + resp.Meta.InstanceID
	}
	fmt.Println("✔ API key dibuat (simpan sekarang, tidak akan ditampilkan lagi)")
	fmt.Println()
	fmt.Printf("  ID        %s\n", resp.Meta.ID)
	fmt.Printf("  Name      %s\n", resp.Meta.Name)
	fmt.Printf("  Scope     %s\n", scope)
	fmt.Printf("  Key       %s\n", resp.Key)
	return nil
}

func runKeyRevoke(ctx context.Context, ctrl *control.Client, args []string) error {
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

