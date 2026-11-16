//go:build linux

package control

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
)

// TestAudit_ControlDenganUID memastikan permintaan control lewat Unix socket mencatat UID pemanggil dari SO_PEERCRED pada Linux.
func TestAudit_ControlDenganUID(t *testing.T) {
	srv, cfg, _, _, audit := setupTestControlServer(t)
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen gagal: %v", err)
	}
	defer ln.Close()

	httpSrv := &http.Server{
		Handler:     srv.Handler(),
		ConnContext: srv.ConnContext,
	}
	go func() { _ = httpSrv.Serve(ln) }()
	defer httpSrv.Close()

	client := NewClient(cfg.ControlSocket())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var out map[string]any
	if err := client.DoJSON(ctx, http.MethodPost, "/control/instances", map[string]any{
		"id": "unix-uid-test",
	}, &out); err != nil {
		t.Fatalf("DoJSON lewat control.sock gagal: %v", err)
	}

	entries, err := audit.List(ctx, auth.AuditQuery{ActionPattern: "instance.create"})
	if err != nil {
		t.Fatal(err)
	}
	wantActor := "control:uid=" + strconv.Itoa(os.Getuid())
	if len(entries) != 1 || entries[0].Actor != wantActor {
		t.Fatalf("actor audit diharapkan %q, didapat %+v", wantActor, entries)
	}
}
