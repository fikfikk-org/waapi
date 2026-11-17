package control

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/guardian"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/mcpserver"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/policy"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type ctrlHeaderRT struct {
	keyID string
}

func (c ctrlHeaderRT) RoundTrip(r *http.Request) (*http.Response, error) {
	clone := r.Clone(r.Context())
	if c.keyID != "" {
		clone.Header.Set("X-Waapi-Key-Id", c.keyID)
	}
	return http.DefaultTransport.RoundTrip(clone)
}

// TestControl_MCPEndpoints memastikan /control/mcp memakai policy operator lokal tanpa key maupun X-Waapi-Key-Id saat disetel.
func TestControl_MCPEndpoints(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp, MCP: "on"}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, _ := store.Open(ctx, cfg.AppDBPath())
	defer st.Close()
	j, _ := journal.Open(ctx, cfg.JournalDBPath())
	defer j.Close()
	authMgr, _ := auth.NewManager(ctx, st)
	cs := ctxstore.New(j.DB(), ctxstore.NewStoreSettings(st))
	waMgr, _ := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j), wa.WithContextStore(cs))
	defer waMgr.Close()
	_, _ = waMgr.Create(ctx, "sales", 60, false)
	_ = st.UpdateInstanceStatus(ctx, "sales", store.StatusConnected, "628000000000@s.whatsapp.net")

	enf := policy.New(authMgr, nil)
	ob := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, waMgr, nil, enf, outbox.Options{})
	mcpSrv := mcpserver.New(mcpserver.Deps{
		Auth:      authMgr,
		Instances: waMgr,
		Outbox:    ob,
		Journal:   j,
		CtxStore:  cs,
		Policy:    enf,
		Health:    func(context.Context, string) (guardian.HealthReport, error) { return guardian.HealthReport{}, nil },
	}, mcpserver.Config{Enabled: true})

	srv := NewServer(cfg, st, authMgr, waMgr, nil, WithJournal(j), WithContextStore(cs), WithMCP(mcpSrv))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Tanpa X-Waapi-Key-Id -> operator lokal dengan semua scope & first_contact=ask -> klien tanpa elisitasi ditolak first_contact_requires_approval
	cLocal := mcp.NewClient(&mcp.Implementation{Name: "ctrl-local", Version: "1.0"}, nil)
	sessLocal, err := cLocal.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             ts.URL + "/control/mcp",
		DisableStandaloneSSE: true,
		HTTPClient:           &http.Client{Transport: ctrlHeaderRT{}},
	}, nil)
	if err != nil {
		t.Fatalf("connect /control/mcp lokal gagal: %v", err)
	}
	resSend, errSend := sessLocal.CallTool(ctx, &mcp.CallToolParams{
		Name:      "send_message",
		Arguments: map[string]any{"instance": "sales", "to": "628111999888", "type": "text", "text": "hai"},
	})
	_ = sessLocal.Close()
	if errSend == nil && (resSend == nil || !resSend.IsError) {
		t.Fatalf("operator /control/mcp tanpa elisitasi wajib menuntut persetujuan first_contact:ask")
	}

	// 2. Dengan X-Waapi-Key-Id dari key ber-policy terbatas -> hanya melihat tool key tsb
	sc, _ := auth.ParseScopes("mcp,instances:read")
	_, meta, _ := authMgr.CreateWithOptions(ctx, auth.CreateKeyRequest{
		Name:   "limited-ctrl",
		Scopes: sc,
		Policy: auth.KeyPolicy{FirstContact: "deny", Destructive: "deny", Tools: []string{"list_instances"}},
	})
	cScoped := mcp.NewClient(&mcp.Implementation{Name: "ctrl-scoped", Version: "1.0"}, nil)
	sessScoped, err := cScoped.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             ts.URL + "/control/mcp",
		DisableStandaloneSSE: true,
		HTTPClient:           &http.Client{Transport: ctrlHeaderRT{keyID: meta.ID}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	toolsRes, err := sessScoped.ListTools(ctx, nil)
	_ = sessScoped.Close()
	if err != nil || len(toolsRes.Tools) != 1 || toolsRes.Tools[0].Name != "list_instances" {
		t.Fatalf("X-Waapi-Key-Id harus menerapkan allowlist key tersebut: err=%v tools=%+v", err, toolsRes)
	}
	_ = strings.TrimSpace
}
