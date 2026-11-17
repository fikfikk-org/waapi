package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

// TestHTTP_MCPMountDanDisable memastikan POST /mcp di router publik menegakkan auth, Origin, dan kembali 404 saat WAAPI_MCP=off.
func TestHTTP_MCPMountDanDisable(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, _ := store.Open(ctx, cfg.AppDBPath())
	defer st.Close()
	authMgr, _ := auth.NewManager(ctx, st)
	j, _ := journal.Open(ctx, cfg.JournalDBPath())
	defer j.Close()
	cs := ctxstore.New(j.DB(), ctxstore.NewStoreSettings(st))
	waMgr, _ := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j), wa.WithContextStore(cs))
	defer waMgr.Close()
	_, _ = waMgr.Create(ctx, "sales", 60, false)

	enf := policy.New(authMgr, nil)
	ob := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, waMgr, nil, enf, outbox.Options{})

	mcpOn := mcpserver.New(mcpserver.Deps{
		Auth:      authMgr,
		Instances: waMgr,
		Outbox:    ob,
		Journal:   j,
		CtxStore:  cs,
		Policy:    enf,
		Health:    func(context.Context, string) (guardian.HealthReport, error) { return guardian.HealthReport{}, nil },
	}, mcpserver.Config{Enabled: true})

	srvOn := NewServer(authMgr, waMgr, WithDB(st.DB), WithJournal(j), WithOutbox(ob), WithContextStore(cs), WithMCP(mcpOn))
	tsOn := httptest.NewServer(srvOn)
	defer tsOn.Close()

	key := mustCreateBearerMCPKey(t, authMgr)
	c := mcp.NewClient(&mcp.Implementation{Name: "http-test", Version: "1.0"}, nil)
	sess, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             tsOn.URL + "/mcp",
		DisableStandaloneSSE: true,
		HTTPClient:           &http.Client{Transport: httpTestBearer{token: key}},
	}, nil)
	if err != nil {
		t.Fatalf("koneksi MCP lewat httpapi gagal: %v", err)
	}
	if res, cErr := sess.CallTool(ctx, &mcp.CallToolParams{Name: "list_instances"}); cErr != nil || res.IsError {
		t.Fatalf("list_instances via /mcp gagal: err=%v res=%+v", cErr, res)
	}
	_ = sess.Close()

	// Mode WAAPI_MCP=off -> 404
	mcpOff := mcpserver.New(mcpserver.Deps{Auth: authMgr, Instances: waMgr}, mcpserver.Config{Enabled: false})
	srvOff := NewServer(authMgr, waMgr, WithDB(st.DB), WithMCP(mcpOff))
	recOff := httptest.NewRecorder()
	reqOff := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{}`))
	reqOff.Header.Set("Authorization", "Bearer "+key)
	srvOff.ServeHTTP(recOff, reqOff)
	if recOff.Code != http.StatusNotFound {
		t.Fatalf("POST /mcp saat WAAPI_MCP=off wajib 404, didapat %d", recOff.Code)
	}
}

type httpTestBearer struct{ token string }

func (b httpTestBearer) RoundTrip(r *http.Request) (*http.Response, error) {
	clone := r.Clone(r.Context())
	clone.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(clone)
}

func mustCreateBearerMCPKey(t *testing.T, authMgr *auth.Manager) string {
	t.Helper()
	sc, _ := auth.ParseScopes("mcp,instances:read")
	raw, _, err := authMgr.CreateWithOptions(context.Background(), auth.CreateKeyRequest{
		Name:       "http-mcp",
		Scopes:     sc,
		RatePerSec: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

var _ = filepath.Join
