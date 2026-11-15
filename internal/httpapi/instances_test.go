package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// TestInstancesAPIAdditiveSupervisorFields memastikan endpoint /v1/instances* menyertakan field aditif Supervisor.
func TestInstancesAPIAdditiveSupervisorFields(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	cfg := &config.Config{DataDir: tmpDir}
	cfg.Normalize()
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs gagal: %v", err)
	}

	st, err := store.Open(ctx, filepath.Join(tmpDir, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatalf("auth.NewManager gagal: %v", err)
	}
	rawKey, _, err := authMgr.Create(ctx, "ci-key", "")
	if err != nil {
		t.Fatalf("authMgr.Create gagal: %v", err)
	}

	waMgr, err := wa.NewManager(ctx, cfg, st, nil)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	defer waMgr.Close()

	if _, err := waMgr.Create(ctx, "sales", 20, true); err != nil {
		t.Fatalf("waMgr.Create gagal: %v", err)
	}

	srv := NewHandler(authMgr, waMgr)

	// Periksa GET /v1/instances/{id}
	reqOne := httptest.NewRequest(http.MethodGet, "/v1/instances/sales", nil)
	reqOne.Header.Set("X-API-Key", rawKey)
	recOne := httptest.NewRecorder()
	srv.ServeHTTP(recOne, reqOne)
	if recOne.Code != http.StatusOK {
		t.Fatalf("status /v1/instances/sales diharapkan 200, didapat %d (%s)", recOne.Code, recOne.Body.String())
	}

	var one map[string]any
	if err := json.Unmarshal(recOne.Body.Bytes(), &one); err != nil {
		t.Fatalf("unmarshal respons detail gagal: %v", err)
	}
	for _, field := range []string{"state", "reconnect_attempts", "next_retry_in", "ban"} {
		if _, ok := one[field]; !ok {
			t.Fatalf("field aditif %q tidak ditemukan pada /v1/instances/sales: %v", field, one)
		}
	}
	if one["state"] != "idle" {
		t.Fatalf("state awal diharapkan idle, didapat %v", one["state"])
	}

	// Periksa GET /v1/instances
	reqList := httptest.NewRequest(http.MethodGet, "/v1/instances", nil)
	reqList.Header.Set("X-API-Key", rawKey)
	recList := httptest.NewRecorder()
	srv.ServeHTTP(recList, reqList)
	if recList.Code != http.StatusOK {
		t.Fatalf("status /v1/instances diharapkan 200, didapat %d", recList.Code)
	}

	var listBody struct {
		Instances []map[string]any `json:"instances"`
	}
	if err := json.Unmarshal(recList.Body.Bytes(), &listBody); err != nil || len(listBody.Instances) != 1 {
		t.Fatalf("respons /v1/instances tidak sesuai: %s", recList.Body.String())
	}
	if _, ok := listBody.Instances[0]["state"]; !ok {
		t.Fatalf("field state tidak ditemukan di item daftar instances")
	}
}
