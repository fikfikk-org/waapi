package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/store"
)

type stubGovernor struct {
	lvl governor.Level
}

func (s stubGovernor) Level() governor.Level { return s.lvl }

// TestServerRoutes memastikan seluruh endpoint publik terautentikasi tercatat bersama scope masing-masing.
func TestServerRoutes(t *testing.T) {
	srv := NewServer(nil, nil)
	routes := srv.Routes()
	if len(routes) != 19 {
		t.Fatalf("diharapkan 19 route terdaftar, didapat %d", len(routes))
	}
	for _, rt := range routes {
		if rt.Pattern == "" || rt.Scope == "" {
			t.Fatalf("route harus memiliki pattern dan scope: %+v", rt)
		}
	}
}

// TestReadyz memastikan /readyz mengembalikan 200 saat normal dan 503 not_ready saat governor berada pada level Red.
func TestReadyz(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	// Kondisi normal (Green + app.db sehat) -> 200 {"status":"ready"}
	srvGreen := NewServer(nil, nil, WithDB(st.DB), WithGovernor(stubGovernor{lvl: governor.Green}))
	rec := httptest.NewRecorder()
	srvGreen.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status /readyz normal diharapkan 200, didapat %d (%s)", rec.Code, rec.Body.String())
	}
	var okBody map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &okBody); err != nil || okBody["status"] != "ready" {
		t.Fatalf("body /readyz normal tidak sesuai: %s", rec.Body.String())
	}

	// Kondisi Red -> 503 not_ready
	srvRed := NewServer(nil, nil, WithDB(st.DB), WithGovernor(stubGovernor{lvl: governor.Red}))
	recRed := httptest.NewRecorder()
	srvRed.ServeHTTP(recRed, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recRed.Code != http.StatusServiceUnavailable {
		t.Fatalf("status /readyz Red diharapkan 503, didapat %d (%s)", recRed.Code, recRed.Body.String())
	}
	var errBody ErrorBody
	if err := json.Unmarshal(recRed.Body.Bytes(), &errBody); err != nil || errBody.Error.Code != "not_ready" {
		t.Fatalf("body /readyz Red harus berkode not_ready, didapat %s", recRed.Body.String())
	}
}

