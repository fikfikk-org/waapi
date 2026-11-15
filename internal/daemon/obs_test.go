package daemon

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/httpapi"
	"github.com/fikfikk-org/waapi/internal/obs"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/fikfikk-org/waapi/internal/webhook"
)

// TestObservabilityEndpoints memverifikasi /control/doctor?deep=1, /control/debug/pprof/*, /metrics, serta isolasi port publik.
func TestObservabilityEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	cfg := &config.Config{
		DataDir:  tmpDir,
		HTTPAddr: "127.0.0.1:0",
	}
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
	wh := webhook.NewDispatcher(st, func(context.Context, string) (string, string, string, error) {
		return "", "", "", nil
	}, 1)
	waMgr, err := wa.NewManager(ctx, cfg, st, wh)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	defer waMgr.Close()

	ctrlSrv := control.NewServer(cfg, st, authMgr, waMgr, wh)
	ctrlSrv.SetVersion("2.0.0-test")
	ctrlHandler := ctrlSrv.Handler()

	// 1. Pastikan GET /control/doctor?deep=1 mengembalikan DeepReport lengkap
	recDeep := httptest.NewRecorder()
	ctrlHandler.ServeHTTP(recDeep, httptest.NewRequest(http.MethodGet, "/control/doctor?deep=1", nil))
	if recDeep.Code != http.StatusOK {
		t.Fatalf("/control/doctor?deep=1 gagal HTTP %d: %s", recDeep.Code, recDeep.Body.String())
	}
	var deep obs.DeepReport
	if err := json.Unmarshal(recDeep.Body.Bytes(), &deep); err != nil {
		t.Fatalf("decode DeepReport gagal: %v", err)
	}
	if deep.Runtime.Goroutines == 0 || deep.Build.Version != "2.0.0-test" || len(deep.SQLite) < 2 {
		t.Fatalf("isi DeepReport tidak lengkap: %+v", deep)
	}

	// 2. Pastikan /control/debug/pprof/heap menghasilkan profil pprof gzip yang valid dan terbaca oleh go tool pprof
	recPprof := httptest.NewRecorder()
	ctrlHandler.ServeHTTP(recPprof, httptest.NewRequest(http.MethodGet, "/control/debug/pprof/heap", nil))
	if recPprof.Code != http.StatusOK {
		t.Fatalf("/control/debug/pprof/heap gagal HTTP %d", recPprof.Code)
	}
	gz, err := gzip.NewReader(recPprof.Body)
	if err != nil {
		t.Fatalf("pprof heap bukan stream gzip valid: %v", err)
	}
	rawPprof, err := io.ReadAll(gz)
	_ = gz.Close()
	if err != nil || len(rawPprof) == 0 {
		t.Fatalf("baca isi gzip pprof gagal: %v (len=%d)", err, len(rawPprof))
	}

	// Validasi tambahan langsung via `go tool pprof -top`
	profPath := filepath.Join(tmpDir, "heap.pb.gz")
	// Tulis ulang gzip utuh dari HTTP recorder dengan request segar
	recPprof2 := httptest.NewRecorder()
	ctrlHandler.ServeHTTP(recPprof2, httptest.NewRequest(http.MethodGet, "/control/debug/pprof/heap", nil))
	if err := os.WriteFile(profPath, recPprof2.Body.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("go", "tool", "pprof", "-top", "-nodecount=5", profPath).CombinedOutput()
	if err != nil {
		t.Fatalf("go tool pprof gagal membaca %s: %v\n%s", profPath, err, string(out))
	}

	// 3. Pastikan pprof TIDAK terekspos pada router HTTP publik
	pub := httpapi.NewHandler(authMgr, waMgr, httpapi.WithDB(st.DB))
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/control/debug/pprof/heap"} {
		recPub := httptest.NewRecorder()
		pub.ServeHTTP(recPub, httptest.NewRequest(http.MethodGet, path, nil))
		if recPub.Code != http.StatusNotFound {
			t.Errorf("path publik %s seharusnya 404, didapat %d", path, recPub.Code)
		}
	}

	// 4. Pastikan handler /metrics berisi seluruh 9 metrik awal Prometheus
	mHandler := newMetricsHandler(cfg, st, waMgr)
	recMetrics := httptest.NewRecorder()
	mHandler.ServeHTTP(recMetrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recMetrics.Code != http.StatusOK {
		t.Fatalf("/metrics gagal HTTP %d", recMetrics.Code)
	}
	body := recMetrics.Body.String()
	wantMetrics := []string{
		"waapi_process_rss_bytes",
		"waapi_process_anon_bytes",
		"waapi_go_heap_objects_bytes",
		"waapi_go_goroutines",
		"waapi_go_gc_cycles_total",
		"waapi_cgroup_memory_current_bytes",
		"waapi_cgroup_memory_pressure_some_avg10",
		"waapi_sqlite_cache_used_bytes{",
		"waapi_instances{",
	}
	for _, m := range wantMetrics {
		if !strings.Contains(body, m) {
			t.Errorf("/metrics tidak memuat %q:\n%s", m, body)
		}
	}

	// 5. Verifikasi deteksi loopback untuk warning listener non-loopback
	if !isLoopbackListenAddr("127.0.0.1:9108") || !isLoopbackListenAddr("localhost:9108") || !isLoopbackListenAddr("[::1]:9108") {
		t.Error("alamat loopback seharusnya terdeteksi sebagai loopback")
	}
	if isLoopbackListenAddr("0.0.0.0:9108") || isLoopbackListenAddr(":9108") {
		t.Error("alamat non-loopback tidak boleh terdeteksi sebagai loopback")
	}
}
