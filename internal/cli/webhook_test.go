package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
)

func startFakeControlUnixServer(t *testing.T, h http.Handler) *control.Client {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "ctrl.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Skipf("unix domain socket tidak tersedia di lingkungan uji: %v", err)
	}
	srv := &http.Server{Handler: h}
	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = ln.Close()
	})
	return control.NewClient(sock)
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		// Kuras pipe secara paralel agar penulisan > 4 KiB di Windows tidak deadlock.
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	runErr := fn()
	_ = w.Close()
	os.Stdout = old
	<-done
	_ = r.Close()
	return buf.String(), runErr
}

// TestCLIWebhookSetResumeDanDLQValidation memverifikasi validasi flag CLI serta request control yang dikirim untuk --from, resume, dan dlq.
func TestCLIWebhookSetResumeDanDLQValidation(t *testing.T) {
	ctx := context.Background()

	// Nilai --from yang tidak valid harus ditolak sebelum mengirim request.
	err := runWebhookSet(ctx, nil, []string{"sales", "https://example.com/wh", "--from", "yesterday"})
	if err == nil || !strings.Contains(err.Error(), "now") {
		t.Fatalf("diharapkan error validasi --from, didapat: %v", err)
	}

	// dlq tanpa subaksi dan dlq replay tanpa --all/--seq harus gagal.
	if err := runWebhookDLQ(ctx, nil, []string{"sales"}); err == nil {
		t.Fatal("diharapkan error penggunaan dlq tanpa aksi")
	}
	if err := runWebhookDLQReplay(ctx, nil, "sales", nil); err == nil {
		t.Fatal("diharapkan error replay tanpa --all atau --seq")
	}

	var gotPatch map[string]any
	var gotReplay map[string]any
	var resumeCalled, purgeCalled bool

	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /control/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotPatch)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":             r.PathValue("id"),
			"webhook_url":    gotPatch["webhook_url"],
			"webhook_events": gotPatch["webhook_events"],
		})
	})
	mux.HandleFunc("POST /control/instances/{id}/webhook/resume", func(w http.ResponseWriter, r *http.Request) {
		resumeCalled = true
		_ = json.NewEncoder(w).Encode(map[string]any{"id": r.PathValue("id")})
	})
	mux.HandleFunc("GET /control/instances/{id}/webhook/dlq", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{
				"instance_id": r.PathValue("id"),
				"seq":         7,
				"attempts":    3,
				"last_status": 404,
				"last_error":  "HTTP 404",
				"dead_at":     time.Now().UnixMilli(),
				"event_type":  "message",
			},
		})
	})
	mux.HandleFunc("POST /control/instances/{id}/webhook/dlq/replay", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotReplay)
		_ = json.NewEncoder(w).Encode(map[string]any{"replayed": 1, "failed": 0, "seqs": []int64{7}})
	})
	mux.HandleFunc("DELETE /control/instances/{id}/webhook/dlq", func(w http.ResponseWriter, r *http.Request) {
		purgeCalled = true
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "purged", "purged": 1})
	})

	ctrl := startFakeControlUnixServer(t, mux)

	outSet, err := captureStdout(t, func() error {
		return runWebhookSet(ctx, ctrl, []string{"sales", "https://example.com/hook", "--events", "message", "--from", "retained"})
	})
	if err != nil {
		t.Fatalf("runWebhookSet --from retained gagal: %v", err)
	}
	if gotPatch["webhook_from"] != "retained" || !strings.Contains(outSet, "sales") {
		t.Fatalf("payload/output webhook set tidak sesuai: patch=%+v out=%q", gotPatch, outSet)
	}

	if _, err := captureStdout(t, func() error {
		return runWebhookResume(ctx, ctrl, []string{"sales"})
	}); err != nil || !resumeCalled {
		t.Fatalf("runWebhookResume gagal: err=%v called=%v", err, resumeCalled)
	}

	outList, err := captureStdout(t, func() error {
		return runWebhookDLQ(ctx, ctrl, []string{"sales", "ls"})
	})
	if err != nil || !strings.Contains(outList, "seq=7") {
		t.Fatalf("runWebhookDLQ ls tidak sesuai: err=%v out=%q", err, outList)
	}

	if _, err := captureStdout(t, func() error {
		return runWebhookDLQ(ctx, ctrl, []string{"sales", "replay", "--seq", "7"})
	}); err != nil || gotReplay["seq"] != float64(7) {
		t.Fatalf("runWebhookDLQ replay gagal: err=%v body=%+v", err, gotReplay)
	}

	if _, err := captureStdout(t, func() error {
		return runWebhookDLQ(ctx, ctrl, []string{"sales", "purge"})
	}); err != nil || !purgeCalled {
		t.Fatalf("runWebhookDLQ purge gagal: err=%v called=%v", err, purgeCalled)
	}
}

// TestCLIConsumeEventStreamSSE memverifikasi parsing frame SSE dan pengabaian komentar heartbeat di `events --follow`.
func TestCLIConsumeEventStreamSSE(t *testing.T) {
	stream := strings.Join([]string{
		": ping",
		"",
		"id: 11",
		"event: message",
		`data: {"id":"11","event":"message","timestamp":1794360100,"data":{"chat":"6281@s.whatsapp.net","text":"uji"}}`,
		"",
		"id: 12",
		"event: receipt",
		`data: {"id":"12","event":"receipt","timestamp":1794360105,"data":{"type":"read"}}`,
		"",
		"",
	}, "\n")

	out, err := captureStdout(t, func() error {
		return consumeCLIEventStream(context.Background(), strings.NewReader(stream), true)
	})
	if err != nil {
		t.Fatalf("consumeCLIEventStream gagal: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("diharapkan 2 baris NDJSON output, didapat %d (%q)", len(lines), out)
	}
	if !strings.Contains(lines[0], `"id":"11"`) || !strings.Contains(lines[1], `"id":"12"`) {
		t.Fatalf("isi output stream SSE tidak sesuai: %q", out)
	}
}
