package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type sseFrame struct {
	ID    string
	Event string
	Data  string
}

func readSSEFrames(r *bufio.Reader, count int, deadline time.Duration) ([]sseFrame, error) {
	ch := make(chan sseFrame, count)
	errCh := make(chan error, 1)
	go func() {
		var cur sseFrame
		for collected := 0; collected < count; {
			line, err := r.ReadString('\n')
			if err != nil {
				errCh <- err
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if cur.Data != "" || cur.ID != "" || cur.Event != "" {
					ch <- cur
					cur = sseFrame{}
					collected++
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				// Abaikan komentar heartbeat SSE
				continue
			}
			switch {
			case strings.HasPrefix(line, "id:"):
				cur.ID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			case strings.HasPrefix(line, "event:"):
				cur.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				cur.Data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
		errCh <- nil
	}()

	frames := make([]sseFrame, 0, count)
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for len(frames) < count {
		select {
		case f := <-ch:
			frames = append(frames, f)
		case err := <-errCh:
			if err != nil {
				return frames, err
			}
		case <-timer.C:
			return frames, fmt.Errorf("timeout membaca %d frame SSE (baru dapat %d)", count, len(frames))
		}
	}
	return frames, nil
}

func setupSSETestServer(t *testing.T, opts ...ServerOption) (http.Handler, *journal.Journal, string) {
	t.Helper()
	ctx := context.Background()
	tmp := t.TempDir()

	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatalf("auth.NewManager gagal: %v", err)
	}
	waMgr, err := wa.NewManager(ctx, cfg, st, nil)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })

	if _, err := waMgr.Create(ctx, "sales", 20, false); err != nil {
		t.Fatalf("waMgr.Create gagal: %v", err)
	}

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns:    2,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	key, _, err := authMgr.Create(ctx, "sse-client", "sales")
	if err != nil {
		t.Fatalf("authMgr.Create gagal: %v", err)
	}

	allOpts := append([]ServerOption{
		WithDB(st.DB),
		WithJournal(j),
		WithGovernor(governor.AlwaysGreen),
	}, opts...)
	h := NewHandler(authMgr, waMgr, allOpts...)
	return h, j, key
}

// TestSSE_ResumeLastEventID memastikan SSE mengirim backlog setelah Last-Event-ID lalu mengalirkan event live berurutan.
func TestSSE_ResumeLastEventID(t *testing.T) {
	h, j, key := setupSSETestServer(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		if _, err := j.Append(ctx, &journal.Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       "628111@s.whatsapp.net",
			TS:         1794360000 + int64(i),
			Payload:    []byte(fmt.Sprintf(`{"text":"msg-%d"}`, i)),
		}); err != nil {
			t.Fatalf("Append backlog %d gagal: %v", i, err)
		}
	}

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, srv.URL+"/v2/instances/sales/events/stream", nil)
	req.Header.Set("X-API-Key", key)
	req.Header.Set("Last-Event-ID", "2")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("buka stream SSE gagal: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status SSE diharapkan 200, didapat %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type SSE tidak valid: %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control diharapkan no-store, didapat %q", cc)
	}
	if xab := resp.Header.Get("X-Accel-Buffering"); xab != "no" {
		t.Fatalf("X-Accel-Buffering diharapkan no, didapat %q", xab)
	}

	br := bufio.NewReader(resp.Body)
	backlog, err := readSSEFrames(br, 3, 3*time.Second)
	if err != nil {
		t.Fatalf("baca backlog SSE gagal: %v", err)
	}
	wantBacklog := []string{"3", "4", "5"}
	for idx, wantID := range wantBacklog {
		if backlog[idx].ID != wantID || backlog[idx].Event != "message" {
			t.Fatalf("frame backlog[%d] diharapkan id=%s event=message, didapat %+v", idx, wantID, backlog[idx])
		}
	}

	// Tambahkan event live seq=6 -> harus segera diterima via Subscribe.
	if _, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "receipt",
		Chat:       "628111@s.whatsapp.net",
		TS:         1794360100,
		Payload:    []byte(`{"status":"read"}`),
	}); err != nil {
		t.Fatalf("Append event live gagal: %v", err)
	}

	live, err := readSSEFrames(br, 1, 3*time.Second)
	if err != nil {
		t.Fatalf("baca event live SSE gagal: %v", err)
	}
	if live[0].ID != "6" || live[0].Event != "receipt" {
		t.Fatalf("frame live diharapkan id=6 event=receipt, didapat %+v", live[0])
	}
}

// TestSSE_Limit429 memverifikasi semaphore global WAAPI_SSE_MAX mengembalikan 429 sse_limit + Retry-After: 10.
func TestSSE_Limit429(t *testing.T) {
	t.Setenv("WAAPI_SSE_MAX", "1")
	h, _, key := setupSSETestServer(t, WithSSEMax(1))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req1Ctx, cancel1 := context.WithCancel(context.Background())
	req1, _ := http.NewRequestWithContext(req1Ctx, http.MethodGet, srv.URL+"/v2/instances/sales/events/stream", nil)
	req1.Header.Set("X-API-Key", key)
	resp1, err := srv.Client().Do(req1)
	if err != nil {
		t.Fatalf("koneksi SSE pertama gagal: %v", err)
	}
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("koneksi SSE pertama diharapkan 200, didapat %d", resp1.StatusCode)
	}

	// Klien kedua saat slot penuh harus langsung ditolak 429 sse_limit.
	req2, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/v2/instances/sales/events/stream", nil)
	req2.Header.Set("X-API-Key", key)
	resp2, err := srv.Client().Do(req2)
	if err != nil {
		t.Fatalf("request klien kedua gagal: %v", err)
	}
	var errBody ErrorBody
	_ = json.NewDecoder(resp2.Body).Decode(&errBody)
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("klien kedua diharapkan HTTP 429, didapat %d", resp2.StatusCode)
	}
	if errBody.Error.Code != "sse_limit" || resp2.Header.Get("Retry-After") != "10" {
		t.Fatalf("respons 429 sse_limit / Retry-After tidak sesuai: header=%q body=%+v", resp2.Header.Get("Retry-After"), errBody)
	}

	// Tutup klien pertama dan pastikan slot kembali bebas untuk klien ketiga.
	cancel1()
	resp1.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		req3Ctx, cancel3 := context.WithCancel(context.Background())
		req3, _ := http.NewRequestWithContext(req3Ctx, http.MethodGet, srv.URL+"/v2/instances/sales/events/stream", nil)
		req3.Header.Set("X-API-Key", key)
		resp3, err := srv.Client().Do(req3)
		if err == nil && resp3.StatusCode == http.StatusOK {
			cancel3()
			resp3.Body.Close()
			return
		}
		if resp3 != nil {
			resp3.Body.Close()
		}
		cancel3()
		if time.Now().After(deadline) {
			t.Fatal("slot semaphore SSE tidak dilepas setelah klien pertama putus")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type fakeSSEJournal struct {
	events []journal.Event
}

func (f *fakeSSEJournal) HeadSeq(_ context.Context, _ string) (int64, error) {
	if len(f.events) == 0 {
		return 0, nil
	}
	return f.events[len(f.events)-1].Seq, nil
}

func (f *fakeSSEJournal) Read(_ context.Context, q journal.Query) (journal.ReadResult, error) {
	out := make([]journal.Event, 0)
	for _, ev := range f.events {
		if ev.Seq > q.AfterSeq {
			out = append(out, ev)
		}
	}
	head, _ := f.HeadSeq(context.Background(), q.InstanceID)
	return journal.ReadResult{Events: out, ScannedTo: head}, nil
}

func (f *fakeSSEJournal) Subscribe(_ string) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() { close(ch) }
}

// blockAfterWriter menyimulasikan klien lambat yang sukses pada write awal lalu memblokir penulisan berikutnya sampai deadline memutus.
type blockAfterWriter struct {
	header        http.Header
	wroteFirst    bool
	deadlineErr   bool
	writeDeadline time.Time
}

func (w *blockAfterWriter) Header() http.Header         { return w.header }
func (w *blockAfterWriter) WriteHeader(int)             {}
func (w *blockAfterWriter) Flush()                      {}
func (w *blockAfterWriter) SetWriteDeadline(t time.Time) error {
	w.writeDeadline = t
	return nil
}

func (w *blockAfterWriter) Write(p []byte) (int, error) {
	if !w.wroteFirst {
		w.wroteFirst = true
		return len(p), nil
	}
	if !w.writeDeadline.IsZero() {
		time.Sleep(time.Until(w.writeDeadline))
		w.deadlineErr = true
		return 0, errors.New("i/o timeout")
	}
	return len(p), nil
}

// TestSSE_KlienLambatDiputus memastikan write deadline memutus klien SSE yang berhenti membaca tanpa membocorkan goroutine handler.
func TestSSE_KlienLambatDiputus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fj := &fakeSSEJournal{
			events: []journal.Event{
				{
					Seq:        1,
					InstanceID: "sales",
					Type:       "message",
					TS:         1794360001,
					Payload:    []byte(`{"text":"pertama"}`),
				},
				{
					Seq:        2,
					InstanceID: "sales",
					Type:       "message",
					TS:         1794360002,
					Payload:    []byte(`{"text":"macet"}`),
				},
			},
		}

		sem := make(chan struct{}, 1)
		sem <- struct{}{} // slot diambil seperti oleh handler

		bw := &blockAfterWriter{header: make(http.Header)}
		reqCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			defer func() {
				<-sem
				close(done)
			}()
			serveSSEStream(reqCtx, bw, fj, "sales", 0, nil, 25*time.Second, 30*time.Second)
		}()

		// Tunggu sampai waktu sintetik memicu write deadline 30 detik pada frame ke-2.
		<-done
		if !bw.deadlineErr {
			t.Fatal("diharapkan error deadline saat menulis ke klien lambat")
		}
		if len(sem) != 0 {
			t.Fatalf("semaphore SSE harus dilepas saat klien lambat diputus, tersisa %d", len(sem))
		}
	})
}
