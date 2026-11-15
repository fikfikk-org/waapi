package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
)

type stubCtrlExecutor struct {
	paired bool
}

func (s stubCtrlExecutor) Ready(string) bool { return s.paired }

func (s stubCtrlExecutor) NewMessageID(string) (string, error) {
	if !s.paired {
		return "", outbox.ErrNotPaired
	}
	return "3EB0CTRL001", nil
}

func (s stubCtrlExecutor) Execute(context.Context, *outbox.Job) (outbox.ExecResult, error) {
	return outbox.ExecResult{ServerTS: time.Now()}, nil
}

// TestControlSendMessage memverifikasi endpoint privat control POST /control/instances/{id}/messages untuk antrian dan replay idempoten.
func TestControlSendMessage(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.CreateInstance(ctx, store.Instance{
		ID:        "sales",
		Status:    store.StatusDisconnected,
		Settings:  store.Defaults(),
		RatePerMin: 20,
	}); err != nil {
		t.Fatalf("CreateInstance gagal: %v", err)
	}

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns: 2,
		CacheKiB: 512,
		TxLock:   "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	outSvc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, stubCtrlExecutor{paired: true}, nil, nil, outbox.Options{})
	srv := NewServer(cfg, st, nil, nil, nil, WithJournal(j), WithOutbox(outSvc))
	h := srv.Handler()

	sendReq := func(body any) *httptest.ResponseRecorder {
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(body)
		req := httptest.NewRequest(http.MethodPost, "/control/instances/sales/messages", &buf)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	rec1 := sendReq(map[string]any{
		"to":       "6281234567890",
		"text":     "halo cli",
		"idem_key": "cli-1",
	})
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("diharapkan 202, dapat %d body=%s", rec1.Code, rec1.Body.String())
	}

	rec2 := sendReq(map[string]any{
		"to":       "6281234567890",
		"text":     "halo cli",
		"idem_key": "cli-1",
	})
	if rec2.Code != http.StatusOK {
		t.Fatalf("replay idempoten harus 200, dapat %d body=%s", rec2.Code, rec2.Body.String())
	}

	recBad := sendReq(map[string]any{
		"to":   "6281234567890",
		"text": "halo",
		"wait": "invalid",
	})
	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("wait tidak valid harus 400, dapat %d", recBad.Code)
	}
}
