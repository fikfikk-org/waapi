package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

// TestDispatcherAndOutboxSpill menguji pengiriman HMAC dan penyimpanan ke outbox saat server tujuan gagal.
func TestDispatcherAndOutboxSpill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	var receivedSig atomic.Value
	shouldFail := atomic.Bool{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig.Store(r.Header.Get("X-WA-Signature"))
		if shouldFail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := st.CreateInstance(ctx, store.Instance{
		ID:            "toko",
		WebhookURL:    srv.URL,
		WebhookSecret: "whsec_test123",
		WebhookEvents: "message,connection",
		RatePerMin:    20,
		AutoStart:     true,
	}); err != nil {
		t.Fatalf("CreateInstance gagal: %v", err)
	}

	d := NewDispatcher(st, func(ctx context.Context, instanceID string) (string, string, string, error) {
		inst, err := st.GetInstance(ctx, instanceID)
		if err != nil {
			return "", "", "", err
		}
		return inst.WebhookURL, inst.WebhookSecret, inst.WebhookEvents, nil
	}, 1)
	d.Start(ctx)
	defer func() {
		cancel()
		d.Stop()
	}()

	// 1. Test sinkron berhasil dan signature HMAC terpasang
	code, err := d.SendTest(ctx, "toko")
	if err != nil || code != http.StatusOK {
		t.Fatalf("SendTest gagal: code=%d, err=%v", code, err)
	}
	sig, _ := receivedSig.Load().(string)
	if len(sig) < 10 || sig[:7] != "sha256=" {
		t.Fatalf("signature HMAC tidak valid: %q", sig)
	}

	// 2. Saat server 500, event harus masuk ke webhook_outbox di SQLite
	shouldFail.Store(true)
	d.Enqueue("toko", "message", map[string]any{"text": "uji outbox"})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n, _ := st.CountOutbox(ctx)
		if n == 1 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("event gagal tidak masuk ke webhook_outbox dalam batas waktu")
}
