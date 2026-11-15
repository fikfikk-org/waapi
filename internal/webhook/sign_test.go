package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

// TestSignStandard_VektorSpesifikasi memverifikasi tanda tangan Standard Webhooks terhadap vektor resmi spesifikasi.
func TestSignStandard_VektorSpesifikasi(t *testing.T) {
	secret := "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	id := "msg_p5jXN8AQM9LWM0D4loKWxJek"
	ts := int64(1614265330)
	body := []byte(`{"test": 2432232314}`)

	got, err := SignStandard(secret, id, ts, body)
	if err != nil {
		t.Fatalf("SignStandard gagal: %v", err)
	}
	const want = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
	if got != want {
		t.Fatalf("signature Standard Webhooks tidak sesuai:\ndapat: %s\ningin: %s", got, want)
	}

	// Secret v1 (whsec_ + 48 hex = valid base64) harus tetap bisa dipakai langsung di mode standard.
	v1Secret := "whsec_0123456789abcdef0123456789abcdef0123456789abcdef"
	v1Sig, err := SignStandard(v1Secret, id, ts, body)
	if err != nil {
		t.Fatalf("SignStandard dengan secret v1 gagal: %v", err)
	}
	if !strings.HasPrefix(v1Sig, "v1,") {
		t.Fatalf("format prefix signature standard tidak valid: %q", v1Sig)
	}

	// Secret baru V2 dari auth.GenerateSecret harus berupa whsec_ + base64 dari 32 byte acak dan valid di kedua mode.
	genSecret, err := auth.GenerateSecret()
	if err != nil {
		t.Fatalf("auth.GenerateSecret gagal: %v", err)
	}
	if !strings.HasPrefix(genSecret, "whsec_") {
		t.Fatalf("secret baru harus berawalan whsec_: %q", genSecret)
	}
	rawKey, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(genSecret, "whsec_"))
	if err != nil {
		t.Fatalf("bagian setelah whsec_ harus base64 valid: %v (secret=%q)", err, genSecret)
	}
	if len(rawKey) != 32 {
		t.Fatalf("panjang kunci hasil decode harus 32 byte, didapat %d byte", len(rawKey))
	}
	if _, err := SignStandard(genSecret, id, ts, body); err != nil {
		t.Fatalf("SignStandard dengan secret baru V2 gagal: %v", err)
	}
}

// TestSignV1_TidakBerubah memastikan fungsi Sign v1 menghasilkan signature hex yang persis sama dengan fixture v1.
func TestSignV1_TidakBerubah(t *testing.T) {
	secret := "whsec_test_fixture_v1"
	ts := int64(1700000000)
	body := []byte(`{"event":"message","instance":"sales","timestamp":1700000000,"data":{"text":"halo"}}`)

	got := Sign(secret, ts, body)

	// Fixture v1 independen: sha256=<hex HMAC-SHA256(secret, "<ts>.<body>")>
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("1700000000."))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if got != want {
		t.Fatalf("Sign v1 berubah!\ndapat: %s\ningin: %s", got, want)
	}
}

// TestDeliverer_StandardFormat memverifikasi pengiriman mode standard memakai header webhook-id, webhook-timestamp, webhook-signature yang valid.
func TestDeliverer_StandardFormat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	secret := "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	type capturedReq struct {
		whID   string
		whTS   string
		whSig  string
		waSign string
		body   []byte
	}
	gotCh := make(chan capturedReq, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		gotCh <- capturedReq{
			whID:   r.Header.Get("webhook-id"),
			whTS:   r.Header.Get("webhook-timestamp"),
			whSig:  r.Header.Get("webhook-signature"),
			waSign: r.Header.Get("X-WA-Signature"),
			body:   raw,
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fj := newFakeJournal()
	seq := fj.appendEvent(journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "6281@s.whatsapp.net",
		TS:         1794360100,
		Payload:    []byte(`{"id":"MSG-STD","text":"halo standard"}`),
	})

	tc := NewTargetCache()
	tc.Set("sales", Target{
		URL:         srv.URL,
		Secret:      secret,
		EventsCSV:   "*",
		Format:      "standard",
		Lanes:       1,
		MaxAttempts: 3,
	})

	svc := newServiceForTest(fj, tc, &fakeDeadStore{}, &fakePauser{}, srv.Client(), nil, Options{
		CommitEvery:    1,
		CommitInterval: 100 * time.Millisecond,
	})
	svc.Start(ctx, []string{"sales"})
	defer svc.Stop()

	select {
	case req := <-gotCh:
		if req.waSign != "" {
			t.Fatalf("mode standard tidak boleh mengirim X-WA-Signature, didapat %q", req.waSign)
		}
		wantID := "sales_" + strconv.FormatInt(seq, 10)
		if req.whID != wantID {
			t.Fatalf("webhook-id tidak sesuai: dapat %q, ingin %q", req.whID, wantID)
		}
		tsInt, err := strconv.ParseInt(req.whTS, 10, 64)
		if err != nil || tsInt <= 0 {
			t.Fatalf("webhook-timestamp tidak valid: %q (err=%v)", req.whTS, err)
		}
		wantSig, err := SignStandard(secret, req.whID, tsInt, req.body)
		if err != nil {
			t.Fatalf("SignStandard gagal: %v", err)
		}
		if req.whSig != wantSig {
			t.Fatalf("webhook-signature tidak cocok: dapat %q, ingin %q", req.whSig, wantSig)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout menunggu pengiriman webhook format standard")
	}
}

// TestDLQ_ReplayDanPurge menguji Service.Replay serta DeadStore.List dan DeadStore.Purge terhadap SQLite nyata.
func TestDLQ_ReplayDanPurge(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns: 2, CacheKiB: 512, IdleTimeout: time.Minute, TxLock: "immediate", Sync: "NORMAL",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	r1, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "62811@s.whatsapp.net",
		WAID:       "DLQ1",
		TS:         1794360100,
		Payload:    []byte(`{"id":"DLQ1","text":"satu"}`),
	})
	if err != nil {
		t.Fatalf("Append seq1 gagal: %v", err)
	}
	seq1 := r1.Seq
	r2, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "62812@s.whatsapp.net",
		WAID:       "DLQ2",
		TS:         1794360101,
		Payload:    []byte(`{"id":"DLQ2","text":"dua"}`),
	})
	if err != nil {
		t.Fatalf("Append seq2 gagal: %v", err)
	}
	seq2 := r2.Seq

	dead := NewDeadStore(j.DB())
	if err := dead.Record(ctx, DeadEntry{
		InstanceID: "sales", Seq: seq1, Attempts: 3, LastStatus: 404, LastError: "HTTP 404",
	}); err != nil {
		t.Fatalf("Record seq1 gagal: %v", err)
	}
	if err := dead.Record(ctx, DeadEntry{
		InstanceID: "sales", Seq: seq2, Attempts: 3, LastStatus: 500, LastError: "HTTP 500",
	}); err != nil {
		t.Fatalf("Record seq2 gagal: %v", err)
	}

	entries, err := dead.List(ctx, "sales", 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("DeadStore.List harus berisi 2 entri, didapat %d (err=%v)", len(entries), err)
	}

	// Server mengembalikan 200 untuk seq1 dan 500 untuk seq2 saat replay pertama.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-WA-Event-Id") == strconv.FormatInt(seq1, 10) {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	tc := NewTargetCache()
	tc.Set("sales", Target{
		URL:         srv.URL,
		Secret:      "whsec_dlq_test",
		EventsCSV:   "*",
		Format:      "waapi",
		Lanes:       2,
		MaxAttempts: 5,
	})
	svc := NewService(j, tc, dead, srv.Client(), nil, Options{})

	res, err := svc.Replay(ctx, "sales", []int64{seq1, seq2})
	if err != nil {
		t.Fatalf("Service.Replay gagal: %v", err)
	}
	if res.Replayed != 1 || res.Failed != 1 || len(res.Seqs) != 1 || res.Seqs[0] != seq1 {
		t.Fatalf("hasil replay tidak sesuai: %+v", res)
	}

	// seq1 harus hilang dari DLQ sementara seq2 masih tersisa.
	left, err := dead.List(ctx, "sales", 10)
	if err != nil || len(left) != 1 || left[0].Seq != seq2 {
		t.Fatalf("sisa DLQ setelah replay harus seq2 saja, didapat %+v (err=%v)", left, err)
	}

	purged, err := dead.Purge(ctx, "sales")
	if err != nil || purged != 1 {
		t.Fatalf("DeadStore.Purge harus menghapus 1 entri, didapat %d (err=%v)", purged, err)
	}
	cnt, err := dead.Count(ctx, "sales")
	if err != nil || cnt != 0 {
		t.Fatalf("DLQ harus kosong setelah purge, didapat count=%d (err=%v)", cnt, err)
	}
}
