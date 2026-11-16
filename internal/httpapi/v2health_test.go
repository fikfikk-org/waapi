package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/guardian"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type fakeHealthDriver struct {
	rep guardian.HealthReport
	err error
}

func (f *fakeHealthDriver) Health(_ context.Context, _ string) (guardian.HealthReport, error) {
	if f.err != nil {
		return guardian.HealthReport{}, f.err
	}
	return f.rep, nil
}

// TestHealthAPI_Kontrak memverifikasi kontrak GET /v2/instances/{id}/health, penambahan field health di GET /v1/instances/{id}, dan kode 409/403/429 Precheck di POST /messages.
func TestHealthAPI_Kontrak(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := st.CreateInstance(ctx, store.Instance{
		ID:     "sales",
		JID:    "628111@s.whatsapp.net",
		Status: store.StatusConnected,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateInstance(ctx, store.Instance{
		ID:       "promo",
		JID:      "628222@s.whatsapp.net",
		Status:   store.StatusConnected,
		Settings: func() store.InstanceSettings { s := store.Defaults(); s.GuardianProfile = guardian.ProfileStrict; return s }(),
	}); err != nil {
		t.Fatal(err)
	}

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()

	authMgr, err := auth.NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	rawKey, _, err := authMgr.Create(ctx, "test", "")
	if err != nil {
		t.Fatal(err)
	}

	hd := &fakeHealthDriver{
		rep: guardian.HealthReport{
			Instance: "sales",
			State:    guardian.StateCautious,
			Score:    58,
			Profile:  guardian.ProfileStandard,
			Shadow:   false,
			Factors: []guardian.Factor{
				{Code: "undelivered_ratio", Impact: -22, Detail: "38% pesan 24 jam terakhir belum delivered"},
				{Code: "reply_rate", Impact: 5, Detail: "25% penerima membalas"},
			},
			Counters: guardian.HealthCounters{Sent24h: 140, FirstContact24h: 61, Delivered24h: 80, Replied24h: 12},
			Limits:   guardian.HealthLimits{DailyCap: 1000, DailyRemaining: 860, FirstContactCap: 50, FirstContactRemaining: 0, WarmupFactor: 1.0},
			Ban:      guardian.HealthBan{Active: false},
		},
	}

	waMgr, err := wa.NewManager(ctx, cfg, st, nil, wa.WithJournal(j), wa.WithHealthSummary(func(ctx context.Context, id string) (*wa.HealthSummary, error) {
		r, err := hd.Health(ctx, id)
		if err != nil {
			return nil, err
		}
		return &wa.HealthSummary{State: r.State, Score: r.Score}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = waMgr.Close() }()

	srv := NewServer(authMgr, waMgr, WithHealthDriver(hd))

	// 1. GET /v2/instances/sales/health harus memuat seluruh field kontrak.
	req := httptest.NewRequest(http.MethodGet, "/v2/instances/sales/health", nil)
	req.Header.Set("X-API-Key", rawKey)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health diharapkan 200, didapat %d (%s)", rec.Code, rec.Body.String())
	}
	var got guardian.HealthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode HealthReport gagal: %v", err)
	}
	if got.Instance != "sales" || got.State != "cautious" || got.Score != 58 || len(got.Factors) != 2 || got.Factors[0].Impact > got.Factors[1].Impact {
		t.Fatalf("HealthReport tidak sesuai kontrak: %+v", got)
	}

	// 2. GET /v1/instances/sales harus memiliki field aditif health { state, score }.
	reqV1 := httptest.NewRequest(http.MethodGet, "/v1/instances/sales", nil)
	reqV1.Header.Set("X-API-Key", rawKey)
	recV1 := httptest.NewRecorder()
	srv.ServeHTTP(recV1, reqV1)
	if recV1.Code != http.StatusOK {
		t.Fatalf("GET /v1/instances/sales diharapkan 200, didapat %d", recV1.Code)
	}
	var v1Map map[string]any
	_ = json.Unmarshal(recV1.Body.Bytes(), &v1Map)
	hObj, ok := v1Map["health"].(map[string]any)
	if !ok || hObj["state"] != "cautious" || int(hObj["score"].(float64)) != 58 {
		t.Fatalf("field health pada /v1/instances/{id} tidak sesuai: %+v", v1Map["health"])
	}

	// 3. Uji Precheck guardian nyata lewat POST /v2/instances/sales/messages untuk kode 409 / 403 / 429.
	metaAdapter := &httpTestMetaSource{st: st}
	gReal, err := guardian.New(j.DB(), guardian.Options{Settings: metaAdapter})
	if err != nil {
		t.Fatal(err)
	}
	outSvc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, waMgr, gReal, nil, outbox.Options{
		DefaultTTL: 24 * time.Hour,
		MaxQueued:  1000,
	})
	srvOut := NewServer(authMgr, waMgr, WithOutbox(outSvc), WithHealthDriver(gReal))

	// Opt-out -> 409 recipient_opted_out
	_ = gReal.Store().UpsertContactDirect(ctx, "sales", "6281110001@s.whatsapp.net", 0, 0, 1)
	rec409 := httptest.NewRecorder()
	req409 := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages", bytes.NewReader([]byte(`{"to":"6281110001","type":"text","text":"Halo"}`)))
	req409.Header.Set("X-API-Key", rawKey)
	srvOut.ServeHTTP(rec409, req409)
	if rec409.Code != http.StatusConflict {
		t.Fatalf("opt-out diharapkan HTTP 409, didapat %d (%s)", rec409.Code, rec409.Body.String())
	}

	// Restricted -> 403 guardian_restricted (memicu rate overlimit + penalti ban).
	_ = gReal.ObserveTempBan(ctx, "sales", 104, 0)
	gReal.RecordGroupCreated(ctx, "sales")
	gReal.Record(ctx, &outbox.Job{InstanceID: "sales", ToJID: "6281110002@s.whatsapp.net"}, outbox.Outcome{RateLimited: true})
	gReal.Record(ctx, &outbox.Job{InstanceID: "sales", ToJID: "6281110002@s.whatsapp.net"}, outbox.Outcome{RateLimited: true})
	rec403 := httptest.NewRecorder()
	req403 := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages", bytes.NewReader([]byte(`{"to":"6281110002","type":"text","text":"Halo"}`)))
	req403.Header.Set("X-API-Key", rawKey)
	srvOut.ServeHTTP(rec403, req403)
	if rec403.Code != http.StatusForbidden {
		t.Fatalf("restricted diharapkan HTTP 403, didapat %d (%s)", rec403.Code, rec403.Body.String())
	}

	// Fan-out penuh -> 429 guardian_limited + header Retry-After.
	day := time.Now().Format("2006-01-02")
	for k := 0; k < 10; k++ {
		_ = gReal.Store().RecordFanout(ctx, "promo", day, guardian.ContentHash("flash sale"), "62890000"+string(rune('0'+k))+"@s.whatsapp.net")
	}
	rec429 := httptest.NewRecorder()
	req429 := httptest.NewRequest(http.MethodPost, "/v2/instances/promo/messages", bytes.NewReader([]byte(`{"to":"6281110099","type":"text","text":"Flash Sale"}`)))
	req429.Header.Set("X-API-Key", rawKey)
	srvOut.ServeHTTP(rec429, req429)
	if rec429.Code != http.StatusTooManyRequests || rec429.Header().Get("Retry-After") == "" {
		t.Fatalf("fanout diharapkan HTTP 429 + Retry-After, didapat %d header=%q body=%s", rec429.Code, rec429.Header().Get("Retry-After"), rec429.Body.String())
	}
}

type httpTestMetaSource struct {
	st *store.Store
}

func (m *httpTestMetaSource) InstanceMeta(ctx context.Context, id string) (store.Instance, bool) {
	it, err := m.st.GetInstance(ctx, id)
	if err != nil || it == nil {
		return store.Instance{}, false
	}
	return *it, true
}

func (m *httpTestMetaSource) ListInstances(ctx context.Context) ([]store.Instance, error) {
	return m.st.ListInstances(ctx)
}

func (m *httpTestMetaSource) SetShadowUntil(ctx context.Context, id string, until int64) error {
	it, err := m.st.GetInstance(ctx, id)
	if err != nil {
		return err
	}
	s := it.Settings
	s.GuardianShadowUntil = until
	_, err = m.st.UpdateInstance(ctx, id, store.InstanceUpdate{Settings: &s})
	return err
}
