package obs

import (
	"bytes"
	"testing"
)

// TestPrometheus_Golden memastikan keluaran WritePrometheus sesuai format Prometheus 0.0.4 secara deterministik.
func TestPrometheus_Golden(t *testing.T) {
	reg := NewRegistry()

	reqTotal := reg.NewCounterVec("requests_total", "Total request per \"rute\"\nbaris2", "route")
	reqTotal.With("/v1/a\\b\"c\nd").Add(3)
	reqTotal.With("/healthz").Inc()

	rss := reg.NewGauge("process_rss_bytes", "RSS proses dalam byte")
	rss.Set(1048576)

	gf := reg.NewGaugeFunc("custom_gauge", "Nilai dinamis", func() float64 { return 42.5 })
	_ = gf

	lat := reg.NewHistogram("http_duration_seconds", "Latensi HTTP dalam detik", []float64{0.1, 0.5, 1})
	lat.Observe(0.05)
	lat.Observe(0.4)
	lat.Observe(2.0)

	var buf bytes.Buffer
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatalf("WritePrometheus gagal: %v", err)
	}

	want := "" +
		"# HELP waapi_custom_gauge Nilai dinamis\n" +
		"# TYPE waapi_custom_gauge gauge\n" +
		"waapi_custom_gauge 42.5\n" +
		"# HELP waapi_http_duration_seconds Latensi HTTP dalam detik\n" +
		"# TYPE waapi_http_duration_seconds histogram\n" +
		"waapi_http_duration_seconds_bucket{le=\"0.1\"} 1\n" +
		"waapi_http_duration_seconds_bucket{le=\"0.5\"} 2\n" +
		"waapi_http_duration_seconds_bucket{le=\"1\"} 2\n" +
		"waapi_http_duration_seconds_bucket{le=\"+Inf\"} 3\n" +
		"waapi_http_duration_seconds_sum 2.45\n" +
		"waapi_http_duration_seconds_count 3\n" +
		"# HELP waapi_process_rss_bytes RSS proses dalam byte\n" +
		"# TYPE waapi_process_rss_bytes gauge\n" +
		"waapi_process_rss_bytes 1048576\n" +
		"# HELP waapi_requests_total Total request per \"rute\"\\nbaris2\n" +
		"# TYPE waapi_requests_total counter\n" +
		"waapi_requests_total{route=\"/healthz\"} 1\n" +
		"waapi_requests_total{route=\"/v1/a\\\\b\\\"c\\nd\"} 3\n"

	if got := buf.String(); got != want {
		t.Fatalf("output Prometheus tidak cocok dengan golden:\n--- GOT ---\n%s--- WANT ---\n%s", got, want)
	}
}

// TestCounter_TanpaAlokasi memastikan Inc, Add, Observe, dan With pada label yang sudah ada tidak mengalokasikan memori.
func TestCounter_TanpaAlokasi(t *testing.T) {
	reg := NewRegistry()
	c := reg.NewCounter("ops_total", "Total operasi")
	cv := reg.NewCounterVec("vec_total", "Total per label", "status")
	_ = cv.With("ok")
	h := reg.NewHistogram("lat_seconds", "Latensi", []float64{0.1, 0.5, 1})

	if allocs := testing.AllocsPerRun(100, func() { c.Inc() }); allocs != 0 {
		t.Errorf("Counter.Inc alokasi = %v, diharapkan 0", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() { c.Add(2.5) }); allocs != 0 {
		t.Errorf("Counter.Add alokasi = %v, diharapkan 0", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() { h.Observe(0.25) }); allocs != 0 {
		t.Errorf("Histogram.Observe alokasi = %v, diharapkan 0", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() { cv.With("ok").Inc() }); allocs != 0 {
		t.Errorf("CounterVec.With('ok').Inc alokasi = %v, diharapkan 0", allocs)
	}
}

// TestCounterVec_BatasOverflow memastikan seri label dibatasi maksimal 256 kombinasi dan selebihnya masuk _overflow.
func TestCounterVec_BatasOverflow(t *testing.T) {
	reg := NewRegistry()
	cv := reg.NewCounterVec("card_test", "Uji batas kardinalitas", "k")
	for i := 0; i < maxVecSeriesCap+10; i++ {
		cv.With(string(rune('A'+i/26)) + string(rune('a'+i%26))).Inc()
	}

	var buf bytes.Buffer
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatalf("WritePrometheus gagal: %v", err)
	}
	out := buf.String()
	if !bytes.Contains([]byte(out), []byte(`k="_overflow"} 10`)) {
		t.Fatalf("diharapkan seri _overflow bernilai 10, output:\n%s", out)
	}
}
