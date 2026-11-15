package obs

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	metricPrefix    = "waapi_"
	maxVecSeriesCap = 256
	overflowLabel   = "_overflow"
)

type metricKind int

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
	kindCounterVec
	kindGaugeVec
	kindGaugeFunc
)

type registeredMetric struct {
	name string
	help string
	kind metricKind
	impl any
}

// Registry menyimpan daftar metrik tanpa ketergantungan ke client Prometheus eksternal.
type Registry struct {
	mu      sync.RWMutex
	metrics map[string]*registeredMetric
}

// NewRegistry membuat Registry metrik baru yang independen.
func NewRegistry() *Registry {
	return &Registry{
		metrics: make(map[string]*registeredMetric),
	}
}

func normalizeMetricName(name string) string {
	if strings.HasPrefix(name, metricPrefix) {
		return name
	}
	return metricPrefix + name
}

func (r *Registry) register(name, help string, kind metricKind, impl any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics[name] = &registeredMetric{
		name: name,
		help: help,
		kind: kind,
		impl: impl,
	}
}

// Counter mencatat nilai kumulatif monotik naik tanpa alokasi saat inkremen.
type Counter struct {
	bits atomic.Uint64
}

// Inc menambah counter sebesar 1 tanpa alokasi.
func (c *Counter) Inc() {
	c.Add(1)
}

// Add menambah counter sebesar delta (nilai negatif diabaikan agar tetap monotik) tanpa alokasi.
func (c *Counter) Add(delta float64) {
	if delta <= 0 || math.IsNaN(delta) {
		return
	}
	for {
		oldBits := c.bits.Load()
		next := math.Float64frombits(oldBits) + delta
		if c.bits.CompareAndSwap(oldBits, math.Float64bits(next)) {
			return
		}
	}
}

// setMonotonic memajukan counter absolut jika v lebih besar dari nilai saat ini.
func (c *Counter) setMonotonic(v float64) {
	if math.IsNaN(v) || v < 0 {
		return
	}
	for {
		oldBits := c.bits.Load()
		cur := math.Float64frombits(oldBits)
		if v <= cur {
			return
		}
		if c.bits.CompareAndSwap(oldBits, math.Float64bits(v)) {
			return
		}
	}
}

func (c *Counter) load() float64 {
	return math.Float64frombits(c.bits.Load())
}

// NewCounter mendaftarkan Counter baru ke Registry dengan prefix waapi_.
func (r *Registry) NewCounter(name, help string) *Counter {
	full := normalizeMetricName(name)
	c := &Counter{}
	r.register(full, help, kindCounter, c)
	return c
}

// Gauge mencatat nilai sesaat yang dapat naik atau turun tanpa alokasi.
type Gauge struct {
	bits atomic.Uint64
}

// Set menetapkan nilai gauge secara atomik tanpa alokasi.
func (g *Gauge) Set(v float64) {
	g.bits.Store(math.Float64bits(v))
}

// Add menambah nilai gauge sebesar delta tanpa alokasi.
func (g *Gauge) Add(delta float64) {
	if math.IsNaN(delta) {
		return
	}
	for {
		oldBits := g.bits.Load()
		next := math.Float64frombits(oldBits) + delta
		if g.bits.CompareAndSwap(oldBits, math.Float64bits(next)) {
			return
		}
	}
}

// Inc menaikkan nilai gauge sebesar 1 tanpa alokasi.
func (g *Gauge) Inc() { g.Add(1) }

// Dec menurunkan nilai gauge sebesar 1 tanpa alokasi.
func (g *Gauge) Dec() { g.Add(-1) }

func (g *Gauge) load() float64 {
	return math.Float64frombits(g.bits.Load())
}

// NewGauge mendaftarkan Gauge baru ke Registry dengan prefix waapi_.
func (r *Registry) NewGauge(name, help string) *Gauge {
	full := normalizeMetricName(name)
	g := &Gauge{}
	r.register(full, help, kindGauge, g)
	return g
}

// GaugeFunc mengevaluasi nilai gauge secara dinamis saat WritePrometheus dipanggil.
type GaugeFunc struct {
	fn func() float64
}

// NewGaugeFunc mendaftarkan GaugeFunc baru ke Registry dengan prefix waapi_.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) *GaugeFunc {
	full := normalizeMetricName(name)
	gf := &GaugeFunc{fn: fn}
	r.register(full, help, kindGaugeFunc, gf)
	return gf
}

// Histogram menghitung distribusi nilai observasi pada bucket yang ditentukan tanpa alokasi.
type Histogram struct {
	bounds  []float64
	buckets []atomic.Uint64 // panjang len(bounds)+1; elemen terakhir untuk overflow (> bounds terakhir)
	count   atomic.Uint64
	sumBits atomic.Uint64
}

// NewHistogram mendaftarkan Histogram baru ke Registry dengan batas bucket terurut naik.
func (r *Registry) NewHistogram(name, help string, buckets []float64) *Histogram {
	full := normalizeMetricName(name)
	copied := make([]float64, len(buckets))
	copy(copied, buckets)
	sort.Float64s(copied)

	h := &Histogram{
		bounds:  copied,
		buckets: make([]atomic.Uint64, len(copied)+1),
	}
	r.register(full, help, kindHistogram, h)
	return h
}

// Observe mencatat satu observasi ke bucket terkait tanpa alokasi heap.
func (h *Histogram) Observe(v float64) {
	if math.IsNaN(v) {
		return
	}
	idx := len(h.bounds)
	for i, b := range h.bounds {
		if v <= b {
			idx = i
			break
		}
	}
	h.buckets[idx].Add(1)
	h.count.Add(1)

	for {
		oldBits := h.sumBits.Load()
		next := math.Float64frombits(oldBits) + v
		if h.sumBits.CompareAndSwap(oldBits, math.Float64bits(next)) {
			return
		}
	}
}

type vecEntry[T any] struct {
	labels []string
	key    string
	metric *T
}

// CounterVec mengelola sekumpulan Counter berlabel dengan batas 256 kombinasi seri.
type CounterVec struct {
	labelNames []string
	seriesMap  sync.Map // cache kanonik string -> *vecEntry[Counter] sesuai kontrak
	fast       atomic.Pointer[[]vecEntry[Counter]]
	mu         sync.Mutex
	count      int
	overflow   *Counter
}

// NewCounterVec mendaftarkan CounterVec baru ke Registry dengan labelNames.
func (r *Registry) NewCounterVec(name, help string, labelNames ...string) *CounterVec {
	full := normalizeMetricName(name)
	names := make([]string, len(labelNames))
	copy(names, labelNames)
	v := &CounterVec{labelNames: names}
	r.register(full, help, kindCounterVec, v)
	return v
}

// With mengembalikan Counter untuk kombinasi nilai label tanpa alokasi jika seri sudah ada.
func (v *CounterVec) With(values ...string) *Counter {
	if snap := v.fast.Load(); snap != nil {
		entries := *snap
		for i := range entries {
			if equalLabels(entries[i].labels, values, len(v.labelNames)) {
				return entries[i].metric
			}
		}
	}
	return v.withSlow(values)
}

func (v *CounterVec) withSlow(values []string) *Counter {
	n := len(v.labelNames)
	norm := make([]string, n)
	for i := 0; i < n && i < len(values); i++ {
		norm[i] = values[i]
	}
	key := joinLabelKey(norm)

	v.mu.Lock()
	defer v.mu.Unlock()

	if existing, ok := v.seriesMap.Load(key); ok {
		return existing.(*vecEntry[Counter]).metric
	}

	if v.count >= maxVecSeriesCap {
		if v.overflow == nil {
			ovLabels := make([]string, n)
			for i := range ovLabels {
				ovLabels[i] = overflowLabel
			}
			ovKey := joinLabelKey(ovLabels)
			v.overflow = &Counter{}
			ovEntry := &vecEntry[Counter]{
				labels: ovLabels,
				key:    ovKey,
				metric: v.overflow,
			}
			v.seriesMap.Store(ovKey, ovEntry)
			v.rebuildFastLocked()
		}
		return v.overflow
	}

	c := &Counter{}
	entry := &vecEntry[Counter]{
		labels: norm,
		key:    key,
		metric: c,
	}
	v.seriesMap.Store(key, entry)
	v.count++
	v.rebuildFastLocked()
	return c
}

func (v *CounterVec) rebuildFastLocked() {
	var list []vecEntry[Counter]
	v.seriesMap.Range(func(_, val any) bool {
		e := val.(*vecEntry[Counter])
		list = append(list, *e)
		return true
	})
	sort.Slice(list, func(i, j int) bool {
		return compareLabelSlices(list[i].labels, list[j].labels)
	})
	v.fast.Store(&list)
}

// GaugeVec mengelola sekumpulan Gauge berlabel dengan batas 256 kombinasi seri.
type GaugeVec struct {
	labelNames []string
	seriesMap  sync.Map
	fast       atomic.Pointer[[]vecEntry[Gauge]]
	mu         sync.Mutex
	count      int
	overflow   *Gauge
}

// NewGaugeVec mendaftarkan GaugeVec baru ke Registry dengan labelNames.
func (r *Registry) NewGaugeVec(name, help string, labelNames ...string) *GaugeVec {
	full := normalizeMetricName(name)
	names := make([]string, len(labelNames))
	copy(names, labelNames)
	v := &GaugeVec{labelNames: names}
	r.register(full, help, kindGaugeVec, v)
	return v
}

// With mengembalikan Gauge untuk kombinasi nilai label tanpa alokasi jika seri sudah ada.
func (v *GaugeVec) With(values ...string) *Gauge {
	if snap := v.fast.Load(); snap != nil {
		entries := *snap
		for i := range entries {
			if equalLabels(entries[i].labels, values, len(v.labelNames)) {
				return entries[i].metric
			}
		}
	}
	return v.withSlow(values)
}

func (v *GaugeVec) withSlow(values []string) *Gauge {
	n := len(v.labelNames)
	norm := make([]string, n)
	for i := 0; i < n && i < len(values); i++ {
		norm[i] = values[i]
	}
	key := joinLabelKey(norm)

	v.mu.Lock()
	defer v.mu.Unlock()

	if existing, ok := v.seriesMap.Load(key); ok {
		return existing.(*vecEntry[Gauge]).metric
	}

	if v.count >= maxVecSeriesCap {
		if v.overflow == nil {
			ovLabels := make([]string, n)
			for i := range ovLabels {
				ovLabels[i] = overflowLabel
			}
			ovKey := joinLabelKey(ovLabels)
			v.overflow = &Gauge{}
			ovEntry := &vecEntry[Gauge]{
				labels: ovLabels,
				key:    ovKey,
				metric: v.overflow,
			}
			v.seriesMap.Store(ovKey, ovEntry)
			v.rebuildFastLocked()
		}
		return v.overflow
	}

	g := &Gauge{}
	entry := &vecEntry[Gauge]{
		labels: norm,
		key:    key,
		metric: g,
	}
	v.seriesMap.Store(key, entry)
	v.count++
	v.rebuildFastLocked()
	return g
}

func (v *GaugeVec) rebuildFastLocked() {
	var list []vecEntry[Gauge]
	v.seriesMap.Range(func(_, val any) bool {
		e := val.(*vecEntry[Gauge])
		list = append(list, *e)
		return true
	})
	sort.Slice(list, func(i, j int) bool {
		return compareLabelSlices(list[i].labels, list[j].labels)
	})
	v.fast.Store(&list)
}

func equalLabels(entryLabels, values []string, arity int) bool {
	if len(entryLabels) != arity {
		return false
	}
	for i := 0; i < arity; i++ {
		var val string
		if i < len(values) {
			val = values[i]
		}
		if entryLabels[i] != val {
			return false
		}
	}
	return true
}

func joinLabelKey(labels []string) string {
	return strings.Join(labels, "\x00")
}

func compareLabelSlices(a, b []string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// WritePrometheus menulis seluruh metrik terdaftar ke w dalam format teks Prometheus 0.0.4 secara deterministik.
func (r *Registry) WritePrometheus(w io.Writer) error {
	r.mu.RLock()
	items := make([]*registeredMetric, 0, len(r.metrics))
	for _, m := range r.metrics {
		items = append(items, m)
	}
	r.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].name < items[j].name
	})

	bw := bufio.NewWriter(w)
	for _, m := range items {
		if err := writePrometheusMetric(bw, m); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func writePrometheusMetric(w *bufio.Writer, m *registeredMetric) error {
	if _, err := fmt.Fprintf(w, "# HELP %s %s\n", m.name, escapePromHelp(m.help)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", m.name, promTypeName(m.kind)); err != nil {
		return err
	}

	switch m.kind {
	case kindCounter:
		c := m.impl.(*Counter)
		_, err := fmt.Fprintf(w, "%s %s\n", m.name, formatPromFloat(c.load()))
		return err
	case kindGauge:
		g := m.impl.(*Gauge)
		_, err := fmt.Fprintf(w, "%s %s\n", m.name, formatPromFloat(g.load()))
		return err
	case kindGaugeFunc:
		gf := m.impl.(*GaugeFunc)
		var val float64
		if gf.fn != nil {
			val = gf.fn()
		}
		_, err := fmt.Fprintf(w, "%s %s\n", m.name, formatPromFloat(val))
		return err
	case kindHistogram:
		h := m.impl.(*Histogram)
		var cum uint64
		for i, b := range h.bounds {
			cum += h.buckets[i].Load()
			if _, err := fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", m.name, formatPromFloat(b), cum); err != nil {
				return err
			}
		}
		cum += h.buckets[len(h.bounds)].Load()
		if _, err := fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", m.name, cum); err != nil {
			return err
		}
		sum := math.Float64frombits(h.sumBits.Load())
		if _, err := fmt.Fprintf(w, "%s_sum %s\n", m.name, formatPromFloat(sum)); err != nil {
			return err
		}
		_, err := fmt.Fprintf(w, "%s_count %d\n", m.name, h.count.Load())
		return err
	case kindCounterVec:
		v := m.impl.(*CounterVec)
		snap := v.fast.Load()
		if snap == nil {
			return nil
		}
		for _, e := range *snap {
			if err := writePromLabelLine(w, m.name, v.labelNames, e.labels, e.metric.load()); err != nil {
				return err
			}
		}
		return nil
	case kindGaugeVec:
		v := m.impl.(*GaugeVec)
		snap := v.fast.Load()
		if snap == nil {
			return nil
		}
		for _, e := range *snap {
			if err := writePromLabelLine(w, m.name, v.labelNames, e.labels, e.metric.load()); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

func writePromLabelLine(w *bufio.Writer, name string, labelNames, labelVals []string, val float64) error {
	if _, err := w.WriteString(name); err != nil {
		return err
	}
	if len(labelNames) > 0 {
		if err := w.WriteByte('{'); err != nil {
			return err
		}
		for i, ln := range labelNames {
			if i > 0 {
				if err := w.WriteByte(','); err != nil {
					return err
				}
			}
			var lv string
			if i < len(labelVals) {
				lv = labelVals[i]
			}
			if _, err := fmt.Fprintf(w, "%s=\"%s\"", ln, escapePromLabel(lv)); err != nil {
				return err
			}
		}
		if err := w.WriteByte('}'); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, " %s\n", formatPromFloat(val))
	return err
}

func promTypeName(k metricKind) string {
	switch k {
	case kindCounter, kindCounterVec:
		return "counter"
	case kindHistogram:
		return "histogram"
	default:
		return "gauge"
	}
}

func escapePromHelp(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func escapePromLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func formatPromFloat(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case v == 0:
		return "0"
	case math.Trunc(v) == v && math.Abs(v) <= 9007199254740992:
		return strconv.FormatInt(int64(v), 10)
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}

// Default adalah Registry global proses yang menampung metrik bawaan waapi.
var Default = NewRegistry()

var (
	// MetricProcessRSSBytes mencatat ukuran VmRSS proses saat ini dalam byte.
	MetricProcessRSSBytes = Default.NewGauge("process_rss_bytes", "Ukuran resident set size proses (VmRSS) dalam byte")
	// MetricProcessAnonBytes mencatat ukuran memori anonim proses dalam byte.
	MetricProcessAnonBytes = Default.NewGauge("process_anon_bytes", "Ukuran memori anonim proses (RssAnon atau Pss_Anon) dalam byte")
	// MetricGoHeapObjectsBytes mencatat memori objek heap Go aktif dari runtime/metrics dalam byte.
	MetricGoHeapObjectsBytes = Default.NewGauge("go_heap_objects_bytes", "Memori objek heap Go yang sedang dipakai dalam byte")
	// MetricGoGoroutines mencatat jumlah goroutine Go saat ini.
	MetricGoGoroutines = Default.NewGauge("go_goroutines", "Jumlah goroutine Go aktif")
	// MetricGoGCCyclesTotal mencatat total siklus GC Go kumulatif.
	MetricGoGCCyclesTotal = Default.NewCounter("go_gc_cycles_total", "Total kumulatif siklus garbage collection Go")
	// MetricCgroupMemoryCurrentBytes mencatat pemakaian memori cgroup v2 saat ini dalam byte.
	MetricCgroupMemoryCurrentBytes = Default.NewGauge("cgroup_memory_current_bytes", "Pemakaian memori cgroup v2 saat ini dalam byte")
	// MetricCgroupMemoryPressureSomeAvg10 mencatat PSI memory some avg10 cgroup v2.
	MetricCgroupMemoryPressureSomeAvg10 = Default.NewGauge("cgroup_memory_pressure_some_avg10", "Rata-rata 10 detik tekanan memori PSI some pada cgroup v2")
	// MetricSQLiteCacheUsedBytes mencatat memori cache halaman modernc SQLite per database dalam byte.
	MetricSQLiteCacheUsedBytes = Default.NewGaugeVec("sqlite_cache_used_bytes", "Memori page cache SQLite yang digunakan per database dalam byte", "db")
	// MetricInstances mencatat jumlah instance WhatsApp saat ini per status.
	MetricInstances = Default.NewGaugeVec("instances", "Jumlah instance WhatsApp berdasarkan status", "status")
)

// RefreshDefaultMetrics memperbarui seluruh metrik bawaan pada Default tanpa memakai runtime.ReadMemStats.
func RefreshDefaultMetrics(dbStats []DBStats, instCounts map[string]int) {
	rt := ReadRuntime()
	MetricGoHeapObjectsBytes.Set(float64(rt.HeapObjects))
	MetricGoGoroutines.Set(float64(rt.Goroutines))
	MetricGoGCCyclesTotal.setMonotonic(float64(rt.GCCycles))

	var rssBytes, anonBytes uint64
	if pm, err := ReadProcStatus("/proc/self/status"); err == nil {
		rssBytes = pm.VmRSS
		anonBytes = pm.RssAnon
	} else {
		rssBytes = rt.Total
	}
	if sm, err := ReadSmapsRollup("/proc/self/smaps_rollup"); err == nil && sm.PssAnon > 0 {
		anonBytes = sm.PssAnon
	}
	MetricProcessRSSBytes.Set(float64(rssBytes))
	MetricProcessAnonBytes.Set(float64(anonBytes))

	if cgDir, ok := CgroupDir("/proc/self/cgroup", "/sys/fs/cgroup"); ok {
		if cg, err := ReadCgroupMem(cgDir); err == nil {
			MetricCgroupMemoryCurrentBytes.Set(float64(cg.Current))
			MetricCgroupMemoryPressureSomeAvg10.Set(cg.PSI.SomeAvg10)
		}
	}

	for _, ds := range dbStats {
		MetricSQLiteCacheUsedBytes.With(ds.Name).Set(float64(ds.CacheUsed))
	}
	for status, count := range instCounts {
		MetricInstances.With(status).Set(float64(count))
	}
}
