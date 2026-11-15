package obs

import (
	"log/slog"
	"math"
	"runtime/metrics"
	"sync"
)

const (
	metricHeapObjects  = "/memory/classes/heap/objects:bytes"
	metricHeapGoal     = "/gc/heap/goal:bytes"
	metricTotalBytes   = "/memory/classes/total:bytes"
	metricHeapReleased = "/memory/classes/heap/released:bytes"
	metricHeapFree     = "/memory/classes/heap/free:bytes"
	metricStacks       = "/memory/classes/heap/stacks:bytes"
	metricGoroutines   = "/sched/goroutines:goroutines"
	metricGCCycles     = "/gc/cycles/total:gc-cycles"
	metricGCPauses     = "/sched/pauses/total/gc:seconds"
	metricMemoryLimit  = "/gc/gomemlimit:bytes"
	metricGOGC         = "/gc/gogc:percent"
)

var requiredRuntimeMetrics = []string{
	metricHeapObjects,
	metricHeapGoal,
	metricTotalBytes,
	metricHeapReleased,
	metricHeapFree,
	metricStacks,
	metricGoroutines,
	metricGCCycles,
	metricGCPauses,
	metricMemoryLimit,
	metricGOGC,
}

// RuntimeStats merangkum statistik runtime Go dari package runtime/metrics tanpa memicu stop-the-world.
type RuntimeStats struct {
	HeapObjects  uint64 `json:"heap_objects_bytes"`
	HeapGoal     uint64 `json:"heap_goal_bytes"`
	Total        uint64 `json:"total_bytes"`
	HeapReleased uint64 `json:"heap_released_bytes"`
	HeapFree     uint64 `json:"heap_free_bytes"`
	Stacks       uint64 `json:"stacks_bytes"`
	Goroutines   uint64 `json:"goroutines"`
	GCCycles     uint64 `json:"gc_cycles"`
	GCPauseP99Ns uint64 `json:"gc_pause_p99_ns"`
	MemoryLimit  uint64 `json:"memory_limit_bytes"`
	GOGC         uint64 `json:"gogc"`
}

var (
	runtimeMetricInitOnce sync.Once
	knownRuntimeMetrics   []string
	runtimeSampleMu       sync.Mutex
	runtimeSampleBuf      []metrics.Sample
)

func initRuntimeMetrics() {
	runtimeMetricInitOnce.Do(func() {
		available := make(map[string]bool)
		for _, desc := range metrics.All() {
			available[desc.Name] = true
		}
		for _, name := range requiredRuntimeMetrics {
			if !available[name] {
				slog.Debug("metrik runtime/metrics tidak tersedia di versi Go ini", "metric", name)
				continue
			}
			knownRuntimeMetrics = append(knownRuntimeMetrics, name)
		}
		runtimeSampleBuf = make([]metrics.Sample, len(knownRuntimeMetrics))
		for i, name := range knownRuntimeMetrics {
			runtimeSampleBuf[i].Name = name
		}
	})
}

// ReadRuntime membaca metrik runtime Go terkini melalui runtime/metrics tanpa stop-the-world.
func ReadRuntime() RuntimeStats {
	initRuntimeMetrics()
	if len(knownRuntimeMetrics) == 0 {
		return RuntimeStats{}
	}

	runtimeSampleMu.Lock()
	defer runtimeSampleMu.Unlock()

	metrics.Read(runtimeSampleBuf)

	var out RuntimeStats
	for _, s := range runtimeSampleBuf {
		switch s.Name {
		case metricHeapObjects:
			out.HeapObjects = sampleUint64(s.Value)
		case metricHeapGoal:
			out.HeapGoal = sampleUint64(s.Value)
		case metricTotalBytes:
			out.Total = sampleUint64(s.Value)
		case metricHeapReleased:
			out.HeapReleased = sampleUint64(s.Value)
		case metricHeapFree:
			out.HeapFree = sampleUint64(s.Value)
		case metricStacks:
			out.Stacks = sampleUint64(s.Value)
		case metricGoroutines:
			out.Goroutines = sampleUint64(s.Value)
		case metricGCCycles:
			out.GCCycles = sampleUint64(s.Value)
		case metricGCPauses:
			if s.Value.Kind() == metrics.KindFloat64Histogram {
				out.GCPauseP99Ns = histogramQuantileNs(s.Value.Float64Histogram(), 0.99)
			}
		case metricMemoryLimit:
			out.MemoryLimit = sampleUint64(s.Value)
		case metricGOGC:
			out.GOGC = sampleUint64(s.Value)
		}
	}
	return out
}

func sampleUint64(v metrics.Value) uint64 {
	switch v.Kind() {
	case metrics.KindUint64:
		return v.Uint64()
	case metrics.KindFloat64:
		f := v.Float64()
		if f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0
		}
		return uint64(f)
	default:
		return 0
	}
}

func histogramQuantileNs(h *metrics.Float64Histogram, q float64) uint64 {
	if h == nil || len(h.Counts) == 0 || len(h.Buckets) != len(h.Counts)+1 {
		return 0
	}
	var total uint64
	for _, c := range h.Counts {
		total += c
	}
	if total == 0 {
		return 0
	}

	target := math.Ceil(float64(total) * q)
	var cum uint64
	for i, c := range h.Counts {
		cum += c
		if float64(cum) >= target {
			valSec := h.Buckets[i+1]
			if math.IsInf(valSec, 1) || math.IsNaN(valSec) {
				valSec = h.Buckets[i]
			}
			if valSec <= 0 || math.IsInf(valSec, 0) || math.IsNaN(valSec) {
				return 0
			}
			return uint64(valSec * 1e9)
		}
	}
	return 0
}
