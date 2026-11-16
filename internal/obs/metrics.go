// Package obs mengumpulkan metrik runtime, procfs, cgroup, THP, SQLite, serta mengekspos Registry Prometheus.
package obs

import (
	"os"
	"strconv"
	"time"
)

// DoctorReport merangkum statistik kesehatan dan memori daemon dalam format v1.
type DoctorReport struct {
	PID           int               `json:"pid"`
	UptimeSec     int64             `json:"uptime_sec"`
	RSSBytes      uint64            `json:"rss_bytes"`
	HeapAlloc     uint64            `json:"heap_alloc_bytes"`
	HeapSys       uint64            `json:"heap_sys_bytes"`
	SysBytes      uint64            `json:"sys_bytes"`
	Goroutines    int               `json:"goroutines"`
	NumGC         uint32            `json:"num_gc"`
	LastGCPauseNs uint64            `json:"last_gc_pause_ns"`
	AppDBBytes    int64             `json:"app_db_bytes"`
	AppWALBytes   int64             `json:"app_wal_bytes"`
	WADBBytes     int64             `json:"wa_db_bytes"`
	WAWALBytes    int64             `json:"wa_wal_bytes"`
	OutboxPending int               `json:"outbox_pending"`
	InstanceCount map[string]int    `json:"instance_count"`
	GovernorLevel string            `json:"governor_level,omitempty"`
	Env           map[string]string `json:"env"`
}

// Collect mengumpulkan metrik Go runtime dari runtime/metrics (tanpa ReadMemStats), RSS OS, dan ukuran file DB.
func Collect(startedAt time.Time, appDBPath, waDBPath string, outbox int, instCounts map[string]int) DoctorReport {
	rt := ReadRuntime()

	var rss uint64
	if pm, err := ReadProcStatus("/proc/self/status"); err == nil && pm.VmRSS > 0 {
		rss = pm.VmRSS
	} else {
		// Fallback di OS non-Linux (mis. Windows dev): gunakan total bytes runtime
		rss = rt.Total
	}

	gomax := os.Getenv("GOMAXPROCS")
	if gomax == "" {
		if n := ReadRuntimeMaxProcs(); n > 0 {
			gomax = strconv.Itoa(n)
		}
	}

	return DoctorReport{
		PID:           os.Getpid(),
		UptimeSec:     int64(time.Since(startedAt).Seconds()),
		RSSBytes:      rss,
		HeapAlloc:     rt.HeapObjects,
		HeapSys:       rt.Total,
		SysBytes:      rt.Total,
		Goroutines:    int(rt.Goroutines),
		NumGC:         uint32(rt.GCCycles),
		LastGCPauseNs: rt.GCPauseP99Ns,
		AppDBBytes:    fileSize(appDBPath),
		AppWALBytes:   fileSize(appDBPath + "-wal"),
		WADBBytes:     fileSize(waDBPath),
		WAWALBytes:    fileSize(waDBPath + "-wal"),
		OutboxPending: outbox,
		InstanceCount: instCounts,
		Env: map[string]string{
			"GOMEMLIMIT": formatMemoryLimitEnv(rt.MemoryLimit),
			"GOGC":       formatGOGCEnv(rt.GOGC),
			"GOMAXPROCS": gomax,
		},
	}
}

func formatMemoryLimitEnv(limitBytes uint64) string {
	if env := os.Getenv("GOMEMLIMIT"); env != "" {
		return env
	}
	if limitBytes > 0 {
		return strconv.FormatUint(limitBytes>>20, 10) + "MiB"
	}
	return ""
}

func formatGOGCEnv(gogc uint64) string {
	if env := os.Getenv("GOGC"); env != "" {
		return env
	}
	if gogc > 0 {
		return strconv.FormatUint(gogc, 10)
	}
	return ""
}

func fileSize(p string) int64 {
	if p == "" {
		return 0
	}
	st, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return st.Size()
}
