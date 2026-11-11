// Package obs mengumpulkan metrik runtime, memori, dan ukuran database untuk perintah doctor.
package obs

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// DoctorReport merangkum statistik kesehatan dan memori daemon.
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
	Env           map[string]string `json:"env"`
}

// Collect mengumpulkan metrik Go runtime, RSS OS (jika tersedia), dan ukuran file DB.
func Collect(startedAt time.Time, appDBPath, waDBPath string, outbox int, instCounts map[string]int) DoctorReport {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	var lastPause uint64
	if ms.NumGC > 0 {
		lastPause = ms.PauseNs[(ms.NumGC+255)%256]
	}

	rss := readLinuxRSS()
	if rss == 0 {
		// Fallback di OS non-Linux (mis. Windows dev): gunakan Sys dari runtime
		rss = ms.Sys
	}

	return DoctorReport{
		PID:           os.Getpid(),
		UptimeSec:     int64(time.Since(startedAt).Seconds()),
		RSSBytes:      rss,
		HeapAlloc:     ms.HeapAlloc,
		HeapSys:       ms.HeapSys,
		SysBytes:      ms.Sys,
		Goroutines:    runtime.NumGoroutine(),
		NumGC:         ms.NumGC,
		LastGCPauseNs: lastPause,
		AppDBBytes:    fileSize(appDBPath),
		AppWALBytes:   fileSize(appDBPath + "-wal"),
		WADBBytes:     fileSize(waDBPath),
		WAWALBytes:    fileSize(waDBPath + "-wal"),
		OutboxPending: outbox,
		InstanceCount: instCounts,
		Env: map[string]string{
			"GOMEMLIMIT": os.Getenv("GOMEMLIMIT"),
			"GOGC":       os.Getenv("GOGC"),
			"GOMAXPROCS": strconv.Itoa(runtime.GOMAXPROCS(0)),
		},
	}
}

// readLinuxRSS membaca VmRSS dari /proc/self/status pada sistem Linux.
func readLinuxRSS() uint64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				return kb * 1024
			}
		}
	}
	return 0
}

func fileSize(p string) int64 {
	st, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return st.Size()
}

