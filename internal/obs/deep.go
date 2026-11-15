package obs

import (
	"context"
	"database/sql"
	"runtime"
	"runtime/pprof"
	"time"
)

// BuildInfo menyimpan metadata versi Go dan versi aplikasi pada DeepReport.
type BuildInfo struct {
	Go      string `json:"go"`
	Version string `json:"version"`
}

// DBInput mendeskripsikan satu handle database SQLite untuk dikumpulkan statistiknya oleh CollectDeep.
type DBInput struct {
	Name string
	DB   *sql.DB
	Path string
}

// WebhookInstanceReport merangkum status lag webhook, DLQ, pause, dan event terbuang per instance.
type WebhookInstanceReport struct {
	InstanceID         string `json:"instance_id"`
	HeadSeq            int64  `json:"head_seq"`
	MinCursor          int64  `json:"min_cursor"`
	Lag                int64  `json:"lag"`
	OldestUndeliveredS int64  `json:"oldest_undelivered_sec"`
	DLQ                int    `json:"dlq"`
	Paused             string `json:"paused,omitempty"`
	DroppedUndelivered int64  `json:"dropped_undelivered"`
}

// DeepReport merangkum anatomi memori lengkap daemon (Go runtime, procfs, cgroup, THP, SQLite per DB, leak).
type DeepReport struct {
	Doctor         DoctorReport            `json:"doctor"`
	Runtime        RuntimeStats            `json:"runtime"`
	Proc           ProcMem                 `json:"proc"`
	Smaps          Smaps                   `json:"smaps"`
	Cgroup         *CgroupMem              `json:"cgroup"`
	THP            string                  `json:"thp"`
	SQLite         []DBStats               `json:"sqlite"`
	Webhook        []WebhookInstanceReport `json:"webhook,omitempty"`
	Build          BuildInfo               `json:"build"`
	GoroutineLeaks int                     `json:"goroutine_leaks"`
}

// ReadRuntimeMaxProcs mengembalikan jumlah GOMAXPROCS aktif saat ini.
func ReadRuntimeMaxProcs() int {
	return runtime.GOMAXPROCS(0)
}

// CollectDeep mengumpulkan laporan diagnostik mendalam proses, runtime, cgroup v2, THP, dan statistik SQLite.
func CollectDeep(
	ctx context.Context,
	startedAt time.Time,
	appDBPath, waDBPath string,
	outbox int,
	instCounts map[string]int,
	dbs []DBInput,
	version string,
) DeepReport {
	doc := Collect(startedAt, appDBPath, waDBPath, outbox, instCounts)
	rt := ReadRuntime()
	pm, _ := ReadProcStatus("/proc/self/status")
	sm, _ := ReadSmapsRollup("/proc/self/smaps_rollup")

	var cgPtr *CgroupMem
	if cgDir, ok := CgroupDir("/proc/self/cgroup", "/sys/fs/cgroup"); ok {
		if cg, err := ReadCgroupMem(cgDir); err == nil {
			cgPtr = &cg
		}
	}

	thp := ReadTHPMode("")

	sqliteStats := make([]DBStats, 0, len(dbs))
	for _, in := range dbs {
		sqliteStats = append(sqliteStats, CollectDBStats(ctx, in.Name, in.DB, in.Path))
	}

	leaks := -1
	if prof := pprof.Lookup("goroutineleak"); prof != nil {
		leaks = prof.Count()
	}

	return DeepReport{
		Doctor:  doc,
		Runtime: rt,
		Proc:    pm,
		Smaps:   sm,
		Cgroup:  cgPtr,
		THP:     thp,
		SQLite:  sqliteStats,
		Build: BuildInfo{
			Go:      runtime.Version(),
			Version: version,
		},
		GoroutineLeaks: leaks,
	}
}
