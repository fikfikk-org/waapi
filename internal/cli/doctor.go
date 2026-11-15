package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/obs"
)

func runDoctor(ctx context.Context, ctrl *control.Client, args []string) error {
	var deep, asJSON bool
	for _, a := range args {
		switch a {
		case "--deep":
			deep = true
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("opsi tidak dikenal untuk doctor: %q (gunakan --deep atau --json)", a)
		}
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if deep {
		var rep obs.DeepReport
		if err := ctrl.DoJSON(reqCtx, http.MethodGet, "/control/doctor?deep=1", nil, &rep); err != nil {
			return err
		}
		if asJSON {
			return printJSON(rep)
		}
		printDeepDoctor(rep)
		return nil
	}

	var rep obs.DoctorReport
	if err := ctrl.DoJSON(reqCtx, http.MethodGet, "/control/doctor", nil, &rep); err != nil {
		return err
	}
	if asJSON {
		return printJSON(rep)
	}

	memLimit := rep.Env["GOMEMLIMIT"]
	if memLimit == "" {
		memLimit = "80MiB (default)"
	}
	gogc := rep.Env["GOGC"]
	if gogc == "" {
		gogc = "100"
	}

	total := totalInstances(rep.InstanceCount)

	fmt.Printf("Daemon        running (pid %d, uptime %s)\n", rep.PID, formatDuration(rep.UptimeSec))
	fmt.Printf("RSS           %s   (hard target < 100 MB)\n", formatBytes(int64(rep.RSSBytes)))
	fmt.Printf("Go heap       %s alloc / %s sys   (GOMEMLIMIT %s, GOGC %s)\n",
		formatBytes(int64(rep.HeapAlloc)), formatBytes(int64(rep.HeapSys)), memLimit, gogc)
	fmt.Printf("Goroutines    %d\n", rep.Goroutines)
	fmt.Printf("GC cycles     %d    (last pause %dµs)\n", rep.NumGC, rep.LastGCPauseNs/1000)
	fmt.Printf("app.db        %s    WAL %s\n", formatBytes(rep.AppDBBytes), formatBytes(rep.AppWALBytes))
	fmt.Printf("wa.db         %s    WAL %s\n", formatBytes(rep.WADBBytes), formatBytes(rep.WAWALBytes))
	fmt.Printf("Outbox        %d pending\n", rep.OutboxPending)
	fmt.Printf("Instances     %d total (%d connected · %d created · %d disconnected · %d logged_out · %d disabled)\n",
		total,
		rep.InstanceCount["connected"],
		rep.InstanceCount["created"],
		rep.InstanceCount["disconnected"],
		rep.InstanceCount["logged_out"],
		rep.InstanceCount["disabled"],
	)
	return nil
}

func printDeepDoctor(rep obs.DeepReport) {
	doc := rep.Doctor
	rt := rep.Runtime
	total := totalInstances(doc.InstanceCount)

	fmt.Println("=== Proses ===")
	fmt.Printf("PID           %d (uptime %s, waapi v%s, %s)\n", doc.PID, formatDuration(doc.UptimeSec), rep.Build.Version, rep.Build.Go)
	fmt.Printf("RSS           %s (HWM %s · threads %d)\n",
		formatBytes(int64(rep.Proc.VmRSS)), formatBytes(int64(rep.Proc.VmHWM)), rep.Proc.Threads)
	fmt.Printf("Proc RSS      anon %s · file %s · shmem %s\n",
		formatBytes(int64(rep.Proc.RssAnon)), formatBytes(int64(rep.Proc.RssFile)), formatBytes(int64(rep.Proc.RssShmem)))
	fmt.Printf("Smaps PSS     total %s · anon %s · file %s · shmem %s · dirty %s\n",
		formatBytes(int64(rep.Smaps.Pss)),
		formatBytes(int64(rep.Smaps.PssAnon)),
		formatBytes(int64(rep.Smaps.PssFile)),
		formatBytes(int64(rep.Smaps.PssShmem)),
		formatBytes(int64(rep.Smaps.PrivateDirty)),
	)

	fmt.Println("\n=== Go Runtime ===")
	fmt.Printf("Heap objects  %s (goal %s · free %s · released %s)\n",
		formatBytes(int64(rt.HeapObjects)),
		formatBytes(int64(rt.HeapGoal)),
		formatBytes(int64(rt.HeapFree)),
		formatBytes(int64(rt.HeapReleased)),
	)
	fmt.Printf("Runtime total %s · stacks %s\n", formatBytes(int64(rt.Total)), formatBytes(int64(rt.Stacks)))
	fmt.Printf("Goroutines    %d (leaks: %d) · GC cycles %d (P99 pause %dµs)\n",
		rt.Goroutines, rep.GoroutineLeaks, rt.GCCycles, rt.GCPauseP99Ns/1000)
	fmt.Printf("GC tuning     GOMEMLIMIT %s · GOGC %d\n", formatBytes(int64(rt.MemoryLimit)), rt.GOGC)

	fmt.Println("\n=== Cgroup v2 ===")
	if rep.Cgroup == nil {
		fmt.Println("cgroup v2 tidak terdeteksi")
	} else {
		cg := rep.Cgroup
		fmt.Printf("Current       %s (peak %s · high %s · max %s)\n",
			formatBytes(int64(cg.Current)),
			formatBytes(int64(cg.Peak)),
			formatCgroupLimit(cg.High),
			formatCgroupLimit(cg.Max),
		)
		fmt.Printf("Stat          anon %s · file %s · kernel %s · sock %s · dirty %s\n",
			formatBytes(int64(cg.Stat.Anon)),
			formatBytes(int64(cg.Stat.File)),
			formatBytes(int64(cg.Stat.Kernel)),
			formatBytes(int64(cg.Stat.Sock)),
			formatBytes(int64(cg.Stat.FileDirty)),
		)
		fmt.Printf("Events        high %d · max %d · oom %d · oom_kill %d\n",
			cg.Events.High, cg.Events.Max, cg.Events.OOM, cg.Events.OOMKill)
		fmt.Printf("PSI memory    some avg10=%.2f avg60=%.2f · full avg10=%.2f avg60=%.2f\n",
			cg.PSI.SomeAvg10, cg.PSI.SomeAvg60, cg.PSI.FullAvg10, cg.PSI.FullAvg60)
	}

	fmt.Println("\n=== THP ===")
	thp := rep.THP
	if thp == "" {
		thp = "tidak tersedia"
	}
	fmt.Printf("Mode          %s\n", thp)

	fmt.Println("\n=== SQLite ===")
	for _, db := range rep.SQLite {
		fmt.Printf("%-12s  file %s (WAL %s) · conns %d (in_use %d, idle %d, sampled %d)\n",
			db.Name, formatBytes(db.FileBytes), formatBytes(db.WALBytes),
			db.OpenConns, db.InUse, db.Idle, db.SampledConns)
		fmt.Printf("              cache_used %s · schema %s · stmt %s · hit %d · miss %d · spill %d\n",
			formatBytes(db.CacheUsed), formatBytes(db.SchemaUsed), formatBytes(db.StmtUsed),
			db.CacheHit, db.CacheMiss, db.CacheSpill)
	}

	fmt.Println("\n=== Ringkasan ===")
	fmt.Printf("Outbox        %d pending\n", doc.OutboxPending)
	fmt.Printf("Instances     %d total (%d connected · %d created · %d disconnected · %d logged_out · %d disabled)\n",
		total,
		doc.InstanceCount["connected"],
		doc.InstanceCount["created"],
		doc.InstanceCount["disconnected"],
		doc.InstanceCount["logged_out"],
		doc.InstanceCount["disabled"],
	)
	if len(rep.Webhook) > 0 {
		fmt.Println("\n=== Webhook & Journal ===")
		for _, w := range rep.Webhook {
			paused := w.Paused
			if paused == "" {
				paused = "-"
			}
			fmt.Printf("%-14s lag %d (head %d · cursor %d · tertua %s) · dlq %d · pause %s · dropped %d\n",
				w.InstanceID, w.Lag, w.HeadSeq, w.MinCursor, formatDuration(w.OldestUndeliveredS),
				w.DLQ, paused, w.DroppedUndelivered)
		}
	}
}

func totalInstances(counts map[string]int) int {
	return counts["connected"] +
		counts["created"] +
		counts["pairing"] +
		counts["disconnected"] +
		counts["logged_out"] +
		counts["disabled"]
}

func formatCgroupLimit(v uint64) string {
	if v == 0 {
		return "max"
	}
	return formatBytes(int64(v))
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
