package cli

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/fikfikk-org/waapi/internal/control"
	"github.com/fikfikk-org/waapi/internal/obs"
)

func runDoctor(ctx context.Context, ctrl *control.Client) error {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var rep obs.DoctorReport
	if err := ctrl.DoJSON(reqCtx, http.MethodGet, "/control/doctor", nil, &rep); err != nil {
		return err
	}

	memLimit := rep.Env["GOMEMLIMIT"]
	if memLimit == "" {
		memLimit = "80MiB (default)"
	}
	gogc := rep.Env["GOGC"]
	if gogc == "" {
		gogc = "100"
	}

	fmt.Printf("Daemon        running (pid %d, uptime %s)\n", rep.PID, formatDuration(rep.UptimeSec))
	fmt.Printf("RSS           %s   (hard target < 100 MB)\n", formatBytes(int64(rep.RSSBytes)))
	fmt.Printf("Go heap       %s alloc / %s sys   (GOMEMLIMIT %s, GOGC %s)\n",
		formatBytes(int64(rep.HeapAlloc)), formatBytes(int64(rep.HeapSys)), memLimit, gogc)
	fmt.Printf("Goroutines    %d\n", rep.Goroutines)
	fmt.Printf("GC cycles     %d    (last pause %dµs)\n", rep.NumGC, rep.LastGCPauseNs/1000)
	fmt.Printf("app.db        %s    WAL %s\n", formatBytes(rep.AppDBBytes), formatBytes(rep.AppWALBytes))
	fmt.Printf("wa.db         %s    WAL %s\n", formatBytes(rep.WADBBytes), formatBytes(rep.WAWALBytes))
	fmt.Printf("Outbox        %d pending\n", rep.OutboxPending)
	fmt.Printf("Instances     %d connected · %d disconnected · %d logged_out · %d disabled\n",
		rep.InstanceCount["connected"],
		rep.InstanceCount["disconnected"],
		rep.InstanceCount["logged_out"],
		rep.InstanceCount["disabled"],
	)
	return nil
}

