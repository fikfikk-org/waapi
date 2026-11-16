package governor

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/fikfikk-org/waapi/internal/obs"
)

// DBPool mendeskripsikan satu pool koneksi SQLite beserta nama logiknya untuk mitigasi PRAGMA shrink_memory.
type DBPool struct {
	Name string
	DB   *sql.DB
}

// NewProcSampler membangun Sampler yang memprioritaskan memory.stat anon + PSI cgroup v2 dan fallback ke RssAnon procfs.
func NewProcSampler(cgroupProcPath, cgroupRoot, procStatusPath string) Sampler {
	if cgroupProcPath == "" {
		cgroupProcPath = "/proc/self/cgroup"
	}
	if cgroupRoot == "" {
		cgroupRoot = "/sys/fs/cgroup"
	}
	if procStatusPath == "" {
		procStatusPath = "/proc/self/status"
	}

	return func() (Sample, error) {
		if cgDir, ok := obs.CgroupDir(cgroupProcPath, cgroupRoot); ok {
			if cg, err := obs.ReadCgroupMem(cgDir); err == nil {
				return Sample{
					AnonBytes:    int64(cg.Stat.Anon),
					PSISomeAvg10: cg.PSI.SomeAvg10,
					PSIFullAvg10: cg.PSI.FullAvg10,
				}, nil
			}
		}

		pm, err := obs.ReadProcStatus(procStatusPath)
		if err != nil {
			return Sample{}, err
		}
		return Sample{
			AnonBytes: int64(pm.RssAnon),
		}, nil
	}
}

// ShrinkPoolsAction membangun callback aksi yang menjalankan PRAGMA shrink_memory pada seluruh pool dengan timeout koneksi 100 ms.
func ShrinkPoolsAction(pools []DBPool) func(ctx context.Context) {
	list := make([]DBPool, 0, len(pools))
	for _, p := range pools {
		if p.DB != nil {
			list = append(list, p)
		}
	}

	return func(ctx context.Context) {
		for _, p := range list {
			cCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			conn, err := p.DB.Conn(cCtx)
			cancel()
			if err != nil {
				slog.Warn("gagal mengambil koneksi untuk PRAGMA shrink_memory", "db", p.Name, "err", err)
				continue
			}

			eCtx, eCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			if _, execErr := conn.ExecContext(eCtx, "PRAGMA shrink_memory"); execErr != nil {
				slog.Warn("PRAGMA shrink_memory gagal", "db", p.Name, "err", execErr)
			}
			eCancel()
			_ = conn.Close()
		}
	}
}
