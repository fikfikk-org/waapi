package obs

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"modernc.org/sqlite"
)

// DBStats merangkum ukuran berkas, pool koneksi, dan statistik cache internal modernc sqlite per database.
type DBStats struct {
	Name         string `json:"name"`
	FileBytes    int64  `json:"file_bytes"`
	WALBytes     int64  `json:"wal_bytes"`
	OpenConns    int    `json:"open_conns"`
	InUse        int    `json:"in_use"`
	Idle         int    `json:"idle"`
	CacheUsed    int64  `json:"cache_used_bytes"`
	SchemaUsed   int64  `json:"schema_used_bytes"`
	StmtUsed     int64  `json:"stmt_used_bytes"`
	CacheHit     int64  `json:"cache_hit"`
	CacheMiss    int64  `json:"cache_miss"`
	CacheSpill   int64  `json:"cache_spill"`
	SampledConns int    `json:"sampled_conns"`
}

// CollectDBStats mengumpulkan statistik ukuran file, pool koneksi, dan sqlite.DBStatus hingga MaxOpenConns koneksi.
func CollectDBStats(ctx context.Context, name string, db *sql.DB, path string) DBStats {
	out := DBStats{
		Name:      name,
		FileBytes: fileSize(path),
		WALBytes:  fileSize(path + "-wal"),
	}
	if db == nil {
		return out
	}

	st := db.Stats()
	out.OpenConns = st.OpenConnections
	out.InUse = st.InUse
	out.Idle = st.Idle

	maxConns := st.MaxOpenConnections
	if maxConns <= 0 {
		maxConns = 1
	}
	target := st.OpenConnections
	if target > maxConns {
		target = maxConns
	}
	if target <= 0 {
		return out
	}

	// Tahan koneksi yang sudah disampel sampai loop selesai agar koneksi di pool tidak terbaca ganda
	held := make([]*sql.Conn, 0, target)
	defer func() {
		for _, c := range held {
			_ = c.Close()
		}
	}()

	for i := 0; i < target; i++ {
		cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		conn, err := db.Conn(cctx)
		cancel()
		if err != nil {
			// Berhenti saat timeout per koneksi tercapai atau konteks dibatalkan
			break
		}
		held = append(held, conn)

		var connStats DBStats
		rawErr := conn.Raw(func(dc any) error {
			status, ok := dc.(sqlite.DBStatus)
			if !ok {
				return errors.New("driver bukan modernc sqlite")
			}
			ops := []struct {
				op  sqlite.DBStatusOp
				dst *int64
			}{
				{sqlite.DBStatusCacheUsed, &connStats.CacheUsed},
				{sqlite.DBStatusSchemaUsed, &connStats.SchemaUsed},
				{sqlite.DBStatusStmtUsed, &connStats.StmtUsed},
				{sqlite.DBStatusCacheHit, &connStats.CacheHit},
				{sqlite.DBStatusCacheMiss, &connStats.CacheMiss},
				{sqlite.DBStatusCacheSpill, &connStats.CacheSpill},
			}
			for _, item := range ops {
				cur, _, err := status.Status(item.op, false)
				if err != nil {
					return err
				}
				*item.dst += int64(cur)
			}
			return nil
		})
		if rawErr != nil {
			continue
		}

		out.CacheUsed += connStats.CacheUsed
		out.SchemaUsed += connStats.SchemaUsed
		out.StmtUsed += connStats.StmtUsed
		out.CacheHit += connStats.CacheHit
		out.CacheMiss += connStats.CacheMiss
		out.CacheSpill += connStats.CacheSpill
		out.SampledConns++
	}

	return out
}
