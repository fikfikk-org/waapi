//go:build linux

package governor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/obs"
	"golang.org/x/sys/unix"
)

// ApplyPageout menerapkan MADV_COLD atau MADV_PAGEOUT pada halaman teks dan rodata milik executable saat ini tanpa unsafe.Pointer.
func ApplyPageout(mode string) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	var advice uintptr
	switch mode {
	case "", "off":
		return nil
	case "cold":
		advice = unix.MADV_COLD
	case "pageout":
		advice = unix.MADV_PAGEOUT
	default:
		return fmt.Errorf("mode pageout tidak dikenal: %q", mode)
	}

	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}
	if resolved, rErr := filepath.EvalSymlinks(exePath); rErr == nil && resolved != "" {
		exePath = resolved
	}

	beforePM, _ := obs.ReadProcStatus("/proc/self/status")

	f, err := os.Open("/proc/self/maps")
	if err != nil {
		return fmt.Errorf("buka /proc/self/maps: %w", err)
	}
	ranges, err := ParseExecutableMaps(f, exePath)
	_ = f.Close()
	if err != nil {
		return err
	}

	applied := 0
	for _, seg := range ranges {
		// Kirim uintptr langsung ke SYS_MADVISE agar terhindar dari konversi unsafe.Pointer yang melanggar aturan checkptr.
		_, _, errno := unix.Syscall(unix.SYS_MADVISE, seg.Start, seg.Length, advice)
		if errno != 0 {
			slog.Warn("syscall madvise pada segmen binary gagal",
				"mode", mode, "perms", seg.Perms, "len", uint64(seg.Length), "err", errno)
			continue
		}
		applied++
	}

	afterPM, _ := obs.ReadProcStatus("/proc/self/status")
	slog.Info("eksperimen pageout segmen binary selesai",
		"mode", mode,
		"segments", applied,
		"rss_before_bytes", beforePM.VmRSS,
		"rss_after_bytes", afterPM.VmRSS,
		"file_before_bytes", beforePM.RssFile,
		"file_after_bytes", afterPM.RssFile,
	)
	return nil
}

// ScheduleBootPageout menjadwalkan ApplyPageout 90 detik setelah startup daemon bila WAAPI_PAGEOUT_AFTER_BOOT aktif.
func ScheduleBootPageout(ctx context.Context, mode string) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" || mode == "off" {
		return
	}
	go func() {
		timer := time.NewTimer(90 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := ApplyPageout(mode); err != nil {
				slog.Warn("eksperimen pageout pasca-boot gagal", "mode", mode, "err", err)
			}
		}
	}()
}
