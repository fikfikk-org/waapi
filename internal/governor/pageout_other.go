//go:build !linux

package governor

import (
	"context"
	"log/slog"
	"strings"
)

// ApplyPageout merupakan no-op pada sistem operasi non-Linux.
func ApplyPageout(mode string) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "" && mode != "off" {
		slog.Debug("eksperimen pageout diabaikan pada OS non-Linux", "mode", mode)
	}
	return nil
}

// ScheduleBootPageout merupakan no-op pada sistem operasi non-Linux.
func ScheduleBootPageout(_ context.Context, _ string) {}
