//go:build !linux && !windows

package media

import (
	"fmt"
	"os"
)

// CreateAnonymous membuat file sementara yang segera di-unlink selagi terbuka pada sistem Unix non-Linux.
func CreateAnonymous(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("siapkan direktori sementara media: %w", err)
	}
	return createTempUnlinked(dir)
}
