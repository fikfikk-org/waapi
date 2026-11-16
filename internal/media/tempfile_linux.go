//go:build linux

package media

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// CreateAnonymous membuka file sementara anonim melalui O_TMPFILE pada Linux dan fallback ke unlink-while-open jika filesystem tidak mendukung.
func CreateAnonymous(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("siapkan direktori sementara media: %w", err)
	}
	fd, err := unix.Open(dir, unix.O_RDWR|unix.O_TMPFILE|unix.O_CLOEXEC, 0o600)
	if err == nil {
		return os.NewFile(uintptr(fd), ""), nil
	}
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EISDIR) || errors.Is(err, unix.ENOSYS) {
		return createTempUnlinked(dir)
	}
	return nil, fmt.Errorf("buka file sementara O_TMPFILE: %w", err)
}
