//go:build windows

package media

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// CreateAnonymous membuat file sementara pada Windows dengan atribut FILE_FLAG_DELETE_ON_CLOSE agar terhapus otomatis saat ditutup.
func CreateAnonymous(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("siapkan direktori sementara media: %w", err)
	}

	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return nil, fmt.Errorf("acak nama file sementara: %w", err)
	}
	path := filepath.Join(dir, "waapi-media-"+hex.EncodeToString(suffix[:]))
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("konversi path file sementara: %w", err)
	}

	const fileFlagDeleteOnClose = 0x04000000
	h, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.CREATE_NEW,
		windows.FILE_ATTRIBUTE_TEMPORARY|fileFlagDeleteOnClose,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("buat file sementara Windows: %w", err)
	}
	return os.NewFile(uintptr(h), path), nil
}
