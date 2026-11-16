package media

import (
	"fmt"
	"os"
)

// createTempUnlinked membuat file sementara bernama lalu segera melepaskannya (unlink) agar otomatis hilang saat ditutup.
func createTempUnlinked(dir string) (*os.File, error) {
	f, err := os.CreateTemp(dir, "waapi-media-*")
	if err != nil {
		return nil, fmt.Errorf("buat file sementara: %w", err)
	}
	if rmErr := os.Remove(f.Name()); rmErr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("unlink file sementara: %w", rmErr)
	}
	return f, nil
}
