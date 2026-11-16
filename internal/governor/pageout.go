package governor

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// MapRange mendeskripsikan satu segmen memori proses dari /proc/self/maps yang layak menerima madvise.
type MapRange struct {
	Start  uintptr
	Length uintptr
	Perms  string
	Path   string
}

// ParseExecutableMaps mem-parse /proc/self/maps dan mengembalikan hanya rentang r-xp dan r--p milik binary exePath.
func ParseExecutableMaps(r io.Reader, exePath string) ([]MapRange, error) {
	if r == nil || strings.TrimSpace(exePath) == "" {
		return nil, nil
	}

	var out []MapRange
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		perms := fields[1]
		if perms != "r-xp" && perms != "r--p" {
			continue
		}
		mappedPath := strings.Join(fields[5:], " ")
		if mappedPath != exePath {
			continue
		}
		startHex, endHex, ok := strings.Cut(fields[0], "-")
		if !ok {
			continue
		}
		startVal, err := strconv.ParseUint(startHex, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("parse alamat awal map %q: %w", fields[0], err)
		}
		endVal, err := strconv.ParseUint(endHex, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("parse alamat akhir map %q: %w", fields[0], err)
		}
		if endVal <= startVal {
			continue
		}
		out = append(out, MapRange{
			Start:  uintptr(startVal),
			Length: uintptr(endVal - startVal),
			Perms:  perms,
			Path:   mappedPath,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan maps: %w", err)
	}
	return out, nil
}
