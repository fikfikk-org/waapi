package media

import (
	"os"
	"strings"
)

const defaultDropBehindChunk = 4 << 20

type syscalls interface {
	SyncRange(fd uintptr, off, n int64) error // sync_file_range(WAIT_BEFORE|WRITE|WAIT_AFTER)
	DontNeed(fd uintptr, off, n int64) error  // posix_fadvise(POSIX_FADV_DONTNEED)
}

// DropBehindFile membungkus *os.File untuk membuang page cache kotor dan terpakai secara bertahap per chunk selama proses upload.
type DropBehindFile struct {
	f          *os.File
	fd         uintptr
	chunk      int64
	sys        syscalls
	enabled    bool
	written    int64
	flushed    int64
	readOff    int64
	dontNeedAt int64
}

// NewDropBehind membungkus *os.File dengan drop-behind per chunk (default 4 MiB) sesuai status WAAPI_MEDIA_DROPBEHIND.
func NewDropBehind(f *os.File, chunk int64) *DropBehindFile {
	enabled := !strings.EqualFold(strings.TrimSpace(os.Getenv("WAAPI_MEDIA_DROPBEHIND")), "off")
	return newDropBehindWithSys(f, chunk, platformSyscalls(), enabled)
}

func newDropBehindWithSys(f *os.File, chunk int64, sys syscalls, enabled bool) *DropBehindFile {
	if chunk <= 0 {
		chunk = defaultDropBehindChunk
	}
	var fd uintptr
	if f != nil {
		fd = f.Fd()
	}
	return &DropBehindFile{
		f:       f,
		fd:      fd,
		chunk:   chunk,
		sys:     sys,
		enabled: enabled,
	}
}

// Write menulis p ke file temp dan memanggil SyncRange+DontNeed untuk chunk tertinggal saat selisih >= 2*chunk.
func (d *DropBehindFile) Write(p []byte) (int, error) {
	n, err := d.f.Write(p)
	d.written += int64(n)
	if d.enabled && d.sys != nil && d.chunk > 0 {
		for d.written-d.flushed >= 2*d.chunk {
			_ = d.sys.SyncRange(d.fd, d.flushed, d.chunk)
			_ = d.sys.DontNeed(d.fd, d.flushed, d.chunk)
			d.flushed += d.chunk
		}
	}
	return n, err
}

// Read membaca dari file temp dan menandai page cache chunk yang sudah selesai dibaca sebagai DONTNEED.
func (d *DropBehindFile) Read(p []byte) (int, error) {
	n, err := d.f.Read(p)
	d.readOff += int64(n)
	if d.enabled && d.sys != nil && d.chunk > 0 {
		for d.readOff-d.dontNeedAt >= d.chunk {
			_ = d.sys.DontNeed(d.fd, d.dontNeedAt, d.chunk)
			d.dontNeedAt += d.chunk
		}
	}
	return n, err
}

// Seek menggeser offset file temp sekaligus menyelaraskan pelacakan posisi baca drop-behind.
func (d *DropBehindFile) Seek(offset int64, whence int) (int64, error) {
	pos, err := d.f.Seek(offset, whence)
	if err == nil {
		d.readOff = pos
		if d.chunk > 0 {
			d.dontNeedAt = (pos / d.chunk) * d.chunk
		} else {
			d.dontNeedAt = 0
		}
	}
	return pos, err
}
