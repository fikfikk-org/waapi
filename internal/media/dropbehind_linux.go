//go:build linux

package media

import "golang.org/x/sys/unix"

type linuxSyscalls struct{}

func platformSyscalls() syscalls {
	return linuxSyscalls{}
}

func (linuxSyscalls) SyncRange(fd uintptr, off, n int64) error {
	return unix.SyncFileRange(int(fd), off, n, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
}

func (linuxSyscalls) DontNeed(fd uintptr, off, n int64) error {
	return unix.Fadvise(int(fd), off, n, unix.FADV_DONTNEED)
}
