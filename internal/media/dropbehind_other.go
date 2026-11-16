//go:build !linux

package media

type noopSyscalls struct{}

func platformSyscalls() syscalls {
	return noopSyscalls{}
}

func (noopSyscalls) SyncRange(uintptr, int64, int64) error { return nil }
func (noopSyscalls) DontNeed(uintptr, int64, int64) error  { return nil }
