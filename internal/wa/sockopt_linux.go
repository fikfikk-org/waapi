//go:build linux

package wa

import (
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// tcpUserTimeoutControl menyetel TCP_USER_TIMEOUT agar kernel memutuskan soket half-open sesuai konfigurasi.
func tcpUserTimeoutControl(userTimeout time.Duration) func(network, address string, c syscall.RawConn) error {
	if userTimeout <= 0 {
		return nil
	}
	ms := int(userTimeout.Milliseconds())
	if ms <= 0 {
		return nil
	}
	return func(network, _ string, c syscall.RawConn) error {
		if !strings.HasPrefix(network, "tcp") {
			return nil
		}
		var sockErr error
		if err := c.Control(func(fd uintptr) {
			sockErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, ms)
		}); err != nil {
			return err
		}
		return sockErr
	}
}
