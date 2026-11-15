//go:build !linux

package wa

import (
	"syscall"
	"time"
)

// tcpUserTimeoutControl adalah no-op pada platform selain Linux.
func tcpUserTimeoutControl(_ time.Duration) func(network, address string, c syscall.RawConn) error {
	return nil
}
