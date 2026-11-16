//go:build linux

package control

import (
	"context"
	"net"

	"golang.org/x/sys/unix"
)

// ConnContext membaca kredensial UID pemanggil CLI lewat SO_PEERCRED pada koneksi Unix socket Linux.
func (s *Server) ConnContext(ctx context.Context, c net.Conn) context.Context {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return ctx
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return ctx
	}
	var cred *unix.Ucred
	_ = raw.Control(func(fd uintptr) {
		cred, _ = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if cred == nil {
		return ctx
	}
	return context.WithValue(ctx, peerUIDKey{}, cred.Uid)
}
