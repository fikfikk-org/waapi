//go:build !linux

package control

import (
	"context"
	"net"
)

// ConnContext pada OS non-Linux mengembalikan context apa adanya tanpa SO_PEERCRED.
func (s *Server) ConnContext(ctx context.Context, _ net.Conn) context.Context {
	return ctx
}
