//go:build linux

package wa

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestTCPUserTimeoutTerpasang memastikan socket dialer websocket memasang TCP_USER_TIMEOUT = 45000 ms di Linux.
func TestTCPUserTimeoutTerpasang(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gagal listen loopback: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()

	cli := newWebsocketHTTPClient(45 * time.Second)
	tr, ok := cli.Transport.(*http.Transport)
	if !ok || tr.DialContext == nil {
		t.Fatal("Transport http.Client websocket harus berupa *http.Transport dengan DialContext")
	}

	conn, err := tr.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("DialContext gagal: %v", err)
	}
	defer conn.Close()
	<-done

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("diharapkan *net.TCPConn, didapat %T", conn)
	}
	raw, err := tcpConn.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn gagal: %v", err)
	}

	var gotTimeout int
	var optErr error
	if err := raw.Control(func(fd uintptr) {
		gotTimeout, optErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
	}); err != nil {
		t.Fatalf("Control gagal: %v", err)
	}
	if optErr != nil {
		t.Fatalf("GetsockoptInt TCP_USER_TIMEOUT gagal: %v", optErr)
	}
	if gotTimeout != 45000 {
		t.Fatalf("TCP_USER_TIMEOUT diharapkan 45000 ms, didapat %d ms", gotTimeout)
	}
}
