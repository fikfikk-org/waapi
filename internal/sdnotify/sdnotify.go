// Package sdnotify mengirimkan notifikasi siklus hidup proses dan heartbeat watchdog ke systemd melalui NOTIFY_SOCKET.
package sdnotify

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Notify mengirimkan pesan status systemd ke alamat datagram socket NOTIFY_SOCKET bila disetel.
func Notify(state string) error {
	addr := strings.TrimSpace(os.Getenv("NOTIFY_SOCKET"))
	if addr == "" {
		return nil
	}
	if !strings.HasPrefix(addr, "/") && !strings.HasPrefix(addr, "@") {
		return nil
	}
	raddr := &net.UnixAddr{Name: addr, Net: "unixgram"}
	conn, err := net.DialUnix("unixgram", nil, raddr)
	if err != nil {
		return fmt.Errorf("dial NOTIFY_SOCKET %s: %w", addr, err)
	}
	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
	if _, err := conn.Write([]byte(state)); err != nil {
		return fmt.Errorf("kirim sd_notify %q: %w", state, err)
	}
	return nil
}

// Ready memberi tahu systemd bahwa daemon telah selesai startup dan siap melayani permintaan.
func Ready() error {
	return Notify("READY=1")
}

// Watchdog mengirimkan sinyal detak jantung berkala kepada systemd WatchdogSec.
func Watchdog() error {
	return Notify("WATCHDOG=1")
}

// Stopping memberi tahu systemd bahwa daemon sedang memasuki fase graceful shutdown.
func Stopping() error {
	return Notify("STOPPING=1")
}

// Status mengirimkan baris deskripsi status operasional daemon ke systemd.
func Status(msg string) error {
	return Notify("STATUS=" + msg)
}

// WatchdogInterval membaca WATCHDOG_USEC dan memvalidasi WATCHDOG_PID terhadap PID proses saat ini.
func WatchdogInterval() (time.Duration, bool) {
	rawUsec := strings.TrimSpace(os.Getenv("WATCHDOG_USEC"))
	if rawUsec == "" {
		return 0, false
	}
	usec, err := strconv.ParseInt(rawUsec, 10, 64)
	if err != nil || usec <= 0 {
		return 0, false
	}
	if rawPID := strings.TrimSpace(os.Getenv("WATCHDOG_PID")); rawPID != "" {
		pid, err := strconv.Atoi(rawPID)
		if err != nil || pid != os.Getpid() {
			return 0, false
		}
	}
	return time.Duration(usec) * time.Microsecond, true
}
