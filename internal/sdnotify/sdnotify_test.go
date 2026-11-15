package sdnotify

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TestSdNotify_KirimDatagram memverifikasi pengiriman datagram READY=1 ke listener unixgram NOTIFY_SOCKET.
func TestSdNotify_KirimDatagram(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "notify.sock")
	pc, err := net.ListenPacket("unixgram", sockPath)
	if err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("unixgram tidak didukung oleh Windows Winsock: %v", err)
		}
		t.Fatalf("gagal membuka listener unixgram: %v", err)
	}
	defer pc.Close()

	t.Setenv("NOTIFY_SOCKET", sockPath)
	if err := Ready(); err != nil {
		t.Fatalf("sdnotify.Ready gagal: %v", err)
	}

	_ = pc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 128)
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatalf("gagal membaca datagram dari unixgram socket: %v", err)
	}
	if got := string(buf[:n]); got != "READY=1" {
		t.Fatalf("payload datagram diharapkan READY=1, didapat %q", got)
	}
}

// TestSdNotify_WatchdogInterval memverifikasi pembacaan WATCHDOG_USEC dan validasi WATCHDOG_PID.
func TestSdNotify_WatchdogInterval(t *testing.T) {
	t.Run("PID_sendiri_valid", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "30000000")
		t.Setenv("WATCHDOG_PID", strconv.Itoa(os.Getpid()))
		dur, ok := WatchdogInterval()
		if !ok || dur != 30*time.Second {
			t.Fatalf("diharapkan (30s, true), didapat (%v, %v)", dur, ok)
		}
	})

	t.Run("PID_kosong_valid", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "30000000")
		t.Setenv("WATCHDOG_PID", "")
		dur, ok := WatchdogInterval()
		if !ok || dur != 30*time.Second {
			t.Fatalf("diharapkan (30s, true), didapat (%v, %v)", dur, ok)
		}
	})

	t.Run("PID_lain_ditolak", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "30000000")
		t.Setenv("WATCHDOG_PID", strconv.Itoa(os.Getpid()+12345))
		if dur, ok := WatchdogInterval(); ok || dur != 0 {
			t.Fatalf("PID lain harus mengembalikan (0, false), didapat (%v, %v)", dur, ok)
		}
	})
}
