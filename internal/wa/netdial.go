package wa

import (
	"net"
	"net/http"
	"time"
)

// newWebsocketHTTPClient menambahkan TCP keepalive dan TCP_USER_TIMEOUT pada koneksi websocket WhatsApp.
func newWebsocketHTTPClient(userTimeout time.Duration) *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone() // sama dengan basis transport bawaan whatsmeow
	d := &net.Dialer{
		Timeout:         10 * time.Second,
		KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: 30 * time.Second, Interval: 10 * time.Second, Count: 3},
		Control:         tcpUserTimeoutControl(userTimeout), // Linux: unix.TCP_USER_TIMEOUT; OS lain: no-op
	}
	tr.DialContext = d.DialContext
	return &http.Client{Transport: tr}
}
