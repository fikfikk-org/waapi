// Package netguard menyediakan proteksi anti-SSRF serta HTTP client/transport bersama untuk unduhan eksternal.
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrBlockedIP dikembalikan saat hostname me-resolve ke alamat IP privat, loopback, link-local, atau terlarang lainnya.
	ErrBlockedIP = errors.New("alamat IP tujuan termasuk jaringan privat atau terlarang (anti-SSRF)")
	// ErrNonHTTPS dikembalikan saat URL atau target redirect tidak menggunakan skema HTTPS.
	ErrNonHTTPS = errors.New("url harus menggunakan skema https yang valid")
	// ErrTooManyRedirects dikembalikan saat rantai redirect HTTP melampaui batas maksimum.
	ErrTooManyRedirects = errors.New("terlalu banyak pengalihan URL (maksimal 3)")
)

var blockedPrefixes = mustParsePrefixes([]string{
	"0.0.0.0/8",
	"10.0.0.0/8",
	"100.64.0.0/10",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"172.16.0.0/12",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"192.168.0.0/16",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"224.0.0.0/4",
	"240.0.0.0/4",
	"::/128",
	"::1/128",
	"64:ff9b::/96",
	"64:ff9b:1::/48",
	"100::/64",
	"2001::/32",
	"2001:db8::/32",
	"2002::/16",
	"fc00::/7",
	"fe80::/10",
	"ff00::/8",
})

func mustParsePrefixes(list []string) []netip.Prefix {
	out := make([]netip.Prefix, 0, len(list))
	for _, raw := range list {
		p := netip.MustParsePrefix(raw)
		out = append(out, p)
	}
	return out
}

// Policy mengendalikan pengecualian akses host atau IP privat pada koneksi keluar.
type Policy struct {
	AllowPrivate bool     // true hanya untuk URL yang dikonfigurasi operator (webhook, AI base URL)
	AllowHosts   []string // pengecualian eksplisit host tertentu (pengujian)
}

// IsBlocked mengembalikan true jika alamat IP (setelah unmap IPv4-mapped) termasuk rentang privat atau terlarang.
func IsBlocked(a netip.Addr) bool {
	if !a.IsValid() {
		return true
	}
	a = a.Unmap()
	for _, p := range blockedPrefixes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// IsPermanentSSRF melaporkan apakah error berasal dari pelanggaran kebijakan SSRF/skema yang tidak boleh di-retry.
func IsPermanentSSRF(err error) bool {
	return errors.Is(err, ErrBlockedIP) || errors.Is(err, ErrNonHTTPS) || errors.Is(err, ErrTooManyRedirects)
}

// ValidateHTTPSURL memastikan rawURL memiliki skema https dan host yang tidak kosong.
func ValidateHTTPSURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || !strings.EqualFold(u.Scheme, "https") || strings.TrimSpace(u.Host) == "" {
		return nil, ErrNonHTTPS
	}
	return u, nil
}

func (p Policy) isHostAllowed(host string) bool {
	h := strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	for _, allowed := range p.AllowHosts {
		if strings.Trim(strings.ToLower(strings.TrimSpace(allowed)), "[]") == h {
			return true
		}
	}
	return false
}

// DialContext me-resolve DNS terlebih dahulu, menolak jika ada IP terlarang, lalu mendial IP terverifikasi.
func (p Policy) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("alamat tujuan tidak valid: %w", err)
	}

	var ips []netip.Addr
	if literal, parseErr := netip.ParseAddr(strings.Trim(host, "[]")); parseErr == nil {
		ips = []netip.Addr{literal}
	} else {
		resolved, lookupErr := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if lookupErr != nil || len(resolved) == 0 {
			return nil, fmt.Errorf("resolusi DNS gagal untuk %s", host)
		}
		ips = resolved
	}

	if !p.AllowPrivate && !p.isHostAllowed(host) {
		for _, ip := range ips {
			if IsBlocked(ip) {
				return nil, fmt.Errorf("%w: %s", ErrBlockedIP, ip.Unmap().String())
			}
		}
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var lastErr error
	for _, ip := range ips {
		target := net.JoinHostPort(ip.Unmap().String(), port)
		conn, dErr := dialer.DialContext(ctx, network, target)
		if dErr == nil {
			return conn, nil
		}
		lastErr = dErr
	}
	if lastErr == nil {
		return nil, fmt.Errorf("gagal menyambung ke %s", addr)
	}
	return nil, lastErr
}

// NewTransport membuat *http.Transport dengan pool idle terkelola dan dialer anti-SSRF.
func NewTransport(p Policy) *http.Transport {
	return &http.Transport{
		DialContext:           p.DialContext,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConnsPerHost:   2,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ForceAttemptHTTP2:     true,
	}
}

// NewClient membangun *http.Client berbatas redirect maks 3 hop dan wajib HTTPS.
func NewClient(p Policy, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: NewTransport(p),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return ErrTooManyRedirects
			}
			if !strings.EqualFold(req.URL.Scheme, "https") {
				return ErrNonHTTPS
			}
			return nil
		},
	}
}
