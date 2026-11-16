package netguard_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/netguard"
)

// TestNetguard_Blokir memastikan seluruh rentang IP privat, CGNAT, NAT64, 6to4, link-local, dan unspecified ditolak.
func TestNetguard_Blokir(t *testing.T) {
	kasus := []string{
		"100.64.1.1",
		"::ffff:10.0.0.1",
		"64:ff9b::a00:1",
		"2002:a00:1::1",
		"169.254.169.254",
		"fd00::1",
		"0.1.2.3",
		"127.0.0.1",
		"::1",
		"192.168.1.1",
		"198.18.0.1",
	}
	for _, raw := range kasus {
		addr, err := netip.ParseAddr(raw)
		if err != nil {
			t.Fatalf("parse IP test %q gagal: %v", raw, err)
		}
		if !netguard.IsBlocked(addr) {
			t.Fatalf("IP %s seharusnya diblokir oleh netguard", raw)
		}
	}
}

// TestNetguard_IzinkanPublik memastikan alamat IP publik IPv4 dan IPv6 diizinkan oleh netguard.
func TestNetguard_IzinkanPublik(t *testing.T) {
	publik := []string{
		"1.1.1.1",
		"2606:4700:4700::1111",
	}
	for _, raw := range publik {
		addr, err := netip.ParseAddr(raw)
		if err != nil {
			t.Fatalf("parse IP publik %q gagal: %v", raw, err)
		}
		if netguard.IsBlocked(addr) {
			t.Fatalf("IP publik %s tidak boleh diblokir", raw)
		}
	}
}

// TestNetguard_RedirectKeHTTPDitolak memastikan klien menolak pengalihan dari HTTPS menuju skema HTTP.
func TestNetguard_RedirectKeHTTPDitolak(t *testing.T) {
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "tidak aman")
	}))
	defer httpSrv.Close()

	httpsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, httpSrv.URL+"/plain", http.StatusFound)
	}))
	defer httpsSrv.Close()

	u, err := url.Parse(httpsSrv.URL)
	if err != nil {
		t.Fatalf("parse URL server test gagal: %v", err)
	}
	pol := netguard.Policy{AllowHosts: []string{u.Hostname()}}
	cli := netguard.NewClient(pol, 5*time.Second)
	cli.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	resp, err := cli.Get(httpsSrv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("redirect menuju skema http seharusnya ditolak")
	}
	if !errors.Is(err, netguard.ErrNonHTTPS) && !strings.Contains(strings.ToLower(err.Error()), "https") {
		t.Fatalf("error redirect tidak sesuai harapan: %v", err)
	}
}

// TestNetguard_TransportDipakaiUlang memastikan pemakaian ulang transport pada 50 unduhan tidak membocorkan goroutine.
func TestNetguard_TransportDipakaiUlang(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-media-bytes"))
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse URL server TLS gagal: %v", err)
	}
	pol := netguard.Policy{AllowHosts: []string{u.Hostname()}}
	cli := netguard.NewClient(pol, 10*time.Second)
	tr := cli.Transport.(*http.Transport)
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	defer tr.CloseIdleConnections()

	// Pemanasan awal koneksi TLS agar baseline goroutine stabil sebelum iterasi.
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		resp, err := cli.Do(req)
		if err != nil {
			t.Fatalf("pemanasan unduhan gagal: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	runtime.GC()
	baseG := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		resp, err := cli.Do(req)
		if err != nil {
			t.Fatalf("unduhan ke-%d gagal: %v", i, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	runtime.GC()
	afterG := runtime.NumGoroutine()
	if delta := afterG - baseG; delta > 4 {
		t.Fatalf("lonjakan goroutine setelah 50 unduhan terlalu tinggi: base=%d after=%d delta=%d", baseG, afterG, delta)
	}
}
