package netguard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestValidateHookURL_TrustedDanNonTrusted memastikan pengecualian non-HTTPS trusted hanya mencakup host loopback dan tetap memblokir IP privat LAN.
func TestValidateHookURL_TrustedDanNonTrusted(t *testing.T) {
	cases := []struct {
		name    string
		rawURL  string
		trusted bool
		wantOK  bool
	}{
		{"https publik non-trusted", "https://hooks.example.com/cb", false, true},
		{"https publik trusted", "https://hooks.example.com/cb", true, true},
		{"http publik non-trusted ditolak", "http://hooks.example.com/cb", false, false},
		{"http publik trusted ditolak", "http://hooks.example.com/cb", true, false},
		{"http localhost non-trusted ditolak", "http://localhost:8080/hook", false, false},
		{"http localhost trusted diizinkan", "http://localhost:8080/hook", true, true},
		{"http 127.0.0.1 trusted diizinkan", "http://127.0.0.1:9000/hook", true, true},
		{"http [::1] trusted diizinkan", "http://[::1]:9000/hook", true, true},
		{"https 127.0.0.1 non-trusted ditolak", "https://127.0.0.1:9000/hook", false, false},
		{"http LAN 192.168 trusted tetap ditolak", "http://192.168.1.50/hook", true, false},
		{"https LAN 10.0 trusted tetap ditolak", "https://10.0.0.5/hook", true, false},
		{"http metadata AWS trusted tetap ditolak", "http://169.254.169.254/latest", true, false},
		{"https 172.16 trusted tetap ditolak", "https://172.16.0.10/hook", true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateHookURL(tc.rawURL, tc.trusted)
			if (err == nil) != tc.wantOK {
				t.Fatalf("ValidateHookURL(%q, trusted=%v) got err=%v, wantOK=%v", tc.rawURL, tc.trusted, err, tc.wantOK)
			}
		})
	}

	// Client non-trusted menolak dial ke 127.0.0.1, sedangkan client trusted berhasil memanggil 127.0.0.1 httptest.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	untrustedCli := NewHookClient(false, time.Second)
	if _, err := untrustedCli.Get(srv.URL); err == nil {
		t.Fatalf("client hook non-trusted wajib menolak localhost httptest")
	}

	httpsLocalURL := strings.Replace(srv.URL, "http://", "https://", 1)
	if _, err := untrustedCli.Get(httpsLocalURL); err == nil {
		t.Fatalf("client hook non-trusted wajib menolak https loopback")
	}

	trustedCli := NewHookClient(true, time.Second)
	resp, err := trustedCli.Get(srv.URL)
	if err != nil {
		t.Fatalf("client hook trusted wajib berhasil memanggil loopback: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status HTTP tidak sesuai: %d", resp.StatusCode)
	}
}
