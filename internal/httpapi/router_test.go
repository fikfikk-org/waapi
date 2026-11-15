package httpapi

import "testing"

// TestServerRoutes memastikan seluruh endpoint publik terautentikasi tercatat bersama scope masing-masing.
func TestServerRoutes(t *testing.T) {
	srv := NewServer(nil, nil)
	routes := srv.Routes()
	if len(routes) != 11 {
		t.Fatalf("diharapkan 11 route terdaftar, didapat %d", len(routes))
	}
	for _, rt := range routes {
		if rt.Pattern == "" || rt.Scope == "" {
			t.Fatalf("route harus memiliki pattern dan scope: %+v", rt)
		}
	}
}
