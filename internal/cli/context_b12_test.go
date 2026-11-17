package cli

import (
	"context"
	"net/http"
	"testing"
)

type fakeContextCaller struct {
	calls []struct {
		method string
		path   string
		body   map[string]any
	}
}

func (f *fakeContextCaller) DoJSON(_ context.Context, method, path string, in, _ any) error {
	var body map[string]any
	if m, ok := in.(map[string]any); ok {
		body = m
	}
	f.calls = append(f.calls, struct {
		method string
		path   string
		body   map[string]any
	}{method: method, path: path, body: body})
	return nil
}

// TestCLI_ContextCommand memastikan parsing perintah CLI waapi context on/off/purge memanggil endpoint control plane yang benar.
func TestCLI_ContextCommand(t *testing.T) {
	ctx := context.Background()
	fake := &fakeContextCaller{}

	if err := executeContext(ctx, fake, []string{"sales", "on"}); err != nil {
		t.Fatalf("context sales on gagal: %v", err)
	}
	if err := executeContext(ctx, fake, []string{"sales", "off", "--purge"}); err != nil {
		t.Fatalf("context sales off --purge gagal: %v", err)
	}
	if err := executeContext(ctx, fake, []string{"sales", "purge", "--chat", "628111@s.whatsapp.net"}); err != nil {
		t.Fatalf("context sales purge --chat gagal: %v", err)
	}

	if len(fake.calls) != 3 {
		t.Fatalf("jumlah panggilan API kontrol diharapkan 3, didapat %d", len(fake.calls))
	}
	if fake.calls[0].method != http.MethodPost || fake.calls[0].path != "/control/instances/sales/context" || fake.calls[0].body["enabled"] != true {
		t.Fatalf("panggilan on tidak sesuai: %+v", fake.calls[0])
	}
	if fake.calls[1].path != "/control/instances/sales/context" || fake.calls[1].body["enabled"] != false || fake.calls[1].body["purge"] != true {
		t.Fatalf("panggilan off --purge tidak sesuai: %+v", fake.calls[1])
	}
	if fake.calls[2].path != "/control/instances/sales/context/purge" || fake.calls[2].body["chat"] != "628111@s.whatsapp.net" {
		t.Fatalf("panggilan purge --chat tidak sesuai: %+v", fake.calls[2])
	}
}
