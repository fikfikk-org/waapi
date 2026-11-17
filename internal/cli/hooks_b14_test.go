package cli

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

type fakeHookCaller struct {
	calls []struct {
		method string
		path   string
		in     any
	}
}

func (f *fakeHookCaller) DoJSON(_ context.Context, method, path string, in, out any) error {
	f.calls = append(f.calls, struct {
		method string
		path   string
		in     any
	}{method: method, path: path, in: in})
	return nil
}

// TestCLIHookSubcommands memverifikasi parsing subperintah waapi hook ls/add/rm/enable/disable/logs/test memanggil endpoint control plane yang benar.
func TestCLIHookSubcommands(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	ruleFile := filepath.Join(tmp, "rule.json")
	eventFile := filepath.Join(tmp, "event.json")
	_ = os.WriteFile(ruleFile, []byte(`{"id":"faq","on":["message"],"do":[{"action":"mark_read"}]}`), 0644)
	_ = os.WriteFile(eventFile, []byte(`{"event":"message","data":{"chat":"6281@s.whatsapp.net","type":"text","text":"hai"}}`), 0644)

	fake := &fakeHookCaller{}
	cmds := [][]string{
		{"ls", "sales"},
		{"add", "sales", "--file", ruleFile},
		{"disable", "sales", "faq"},
		{"enable", "sales", "faq"},
		{"logs", "sales", "--hook", "faq", "--since", "1h"},
		{"test", "sales", "--file", ruleFile, "--event", eventFile},
		{"rm", "sales", "faq"},
	}
	for _, args := range cmds {
		if err := executeHook(ctx, fake, args); err != nil {
			t.Fatalf("executeHook(%v) gagal: %v", args, err)
		}
	}

	if len(fake.calls) != 7 {
		t.Fatalf("diharapkan 7 panggilan control, didapat %d", len(fake.calls))
	}
	if fake.calls[0].method != http.MethodGet || fake.calls[0].path != "/control/instances/sales/hooks" {
		t.Fatalf("hook ls salah: %+v", fake.calls[0])
	}
	if fake.calls[1].method != http.MethodPost || fake.calls[1].path != "/control/instances/sales/hooks" {
		t.Fatalf("hook add salah: %+v", fake.calls[1])
	}
	if fake.calls[2].method != http.MethodPost || fake.calls[2].path != "/control/instances/sales/hooks/faq/disable" {
		t.Fatalf("hook disable salah: %+v", fake.calls[2])
	}
	if fake.calls[3].method != http.MethodPost || fake.calls[3].path != "/control/instances/sales/hooks/faq/enable" {
		t.Fatalf("hook enable salah: %+v", fake.calls[3])
	}
	if fake.calls[4].method != http.MethodGet || fake.calls[4].path != "/control/instances/sales/hooks/logs?hook=faq&since=1h" {
		t.Fatalf("hook logs salah: %+v", fake.calls[4])
	}
	if fake.calls[5].method != http.MethodPost || fake.calls[5].path != "/control/instances/sales/hooks/test" {
		t.Fatalf("hook test salah: %+v", fake.calls[5])
	}
	if fake.calls[6].method != http.MethodDelete || fake.calls[6].path != "/control/instances/sales/hooks/faq" {
		t.Fatalf("hook rm salah: %+v", fake.calls[6])
	}
}
