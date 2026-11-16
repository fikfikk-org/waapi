package cli

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/fikfikk-org/waapi/internal/guardian"
)

type fakeHealthCaller struct {
	hitMethod string
	hitPath   string
	resp      guardian.HealthReport
	err       error
}

func (f *fakeHealthCaller) DoJSON(_ context.Context, method, path string, _ any, out any) error {
	f.hitMethod = method
	f.hitPath = path
	if f.err != nil {
		return f.err
	}
	if ptr, ok := out.(*guardian.HealthReport); ok {
		*ptr = f.resp
	}
	return nil
}

// TestCLI_HealthCommand memverifikasi perintah waapi health <id> memanggil /control/instances/{id}/health dan mendukung --json.
func TestCLI_HealthCommand(t *testing.T) {
	fc := &fakeHealthCaller{
		resp: guardian.HealthReport{
			Instance: "sales",
			State:    "healthy",
			Score:    96,
			Profile:  "standard",
			Factors:  []guardian.Factor{},
		},
	}

	if err := executeHealth(context.Background(), fc, []string{"sales", "--json"}); err != nil {
		t.Fatalf("executeHealth gagal: %v", err)
	}
	if fc.hitMethod != http.MethodGet || fc.hitPath != "/control/instances/sales/health" {
		t.Fatalf("permintaan control tidak sesuai: method=%s path=%s", fc.hitMethod, fc.hitPath)
	}
	if err := executeHealth(context.Background(), fc, nil); err == nil {
		t.Fatal("executeHealth tanpa argumen id seharusnya mengembalikan error")
	}
	fc.err = errors.New("gagal koneksi")
	if err := executeHealth(context.Background(), fc, []string{"sales"}); err == nil {
		t.Fatal("error caller seharusnya diteruskan")
	}
}
