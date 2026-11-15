package obs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCgroupDir_V2DanV1 memastikan deteksi direktori cgroup v2 serta penolakan format cgroup v1.
func TestCgroupDir_V2DanV1(t *testing.T) {
	tmp := t.TempDir()

	// Format cgroup v2 (baris tunggal berawalan 0::)
	v2File := filepath.Join(tmp, "cgroup_v2")
	if err := os.WriteFile(v2File, []byte("0::/system.slice/waapi.service\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, ok := CgroupDir(v2File, "/sys/fs/cgroup")
	if !ok {
		t.Fatal("diharapkan ok=true untuk cgroup v2")
	}
	if dir != "/sys/fs/cgroup/system.slice/waapi.service" {
		t.Errorf("path cgroup v2 tidak sesuai: didapat %q", dir)
	}

	// Format cgroup v1 (tanpa baris 0::)
	v1File := filepath.Join(tmp, "cgroup_v1")
	v1Body := "11:memory:/system.slice/waapi.service\n2:cpu:/system.slice/waapi.service\n"
	if err := os.WriteFile(v1File, []byte(v1Body), 0o600); err != nil {
		t.Fatal(err)
	}
	if dir, ok := CgroupDir(v1File, "/sys/fs/cgroup"); ok || dir != "" {
		t.Errorf("diharapkan ok=false dan dir kosong untuk cgroup v1, didapat dir=%q ok=%v", dir, ok)
	}
}

// TestReadCgroupMem_Fixture memastikan file memory.* dibaca benar termasuk memory.max=max dan memory.peak opsional.
func TestReadCgroupMem_Fixture(t *testing.T) {
	got, err := ReadCgroupMem("testdata/cgroup")
	if err != nil {
		t.Fatalf("ReadCgroupMem gagal: %v", err)
	}

	if got.Current != 20766720 {
		t.Errorf("Current: diharapkan 20766720, didapat %d", got.Current)
	}
	if got.Peak != 0 {
		t.Errorf("Peak: diharapkan 0 saat file memory.peak tidak ada, didapat %d", got.Peak)
	}
	if got.High != 94371840 {
		t.Errorf("High: diharapkan 94371840, didapat %d", got.High)
	}
	if got.Max != 0 {
		t.Errorf("Max: diharapkan 0 untuk 'max', didapat %d", got.Max)
	}

	if got.Stat.Anon != 5201920 || got.Stat.File != 14008320 || got.Stat.Kernel != 983040 ||
		got.Stat.Sock != 16384 || got.Stat.Shmem != 1105920 ||
		got.Stat.FileDirty != 40960 || got.Stat.FileWriteback != 8192 {
		t.Errorf("Stat tidak sesuai fixture: %+v", got.Stat)
	}

	if got.Events.High != 3 || got.Events.Max != 1 || got.Events.OOM != 0 || got.Events.OOMKill != 0 {
		t.Errorf("Events tidak sesuai fixture: %+v", got.Events)
	}

	if got.PSI.SomeAvg10 != 1.25 || got.PSI.SomeAvg60 != 0.50 {
		t.Errorf("PSI tidak sesuai fixture: %+v", got.PSI)
	}
}

// TestParsePSI memastikan ParsePSI mem-parse nilai some/full dan mengizinkan baris full opsional.
func TestParsePSI(t *testing.T) {
	psiFull, err := ParsePSI(strings.NewReader(
		"some avg10=1.25 avg60=0.75 avg300=0.10 total=100\n" +
			"full avg10=0.50 avg60=0.25 avg300=0.05 total=40\n",
	))
	if err != nil {
		t.Fatalf("ParsePSI penuh gagal: %v", err)
	}
	if psiFull.SomeAvg10 != 1.25 || psiFull.SomeAvg60 != 0.75 || psiFull.FullAvg10 != 0.50 || psiFull.FullAvg60 != 0.25 {
		t.Errorf("hasil PSI penuh salah: %+v", psiFull)
	}

	// Hanya baris some (tanpa baris full) tetap valid
	psiSomeOnly, err := ParsePSI(strings.NewReader("some avg10=1.25 avg60=0.10 avg300=0.00 total=12\n"))
	if err != nil {
		t.Fatalf("ParsePSI some-only gagal: %v", err)
	}
	if psiSomeOnly.SomeAvg10 != 1.25 || psiSomeOnly.FullAvg10 != 0 || psiSomeOnly.FullAvg60 != 0 {
		t.Errorf("hasil PSI some-only salah: %+v", psiSomeOnly)
	}
}
