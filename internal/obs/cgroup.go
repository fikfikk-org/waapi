package obs

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// CgroupStat merangkum pecahan penggunaan memori pada file memory.stat cgroup v2.
type CgroupStat struct {
	Anon          uint64 `json:"anon_bytes"`
	File          uint64 `json:"file_bytes"`
	Kernel        uint64 `json:"kernel_bytes"`
	Sock          uint64 `json:"sock_bytes"`
	Shmem         uint64 `json:"shmem_bytes"`
	FileDirty     uint64 `json:"file_dirty_bytes"`
	FileWriteback uint64 `json:"file_writeback_bytes"`
}

// CgroupEvents menyimpan penghitung kejadian batas memori pada memory.events cgroup v2.
type CgroupEvents struct {
	High    uint64 `json:"high"`
	Max     uint64 `json:"max"`
	OOM     uint64 `json:"oom"`
	OOMKill uint64 `json:"oom_kill"`
}

// PSI menyimpan rata-rata tekanan memori kernel dari memory.pressure (Pressure Stall Information).
type PSI struct {
	SomeAvg10 float64 `json:"some_avg10"`
	SomeAvg60 float64 `json:"some_avg60"`
	FullAvg10 float64 `json:"full_avg10"`
	FullAvg60 float64 `json:"full_avg60"`
}

// CgroupMem merangkum penggunaan, batas, event, dan tekanan memori cgroup v2 proses.
type CgroupMem struct {
	Current uint64       `json:"current_bytes"`
	Peak    uint64       `json:"peak_bytes"`
	High    uint64       `json:"high_bytes"`
	Max     uint64       `json:"max_bytes"`
	Stat    CgroupStat   `json:"stat"`
	Events  CgroupEvents `json:"events"`
	PSI     PSI          `json:"psi"`
}

// CgroupDir mem-parse /proc/self/cgroup (atau isi teksnya) untuk mendapatkan path direktori cgroup v2 di bawah root.
func CgroupDir(procSelfCgroup, root string) (string, bool) {
	if strings.TrimSpace(root) == "" {
		root = "/sys/fs/cgroup"
	}

	content := procSelfCgroup
	// Terima path file maupun isi teks langsung agar fleksibel dipakai pada produksi dan pengujian
	if !strings.ContainsAny(procSelfCgroup, "\n\r") && !strings.HasPrefix(procSelfCgroup, "0::") {
		b, err := os.ReadFile(procSelfCgroup)
		if err != nil {
			return "", false
		}
		content = string(b)
	}

	sc := bufio.NewScanner(strings.NewReader(content))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "0::") {
			continue
		}
		rel := strings.TrimPrefix(line, "0::")
		rel = strings.TrimSpace(rel)
		if rel == "" {
			rel = "/"
		}
		relClean := strings.TrimPrefix(path.Clean(rel), "/")
		if strings.HasPrefix(root, "/") {
			return path.Join(root, relClean), true
		}
		return filepath.Join(root, filepath.FromSlash(relClean)), true
	}
	return "", false
}

// ReadCgroupMem membaca metrik memory.* cgroup v2 dari direktori dir yang ditentukan.
func ReadCgroupMem(dir string) (CgroupMem, error) {
	var out CgroupMem

	cur, err := readCgroupUintOrMax(filepath.Join(dir, "memory.current"), false)
	if err != nil {
		return CgroupMem{}, err
	}
	out.Current = cur

	// memory.peak bersifat opsional (kernel lama tidak menyediakannya); tidak ada -> 0 tanpa error
	peak, err := readCgroupUintOrMax(filepath.Join(dir, "memory.peak"), true)
	if err != nil {
		return CgroupMem{}, err
	}
	out.Peak = peak

	high, err := readCgroupUintOrMax(filepath.Join(dir, "memory.high"), true)
	if err != nil {
		return CgroupMem{}, err
	}
	out.High = high

	maxVal, err := readCgroupUintOrMax(filepath.Join(dir, "memory.max"), true)
	if err != nil {
		return CgroupMem{}, err
	}
	out.Max = maxVal

	if err := readCgroupKeyValues(filepath.Join(dir, "memory.stat"), func(k string, v uint64) {
		switch k {
		case "anon":
			out.Stat.Anon = v
		case "file":
			out.Stat.File = v
		case "kernel":
			out.Stat.Kernel = v
		case "sock":
			out.Stat.Sock = v
		case "shmem":
			out.Stat.Shmem = v
		case "file_dirty":
			out.Stat.FileDirty = v
		case "file_writeback":
			out.Stat.FileWriteback = v
		}
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return CgroupMem{}, err
	}

	if err := readCgroupKeyValues(filepath.Join(dir, "memory.events"), func(k string, v uint64) {
		switch k {
		case "high":
			out.Events.High = v
		case "max":
			out.Events.Max = v
		case "oom":
			out.Events.OOM = v
		case "oom_kill":
			out.Events.OOMKill = v
		}
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return CgroupMem{}, err
	}

	f, err := os.Open(filepath.Join(dir, "memory.pressure"))
	if err == nil {
		defer f.Close()
		psi, perr := ParsePSI(f)
		if perr != nil {
			return CgroupMem{}, perr
		}
		out.PSI = psi
	} else if !errors.Is(err, os.ErrNotExist) {
		return CgroupMem{}, err
	}

	return out, nil
}

// ParsePSI mem-parse isi memory.pressure format Linux kernel PSI menjadi struct PSI.
func ParsePSI(r io.Reader) (PSI, error) {
	var out PSI
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		kind := fields[0]
		if kind != "some" && kind != "full" {
			continue
		}
		for _, tok := range fields[1:] {
			k, valStr, ok := strings.Cut(tok, "=")
			if !ok {
				continue
			}
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				return PSI{}, fmt.Errorf("nilai PSI tidak valid %q: %w", tok, err)
			}
			switch kind + "." + k {
			case "some.avg10":
				out.SomeAvg10 = val
			case "some.avg60":
				out.SomeAvg60 = val
			case "full.avg10":
				out.FullAvg10 = val
			case "full.avg60":
				out.FullAvg60 = val
			}
		}
	}
	if err := sc.Err(); err != nil {
		return PSI{}, err
	}
	return out, nil
}

func readCgroupUintOrMax(p string, optional bool) (uint64, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		if optional && errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" || s == "max" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s (%q): %w", p, s, err)
	}
	return v, nil
}

func readCgroupKeyValues(p string, onKV func(string, uint64)) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
			onKV(fields[0], v)
		}
	}
	return sc.Err()
}
