package obs

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ProcMem merangkum ukuran memori proses dari /proc/self/status dalam satuan byte.
type ProcMem struct {
	VmRSS    uint64 `json:"vm_rss_bytes"`
	VmHWM    uint64 `json:"vm_hwm_bytes"`
	RssAnon  uint64 `json:"rss_anon_bytes"`
	RssFile  uint64 `json:"rss_file_bytes"`
	RssShmem uint64 `json:"rss_shmem_bytes"`
	Threads  int    `json:"threads"`
}

// Smaps merangkum rincian PSS dan Dirty halaman memori dari /proc/self/smaps_rollup dalam byte.
type Smaps struct {
	Rss          uint64 `json:"rss_bytes"`
	Pss          uint64 `json:"pss_bytes"`
	PssAnon      uint64 `json:"pss_anon_bytes"`
	PssFile      uint64 `json:"pss_file_bytes"`
	PssShmem     uint64 `json:"pss_shmem_bytes"`
	PrivateDirty uint64 `json:"private_dirty_bytes"`
}

// ReadProcStatus membaca field VmRSS, VmHWM, RssAnon, RssFile, RssShmem, dan Threads dari file status procfs.
func ReadProcStatus(path string) (ProcMem, error) {
	f, err := os.Open(path)
	if err != nil {
		return ProcMem{}, err
	}
	defer f.Close()

	var out ProcMem
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		key, valStr, ok := parseProcKV(sc.Text())
		if !ok {
			continue
		}
		switch key {
		case "VmRSS":
			out.VmRSS = parseProcKiBToBytes(valStr)
		case "VmHWM":
			out.VmHWM = parseProcKiBToBytes(valStr)
		case "RssAnon":
			out.RssAnon = parseProcKiBToBytes(valStr)
		case "RssFile":
			out.RssFile = parseProcKiBToBytes(valStr)
		case "RssShmem":
			out.RssShmem = parseProcKiBToBytes(valStr)
		case "Threads":
			if n, err := strconv.Atoi(valStr); err == nil {
				out.Threads = n
			}
		}
	}
	if err := sc.Err(); err != nil {
		return ProcMem{}, fmt.Errorf("scan %s: %w", path, err)
	}
	return out, nil
}

// ReadSmapsRollup membaca ringkasan PSS/RSS dari /proc/self/smaps_rollup dalam byte.
func ReadSmapsRollup(path string) (Smaps, error) {
	f, err := os.Open(path)
	if err != nil {
		return Smaps{}, err
	}
	defer f.Close()

	var out Smaps
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		key, valStr, ok := parseProcKV(sc.Text())
		if !ok {
			continue
		}
		switch key {
		case "Rss":
			out.Rss = parseProcKiBToBytes(valStr)
		case "Pss":
			out.Pss = parseProcKiBToBytes(valStr)
		case "Pss_Anon":
			out.PssAnon = parseProcKiBToBytes(valStr)
		case "Pss_File":
			out.PssFile = parseProcKiBToBytes(valStr)
		case "Pss_Shmem":
			out.PssShmem = parseProcKiBToBytes(valStr)
		case "Private_Dirty":
			out.PrivateDirty = parseProcKiBToBytes(valStr)
		}
	}
	if err := sc.Err(); err != nil {
		return Smaps{}, fmt.Errorf("scan %s: %w", path, err)
	}
	return out, nil
}

func parseProcKV(line string) (key string, firstToken string, ok bool) {
	k, rest, found := strings.Cut(line, ":")
	if !found {
		return "", "", false
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", "", false
	}
	return strings.TrimSpace(k), fields[0], true
}

func parseProcKiBToBytes(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v * 1024
}
