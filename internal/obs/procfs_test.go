package obs

import "testing"

// TestReadProcStatus_Fixture memverifikasi pembacaan /proc/self/status dari fixture asli Linux.
func TestReadProcStatus_Fixture(t *testing.T) {
	got, err := ReadProcStatus("testdata/status")
	if err != nil {
		t.Fatalf("ReadProcStatus gagal: %v", err)
	}

	const (
		wantVmRSS    = uint64(20280 * 1024)
		wantVmHWM    = uint64(21440 * 1024)
		wantRssAnon  = uint64(5120 * 1024)
		wantRssFile  = uint64(14080 * 1024)
		wantRssShmem = uint64(1080 * 1024)
		wantThreads  = 7
	)

	if got.VmRSS != wantVmRSS {
		t.Errorf("VmRSS: diharapkan %d, didapat %d", wantVmRSS, got.VmRSS)
	}
	if got.VmHWM != wantVmHWM {
		t.Errorf("VmHWM: diharapkan %d, didapat %d", wantVmHWM, got.VmHWM)
	}
	if got.RssAnon != wantRssAnon {
		t.Errorf("RssAnon: diharapkan %d, didapat %d", wantRssAnon, got.RssAnon)
	}
	if got.RssFile != wantRssFile {
		t.Errorf("RssFile: diharapkan %d, didapat %d", wantRssFile, got.RssFile)
	}
	if got.RssShmem != wantRssShmem {
		t.Errorf("RssShmem: diharapkan %d, didapat %d", wantRssShmem, got.RssShmem)
	}
	if got.Threads != wantThreads {
		t.Errorf("Threads: diharapkan %d, didapat %d", wantThreads, got.Threads)
	}
}

// TestReadSmapsRollup_Fixture memverifikasi pembacaan /proc/self/smaps_rollup dari fixture asli Linux.
func TestReadSmapsRollup_Fixture(t *testing.T) {
	got, err := ReadSmapsRollup("testdata/smaps_rollup")
	if err != nil {
		t.Fatalf("ReadSmapsRollup gagal: %v", err)
	}

	const (
		wantRss          = uint64(20280 * 1024)
		wantPss          = uint64(19840 * 1024)
		wantPssAnon      = uint64(5080 * 1024)
		wantPssFile      = uint64(13680 * 1024)
		wantPssShmem     = uint64(1080 * 1024)
		wantPrivateDirty = uint64(5120 * 1024)
	)

	if got.Rss != wantRss {
		t.Errorf("Rss: diharapkan %d, didapat %d", wantRss, got.Rss)
	}
	if got.Pss != wantPss {
		t.Errorf("Pss: diharapkan %d, didapat %d", wantPss, got.Pss)
	}
	if got.PssAnon != wantPssAnon {
		t.Errorf("PssAnon: diharapkan %d, didapat %d", wantPssAnon, got.PssAnon)
	}
	if got.PssFile != wantPssFile {
		t.Errorf("PssFile: diharapkan %d, didapat %d", wantPssFile, got.PssFile)
	}
	if got.PssShmem != wantPssShmem {
		t.Errorf("PssShmem: diharapkan %d, didapat %d", wantPssShmem, got.PssShmem)
	}
	if got.PrivateDirty != wantPrivateDirty {
		t.Errorf("PrivateDirty: diharapkan %d, didapat %d", wantPrivateDirty, got.PrivateDirty)
	}
}
