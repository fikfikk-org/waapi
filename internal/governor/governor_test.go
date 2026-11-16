package governor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// TestObserve_Hysteresis memverifikasi seluruh transisi level tekanan memori termasuk hysteresis dan step-down bertahap Red -> Yellow -> Green.
func TestObserve_Hysteresis(t *testing.T) {
	const budget int64 = 100 << 20 // 100 MiB

	g := New(Config{
		BudgetBytes: budget,
		YellowEnter: 0.75,
		YellowExit:  0.65,
		RedEnter:    0.90,
		RedExit:     0.80,
		PSIYellow:   5.0,
		PSIRed:      2.0,
		BaseGOGC:    50,
	}, nil)

	mb := func(n float64) int64 {
		return int64(n * float64(budget))
	}

	steps := []struct {
		name   string
		sample Sample
		want   Level
	}{
		// Mulai Green
		{name: "awal_aman", sample: Sample{AnonBytes: mb(0.50)}, want: Green},
		{name: "hampir_yellow_masih_green", sample: Sample{AnonBytes: mb(0.74), PSISomeAvg10: 4.9}, want: Green},

		// Green -> Yellow via rasio anon
		{name: "masuk_yellow_rasio", sample: Sample{AnonBytes: mb(0.75)}, want: Yellow},
		// Hysteresis Yellow bertahan antara 0.65 dan 0.75
		{name: "tahan_yellow_070", sample: Sample{AnonBytes: mb(0.70)}, want: Yellow},
		{name: "tahan_yellow_065_batas_bawah", sample: Sample{AnonBytes: mb(0.65)}, want: Yellow},
		// Rasionya turun < 0.65 tapi PSI some masih >= PSIYellow/2 (2.5) -> tetap Yellow
		{name: "tahan_yellow_karena_psi_some", sample: Sample{AnonBytes: mb(0.50), PSISomeAvg10: 2.5}, want: Yellow},
		// Yellow -> Green ketika rasio < 0.65 DAN PSI some < 2.5
		{name: "keluar_ke_green", sample: Sample{AnonBytes: mb(0.64), PSISomeAvg10: 2.4}, want: Green},

		// Green -> Yellow via PSI some
		{name: "masuk_yellow_psi", sample: Sample{AnonBytes: mb(0.20), PSISomeAvg10: 5.0}, want: Yellow},
		// Yellow -> Red via rasio
		{name: "naik_ke_red_rasio", sample: Sample{AnonBytes: mb(0.90)}, want: Red},
		// Hysteresis Red bertahan antara 0.80 dan 0.90
		{name: "tahan_red_085", sample: Sample{AnonBytes: mb(0.85)}, want: Red},
		{name: "tahan_red_080_batas_bawah", sample: Sample{AnonBytes: mb(0.80)}, want: Red},
		// Rasio < 0.80 tapi PSI full masih >= PSIRed/2 (1.0) -> tetap Red
		{name: "tahan_red_karena_psi_full", sample: Sample{AnonBytes: mb(0.40), PSIFullAvg10: 1.0}, want: Red},

		// Turun drastis dari Red (< YellowExit): WAJIB bertahap Red -> Yellow dulu, baru sampel berikut -> Green
		{name: "turun_bertahap_red_ke_yellow", sample: Sample{AnonBytes: mb(0.30), PSIFullAvg10: 0.5, PSISomeAvg10: 0.5}, want: Yellow},
		{name: "sampel_berikut_yellow_ke_green", sample: Sample{AnonBytes: mb(0.30), PSIFullAvg10: 0.5, PSISomeAvg10: 0.5}, want: Green},

		// Lompatan langsung Green -> Red via PSI full
		{name: "lompat_green_ke_red_psi", sample: Sample{AnonBytes: mb(0.30), PSIFullAvg10: 2.0}, want: Red},
		// Red -> Yellow meski rasio 0.78 tapi tidak boleh melompat keluar lagi jika kondisi masih Yellow
		{name: "turun_red_ke_yellow_tahan", sample: Sample{AnonBytes: mb(0.78), PSIFullAvg10: 0.9}, want: Yellow},
		{name: "kembali_stabil_green", sample: Sample{AnonBytes: mb(0.40)}, want: Green},
	}

	for _, tc := range steps {
		got := g.Observe(tc.sample)
		if got != tc.want {
			t.Fatalf("langkah %q (%+v): diharapkan level=%v, didapat=%v", tc.name, tc.sample, tc.want, got)
		}
		if g.Level() != tc.want {
			t.Fatalf("Level() setelah langkah %q: diharapkan %v, didapat %v", tc.name, tc.want, g.Level())
		}
	}
}

// TestActions_EdgeTriggeredDenganCooldown memastikan aksi bersifat edge-triggered, menghormati cooldown 60s, serta mendukung aksi durasi Red >= 10m dengan cooldown 30m.
func TestActions_EdgeTriggeredDenganCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cur Sample
		var curMu atomic.Value
		curMu.Store(Sample{AnonBytes: 10 << 20})
		setSample := func(ratio float64) {
			cur = Sample{AnonBytes: int64(ratio * float64(100<<20))}
			curMu.Store(cur)
		}

		g := New(Config{
			BudgetBytes: 100 << 20,
			Interval:    5 * time.Second,
			BaseGOGC:    50,
		}, func() (Sample, error) {
			return curMu.Load().(Sample), nil
		})
		g.setGCPercent = func(int) int { return 50 }

		var shrinkCalls atomic.Int32
		var freeOSCalls atomic.Int32
		var recycleCalls atomic.Int32

		g.RegisterAction(Yellow, "shrink_memory", 60*time.Second, func(context.Context) {
			shrinkCalls.Add(1)
		})
		g.RegisterAction(Red, "free_os_memory", 60*time.Second, func(context.Context) {
			freeOSCalls.Add(1)
		})
		g.RegisterSustainedAction(Red, 10*time.Minute, "auto_recycle", 30*time.Minute, func(context.Context) {
			recycleCalls.Add(1)
		})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			g.Run(ctx)
		}()

		synctest.Wait()

		// 1. Masuk Yellow pertama kali -> shrink_memory jalan 1x
		setSample(0.80)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if shrinkCalls.Load() != 1 {
			t.Fatalf("shrink_memory diharapkan 1x, didapat %d", shrinkCalls.Load())
		}

		// 2. Tetap Yellow beberapa tick -> tidak memicu ulang walau idle
		time.Sleep(15 * time.Second)
		synctest.Wait()
		if shrinkCalls.Load() != 1 {
			t.Fatalf("aksi edge-triggered tidak boleh berulang saat bertahan di Yellow: %d", shrinkCalls.Load())
		}

		// 3. Flapping Green -> Yellow 2x lagi di dalam jendela 60s -> tetap 1x
		for range 2 {
			setSample(0.20)
			time.Sleep(5 * time.Second)
			synctest.Wait()
			setSample(0.80)
			time.Sleep(5 * time.Second)
			synctest.Wait()
		}
		if shrinkCalls.Load() != 1 {
			t.Fatalf("masuk Yellow 3x dalam < 60s harus memicu shrink_memory 1x saja, didapat %d", shrinkCalls.Load())
		}

		// 4. Lewati cooldown 60s dari eksekusi pertama, keluar ke Green lalu masuk Yellow lagi -> jalan ke-2
		setSample(0.20)
		time.Sleep(35 * time.Second)
		synctest.Wait()
		setSample(0.80)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if shrinkCalls.Load() != 2 {
			t.Fatalf("shrink_memory setelah cooldown 60s diharapkan 2x, didapat %d", shrinkCalls.Load())
		}

		// 5. Masuk Red -> FreeOSMemory jalan 1x; flapping Red -> Yellow -> Green -> Red dalam 60s tetap 1x
		setSample(0.95)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if freeOSCalls.Load() != 1 {
			t.Fatalf("free_os_memory pertama diharapkan 1x, didapat %d", freeOSCalls.Load())
		}

		setSample(0.30)
		time.Sleep(10 * time.Second) // Red -> Yellow -> Green
		synctest.Wait()
		setSample(0.95)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if freeOSCalls.Load() != 1 {
			t.Fatalf("free_os_memory dalam cooldown 60s diharapkan tetap 1x, didapat %d", freeOSCalls.Load())
		}

		// 6. Bertahan di Red: sebelum 10 menit auto_recycle 0x, tepat 10 menit menjadi 1x
		time.Sleep(9*time.Minute + 50*time.Second)
		synctest.Wait()
		if recycleCalls.Load() != 0 {
			t.Fatalf("auto_recycle sebelum Red 10m seharusnya 0x, didapat %d", recycleCalls.Load())
		}
		time.Sleep(15 * time.Second)
		synctest.Wait()
		if recycleCalls.Load() != 1 {
			t.Fatalf("auto_recycle setelah Red >= 10m seharusnya 1x, didapat %d", recycleCalls.Load())
		}

		// Masih di Red selama 20 menit lagi (total 20m < cooldown 30m) -> tetap 1x
		time.Sleep(20 * time.Minute)
		synctest.Wait()
		if recycleCalls.Load() != 1 {
			t.Fatalf("auto_recycle dalam cooldown 30m seharusnya tetap 1x, didapat %d", recycleCalls.Load())
		}

		// Lewati cooldown 30m -> jalan ke-2
		time.Sleep(11 * time.Minute)
		synctest.Wait()
		if recycleCalls.Load() != 2 {
			t.Fatalf("auto_recycle setelah cooldown 30m seharusnya 2x, didapat %d", recycleCalls.Load())
		}

		cancel()
		<-done
	})
}

// TestGOGC_DikembalikanSaatGreen memverifikasi debug.SetGCPercent disetel ke 25 saat Yellow/Red dan dikembalikan ke BaseGOGC saat Green.
func TestGOGC_DikembalikanSaatGreen(t *testing.T) {
	t.Setenv("GOGC", "65")

	synctest.Test(t, func(t *testing.T) {
		var curMu atomic.Value
		curMu.Store(Sample{AnonBytes: 20 << 20})
		setSample := func(ratio float64) {
			curMu.Store(Sample{AnonBytes: int64(ratio * float64(100<<20))})
		}

		g := New(Config{
			BudgetBytes: 100 << 20,
			Interval:    5 * time.Second,
		}, func() (Sample, error) {
			return curMu.Load().(Sample), nil
		})
		if g.cfg.BaseGOGC != 65 {
			t.Fatalf("BaseGOGC diharapkan 65 dari env GOGC, didapat %d", g.cfg.BaseGOGC)
		}

		var recorded []int
		g.setGCPercent = func(v int) int {
			recorded = append(recorded, v)
			return v
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			g.Run(ctx)
		}()
		synctest.Wait()

		// Green -> Yellow: SetGCPercent(25)
		setSample(0.76)
		time.Sleep(5 * time.Second)
		synctest.Wait()

		// Yellow -> Green: kembali ke BaseGOGC (65)
		setSample(0.40)
		time.Sleep(5 * time.Second)
		synctest.Wait()

		// Green -> Red langsung: SetGCPercent(25)
		setSample(0.95)
		time.Sleep(5 * time.Second)
		synctest.Wait()

		// Red -> Yellow -> Green: baru dikembalikan ke 65 setelah tiba di Green
		setSample(0.40)
		time.Sleep(5 * time.Second) // Red -> Yellow
		synctest.Wait()
		time.Sleep(5 * time.Second) // Yellow -> Green
		synctest.Wait()

		cancel()
		<-done

		want := []int{25, 65, 25, 65}
		if len(recorded) != len(want) {
			t.Fatalf("riwayat SetGCPercent diharapkan %v, didapat %v", want, recorded)
		}
		for i := range want {
			if recorded[i] != want[i] {
				t.Fatalf("riwayat SetGCPercent[%d] diharapkan %d, didapat %d (semua=%v)", i, want[i], recorded[i], recorded)
			}
		}
	})
}

// TestSampler_CgroupLaluFallbackProc memverifikasi prioritas pembacaan anon cgroup v2 serta fallback RssAnon procfs.
func TestSampler_CgroupLaluFallbackProc(t *testing.T) {
	tmp := t.TempDir()
	cgRoot := filepath.Join(tmp, "cgroup")
	cgSlice := filepath.Join(cgRoot, "system.slice", "waapi.service")
	if err := os.MkdirAll(cgSlice, 0o755); err != nil {
		t.Fatal(err)
	}

	procCgroupV2 := filepath.Join(tmp, "proc_cgroup_v2")
	if err := os.WriteFile(procCgroupV2, []byte("0::/system.slice/waapi.service\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgSlice, "memory.current"), []byte("33554432\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgSlice, "memory.stat"), []byte("anon 12582912\nfile 20971520\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgSlice, "memory.pressure"), []byte(
		"some avg10=4.20 avg60=1.10 avg300=0.00 total=100\n"+
			"full avg10=1.75 avg60=0.40 avg300=0.00 total=20\n",
	), 0o600); err != nil {
		t.Fatal(err)
	}

	procStatus := filepath.Join(tmp, "proc_status")
	if err := os.WriteFile(procStatus, []byte(
		"Name:\twaapi\n"+
			"VmRSS:\t   90000 kB\n"+
			"RssAnon:\t    4096 kB\n"+
			"RssFile:\t   85904 kB\n",
	), 0o600); err != nil {
		t.Fatal(err)
	}

	// Kasus 1: Cgroup v2 tersedia -> pakai memory.stat anon dan PSI (bukan VmRSS/RssAnon)
	cgSampler := NewProcSampler(procCgroupV2, cgRoot, procStatus)
	s1, err := cgSampler()
	if err != nil {
		t.Fatalf("cgSampler gagal: %v", err)
	}
	if s1.AnonBytes != 12582912 {
		t.Fatalf("AnonBytes cgroup diharapkan 12582912, didapat %d", s1.AnonBytes)
	}
	if s1.PSISomeAvg10 != 4.20 || s1.PSIFullAvg10 != 1.75 {
		t.Fatalf("PSI cgroup tidak sesuai: %+v", s1)
	}

	// Kasus 2: Tidak ada cgroup v2 -> fallback ke RssAnon (4096 KiB = 4194304), bukan VmRSS
	procCgroupMissing := filepath.Join(tmp, "proc_cgroup_v1")
	if err := os.WriteFile(procCgroupMissing, []byte("11:memory:/system.slice/waapi.service\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fallbackSampler := NewProcSampler(procCgroupMissing, cgRoot, procStatus)
	s2, err := fallbackSampler()
	if err != nil {
		t.Fatalf("fallbackSampler gagal: %v", err)
	}
	if s2.AnonBytes != 4096<<10 {
		t.Fatalf("AnonBytes fallback proc diharapkan %d (RssAnon), didapat %d", 4096<<10, s2.AnonBytes)
	}
	if s2.PSISomeAvg10 != 0 || s2.PSIFullAvg10 != 0 {
		t.Fatalf("PSI fallback seharusnya 0, didapat %+v", s2)
	}
}

// TestPageout_ParseMaps memverifikasi parser /proc/self/maps memilih hanya mapping r-xp dan r--p milik binary executable.
func TestPageout_ParseMaps(t *testing.T) {
	const rawMaps = "" +
		"00400000-00a00000 r-xp 00000000 fd:01 1001   /usr/local/bin/waapi\n" +
		"00a00000-00c50000 r--p 00600000 fd:01 1001   /usr/local/bin/waapi\n" +
		"00c50000-00d00000 rw-p 00850000 fd:01 1001   /usr/local/bin/waapi\n" +
		"01000000-01100000 rw-p 00000000 00:00 0      [heap]\n" +
		"7f0000000000-7f0000020000 r-xp 00000000 fd:01 2002 /lib/aarch64-linux-gnu/libc.so.6\n" +
		"7f0000100000-7f0000200000 r--p 00000000 fd:01 3003 /usr/local/bin/waapi (deleted)\n" +
		"7fff00000000-7fff00021000 rw-p 00000000 00:00 0 [stack]\n"

	ranges, err := ParseExecutableMaps(strings.NewReader(rawMaps), "/usr/local/bin/waapi")
	if err != nil {
		t.Fatalf("ParseExecutableMaps gagal: %v", err)
	}
	if len(ranges) != 2 {
		t.Fatalf("diharapkan 2 rentang (r-xp dan r--p milik executable), didapat %d: %+v", len(ranges), ranges)
	}
	if ranges[0].Start != 0x00400000 || ranges[0].Length != 0x00a00000-0x00400000 || ranges[0].Perms != "r-xp" {
		t.Fatalf("rentang 0 tidak sesuai: %+v", ranges[0])
	}
	if ranges[1].Start != 0x00a00000 || ranges[1].Length != 0x00c50000-0x00a00000 || ranges[1].Perms != "r--p" {
		t.Fatalf("rentang 1 tidak sesuai: %+v", ranges[1])
	}
}
