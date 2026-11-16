package governor

import (
	"context"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fikfikk-org/waapi/internal/obs"
)

// Config mengatur parameter hysteresis, interval sampling, serta baseline GOGC Governor.
type Config struct {
	BudgetBytes int64         // anggaran memori anonim dari WAAPI_MEMORY_BUDGET_MIB
	Interval    time.Duration // interval sampling berkala (default 5 detik)
	YellowEnter float64       // ambang rasio masuk Yellow (default 0.75)
	YellowExit  float64       // ambang rasio keluar Yellow (default 0.65)
	RedEnter    float64       // ambang rasio masuk Red (default 0.90)
	RedExit     float64       // ambang rasio turun Red -> Yellow (default 0.80)
	PSIYellow   float64       // ambang PSI some avg10 masuk Yellow (default 5.0)
	PSIRed      float64       // ambang PSI full avg10 masuk Red (default 2.0)
	BaseGOGC    int           // nilai GOGC dasar dari env GOGC (default 50)
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Second
	}
	if c.YellowEnter <= 0 {
		c.YellowEnter = 0.75
	}
	if c.YellowExit <= 0 {
		c.YellowExit = 0.65
	}
	if c.RedEnter <= 0 {
		c.RedEnter = 0.90
	}
	if c.RedExit <= 0 {
		c.RedExit = 0.80
	}
	if c.PSIYellow <= 0 {
		c.PSIYellow = 5.0
	}
	if c.PSIRed <= 0 {
		c.PSIRed = 2.0
	}
	if c.BaseGOGC == 0 {
		c.BaseGOGC = parseEnvGOGC()
	}
	return c
}

func parseEnvGOGC() int {
	raw := os.Getenv("GOGC")
	if raw == "" {
		return 50
	}
	if raw == "off" {
		return -1
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < -1 {
		return 50
	}
	return v
}

// Sample menyimpan hasil pembacaan memori anonim (byte) dan tekanan PSI kernel pada satu titik waktu.
type Sample struct {
	AnonBytes    int64   // cgroup memory.stat anon bila tersedia, selain itu RssAnon procfs
	PSISomeAvg10 float64
	PSIFullAvg10 float64
}

// Sampler membaca satu sampel penggunaan memori proses/cgroup saat ini.
type Sampler func() (Sample, error)

type actionEntry struct {
	on          Level
	minDuration time.Duration
	name        string
	cooldown    time.Duration
	lastRun     time.Time
	fn          func(ctx context.Context)
}

// Governor mengawasi tekanan memori anonim + PSI dengan hysteresis bertahap serta menjalankan aksi mitigasi bercooldown.
type Governor struct {
	cfg          Config
	sample       Sampler
	setGCPercent func(int) int
	onTransition func(prev, next Level)

	obsMu      sync.Mutex
	level      atomic.Int32
	actMu      sync.Mutex
	actions    []actionEntry
	freeOSLast time.Time
}

// New membangun instance Governor baru dimulai pada level Green.
func New(cfg Config, sample Sampler) *Governor {
	return &Governor{
		cfg:          cfg.withDefaults(),
		sample:       sample,
		setGCPercent: debug.SetGCPercent,
	}
}

// Level mengembalikan level tekanan memori terkini secara thread-safe.
func (g *Governor) Level() Level {
	if g == nil {
		return Green
	}
	return Level(g.level.Load())
}

// Observe mengevaluasi satu Sample secara murni sesuai tabel transisi hysteresis tanpa menjalankan aksi eksternal.
func (g *Governor) Observe(s Sample) Level {
	if g == nil {
		return Green
	}

	var ratio float64
	if g.cfg.BudgetBytes > 0 && s.AnonBytes > 0 {
		ratio = float64(s.AnonBytes) / float64(g.cfg.BudgetBytes)
	}

	g.obsMu.Lock()
	cur := Level(g.level.Load())
	next := cur

	switch cur {
	case Green:
		if ratio >= g.cfg.RedEnter || s.PSIFullAvg10 >= g.cfg.PSIRed {
			next = Red
		} else if ratio >= g.cfg.YellowEnter || s.PSISomeAvg10 >= g.cfg.PSIYellow {
			next = Yellow
		}
	case Yellow:
		if ratio >= g.cfg.RedEnter || s.PSIFullAvg10 >= g.cfg.PSIRed {
			next = Red
		} else if ratio < g.cfg.YellowExit && s.PSISomeAvg10 < g.cfg.PSIYellow/2 {
			next = Green
		}
	case Red:
		// Turun bertahap satu tingkat per evaluasi (Red -> Yellow), mencegah osilasi langsung Red -> Green.
		if ratio < g.cfg.RedExit && s.PSIFullAvg10 < g.cfg.PSIRed/2 {
			next = Yellow
		}
	}

	g.level.Store(int32(next))
	g.obsMu.Unlock()
	return next
}

// RegisterAction mendaftarkan aksi edge-triggered yang dipanggil saat Governor memasuki level target (menghormati cooldown).
func (g *Governor) RegisterAction(on Level, name string, cooldown time.Duration, fn func(ctx context.Context)) {
	g.RegisterSustainedAction(on, 0, name, cooldown, fn)
}

// RegisterSustainedAction mendaftarkan aksi yang baru aktif setelah Governor bertahan terus pada level target selama minDuration.
func (g *Governor) RegisterSustainedAction(on Level, minDuration time.Duration, name string, cooldown time.Duration, fn func(ctx context.Context)) {
	if g == nil || fn == nil {
		return
	}
	g.actMu.Lock()
	defer g.actMu.Unlock()
	g.actions = append(g.actions, actionEntry{
		on:          on,
		minDuration: minDuration,
		name:        name,
		cooldown:    cooldown,
		fn:          fn,
	})
}

// SetOnTransition memasang callback opsional setiap kali level tekanan memori berubah di loop Run.
func (g *Governor) SetOnTransition(fn func(prev, next Level)) {
	if g == nil {
		return
	}
	g.actMu.Lock()
	g.onTransition = fn
	g.actMu.Unlock()
}

// TriggerFreeOSMemory menjalankan debug.FreeOSMemory() maksimal sekali setiap 60 detik (dipakai saat Red & pasca-pairing).
func (g *Governor) TriggerFreeOSMemory() bool {
	if g == nil {
		debug.FreeOSMemory()
		return true
	}
	now := time.Now()
	g.actMu.Lock()
	if !g.freeOSLast.IsZero() && now.Sub(g.freeOSLast) < 60*time.Second {
		g.actMu.Unlock()
		return false
	}
	g.freeOSLast = now
	g.actMu.Unlock()

	debug.FreeOSMemory()
	return true
}

// Run menjalankan loop sampling periodik hingga ctx dibatalkan serta mengeksekusi penyesuaian GOGC dan aksi terdaftar.
func (g *Governor) Run(ctx context.Context) {
	if g == nil {
		return
	}

	obs.MetricMemoryGovernorLevel.Set(float64(g.Level()))
	ticker := time.NewTicker(g.cfg.Interval)
	defer ticker.Stop()

	var redSince time.Time
	if g.Level() == Red {
		redSince = time.Now()
	}
	lowGOGC := g.Level() >= Yellow

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if g.sample == nil {
				continue
			}
			s, err := g.sample()
			if err != nil {
				slog.Warn("gagal membaca sampel memori governor", "err", err)
				continue
			}

			prev := g.Level()
			next := g.Observe(s)

			if next == Red {
				if prev != Red || redSince.IsZero() {
					redSince = now
				}
			} else {
				redSince = time.Time{}
			}

			if next != prev {
				obs.MetricMemoryGovernorLevel.Set(float64(next))
				g.applyTransitionTuning(next, &lowGOGC)
				g.notifyTransition(prev, next)
				g.runDueActions(ctx, next, now, 0, true)
			}

			if next == Red && !redSince.IsZero() {
				g.runDueActions(ctx, next, now, now.Sub(redSince), false)
			}
		}
	}
}

func (g *Governor) applyTransitionTuning(next Level, lowGOGC *bool) {
	setGC := g.setGCPercent
	if setGC == nil {
		setGC = debug.SetGCPercent
	}
	switch next {
	case Yellow, Red:
		if !*lowGOGC {
			setGC(25)
			*lowGOGC = true
		}
	case Green:
		if *lowGOGC {
			setGC(g.cfg.BaseGOGC)
			*lowGOGC = false
		}
	}
}

func (g *Governor) notifyTransition(prev, next Level) {
	g.actMu.Lock()
	cb := g.onTransition
	g.actMu.Unlock()
	if cb != nil {
		cb(prev, next)
	}
}

func (g *Governor) runDueActions(ctx context.Context, cur Level, now time.Time, redDur time.Duration, edgeOnly bool) {
	g.actMu.Lock()
	var due []func(context.Context)
	for idx := range g.actions {
		a := &g.actions[idx]
		if a.on != cur {
			continue
		}
		if edgeOnly && a.minDuration > 0 {
			continue
		}
		if !edgeOnly && (a.minDuration <= 0 || redDur < a.minDuration) {
			continue
		}
		if a.cooldown > 0 && !a.lastRun.IsZero() && now.Sub(a.lastRun) < a.cooldown {
			continue
		}
		a.lastRun = now
		due = append(due, a.fn)
	}
	g.actMu.Unlock()

	for _, fn := range due {
		if ctx.Err() != nil {
			return
		}
		fn(ctx)
	}
}
