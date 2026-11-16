package outbox

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	defaultRatePerMin = 20
	defaultBurstSize  = 3
)

// TokenBucketGovernor menerapkan pembatas laju per instance berbasis token bucket (paritas v1: burst 3).
type TokenBucketGovernor struct {
	mu            sync.Mutex
	rateFn        func(instanceID string) int
	limiters      map[string]*scaledLimiter
	cooldownUntil map[string]time.Time
}

type scaledLimiter struct {
	rpm int
	lim *rate.Limiter
}

// NewTokenBucketGovernor membuat Governor token bucket per instance menggunakan rateFn untuk membaca batas per menit.
func NewTokenBucketGovernor(rateFn func(instanceID string) int) *TokenBucketGovernor {
	return &TokenBucketGovernor{
		rateFn:        rateFn,
		limiters:      make(map[string]*scaledLimiter),
		cooldownUntil: make(map[string]time.Time),
	}
}

// SetRate memperbarui laju token bucket untuk satu instance secara dinamis.
func (g *TokenBucketGovernor) SetRate(instanceID string, rpm int) {
	if g == nil || instanceID == "" {
		return
	}
	if rpm <= 0 {
		rpm = defaultRatePerMin
	}
	limit := rpmToLimit(rpm)
	g.mu.Lock()
	defer g.mu.Unlock()
	if sl, ok := g.limiters[instanceID]; ok {
		sl.rpm = rpm
		sl.lim.SetLimit(limit)
		return
	}
	g.limiters[instanceID] = &scaledLimiter{rpm: rpm, lim: rate.NewLimiter(limit, defaultBurstSize)}
}

// Precheck selalu mengizinkan saat enqueue karena throttling dilakukan ketika penjadwalan eksekusi.
func (g *TokenBucketGovernor) Precheck(context.Context, *Job) error {
	return nil
}

// Decide memeriksa apakah token bucket mengizinkan pengiriman pada waktu now, atau mengembalikan jeda tunda.
func (g *TokenBucketGovernor) Decide(ctx context.Context, job *Job, now time.Time) Decision {
	return g.DecideWithScale(ctx, job, now, 1.0)
}

// DecideWithScale memeriksa izin token bucket dengan faktor pengali laju (misal 0.5 saat state cautious).
func (g *TokenBucketGovernor) DecideWithScale(_ context.Context, job *Job, now time.Time, scale float64) Decision {
	if g == nil || job == nil {
		return Decision{Allow: true}
	}
	if scale <= 0 {
		scale = 1.0
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if until, ok := g.cooldownUntil[job.InstanceID]; ok {
		if now.Before(until) {
			return Decision{
				Allow:  false,
				Delay:  until.Sub(now),
				Code:   "rate",
				Reason: "jeda setelah batas laju WhatsApp",
			}
		}
		delete(g.cooldownUntil, job.InstanceID)
	}

	lim := g.limiterLocked(job.InstanceID, scale)
	res := lim.ReserveN(now, 1)
	if !res.OK() {
		return Decision{
			Allow:  false,
			Delay:  time.Second,
			Code:   "rate",
			Reason: "reservasi token bucket gagal",
		}
	}
	delay := res.DelayFrom(now)
	if delay > 0 {
		// Batalkan reservasi bila belum waktunya kirim agar burst tidak terkunci oleh job yang ditunda.
		res.CancelAt(now)
		return Decision{
			Allow:  false,
			Delay:  delay,
			Code:   "rate",
			Reason: "batas laju kirim per menit tercapai",
		}
	}
	return Decision{Allow: true}
}

// Record menerima umpan balik eksekusi dan menerapkan jeda darurat jika WhatsApp mengembalikan rate_overlimit.
func (g *TokenBucketGovernor) Record(_ context.Context, job *Job, out Outcome) {
	if g == nil || job == nil || !out.RateLimited {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// Pastikan pengiriman berikutnya pada instance yang sama menunggu minimal 60 detik.
	next := time.Now().Add(60 * time.Second)
	if prev, ok := g.cooldownUntil[job.InstanceID]; !ok || next.After(prev) {
		g.cooldownUntil[job.InstanceID] = next
	}
}

func (g *TokenBucketGovernor) limiterLocked(instanceID string, scale float64) *rate.Limiter {
	baseRPM := defaultRatePerMin
	if g.rateFn != nil {
		if v := g.rateFn(instanceID); v > 0 {
			baseRPM = v
		}
	}
	effRPM := int(float64(baseRPM) * scale)
	if effRPM < 1 {
		effRPM = 1
	}
	if sl, ok := g.limiters[instanceID]; ok {
		if sl.rpm != effRPM {
			sl.rpm = effRPM
			sl.lim.SetLimit(rpmToLimit(effRPM))
		}
		return sl.lim
	}
	lim := rate.NewLimiter(rpmToLimit(effRPM), defaultBurstSize)
	g.limiters[instanceID] = &scaledLimiter{rpm: effRPM, lim: lim}
	return lim
}

func rpmToLimit(rpm int) rate.Limit {
	if rpm <= 0 {
		rpm = defaultRatePerMin
	}
	return rate.Every(time.Minute / time.Duration(rpm))
}
