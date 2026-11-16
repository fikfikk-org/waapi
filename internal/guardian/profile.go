package guardian

import (
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

// Nama profil kesehatan akun Guardian.
const (
	ProfileStrict   = "strict"
	ProfileStandard = "standard"
	ProfileRelaxed  = "relaxed"
	ProfileOff      = "off"
)

// State kesehatan akun Guardian.
const (
	StateHealthy    = "healthy"
	StateCautious   = "cautious"
	StateRestricted = "restricted"
	StateSuspended  = "suspended"
	StateOff        = "off"
)

const shadowBootstrapDuration = 7 * 24 * time.Hour

// ProfileSpec mendeskripsikan ambang batas kuota dan skor untuk satu profil Guardian.
type ProfileSpec struct {
	Name                 string
	DailyCap             int
	FirstContactCap      int
	ContentFanoutCap     int
	GroupsDailyCap       int
	MinFirstContactScore int
	DefaultQuietHours    string
}

var profileSpecs = map[string]ProfileSpec{
	ProfileStrict: {
		Name:                 ProfileStrict,
		DailyCap:             200,
		FirstContactCap:      20,
		ContentFanoutCap:     10,
		GroupsDailyCap:       2,
		MinFirstContactScore: 70,
		DefaultQuietHours:    "21:00-07:00",
	},
	ProfileStandard: {
		Name:                 ProfileStandard,
		DailyCap:             1000,
		FirstContactCap:      50,
		ContentFanoutCap:     30,
		GroupsDailyCap:       5,
		MinFirstContactScore: 50,
	},
	ProfileRelaxed: {
		Name:                 ProfileRelaxed,
		DailyCap:             5000,
		FirstContactCap:      200,
		ContentFanoutCap:     100,
		GroupsDailyCap:       10,
		MinFirstContactScore: 30,
	},
	ProfileOff: {
		Name: ProfileOff,
	},
}

// ProfileFor mengembalikan spesifikasi profil Guardian (default ke standard bila kosong atau tak dikenal).
func ProfileFor(name string) ProfileSpec {
	if spec, ok := profileSpecs[strings.ToLower(strings.TrimSpace(name))]; ok {
		return spec
	}
	return profileSpecs[ProfileStandard]
}

// WarmupFactor mengembalikan pengali kuota berdasarkan umur pairing instance (hari 0-2: 0.25, 3-6: 0.5, 7-13: 0.75, >=14: 1.0).
func WarmupFactor(pairedAt, now time.Time) float64 {
	if pairedAt.IsZero() {
		return 1.0
	}
	age := now.Sub(pairedAt)
	if age < 0 {
		age = 0
	}
	days := int(age / (24 * time.Hour))
	switch {
	case days <= 2:
		return 0.25
	case days <= 6:
		return 0.5
	case days <= 13:
		return 0.75
	default:
		return 1.0
	}
}

// EffectiveLimits menghitung kuota harian dan first-contact efektif sesudah memperhitungkan warm-up dan penalti cautious.
func EffectiveLimits(prof ProfileSpec, state string, warmup float64) (dailyCap, firstContactCap int) {
	if prof.Name == ProfileOff {
		return 0, 0
	}
	dailyCap = int(float64(prof.DailyCap) * warmup)
	firstContactCap = int(float64(prof.FirstContactCap) * warmup)
	if state == StateCautious {
		firstContactCap /= 2
	}
	return dailyCap, firstContactCap
}

// LoadLocation mem-parse zona waktu IANA milik instance dengan fallback ke Asia/Jakarta lalu UTC.
func LoadLocation(tz string) *time.Location {
	if s := strings.TrimSpace(tz); s != "" {
		if loc, err := time.LoadLocation(s); err == nil {
			return loc
		}
	}
	if loc, err := time.LoadLocation("Asia/Jakarta"); err == nil {
		return loc
	}
	return time.UTC
}

// EffectiveQuietHours menentukan string HH:MM-HH:MM aktif untuk instance sesuai profil & settings.
func EffectiveQuietHours(settings store.InstanceSettings, prof ProfileSpec) string {
	if q := strings.TrimSpace(settings.QuietHours); q != "" {
		return q
	}
	if prof.Name == ProfileStrict {
		return prof.DefaultQuietHours
	}
	return ""
}

// CheckQuietHours memeriksa apakah waktu now berada di dalam jendela quiet hours zona waktu lokal serta mengembalikan waktu berakhirnya.
func CheckQuietHours(spec string, tz string, now time.Time) (active bool, endsAt time.Time) {
	startStr, endStr, ok := strings.Cut(strings.TrimSpace(spec), "-")
	if !ok {
		return false, time.Time{}
	}
	startH, startM, ok1 := parseHHMMParts(startStr)
	endH, endM, ok2 := parseHHMMParts(endStr)
	if !ok1 || !ok2 {
		return false, time.Time{}
	}
	startMin := startH*60 + startM
	endMin := endH*60 + endM
	if startMin == endMin {
		return false, time.Time{}
	}

	loc := LoadLocation(tz)
	tLoc := now.In(loc)
	curMin := tLoc.Hour()*60 + tLoc.Minute()
	y, m, d := tLoc.Date()

	if startMin < endMin {
		// Jendela dalam hari kalender yang sama (misal 13:00-15:00).
		if curMin >= startMin && curMin < endMin {
			return true, time.Date(y, m, d, endH, endM, 0, 0, loc)
		}
		return false, time.Time{}
	}

	// Jendela melintasi tengah malam (misal 21:00-07:00).
	if curMin >= startMin {
		return true, time.Date(y, m, d+1, endH, endM, 0, 0, loc)
	}
	if curMin < endMin {
		return true, time.Date(y, m, d, endH, endM, 0, 0, loc)
	}
	return false, time.Time{}
}

func nextMidnightLocal(now time.Time, loc *time.Location) time.Time {
	tLoc := now.In(loc)
	y, m, d := tLoc.Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, loc)
}

func startOfDayLocal(now time.Time, loc *time.Location) time.Time {
	tLoc := now.In(loc)
	y, m, d := tLoc.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, loc)
}

func parseHHMMParts(s string) (h, m int, ok bool) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 {
		return 0, 0, false
	}
	hh, err1 := strconv.Atoi(parts[0])
	mm, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || hh < 0 || hh > 23 || mm < 0 || mm > 59 {
		return 0, 0, false
	}
	return hh, mm, true
}
