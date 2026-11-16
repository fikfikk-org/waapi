// Package guardian menghitung skor kesehatan akun WhatsApp dan mengatur kontrol laju kirim, consent ledger, quiet hours, serta warm-up.
package guardian

import (
	"fmt"
	"math"
	"sort"
	"time"
)

const tempBanWindow = 168 * time.Hour

// ScoreInput berisi sinyal lokal 24 jam dan status ban untuk perhitungan skor kesehatan akun.
type ScoreInput struct {
	SinceTempBan    time.Duration // < 0 bila tidak pernah
	Sent24h         int
	SentEligible24h int // terkirim pada jendela [now−25j, now−1j)
	Delivered24h    int // delivered dari pesan pada jendela yang sama
	FirstContact24h int
	Recipients24h   int
	Replied24h      int
	FanoutMaxRatio  float64 // fan-out konten terbesar / kuota konten identik
	RateOverlimit1h int
}

// Factor menjelaskan satu kontributor penalti atau bonus terhadap skor kesehatan akun.
type Factor struct {
	Code   string `json:"code"`
	Impact int    `json:"impact"`
	Detail string `json:"detail"`
}

// Score menghitung skor kesehatan akun murni [0, 100] beserta daftar faktor terurut penalti terberat lebih dulu.
func Score(in ScoreInput) (score int, factors []Factor) {
	score = 100
	factors = make([]Factor, 0, 6)

	if in.SinceTempBan >= 0 && in.SinceTempBan < tempBanWindow {
		ratio := 1.0 - float64(in.SinceTempBan)/float64(tempBanWindow)
		impact := -int(math.Round(60.0 * ratio))
		if impact != 0 {
			score += impact
			hours := int(in.SinceTempBan / time.Hour)
			factors = append(factors, Factor{
				Code:   "recent_temp_ban",
				Impact: impact,
				Detail: fmt.Sprintf("temporary ban tercatat %d jam yang lalu", hours),
			})
		}
	}

	if in.SentEligible24h >= 20 {
		u := 1.0 - float64(in.Delivered24h)/float64(in.SentEligible24h)
		if u < 0 {
			u = 0
		} else if u > 1 {
			u = 1
		}
		pen := int(math.Round(math.Min(40.0, 80.0*math.Max(0.0, u-0.1))))
		if pen > 0 {
			impact := -pen
			score += impact
			pct := int(math.Round(u * 100))
			factors = append(factors, Factor{
				Code:   "undelivered_ratio",
				Impact: impact,
				Detail: fmt.Sprintf("%d%% pesan jendela 24 jam belum delivered", pct),
			})
		}
	}

	if in.Sent24h >= 10 {
		f := float64(in.FirstContact24h) / float64(in.Sent24h)
		if f > 1 {
			f = 1
		} else if f < 0 {
			f = 0
		}
		pen := int(math.Round(math.Min(30.0, 60.0*math.Max(0.0, f-0.5))))
		if pen > 0 {
			impact := -pen
			score += impact
			pct := int(math.Round(f * 100))
			factors = append(factors, Factor{
				Code:   "first_contact_ratio",
				Impact: impact,
				Detail: fmt.Sprintf("%d%% pesan terkirim merupakan first-contact", pct),
			})
		}
	}

	if in.FanoutMaxRatio >= 0.8 {
		impact := -10
		score += impact
		pct := int(math.Round(in.FanoutMaxRatio * 100))
		factors = append(factors, Factor{
			Code:   "content_fanout",
			Impact: impact,
			Detail: fmt.Sprintf("fan-out konten identik mencapai %d%% kuota harian", pct),
		})
	}

	if in.RateOverlimit1h > 0 {
		pen := 15 * in.RateOverlimit1h
		if pen > 30 {
			pen = 30
		}
		impact := -pen
		score += impact
		factors = append(factors, Factor{
			Code:   "rate_overlimit",
			Impact: impact,
			Detail: fmt.Sprintf("server WhatsApp menolak laju kirim %dx dalam 1 jam terakhir", in.RateOverlimit1h),
		})
	}

	if in.Recipients24h >= 10 && in.Replied24h > 0 {
		r := float64(in.Replied24h) / float64(in.Recipients24h)
		if r > 1 {
			r = 1
		} else if r < 0 {
			r = 0
		}
		bonus := int(math.Round(math.Min(10.0, 20.0*r)))
		if bonus > 0 {
			score += bonus
			pct := int(math.Round(r * 100))
			factors = append(factors, Factor{
				Code:   "reply_rate",
				Impact: bonus,
				Detail: fmt.Sprintf("%d%% penerima aktif membalas pesan", pct),
			})
		}
	}

	sort.Slice(factors, func(i, j int) bool {
		if factors[i].Impact != factors[j].Impact {
			return factors[i].Impact < factors[j].Impact
		}
		return factors[i].Code < factors[j].Code
	})

	if score < 0 {
		score = 0
	} else if score > 100 {
		score = 100
	}
	return score, factors
}

// StateForScore menetapkan status kesehatan akun berdasarkan profil, status ban aktif, dan nilai skor.
func StateForScore(profile string, banActive bool, score int) string {
	if profile == ProfileOff {
		return StateOff
	}
	if banActive {
		return StateSuspended
	}
	switch {
	case score >= 70:
		return StateHealthy
	case score >= 40:
		return StateCautious
	default:
		return StateRestricted
	}
}
