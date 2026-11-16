package guardian

import (
	"testing"
	"time"
)

// TestScore_Tabel memverifikasi rumus murni Score, seluruh faktor penalti/bonus, urutan faktor, clamp [0,100], dan pemetaan state.
func TestScore_Tabel(t *testing.T) {
	cases := []struct {
		name        string
		in          ScoreInput
		wantScore   int
		wantFirst   string
		wantFactors int
	}{
		{
			name:        "akun baru bersih tanpa sinyal",
			in:          ScoreInput{SinceTempBan: -1},
			wantScore:   100,
			wantFactors: 0,
		},
		{
			name:        "temp ban baru terjadi (0 jam)",
			in:          ScoreInput{SinceTempBan: 0},
			wantScore:   40,
			wantFirst:   "recent_temp_ban",
			wantFactors: 1,
		},
		{
			name:        "temp ban setengah jendela 7 hari (84 jam)",
			in:          ScoreInput{SinceTempBan: 84 * time.Hour},
			wantScore:   70,
			wantFirst:   "recent_temp_ban",
			wantFactors: 1,
		},
		{
			name:        "temp ban tepat lewat 168 jam tidak berdampak",
			in:          ScoreInput{SinceTempBan: 168 * time.Hour},
			wantScore:   100,
			wantFactors: 0,
		},
		{
			name:        "undelivered di bawah ambang 20 pesan diabaikan",
			in:          ScoreInput{SinceTempBan: -1, SentEligible24h: 19, Delivered24h: 0},
			wantScore:   100,
			wantFactors: 0,
		},
		{
			name:        "undelivered 50 persen pada 40 eligible (-32)",
			in:          ScoreInput{SinceTempBan: -1, SentEligible24h: 40, Delivered24h: 20},
			wantScore:   68,
			wantFirst:   "undelivered_ratio",
			wantFactors: 1,
		},
		{
			name:        "first contact 90 persen (-24)",
			in:          ScoreInput{SinceTempBan: -1, Sent24h: 100, FirstContact24h: 90},
			wantScore:   76,
			wantFirst:   "first_contact_ratio",
			wantFactors: 1,
		},
		{
			name:        "content fanout >= 0.8 (-10)",
			in:          ScoreInput{SinceTempBan: -1, FanoutMaxRatio: 0.8},
			wantScore:   90,
			wantFirst:   "content_fanout",
			wantFactors: 1,
		},
		{
			name:        "rate overlimit 3x terbatasi -30",
			in:          ScoreInput{SinceTempBan: -1, RateOverlimit1h: 3},
			wantScore:   70,
			wantFirst:   "rate_overlimit",
			wantFactors: 1,
		},
		{
			name:        "bonus reply rate dan clamp maksimal 100",
			in:          ScoreInput{SinceTempBan: -1, Recipients24h: 20, Replied24h: 10},
			wantScore:   100,
			wantFirst:   "reply_rate",
			wantFactors: 1,
		},
		{
			name: "gabungan banyak penalti tetap terurut penalti terberat dan di-clamp ke 0",
			in: ScoreInput{
				SinceTempBan:    0,
				SentEligible24h: 50,
				Delivered24h:    5,
				Sent24h:         50,
				FirstContact24h: 50,
				FanoutMaxRatio:  0.95,
				RateOverlimit1h: 2,
				Recipients24h:   20,
				Replied24h:      4,
			},
			wantScore:   0,
			wantFirst:   "recent_temp_ban",
			wantFactors: 6,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			score, factors := Score(tc.in)
			if score != tc.wantScore {
				t.Fatalf("score diharapkan %d, didapat %d (factors=%+v)", tc.wantScore, score, factors)
			}
			if len(factors) != tc.wantFactors {
				t.Fatalf("jumlah faktor diharapkan %d, didapat %d (%+v)", tc.wantFactors, len(factors), factors)
			}
			if tc.wantFirst != "" && (len(factors) == 0 || factors[0].Code != tc.wantFirst) {
				t.Fatalf("faktor pertama diharapkan %q, didapat %+v", tc.wantFirst, factors)
			}
			for i := 1; i < len(factors); i++ {
				if factors[i-1].Impact > factors[i].Impact {
					t.Fatalf("faktor tidak terurut dari dampak penalti terbesar ke terkecil: %+v", factors)
				}
			}
		})
	}

	// Cek pemetaan state untuk seluruh profil dan batas skor.
	if s := StateForScore(ProfileOff, true, 10); s != StateOff {
		t.Fatalf("profil off harus menghasilkan state off, didapat %q", s)
	}
	if s := StateForScore(ProfileStandard, true, 95); s != StateSuspended {
		t.Fatalf("ban aktif harus menghasilkan state suspended, didapat %q", s)
	}
	if s := StateForScore(ProfileStandard, false, 70); s != StateHealthy {
		t.Fatalf("skor 70 harus healthy, didapat %q", s)
	}
	if s := StateForScore(ProfileStandard, false, 40); s != StateCautious {
		t.Fatalf("skor 40 harus cautious, didapat %q", s)
	}
	if s := StateForScore(ProfileStandard, false, 39); s != StateRestricted {
		t.Fatalf("skor 39 harus restricted, didapat %q", s)
	}
}
