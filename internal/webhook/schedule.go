package webhook

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// permanentMaxAttempts adalah batas maksimum percobaan untuk respons 4xx permanen sebelum masuk DLQ.
	permanentMaxAttempts = 3
	// maxRetryAfter membatasi penundaan dari header Retry-After maksimal 1 jam.
	maxRetryAfter = time.Hour
)

// ResponseClass mengelompokkan hasil HTTP / jaringan ke kelas perlakuan pengiriman webhook.
type ResponseClass uint8

const (
	ClassSuccess ResponseClass = iota
	ClassTransient
	ClassPermanent
	ClassGone
)

// ClassifyResponse memetakan error transport atau kode status HTTP menjadi kelas retry webhook v2.
func ClassifyResponse(status int, err error) ResponseClass {
	if err != nil {
		return ClassTransient
	}
	if status >= 200 && status < 300 {
		return ClassSuccess
	}
	if status == http.StatusGone {
		return ClassGone
	}
	if status == http.StatusRequestTimeout ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		status >= 500 {
		return ClassTransient
	}
	if status >= 400 && status < 500 {
		return ClassPermanent
	}
	// Status non-2xx lain (mis. 3xx) diperlakukan sebagai transient agar aman dicoba ulang.
	return ClassTransient
}

var baseRetrySchedule = []time.Duration{
	0,                // percobaan 1
	5 * time.Second,  // percobaan 2
	15 * time.Second, // percobaan 3
	45 * time.Second, // percobaan 4
	2 * time.Minute,  // percobaan 5
	5 * time.Minute,  // percobaan 6
	15 * time.Minute, // percobaan 7
	30 * time.Minute, // percobaan 8
	time.Hour,        // percobaan 9+
}

// RetryDelay menghitung jeda sebelum percobaan ke-attempt (berbasis 1) beserta jitter +-20%.
func RetryDelay(attempt int, rng func() float64) time.Duration {
	if attempt <= 1 {
		return 0
	}
	idx := attempt - 1
	if idx >= len(baseRetrySchedule) {
		idx = len(baseRetrySchedule) - 1
	}
	base := baseRetrySchedule[idx]
	if rng == nil {
		rng = rand.Float64
	}
	// Rentang pengali [0.80, 1.20] sesuai aturan jitter +-20%.
	mult := 0.8 + 0.4*rng()
	return time.Duration(float64(base) * mult)
}

// ParseRetryAfter membaca header Retry-After dalam bentuk detik bulat atau HTTP-date, maksimal 1 jam.
func ParseRetryAfter(raw string, now time.Time) (time.Duration, bool) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		d := time.Duration(secs) * time.Second
		if d > maxRetryAfter {
			d = maxRetryAfter
		}
		return d, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			return 0, true
		}
		if d > maxRetryAfter {
			d = maxRetryAfter
		}
		return d, true
	}
	return 0, false
}
