package outbox

import (
	"context"
	"errors"
	"time"
)

// Classify mengekstrak kelas kesalahan kirim tanpa bergantung pada whatsmeow maupun internal/wa.
func Classify(err error) (transient bool, code string, minDelay time.Duration) {
	if err == nil {
		return false, "", 0
	}
	var se *SendError
	if errors.As(err, &se) {
		c := se.Code
		if c == "" {
			if se.Transient {
				c = "send_failed"
			} else {
				c = "server_rejected"
			}
		}
		return se.Transient, c, se.MinDelay
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true, "timeout", 0
	}
	if errors.Is(err, ErrMediaExpired) {
		return false, "media_expired", 0
	}
	return true, "send_failed", 0
}
