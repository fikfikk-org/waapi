// Package webhook menangani penandatanganan HMAC dan pengiriman event keluar dengan disk outbox.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// Sign menghitung signature HMAC-SHA256 dengan format sha256=<hex(HMAC(secret, timestamp.body))>.
func Sign(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
