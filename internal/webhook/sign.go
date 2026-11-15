// Package webhook menangani penandatanganan HMAC dan pengiriman event keluar dengan disk outbox.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// Sign menghitung signature HMAC-SHA256 dengan format sha256=<hex(HMAC(secret, timestamp.body))>.
func Sign(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// SignStandard menghitung signature sesuai spesifikasi Standard Webhooks: v1,<base64(HMAC-SHA256(key, id.ts.body))>.
func SignStandard(secret, id string, ts int64, body []byte) (string, error) {
	raw := strings.TrimSpace(strings.TrimPrefix(secret, "whsec_"))
	if raw == "" {
		return "", fmt.Errorf("secret webhook kosong")
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		// Terima juga base64 tanpa padding '=' jika diberikan oleh integrasi eksternal
		key, err = base64.RawStdEncoding.DecodeString(raw)
		if err != nil {
			return "", fmt.Errorf("decode webhook secret base64: %w", err)
		}
	}

	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id))
	mac.Write([]byte("."))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}
