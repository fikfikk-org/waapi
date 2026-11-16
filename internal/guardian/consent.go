package guardian

import (
	"crypto/sha256"
	"strings"

	"github.com/fikfikk-org/waapi/internal/outbox"
)

// NormalizeContent menormalkan teks pesan atau caption media untuk deteksi fan-out identik (angka tetap dipertahankan).
func NormalizeContent(spec outbox.Spec) string {
	var raw string
	switch spec.Type {
	case "text":
		raw = spec.Text
	case "image", "video", "document":
		if spec.Media != nil {
			raw = spec.Media.Caption
		}
	default:
		return ""
	}
	trimmed := strings.TrimSpace(strings.ToLower(raw))
	if trimmed == "" {
		return ""
	}
	return strings.Join(strings.Fields(trimmed), " ")
}

// ContentHash mengembalikan 16 byte pertama SHA-256 atas teks ternormalisasi untuk kolom guardian_fanout.content_hash.
func ContentHash(normalized string) []byte {
	sum := sha256.Sum256([]byte(normalized))
	out := make([]byte, 16)
	copy(out, sum[:16])
	return out
}

// MatchKeyword mencocokkan teks pesan secara persis (case-insensitive setelah trim) terhadap daftar kata kunci.
func MatchKeyword(text string, keywords []string) (string, bool) {
	upper := strings.ToUpper(strings.TrimSpace(text))
	if upper == "" {
		return "", false
	}
	for _, kw := range keywords {
		cand := strings.ToUpper(strings.TrimSpace(kw))
		if cand != "" && upper == cand {
			return cand, true
		}
	}
	return "", false
}
