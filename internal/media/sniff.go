package media

import (
	"bytes"
	"encoding/binary"
	"errors"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"net/http"
	"path/filepath"
	"strings"
)

var (
	// ErrInvalidKind dikembalikan jika jenis media tidak termasuk image, video, audio, ptt, document, atau sticker.
	ErrInvalidKind = errors.New("jenis media tidak valid (gunakan image, video, audio, ptt, document, atau sticker)")
	// ErrMediaTooLarge dikembalikan jika ukuran stream melampaui batas yang diizinkan.
	ErrMediaTooLarge = errors.New("ukuran media melebihi batas maksimum")
	// ErrUnsupportedMedia dikembalikan jika format file tidak diizinkan untuk jenis media yang dipilih.
	ErrUnsupportedMedia = errors.New("format media tidak didukung untuk jenis ini")
	// ErrPTTRequiresOggOpus dikembalikan jika voice note (ptt) bukan audio OGG dengan codec Opus.
	ErrPTTRequiresOggOpus = errors.New("voice note (ptt) wajib berformat audio/ogg dengan codec Opus")
	// ErrStickerRequiresWebP dikembalikan jika sticker bukan berformat image/webp.
	ErrStickerRequiresWebP = errors.New("sticker wajib berformat image/webp")
)

// Kind merepresentasikan kategori media pada Media Engine v2.
type Kind string

const (
	KindImage    Kind = "image"
	KindVideo    Kind = "video"
	KindAudio    Kind = "audio"
	KindPTT      Kind = "ptt"
	KindDocument Kind = "document"
	KindSticker  Kind = "sticker"
)

// Hint berisi metadata opsional yang diberikan oleh pengirim HTTP.
type Hint struct {
	Mimetype string
	Filename string
	Seconds  uint32
}

// Meta menyimpan hasil sniffing header dan metadata pengiriman media.
type Meta struct {
	Mimetype string
	Size     int64
	SHA256   []byte
	Width    uint32
	Height   uint32
	Seconds  uint32
	PTT      bool
	Filename string
}

// ValidateKind menormalisasi dan memvalidasi nilai Kind upload.
func ValidateKind(raw string) (Kind, error) {
	k := Kind(strings.ToLower(strings.TrimSpace(raw)))
	switch k {
	case KindImage, KindVideo, KindAudio, KindPTT, KindDocument, KindSticker:
		return k, nil
	default:
		return "", ErrInvalidKind
	}
}

// Sniff mendeteksi mimetype dari header file (<= 64 KiB), memvalidasi aturan per kind, dan membaca dimensi tanpa decode piksel.
func Sniff(head []byte, kind Kind, hint Hint) (Meta, error) {
	k, err := ValidateKind(string(kind))
	if err != nil {
		return Meta{}, err
	}
	if len(head) == 0 {
		return Meta{}, ErrUnsupportedMedia
	}

	mime := detectMIME(head, k, hint)
	out := Meta{
		Mimetype: mime,
		Seconds:  hint.Seconds,
		Filename: SanitizeFilename(hint.Filename),
	}

	switch k {
	case KindImage:
		if mime != "image/jpeg" && mime != "image/png" && mime != "image/webp" {
			return Meta{}, ErrUnsupportedMedia
		}
		out.Width, out.Height = extractImageDimensions(head, mime)
		return out, nil

	case KindVideo:
		if mime != "video/mp4" && mime != "video/3gpp" {
			return Meta{}, ErrUnsupportedMedia
		}
		return out, nil

	case KindAudio:
		switch mime {
		case "audio/ogg", "audio/mpeg", "audio/mp4", "audio/aac", "audio/amr":
			return out, nil
		default:
			return Meta{}, ErrUnsupportedMedia
		}

	case KindPTT:
		if !isOggOpus(head) {
			return Meta{}, ErrPTTRequiresOggOpus
		}
		out.Mimetype = "audio/ogg"
		out.PTT = true
		return out, nil

	case KindSticker:
		if mime != "image/webp" {
			return Meta{}, ErrStickerRequiresWebP
		}
		out.Width, out.Height = parseWebPDimensions(head)
		return out, nil

	case KindDocument:
		return out, nil

	default:
		return Meta{}, ErrInvalidKind
	}
}

func isOggOpus(head []byte) bool {
	return len(head) >= 36 && string(head[0:4]) == "OggS" && string(head[28:36]) == "OpusHead"
}

func detectMIME(head []byte, kind Kind, hint Hint) string {
	switch {
	case len(head) >= 4 && string(head[0:4]) == "OggS":
		return "audio/ogg"
	case len(head) >= 12 && string(head[0:4]) == "RIFF" && string(head[8:12]) == "WEBP":
		return "image/webp"
	case len(head) >= 12 && string(head[4:8]) == "ftyp":
		brand := string(head[8:12])
		if strings.HasPrefix(brand, "3gp") || strings.HasPrefix(brand, "3g2") {
			return "video/3gpp"
		}
		if kind == KindAudio || brand == "M4A " || brand == "M4B " {
			return "audio/mp4"
		}
		return "video/mp4"
	case bytes.HasPrefix(head, []byte("#!AMR")):
		return "audio/amr"
	case bytes.HasPrefix(head, []byte("ID3")):
		return "audio/mpeg"
	case len(head) >= 2 && head[0] == 0xff && (head[1]&0xe0) == 0xe0:
		if (head[1] & 0x06) == 0 {
			return "audio/aac"
		}
		return "audio/mpeg"
	}

	mime := http.DetectContentType(head)
	if mime == "application/ogg" {
		return "audio/ogg"
	}
	if idx := strings.IndexByte(mime, ';'); idx >= 0 && !strings.HasPrefix(mime, "text/") {
		mime = strings.TrimSpace(mime[:idx])
	}
	if mime == "application/octet-stream" && kind == KindDocument {
		if h := sanitizeHintMIME(hint.Mimetype); h != "" {
			return h
		}
	}
	return mime
}

func sanitizeHintMIME(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.ContainsAny(raw, "\r\n ;") {
		return ""
	}
	if !strings.Contains(raw, "/") {
		return ""
	}
	return strings.ToLower(raw)
}

func extractImageDimensions(head []byte, mime string) (uint32, uint32) {
	if mime == "image/webp" {
		return parseWebPDimensions(head)
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(head))
	if err != nil || cfg.Width <= 0 || cfg.Height <= 0 {
		// Dimensi dikosongkan bila header melewati buffer peek 64 KiB.
		return 0, 0
	}
	return uint32(cfg.Width), uint32(cfg.Height)
}

// parseWebPDimensions membaca dimensi gambar dari chunk VP8X, VP8L, atau VP8 pada header WebP.
func parseWebPDimensions(head []byte) (uint32, uint32) {
	if len(head) < 24 || string(head[0:4]) != "RIFF" || string(head[8:12]) != "WEBP" {
		return 0, 0
	}
	pos := 12
	for pos+8 <= len(head) {
		chunkID := string(head[pos : pos+4])
		chunkSize := int(binary.LittleEndian.Uint32(head[pos+4 : pos+8]))
		payloadStart := pos + 8
		if payloadStart > len(head) {
			break
		}
		p := head[payloadStart:]

		switch chunkID {
		case "VP8X":
			if len(p) < 10 {
				return 0, 0
			}
			w := 1 + (uint32(p[4]) | uint32(p[5])<<8 | uint32(p[6])<<16)
			h := 1 + (uint32(p[7]) | uint32(p[8])<<8 | uint32(p[9])<<16)
			return w, h
		case "VP8L":
			if len(p) < 5 || p[0] != 0x2f {
				return 0, 0
			}
			bits := binary.LittleEndian.Uint32(p[1:5])
			w := (bits & 0x3fff) + 1
			h := ((bits >> 14) & 0x3fff) + 1
			return w, h
		case "VP8 ":
			if len(p) < 10 || p[3] != 0x9d || p[4] != 0x01 || p[5] != 0x2a {
				return 0, 0
			}
			w := uint32(binary.LittleEndian.Uint16(p[6:8]) & 0x3fff)
			h := uint32(binary.LittleEndian.Uint16(p[8:10]) & 0x3fff)
			return w, h
		}

		// Chunk RIFF dipad ke kelipatan genap.
		step := chunkSize + (chunkSize & 1)
		if step < 0 || payloadStart+step <= payloadStart {
			break
		}
		pos = payloadStart + step
	}
	return 0, 0
}

// SanitizeFilename membersihkan path traversal dan karakter kontrol dari nama berkas untuk header Content-Disposition.
func SanitizeFilename(raw string) string {
	s := strings.ReplaceAll(raw, "\\", "/")
	s = strings.TrimSpace(filepath.Base(s))
	if s == "" || s == "." || s == ".." || s == "/" {
		return ""
	}
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == '"' {
			return -1
		}
		return r
	}, s)
	return strings.TrimSpace(s)
}
