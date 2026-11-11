package wa

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// ErrMediaTooLarge dikembalikan jika ukuran stream melampaui batas yang diizinkan.
var ErrMediaTooLarge = errors.New("ukuran media melebihi batas maksimum")

// ErrInvalidMediaType dikembalikan jika tipe media tidak dikenali.
var ErrInvalidMediaType = errors.New("tipe media tidak valid (gunakan image, video, audio, atau document)")

// bufPool menyediakan buffer 64 KB yang dipakai ulang untuk operasi copy stream media.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64<<10)
		return &b
	},
}

// MediaInput merangkum parameter pengiriman pesan media.
type MediaInput struct {
	To        string
	MediaType string // image | video | audio | document
	Caption   string
	Filename  string
	MimeType  string
	Reader    io.Reader
}

// parseMediaType memetakan string tipe media ke konstanta whatsmeow.MediaType dan mmsType.
func parseMediaType(kind string) (whatsmeow.MediaType, string, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "image":
		return whatsmeow.MediaImage, "image", nil
	case "video":
		return whatsmeow.MediaVideo, "video", nil
	case "audio":
		return whatsmeow.MediaAudio, "audio", nil
	case "document":
		return whatsmeow.MediaDocument, "document", nil
	case "sticker":
		return whatsmeow.MediaImage, "sticker", nil
	default:
		return "", "", ErrInvalidMediaType
	}
}

// UploadStream menyalin stream ke file sementara, mengunggah via UploadReader, lalu menghapus file.
func (i *Instance) UploadStream(ctx context.Context, r io.Reader, kind string) (whatsmeow.UploadResponse, string, uint64, error) {
	mt, _, err := parseMediaType(kind)
	if err != nil {
		return whatsmeow.UploadResponse{}, "", 0, err
	}
	maxBytes := i.mgr.cfg.MediaMaxBytes
	if strings.EqualFold(kind, "document") {
		maxBytes = i.mgr.cfg.DocumentMaxBytes
	}

	if err := i.mgr.mediaSem.Acquire(ctx, 1); err != nil {
		return whatsmeow.UploadResponse{}, "", 0, fmt.Errorf("antrian media penuh: %w", err)
	}
	defer i.mgr.mediaSem.Release(1)

	plain, err := os.CreateTemp(i.mgr.cfg.TmpDir(), "plain-*")
	if err != nil {
		return whatsmeow.UploadResponse{}, "", 0, fmt.Errorf("buat temp plain: %w", err)
	}
	plainName := plain.Name()
	defer func() {
		_ = plain.Close()
		_ = os.Remove(plainName)
	}()

	enc, err := os.CreateTemp(i.mgr.cfg.TmpDir(), "enc-*")
	if err != nil {
		return whatsmeow.UploadResponse{}, "", 0, fmt.Errorf("buat temp enc: %w", err)
	}
	encName := enc.Name()
	defer func() {
		_ = enc.Close()
		_ = os.Remove(encName)
	}()

	bp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bp)

	// Salin dengan batas maxBytes+1 untuk mendeteksi file berlebih tanpa memuatnya ke RAM
	n, err := io.CopyBuffer(plain, io.LimitReader(r, maxBytes+1), *bp)
	if err != nil {
		return whatsmeow.UploadResponse{}, "", 0, fmt.Errorf("salin media ke temp: %w", err)
	}
	if n > maxBytes {
		return whatsmeow.UploadResponse{}, "", 0, ErrMediaTooLarge
	}

	headLen := 512
	if int64(headLen) > n {
		headLen = int(n)
	}
	head := (*bp)[:headLen]
	if headLen > 0 {
		if _, err := plain.ReadAt(head, 0); err != nil && !errors.Is(err, io.EOF) {
			return whatsmeow.UploadResponse{}, "", 0, fmt.Errorf("baca header media: %w", err)
		}
	}
	mime := http.DetectContentType(head)
	if _, err := plain.Seek(0, io.SeekStart); err != nil {
		return whatsmeow.UploadResponse{}, "", 0, fmt.Errorf("seek temp plain: %w", err)
	}

	cli := i.getClient()
	if cli == nil || !cli.IsConnected() {
		return whatsmeow.UploadResponse{}, "", 0, ErrNotConnected
	}

	upCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	resp, err := cli.UploadReader(upCtx, plain, enc, mt)
	if err != nil {
		return whatsmeow.UploadResponse{}, "", 0, fmt.Errorf("unggah media ke WhatsApp: %w", err)
	}
	return resp, mime, uint64(n), nil
}

// buildMediaE2EMessage merakit *waE2E.Message dari hasil UploadResponse.
func buildMediaE2EMessage(kind, mime, caption, filename string, up whatsmeow.UploadResponse) *waE2E.Message {
	switch strings.ToLower(kind) {
	case "image":
		return &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				URL:           proto.String(up.URL),
				DirectPath:    proto.String(up.DirectPath),
				MediaKey:      up.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: up.FileEncSHA256,
				FileSHA256:    up.FileSHA256,
				FileLength:    proto.Uint64(up.FileLength),
				Caption:       proto.String(caption),
			},
		}
	case "video":
		return &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				URL:           proto.String(up.URL),
				DirectPath:    proto.String(up.DirectPath),
				MediaKey:      up.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: up.FileEncSHA256,
				FileSHA256:    up.FileSHA256,
				FileLength:    proto.Uint64(up.FileLength),
				Caption:       proto.String(caption),
			},
		}
	case "audio":
		return &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				URL:           proto.String(up.URL),
				DirectPath:    proto.String(up.DirectPath),
				MediaKey:      up.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: up.FileEncSHA256,
				FileSHA256:    up.FileSHA256,
				FileLength:    proto.Uint64(up.FileLength),
			},
		}
	default:
		if filename == "" {
			filename = "attachment"
		}
		return &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				URL:           proto.String(up.URL),
				DirectPath:    proto.String(up.DirectPath),
				MediaKey:      up.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: up.FileEncSHA256,
				FileSHA256:    up.FileSHA256,
				FileLength:    proto.Uint64(up.FileLength),
				FileName:      proto.String(filename),
				Title:         proto.String(filename),
				Caption:       proto.String(caption),
			},
		}
	}
}

// DownloadMediaToWriter mengunduh media dari CDN WhatsApp ke file sementara lalu menyalinnya ke w.
func (i *Instance) DownloadMediaToWriter(ctx context.Context, desc MediaDescriptor, w io.Writer) error {
	mt, mmsType, err := parseMediaType(desc.MediaType)
	if err != nil {
		return err
	}
	mediaKey, err := base64.StdEncoding.DecodeString(desc.MediaKey)
	if err != nil {
		return fmt.Errorf("media_key base64 tidak valid: %w", err)
	}
	encHash, err := base64.StdEncoding.DecodeString(desc.FileEncSHA256)
	if err != nil {
		return fmt.Errorf("file_enc_sha256 base64 tidak valid: %w", err)
	}
	fileHash, err := base64.StdEncoding.DecodeString(desc.FileSHA256)
	if err != nil {
		return fmt.Errorf("file_sha256 base64 tidak valid: %w", err)
	}

	if err := i.mgr.mediaSem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("antrian media penuh: %w", err)
	}
	defer i.mgr.mediaSem.Release(1)

	tmp, err := os.CreateTemp(i.mgr.cfg.TmpDir(), "dl-*")
	if err != nil {
		return fmt.Errorf("buat temp download: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	cli := i.getClient()
	if cli == nil || !cli.IsConnected() {
		return ErrNotConnected
	}

	dlCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := cli.DownloadMediaWithPathToFile(dlCtx, desc.DirectPath, encHash, fileHash, mediaKey, mt, mmsType, false, tmp); err != nil {
		return fmt.Errorf("unduh media dari WhatsApp: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	bp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bp)
	_, err = io.CopyBuffer(w, tmp, *bp)
	return err
}

// OpenSafeHTTPSStream membuka stream unduhan dari URL eksternal dengan proteksi anti-SSRF.
func OpenSafeHTTPSStream(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Host == "" {
		return nil, errors.New("url harus menggunakan skema https yang valid")
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil || len(ips) == 0 {
				return nil, fmt.Errorf("resolusi DNS gagal untuk %s", host)
			}
			for _, ip := range ips {
				if isBlockedIP(ip.IP) {
					return nil, errors.New("alamat IP tujuan termasuk jaringan privat/terlarang (anti-SSRF)")
				}
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
		},
	}
	client := &http.Client{
		Timeout:   60 * time.Second,
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 || !strings.EqualFold(req.URL.Scheme, "https") {
				return errors.New("redirect tidak diizinkan")
			}
			return nil
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unduh url media: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("server media mengembalikan HTTP %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// isBlockedIP menolak loopback, private RFC1918/RFC4193, link-local, dan unspecified IP.
func isBlockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}

// SweepStaleTemp menghapus file media sementara yang berumur lebih dari maxAge saat startup.
func SweepStaleTemp(tmpDir string, maxAge time.Duration) {
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-maxAge)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if info, err := e.Info(); err == nil && info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(tmpDir, e.Name()))
		}
	}
}

