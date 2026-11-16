package wa

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/netguard"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// ErrMediaTooLarge dikembalikan jika ukuran stream melampaui batas yang diizinkan.
var ErrMediaTooLarge = media.ErrMediaTooLarge

// ErrInvalidMediaType dikembalikan jika tipe media tidak dikenali.
var ErrInvalidMediaType = media.ErrInvalidKind

// MediaInput merangkum parameter pengiriman pesan media untuk kompatibilitas v1 langsung.
type MediaInput struct {
	To        string
	MediaType string // image | video | audio | document | sticker
	Caption   string
	Filename  string
	MimeType  string
	Reader    io.Reader
}

type httpStatusError struct {
	status int
}

func (e httpStatusError) Error() string {
	return fmt.Sprintf("server media mengembalikan HTTP %d", e.status)
}

func (e httpStatusError) transient() bool {
	return e.status == http.StatusRequestTimeout ||
		e.status == http.StatusTooEarly ||
		e.status == http.StatusTooManyRequests ||
		e.status >= http.StatusInternalServerError
}

func (m *Manager) ensureMediaPipeline() *media.Pipeline {
	if m == nil {
		return media.NewPipeline(media.Limits{})
	}
	if m.mediaPipe != nil {
		return m.mediaPipe
	}
	m.mediaOnce.Do(func() {
		if m.mediaPipe == nil {
			var limits media.Limits
			if m.cfg != nil {
				limits = media.Limits{
					MediaMaxBytes:    m.cfg.MediaMaxBytes,
					DocumentMaxBytes: m.cfg.DocumentMaxBytes,
					TmpDir:           m.cfg.TmpDir(),
					DropBehindOff:    strings.EqualFold(m.cfg.MediaDropbehind, "off"),
				}
			}
			m.mediaPipe = media.NewPipeline(limits)
		}
	})
	return m.mediaPipe
}

func (m *Manager) mediaHTTPClient() *http.Client {
	if m != nil && m.mediaHTTP != nil {
		return m.mediaHTTP
	}
	return netguard.NewClient(netguard.Policy{}, 60*time.Second)
}

// UploaderFor mengembalikan Uploader whatsmeow yang sedang tersambung untuk instance tertentu.
func (m *Manager) UploaderFor(instanceID string) (media.Uploader, error) {
	if m == nil {
		return nil, ErrNotConnected
	}
	inst, ok := m.Get(instanceID)
	if !ok {
		return nil, ErrNotConnected
	}
	cli := inst.getClient()
	if cli == nil || !cli.IsConnected() {
		return nil, ErrNotConnected
	}
	return cli, nil
}

// DownloaderFor mengembalikan Downloader dan pengirim MediaRetryReceipt milik instance yang tersambung.
func (m *Manager) DownloaderFor(instanceID string) (media.Downloader, media.RetryReceiptSender, error) {
	if m == nil {
		return nil, nil, ErrNotConnected
	}
	inst, ok := m.Get(instanceID)
	if !ok {
		return nil, nil, ErrNotConnected
	}
	cli := inst.getClient()
	if cli == nil || !cli.IsConnected() {
		return nil, nil, ErrNotConnected
	}
	return cli, cli.SendMediaRetryReceipt, nil
}

// RetryWaiter mengembalikan waiter notifikasi MediaRetry yang terdaftar pada Manager.
func (m *Manager) RetryWaiter() *media.RetryWaiter {
	if m == nil {
		return nil
	}
	return m.retryWaiter
}

func (i *Instance) mediaPipeline() *media.Pipeline {
	if i != nil && i.mgr != nil {
		return i.mgr.ensureMediaPipeline()
	}
	return media.NewPipeline(media.Limits{})
}

// UploadStream mengunggah stream plaintext dalam satu pass melalui media.Pipeline tanpa menyentuh file plaintext di disk.
func (i *Instance) UploadStream(ctx context.Context, r io.Reader, kind string) (whatsmeow.UploadResponse, string, uint64, error) {
	k, err := media.ValidateKind(kind)
	if err != nil {
		return whatsmeow.UploadResponse{}, "", 0, err
	}
	cli := i.getClient()
	if cli == nil || !cli.IsConnected() {
		return whatsmeow.UploadResponse{}, "", 0, ErrNotConnected
	}

	upCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	res, err := i.mediaPipeline().Upload(upCtx, cli, r, k, media.Hint{})
	if err != nil {
		return whatsmeow.UploadResponse{}, "", 0, err
	}
	return res.Resp, res.Meta.Mimetype, uint64(res.Meta.Size), nil
}

// DownloadMediaToWriter mengunduh media masuk dari WhatsApp ke file temp anonim melalui media.Pipeline lalu menyalinnya ke w.
func (i *Instance) DownloadMediaToWriter(ctx context.Context, desc MediaDescriptor, w io.Writer) error {
	pDesc, err := toPipelineDescriptor(desc)
	if err != nil {
		return err
	}
	cli := i.getClient()
	if cli == nil || !cli.IsConnected() {
		return ErrNotConnected
	}

	dlCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	_, err = i.mediaPipeline().Download(dlCtx, cli, pDesc, w)
	return err
}

func toPipelineDescriptor(desc MediaDescriptor) (media.MediaDescriptor, error) {
	k, err := media.ValidateKind(desc.MediaType)
	if err != nil {
		return media.MediaDescriptor{}, err
	}
	mediaKey, err := base64.StdEncoding.DecodeString(desc.MediaKey)
	if err != nil {
		return media.MediaDescriptor{}, fmt.Errorf("media_key base64 tidak valid: %w", err)
	}
	encHash, err := base64.StdEncoding.DecodeString(desc.FileEncSHA256)
	if err != nil {
		return media.MediaDescriptor{}, fmt.Errorf("file_enc_sha256 base64 tidak valid: %w", err)
	}
	fileHash, err := base64.StdEncoding.DecodeString(desc.FileSHA256)
	if err != nil {
		return media.MediaDescriptor{}, fmt.Errorf("file_sha256 base64 tidak valid: %w", err)
	}
	return media.MediaDescriptor{
		Mimetype:      desc.Mimetype,
		DirectPath:    desc.DirectPath,
		MediaKey:      mediaKey,
		FileEncSHA256: encHash,
		FileSHA256:    fileHash,
		FileLength:    desc.FileLength,
		MediaType:     k,
		Filename:      desc.Filename,
	}, nil
}

func fetchSafeMediaStream(ctx context.Context, client *http.Client, rawURL string) (io.ReadCloser, error) {
	u, err := netguard.ValidateHTTPSURL(rawURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, httpStatusError{status: resp.StatusCode}
	}
	return resp.Body, nil
}

// isBlockedIP membungkus netguard.IsBlocked agar kompatibel dengan unit test v1.
func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return true
	}
	return netguard.IsBlocked(addr)
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

// FetchSafeMediaForTest tidak digunakan di produksi; membantu wrapper internal menjaga helper media.
func buildFallbackMediaMessage(kind, mime, caption, filename string, up whatsmeow.UploadResponse) (*outbox.Spec, media.UploadRecord) {
	k := media.Kind(strings.ToLower(strings.TrimSpace(kind)))
	return &outbox.Spec{
			Type: string(k),
			Media: &outbox.MediaRef{
				Caption:  caption,
				Filename: filename,
			},
		}, media.UploadRecord{
			Kind:     k,
			Mimetype: mime,
			Size:     int64(up.FileLength),
			Filename: filename,
			Resp:     up,
		}
}

// parseMessageInfoForRetry menyusun types.MessageInfo dari MessageV2 tersimpan guna keperluan MediaRetryReceipt.
func ParseMessageInfoFromV2(chatRaw string, msg MessageV2, senderRaw string, evTS int64) types.MessageInfo {
	chatTarget := strings.TrimSpace(msg.ChatRaw)
	if chatTarget == "" {
		chatTarget = strings.TrimSpace(msg.Chat)
	}
	if chatTarget == "" {
		chatTarget = strings.TrimSpace(chatRaw)
	}
	chatJID, _ := types.ParseJID(chatTarget)

	senderTarget := strings.TrimSpace(senderRaw)
	if senderTarget == "" {
		senderTarget = strings.TrimSpace(msg.From)
	}
	senderJID, _ := types.ParseJID(senderTarget)

	ts := msg.Timestamp
	if ts <= 0 {
		ts = evTS
	}
	return types.MessageInfo{
		ID:        types.MessageID(msg.ID),
		Timestamp: time.Unix(ts, 0),
		MessageSource: types.MessageSource{
			Chat:     chatJID,
			Sender:   senderJID,
			IsFromMe: msg.IsFromMe,
			IsGroup:  msg.IsGroup,
		},
	}
}
