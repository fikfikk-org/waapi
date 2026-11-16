// Package media menyediakan pipeline pemrosesan media satu-pass dengan file temp anonim, drop-behind, sniffing, dan media resurrection.
package media

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"golang.org/x/sync/semaphore"
)

const (
	defaultMediaMaxBytes    = 16 << 20
	defaultDocumentMaxBytes = 64 << 20
	defaultStickerMaxBytes  = 1 << 20
	sniffPeekBytes          = 64 << 10
)

var (
	copyBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, 64<<10)
			return &b
		},
	}
	peekReaderPool = sync.Pool{
		New: func() any {
			return bufio.NewReaderSize(nil, sniffPeekBytes)
		},
	}
)

// Uploader membungkus UploadReader milik whatsmeow agar mudah diuji dengan fake di unit test.
type Uploader interface {
	UploadReader(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
}

// Downloader membungkus unduhan media whatsmeow ke File sementara untuk verifikasi MAC dan dekripsi di tempat.
type Downloader interface {
	DownloadMediaWithPathToFile(ctx context.Context, directPath string, encFileHash, fileHash, mediaKey []byte, mediaType whatsmeow.MediaType, mmsType string, allowNoHash bool, file whatsmeow.File) error
}

// Uploaded merangkum hasil unggahan ke CDN WhatsApp dan metadata hasil sniffing.
type Uploaded struct {
	Resp whatsmeow.UploadResponse
	Meta Meta
}

// MediaDescriptor berisi metadata descriptor untuk mengunduh media masuk dari WhatsApp.
type MediaDescriptor struct {
	Mimetype      string
	DirectPath    string
	MediaKey      []byte
	FileEncSHA256 []byte
	FileSHA256    []byte
	FileLength    uint64
	MediaType     Kind
	Filename      string
}

// DownloadInfo merangkum metadata media yang selesai diunduh dan disalin ke writer tujuan.
type DownloadInfo struct {
	Mimetype   string
	Size       int64
	FileSHA256 []byte
	Filename   string
}

// Limits mengatur batas ukuran upload, direktori temp, dan konfigurasi drop-behind.
type Limits struct {
	MediaMaxBytes    int64
	DocumentMaxBytes int64
	StickerMaxBytes  int64
	TmpDir           string
	DropBehindChunk  int64
	DropBehindOff    bool
}

func (l Limits) withDefaults() Limits {
	if l.MediaMaxBytes <= 0 {
		l.MediaMaxBytes = defaultMediaMaxBytes
	}
	if l.DocumentMaxBytes <= 0 {
		l.DocumentMaxBytes = defaultDocumentMaxBytes
	}
	if l.StickerMaxBytes <= 0 {
		l.StickerMaxBytes = defaultStickerMaxBytes
	}
	if l.DropBehindChunk <= 0 {
		l.DropBehindChunk = defaultDropBehindChunk
	}
	return l
}

// Pipeline mengoordinasikan unggah satu-pass dan unduhan streaming dengan kontrol konkurensi mediaSem.
type Pipeline struct {
	limits Limits
	sem    *semaphore.Weighted
}

// NewPipeline membuat Pipeline media dengan batas konkurensi maksimal 2 transfer aktif bersamaan.
func NewPipeline(limits Limits) *Pipeline {
	return &Pipeline{
		limits: limits.withDefaults(),
		sem:    semaphore.NewWeighted(2),
	}
}

// MaxBytesForKind mengembalikan batas ukuran maksimum dalam byte untuk jenis media tertentu.
func (p *Pipeline) MaxBytesForKind(kind Kind) int64 {
	switch kind {
	case KindDocument:
		return p.limits.DocumentMaxBytes
	case KindSticker:
		return p.limits.StickerMaxBytes
	default:
		return p.limits.MediaMaxBytes
	}
}

// MediaTypeForKind memetakan Kind ke konstanta whatsmeow.MediaType dan mmsType.
func MediaTypeForKind(kind Kind) (whatsmeow.MediaType, string, error) {
	k, err := ValidateKind(string(kind))
	if err != nil {
		return "", "", err
	}
	switch k {
	case KindImage:
		return whatsmeow.MediaImage, "image", nil
	case KindVideo:
		return whatsmeow.MediaVideo, "video", nil
	case KindAudio, KindPTT:
		return whatsmeow.MediaAudio, "audio", nil
	case KindDocument:
		return whatsmeow.MediaDocument, "document", nil
	case KindSticker:
		return whatsmeow.MediaImage, "sticker", nil
	default:
		return "", "", ErrInvalidKind
	}
}

type maxSizeReader struct {
	r    io.Reader
	max  int64
	read int64
}

func (m *maxSizeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	rem := (m.max + 1) - m.read
	if rem <= 0 {
		return 0, ErrMediaTooLarge
	}
	if int64(len(p)) > rem {
		p = p[:rem]
	}
	n, err := m.r.Read(p)
	m.read += int64(n)
	if m.read > m.max {
		return 0, ErrMediaTooLarge
	}
	return n, err
}

// Upload menjalankan pipeline satu-pass: sniff header 64 KiB, limit ukuran, enkripsi ke file temp anonim dengan drop-behind, lalu unggah ke CDN.
func (p *Pipeline) Upload(ctx context.Context, up Uploader, r io.Reader, kind Kind, hint Hint) (Uploaded, error) {
	if p == nil || up == nil || r == nil {
		return Uploaded{}, errors.New("pipeline atau uploader belum siap")
	}
	k, err := ValidateKind(string(kind))
	if err != nil {
		return Uploaded{}, err
	}
	mt, _, err := MediaTypeForKind(k)
	if err != nil {
		return Uploaded{}, err
	}
	maxBytes := p.MaxBytesForKind(k)

	if err := p.sem.Acquire(ctx, 1); err != nil {
		return Uploaded{}, fmt.Errorf("antrian media penuh: %w", err)
	}
	defer p.sem.Release(1)

	sr := &maxSizeReader{r: r, max: maxBytes}
	br := peekReaderPool.Get().(*bufio.Reader)
	br.Reset(sr)
	defer func() {
		br.Reset(nil)
		peekReaderPool.Put(br)
	}()

	peekLen := int(min(int64(sniffPeekBytes), maxBytes+1))
	head, peekErr := br.Peek(peekLen)
	if peekErr != nil && !errors.Is(peekErr, io.EOF) && !errors.Is(peekErr, bufio.ErrBufferFull) {
		return Uploaded{}, peekErr
	}

	meta, err := Sniff(head, k, hint)
	if err != nil {
		return Uploaded{}, err
	}

	tmpFile, err := CreateAnonymous(p.limits.TmpDir)
	if err != nil {
		return Uploaded{}, err
	}
	defer func() {
		_ = tmpFile.Close()
	}()

	var target io.ReadWriteSeeker = tmpFile
	if !p.limits.DropBehindOff {
		target = NewDropBehind(tmpFile, p.limits.DropBehindChunk)
	} else {
		target = newDropBehindWithSys(tmpFile, p.limits.DropBehindChunk, platformSyscalls(), false)
	}

	resp, err := up.UploadReader(ctx, br, target, mt)
	if err != nil {
		if errors.Is(err, ErrMediaTooLarge) {
			return Uploaded{}, ErrMediaTooLarge
		}
		return Uploaded{}, err
	}

	if resp.FileLength > 0 {
		meta.Size = int64(resp.FileLength)
	} else {
		meta.Size = sr.read
	}
	meta.SHA256 = resp.FileSHA256
	return Uploaded{Resp: resp, Meta: meta}, nil
}

// Download mengunduh dan mendekripsi media dari WhatsApp ke file temp anonim sebelum disalin ke w.
func (p *Pipeline) Download(ctx context.Context, d Downloader, desc MediaDescriptor, w io.Writer) (DownloadInfo, error) {
	if p == nil || d == nil || w == nil {
		return DownloadInfo{}, errors.New("pipeline atau downloader belum siap")
	}
	mt, mmsType, err := MediaTypeForKind(desc.MediaType)
	if err != nil {
		return DownloadInfo{}, err
	}

	if err := p.sem.Acquire(ctx, 1); err != nil {
		return DownloadInfo{}, fmt.Errorf("antrian media penuh: %w", err)
	}
	defer p.sem.Release(1)

	tmpFile, err := CreateAnonymous(p.limits.TmpDir)
	if err != nil {
		return DownloadInfo{}, err
	}
	defer func() {
		_ = tmpFile.Close()
	}()

	if err := d.DownloadMediaWithPathToFile(ctx, desc.DirectPath, desc.FileEncSHA256, desc.FileSHA256, desc.MediaKey, mt, mmsType, false, tmpFile); err != nil {
		return DownloadInfo{}, err
	}

	st, err := tmpFile.Stat()
	if err != nil {
		return DownloadInfo{}, fmt.Errorf("periksa ukuran unduhan media: %w", err)
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return DownloadInfo{}, fmt.Errorf("seek awal unduhan media: %w", err)
	}

	size := st.Size()
	mime := desc.Mimetype
	if mime == "" {
		mime = "application/octet-stream"
	}
	info := DownloadInfo{
		Mimetype:   mime,
		Size:       size,
		FileSHA256: desc.FileSHA256,
		Filename:   SanitizeFilename(desc.Filename),
	}
	if hook, ok := w.(interface{ BeforeWrite(DownloadInfo) }); ok {
		hook.BeforeWrite(info)
	}

	bp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bp)
	written, err := io.CopyBuffer(w, tmpFile, *bp)
	if err != nil {
		return DownloadInfo{}, err
	}
	if info.Size <= 0 {
		info.Size = written
	}
	return info, nil
}

// DownloadWithResurrection mengunduh media dan otomatis meminta retry ke pengirim jika CDN membalas 404/410.
func (p *Pipeline) DownloadWithResurrection(
	ctx context.Context,
	d Downloader,
	sender RetryReceiptSender,
	waiter *RetryWaiter,
	msgInfo types.MessageInfo,
	desc MediaDescriptor,
	w io.Writer,
) (DownloadInfo, error) {
	info, err := p.Download(ctx, d, desc, w)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) && !errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410) {
		return DownloadInfo{}, err
	}
	if sender == nil || waiter == nil || len(desc.MediaKey) == 0 || msgInfo.ID == "" {
		return DownloadInfo{}, ErrMediaGone
	}

	msgID := string(msgInfo.ID)
	release, resErr := waiter.Reserve(msgID)
	if resErr != nil {
		return DownloadInfo{}, fmt.Errorf("%w: %v", ErrMediaGone, resErr)
	}
	defer release()

	if sendErr := sender(ctx, &msgInfo, desc.MediaKey); sendErr != nil {
		return DownloadInfo{}, fmt.Errorf("%w: %v", ErrMediaGone, sendErr)
	}

	retryEvt, awaitErr := waiter.Await(ctx, msgID)
	if awaitErr != nil {
		return DownloadInfo{}, ErrMediaGone
	}

	notif, decErr := whatsmeow.DecryptMediaRetryNotification(retryEvt, desc.MediaKey)
	if decErr != nil || notif.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS || notif.GetDirectPath() == "" {
		return DownloadInfo{}, ErrMediaGone
	}

	desc.DirectPath = notif.GetDirectPath()
	retryInfo, retryErr := p.Download(ctx, d, desc, w)
	if retryErr != nil {
		if errors.Is(retryErr, whatsmeow.ErrMediaDownloadFailedWith404) || errors.Is(retryErr, whatsmeow.ErrMediaDownloadFailedWith410) {
			return DownloadInfo{}, ErrMediaGone
		}
		return DownloadInfo{}, retryErr
	}
	return retryInfo, nil
}
