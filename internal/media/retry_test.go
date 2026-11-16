package media

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"testing/synctest"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/proto"
)

type fakeDownloader struct {
	calls int
	fn    func(ctx context.Context, directPath string, encHash, fileHash, mediaKey []byte, mt whatsmeow.MediaType, mmsType string, allowNoHash bool, f whatsmeow.File) error
}

func (d *fakeDownloader) DownloadMediaWithPathToFile(ctx context.Context, directPath string, encHash, fileHash, mediaKey []byte, mt whatsmeow.MediaType, mmsType string, allowNoHash bool, f whatsmeow.File) error {
	d.calls++
	return d.fn(ctx, directPath, encHash, fileHash, mediaKey, mt, mmsType, allowNoHash, f)
}

func encryptFakeMediaRetry(t *testing.T, msgID string, mediaKey []byte, newPath string, res waMmsRetry.MediaRetryNotification_ResultType) *events.MediaRetry {
	t.Helper()
	notif := &waMmsRetry.MediaRetryNotification{
		StanzaID:   proto.String(msgID),
		DirectPath: proto.String(newPath),
		Result:     res.Enum(),
	}
	pt, err := proto.Marshal(notif)
	if err != nil {
		t.Fatalf("marshal MediaRetryNotification gagal: %v", err)
	}
	retryKey := hkdfutil.SHA256(mediaKey, nil, []byte("WhatsApp Media Retry Notification"), 32)
	iv := make([]byte, 12)
	_, _ = rand.Read(iv)
	ct, err := gcmutil.Encrypt(retryKey, iv, pt, []byte(msgID))
	if err != nil {
		t.Fatalf("enkripsi notifikasi retry gagal: %v", err)
	}
	return &events.MediaRetry{
		MessageID:  types.MessageID(msgID),
		Ciphertext: ct,
		IV:         iv,
	}
}

// TestMediaRetry_404LaluBerhasil memastikan unduhan gagal 404 memicu permintaan retry, menerima MediaRetry baru, dan sukses mengunduh ulang.
func TestMediaRetry_404LaluBerhasil(t *testing.T) {
	pipe := NewPipeline(Limits{TmpDir: t.TempDir()})
	waiter := NewRetryWaiter()

	mediaKey := make([]byte, 32)
	_, _ = rand.Read(mediaKey)
	const msgID = "MSG-RETRY-404"
	const oldPath = "/v/old-404"
	const newPath = "/v/fresh-200"
	const wantContent = "konten media hasil resurrection"

	dl := &fakeDownloader{}
	dl.fn = func(ctx context.Context, directPath string, _, _, _ []byte, _ whatsmeow.MediaType, _ string, _ bool, f whatsmeow.File) error {
		if directPath == oldPath {
			return whatsmeow.ErrMediaDownloadFailedWith404
		}
		if directPath != newPath {
			return whatsmeow.ErrMediaDownloadFailedWith410
		}
		_, err := f.Write([]byte(wantContent))
		return err
	}

	sender := func(ctx context.Context, info *types.MessageInfo, key []byte) error {
		if string(info.ID) != msgID || !bytes.Equal(key, mediaKey) {
			t.Fatalf("parameter SendMediaRetryReceipt tidak sesuai: id=%s", info.ID)
		}
		go func() {
			waiter.Deliver(encryptFakeMediaRetry(t, msgID, mediaKey, newPath, waMmsRetry.MediaRetryNotification_SUCCESS))
		}()
		return nil
	}

	var out bytes.Buffer
	info, err := pipe.DownloadWithResurrection(context.Background(), dl, sender, waiter, types.MessageInfo{
		ID: msgID,
		MessageSource: types.MessageSource{
			Chat:   types.NewJID("628123456789", types.DefaultUserServer),
			Sender: types.NewJID("628123456789", types.DefaultUserServer),
		},
	}, MediaDescriptor{
		DirectPath: oldPath,
		MediaKey:   mediaKey,
		MediaType:  KindImage,
		Mimetype:   "image/jpeg",
	}, &out)
	if err != nil {
		t.Fatalf("DownloadWithResurrection gagal: %v", err)
	}
	if dl.calls != 2 {
		t.Fatalf("jumlah percobaan unduhan diharapkan 2, didapat %d", dl.calls)
	}
	if out.String() != wantContent || info.Size != int64(len(wantContent)) {
		t.Fatalf("hasil unduhan tidak sesuai: body=%q size=%d", out.String(), info.Size)
	}
}

// TestMediaRetry_Timeout410 memastikan tidak adanya event MediaRetry dalam 30 detik menghasilkan ErrMediaGone (410).
func TestMediaRetry_Timeout410(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pipe := NewPipeline(Limits{TmpDir: t.TempDir()})
		waiter := NewRetryWaiter()
		mediaKey := make([]byte, 32)

		dl := &fakeDownloader{
			fn: func(ctx context.Context, _ string, _, _, _ []byte, _ whatsmeow.MediaType, _ string, _ bool, _ whatsmeow.File) error {
				return whatsmeow.ErrMediaDownloadFailedWith410
			},
		}
		sender := func(ctx context.Context, info *types.MessageInfo, key []byte) error {
			return nil
		}

		var out bytes.Buffer
		_, err := pipe.DownloadWithResurrection(context.Background(), dl, sender, waiter, types.MessageInfo{
			ID: "MSG-TIMEOUT-410",
		}, MediaDescriptor{
			DirectPath: "/v/gone",
			MediaKey:   mediaKey,
			MediaType:  KindDocument,
			Mimetype:   "application/pdf",
		}, &out)
		if !errors.Is(err, ErrMediaGone) {
			t.Fatalf("diharapkan ErrMediaGone (410), didapat: %v", err)
		}
	})
}
