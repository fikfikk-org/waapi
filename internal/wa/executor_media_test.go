package wa

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/netguard"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow"
)

// TestExecute_ComposeMedia memastikan penyusunan pesan protobuf media outbox per jenis mengisi seluruh field yang diwajibkan kontrak.
func TestExecute_ComposeMedia(t *testing.T) {
	thumb := []byte{0xff, 0xd8, 0xff, 0xe0}
	thumbB64 := base64.StdEncoding.EncodeToString(thumb)
	sidecar := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	mediaKey := bytes.Repeat([]byte{0x11}, 32)
	encSHA := bytes.Repeat([]byte{0x22}, 32)
	fileSHA := bytes.Repeat([]byte{0x33}, 32)

	baseResp := whatsmeow.UploadResponse{
		URL:              "https://mmg.whatsapp.net/d/f/tes",
		DirectPath:       "/d/f/tes",
		MediaKey:         mediaKey,
		FileEncSHA256:    encSHA,
		FileSHA256:       fileSHA,
		FileLength:       4096,
		StreamingSidecar: sidecar,
	}

	t.Run("image", func(t *testing.T) {
		msg, err := composeMediaMessage(outbox.Spec{
			Type:     "image",
			To:       "628123456789@s.whatsapp.net",
			ReplyTo:  &outbox.ReplyRef{ID: "REQ1", Sender: "628111111111@s.whatsapp.net"},
			Mentions: []string{"628222222222@s.whatsapp.net"},
			Media: &outbox.MediaRef{
				Caption:   "foto uji",
				Thumbnail: thumbB64,
			},
		}, media.UploadRecord{
			Kind:     media.KindImage,
			Mimetype: "image/jpeg",
			Size:     4096,
			Width:    800,
			Height:   600,
			Resp:     baseResp,
		})
		if err != nil {
			t.Fatalf("composeMediaMessage image gagal: %v", err)
		}
		img := msg.GetImageMessage()
		if img == nil {
			t.Fatal("ImageMessage bernilai nil")
		}
		if img.GetWidth() != 800 || img.GetHeight() != 600 || img.GetCaption() != "foto uji" {
			t.Fatalf("field ImageMessage tidak sesuai: width=%d height=%d caption=%q", img.GetWidth(), img.GetHeight(), img.GetCaption())
		}
		if !bytes.Equal(img.GetJPEGThumbnail(), thumb) {
			t.Fatalf("JPEGThumbnail tidak cocok")
		}
		ci := img.GetContextInfo()
		if ci == nil || ci.GetStanzaID() != "REQ1" || ci.GetParticipant() != "628111111111@s.whatsapp.net" || len(ci.GetMentionedJID()) != 1 {
			t.Fatalf("ContextInfo image tidak sesuai: %+v", ci)
		}
	})

	t.Run("video", func(t *testing.T) {
		msg, err := composeMediaMessage(outbox.Spec{
			Type: "video",
			To:   "628123456789@s.whatsapp.net",
			Media: &outbox.MediaRef{
				Caption:   "video uji",
				GIF:       true,
				Seconds:   15,
				Thumbnail: thumbB64,
			},
		}, media.UploadRecord{
			Kind:     media.KindVideo,
			Mimetype: "video/mp4",
			Size:     4096,
			Width:    1280,
			Height:   720,
			Seconds:  10,
			Resp:     baseResp,
		})
		if err != nil {
			t.Fatalf("composeMediaMessage video gagal: %v", err)
		}
		vid := msg.GetVideoMessage()
		if vid == nil {
			t.Fatal("VideoMessage bernilai nil")
		}
		if vid.GetSeconds() != 15 || !vid.GetGifPlayback() || vid.GetWidth() != 1280 || vid.GetHeight() != 720 {
			t.Fatalf("field VideoMessage tidak sesuai: sec=%d gif=%v w=%d h=%d", vid.GetSeconds(), vid.GetGifPlayback(), vid.GetWidth(), vid.GetHeight())
		}
		if !bytes.Equal(vid.GetStreamingSidecar(), sidecar) || !bytes.Equal(vid.GetJPEGThumbnail(), thumb) {
			t.Fatalf("sidecar atau thumbnail video tidak cocok")
		}
	})

	t.Run("audio-ptt", func(t *testing.T) {
		msg, err := composeMediaMessage(outbox.Spec{
			Type: "audio",
			To:   "628123456789@s.whatsapp.net",
			Media: &outbox.MediaRef{
				PTT:     true,
				Seconds: 9,
			},
		}, media.UploadRecord{
			Kind:     media.KindPTT,
			Mimetype: "audio/ogg",
			Size:     4096,
			PTT:      true,
			Resp:     baseResp,
		})
		if err != nil {
			t.Fatalf("composeMediaMessage audio gagal: %v", err)
		}
		aud := msg.GetAudioMessage()
		if aud == nil || !aud.GetPTT() || aud.GetSeconds() != 9 {
			t.Fatalf("AudioMessage PTT tidak sesuai: %+v", aud)
		}
	})

	t.Run("document", func(t *testing.T) {
		msg, err := composeMediaMessage(outbox.Spec{
			Type: "document",
			To:   "628123456789@s.whatsapp.net",
			Media: &outbox.MediaRef{
				Caption:  "kontrak lampiran",
				Filename: "kontrak.pdf",
			},
		}, media.UploadRecord{
			Kind:     media.KindDocument,
			Mimetype: "application/pdf",
			Size:     4096,
			Filename: "default.pdf",
			Resp:     baseResp,
		})
		if err != nil {
			t.Fatalf("composeMediaMessage document gagal: %v", err)
		}
		doc := msg.GetDocumentMessage()
		if doc == nil || doc.GetFileName() != "kontrak.pdf" || doc.GetTitle() != "kontrak.pdf" || doc.GetCaption() != "kontrak lampiran" {
			t.Fatalf("DocumentMessage tidak sesuai: %+v", doc)
		}
	})

	t.Run("sticker", func(t *testing.T) {
		msg, err := composeMediaMessage(outbox.Spec{
			Type:  "sticker",
			To:    "628123456789@s.whatsapp.net",
			Media: &outbox.MediaRef{},
		}, media.UploadRecord{
			Kind:     media.KindSticker,
			Mimetype: "image/webp",
			Size:     4096,
			Width:    512,
			Height:   512,
			Resp:     baseResp,
		})
		if err != nil {
			t.Fatalf("composeMediaMessage sticker gagal: %v", err)
		}
		stk := msg.GetStickerMessage()
		if stk == nil || stk.GetMimetype() != "image/webp" || stk.GetWidth() != 512 || stk.GetHeight() != 512 {
			t.Fatalf("StickerMessage tidak sesuai: %+v", stk)
		}
	})
}

type mockFetchExec struct {
	errFn func(job *outbox.Job) error
}

func (m mockFetchExec) Ready(string) bool { return true }
func (m mockFetchExec) NewMessageID(string) (string, error) {
	return "3EB0FETCH001", nil
}
func (m mockFetchExec) Execute(_ context.Context, job *outbox.Job) (outbox.ExecResult, error) {
	return outbox.ExecResult{}, m.errFn(job)
}

// memOutboxStore adalah store minimalis dalam memori untuk menguji siklus retry di dalam synctest bubble.
type memOutboxStore struct {
	jobs map[string]*outbox.Job
}

func newMemOutboxStore() *memOutboxStore {
	return &memOutboxStore{jobs: make(map[string]*outbox.Job)}
}

func (s *memOutboxStore) GetByID(_ context.Context, _, id string) (*outbox.Job, error) {
	j, ok := s.jobs[id]
	if !ok {
		return nil, outbox.ErrNotFound
	}
	cp := *j
	return &cp, nil
}
func (s *memOutboxStore) GetByIdemKey(context.Context, string, string) (*outbox.Job, error) {
	return nil, outbox.ErrNotFound
}
func (s *memOutboxStore) CountQueued(_ context.Context, _ string) (int, error) { return 0, nil }
func (s *memOutboxStore) Insert(_ context.Context, job *outbox.Job) error {
	cp := *job
	s.jobs[job.ID] = &cp
	return nil
}
func (s *memOutboxStore) Cancel(context.Context, string, string, time.Time) (*outbox.Job, error) {
	return nil, outbox.ErrNotFound
}
func (s *memOutboxStore) ClaimNext(_ context.Context, instanceID string, now, leaseUntil time.Time) (*outbox.Job, error) {
	for _, j := range s.jobs {
		if j.InstanceID == instanceID && j.Status == outbox.StatusQueued && j.NotBefore <= now.UnixMilli() {
			j.Status = outbox.StatusSending
			j.LeaseUntil = leaseUntil.UnixMilli()
			cp := *j
			return &cp, nil
		}
	}
	return nil, nil
}
func (s *memOutboxStore) NextDue(_ context.Context, instanceID string) (time.Time, bool, error) {
	var minNB int64
	for _, j := range s.jobs {
		if j.InstanceID == instanceID && j.Status == outbox.StatusQueued {
			if minNB == 0 || j.NotBefore < minNB {
				minNB = j.NotBefore
			}
		}
	}
	if minNB == 0 {
		return time.Time{}, false, nil
	}
	return time.UnixMilli(minNB), true, nil
}
func (s *memOutboxStore) BeginAttempt(_ context.Context, id string, leaseUntil, now time.Time) (int, error) {
	j := s.jobs[id]
	j.Attempts++
	j.LeaseUntil = leaseUntil.UnixMilli()
	j.UpdatedAt = now.UnixMilli()
	return j.Attempts, nil
}
func (s *memOutboxStore) Release(_ context.Context, id string, notBefore, now time.Time, errCode, errMsg string) (*outbox.Job, error) {
	j := s.jobs[id]
	j.Status = outbox.StatusQueued
	j.NotBefore = notBefore.UnixMilli()
	j.UpdatedAt = now.UnixMilli()
	j.ErrorCode = errCode
	j.ErrorMsg = errMsg
	cp := *j
	return &cp, nil
}
func (s *memOutboxStore) MarkSent(context.Context, string, time.Time, time.Time) (*outbox.Job, error) {
	return nil, nil
}
func (s *memOutboxStore) MarkFailed(_ context.Context, id string, now time.Time, errCode, errMsg string) (*outbox.Job, error) {
	j := s.jobs[id]
	j.Status = outbox.StatusFailed
	j.UpdatedAt = now.UnixMilli()
	j.ErrorCode = errCode
	j.ErrorMsg = errMsg
	cp := *j
	return &cp, nil
}
func (s *memOutboxStore) MarkExpired(context.Context, string, time.Time) (*outbox.Job, error) {
	return nil, nil
}
func (s *memOutboxStore) RecoverExpiredLeases(context.Context, time.Time) (int64, error) {
	return 0, nil
}
func (s *memOutboxStore) PruneFinal(context.Context, time.Time, int) (int64, error) { return 0, nil }

// TestExecute_MediaURL_SSRFVsTransientRetry memastikan penolakan SSRF langsung gagal permanen tanpa retry sementara 5xx dicoba ulang maks 3 kali.
func TestExecute_MediaURL_SSRFVsTransientRetry(t *testing.T) {
	ssrfErr := classifyMediaFetchError(netguard.ErrBlockedIP)
	var se *outbox.SendError
	if !errors.As(ssrfErr, &se) || se.Transient || se.Code != "ssrf_blocked" {
		t.Fatalf("error SSRF harus permanen dengan kode ssrf_blocked, didapat: %+v", ssrfErr)
	}

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		st := newMemOutboxStore()
		svc := outbox.NewService(st, nil, mockFetchExec{
			errFn: func(job *outbox.Job) error {
				if job.Spec.Media != nil && job.Spec.Media.UploadID == "" && job.Spec.To == "628111111111@s.whatsapp.net" {
					return classifyMediaFetchError(netguard.ErrBlockedIP)
				}
				return classifyMediaFetchError(httpStatusError{status: http.StatusServiceUnavailable})
			},
		}, nil, nil, outbox.Options{
			DisableSendJitter:  true,
			DisableRetryJitter: true,
		})
		svc.StartScheduler(ctx, "sales")
		defer svc.Stop()

		jSSRF, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: "sales",
			Spec: outbox.Spec{
				Type:  "image",
				To:    "628111111111",
				Media: &outbox.MediaRef{URL: "https://example.com/a.jpg"},
			},
		})
		if err != nil {
			t.Fatalf("enqueue ssrf gagal: %v", err)
		}

		j503, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: "sales",
			Spec: outbox.Spec{
				Type:  "image",
				To:    "628222222222",
				Media: &outbox.MediaRef{URL: "https://example.com/b.jpg"},
			},
		})
		if err != nil {
			t.Fatalf("enqueue 503 gagal: %v", err)
		}

		time.Sleep(30 * time.Second)
		synctest.Wait()

		gotSSRF, _ := svc.Get(ctx, "sales", jSSRF.ID)
		if gotSSRF.Status != outbox.StatusFailed || gotSSRF.Attempts != 1 || gotSSRF.ErrorCode != "ssrf_blocked" {
			t.Fatalf("job SSRF harus langsung failed pada percobaan ke-1, didapat: %+v", gotSSRF)
		}

		got503, _ := svc.Get(ctx, "sales", j503.ID)
		if got503.Status != outbox.StatusFailed || got503.Attempts != 3 || got503.ErrorCode != "media_fetch_failed" {
			t.Fatalf("job HTTP 503 harus gagal di percobaan ke-3 dengan media_fetch_failed, didapat: %+v", got503)
		}
	})
}
