package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/config"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"github.com/fikfikk-org/waapi/internal/wa"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/proto"
)

type fakeMediaDriver struct {
	mu          sync.Mutex
	uploadCalls int
	dlCalls     int
	uploadFn    func(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	dlFn        func(ctx context.Context, directPath string, encHash, fileHash, mediaKey []byte, mt whatsmeow.MediaType, mmsType string, allowNoHash bool, f whatsmeow.File) error
	retryFn     func(ctx context.Context, info *types.MessageInfo, mediaKey []byte) error
}

func (f *fakeMediaDriver) UploadReader(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	f.mu.Lock()
	f.uploadCalls++
	fn := f.uploadFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, plaintext, tempFile, appInfo)
	}
	data, err := io.ReadAll(plaintext)
	if err != nil {
		return whatsmeow.UploadResponse{}, err
	}
	if _, err := tempFile.Write(data); err != nil {
		return whatsmeow.UploadResponse{}, err
	}
	sum := sha256.Sum256(data)
	return whatsmeow.UploadResponse{
		URL:           "https://mmg.whatsapp.net/d/f/mock",
		DirectPath:    "/d/f/mock",
		MediaKey:      bytes.Repeat([]byte{0x44}, 32),
		FileEncSHA256: sum[:],
		FileSHA256:    sum[:],
		FileLength:    uint64(len(data)),
	}, nil
}

func (f *fakeMediaDriver) DownloadMediaWithPathToFile(ctx context.Context, directPath string, encHash, fileHash, mediaKey []byte, mt whatsmeow.MediaType, mmsType string, allowNoHash bool, file whatsmeow.File) error {
	f.mu.Lock()
	f.dlCalls++
	fn := f.dlFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, directPath, encHash, fileHash, mediaKey, mt, mmsType, allowNoHash, file)
	}
	_, err := file.Write([]byte("media-downloaded"))
	return err
}

func (f *fakeMediaDriver) SendMediaRetryReceipt(ctx context.Context, info *types.MessageInfo, mediaKey []byte) error {
	if f.retryFn != nil {
		return f.retryFn(ctx, info, mediaKey)
	}
	return nil
}

func (f *fakeMediaDriver) UploaderFor(_ string) (media.Uploader, error) {
	return f, nil
}

func (f *fakeMediaDriver) DownloaderFor(_ string) (media.Downloader, media.RetryReceiptSender, error) {
	return f, f.SendMediaRetryReceipt, nil
}

func makeTestPNG(w, h int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	var b bytes.Buffer
	_ = png.Encode(&b, img)
	return b.Bytes()
}

func makeTestJPEG(w, h int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	var b bytes.Buffer
	_ = jpeg.Encode(&b, img, nil)
	return b.Bytes()
}

func newMultipartBody(t *testing.T, fields []struct{ k, v string }, fileField, fileName string, fileBytes []byte) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, f := range fields {
		_ = mw.WriteField(f.k, f.v)
	}
	if fileField != "" {
		fw, err := mw.CreateFormFile(fileField, fileName)
		if err != nil {
			t.Fatalf("CreateFormFile gagal: %v", err)
		}
		_, _ = fw.Write(fileBytes)
	}
	_ = mw.Close()
	return buf.Bytes(), mw.FormDataContentType()
}

type switchableGovernor struct {
	lvl governor.Level
}

func (g *switchableGovernor) Level() governor.Level { return g.lvl }

// TestV2MediaAPI_Kontrak memverifikasi kontrak POST /v2/media (201, 413, 415, 503), validasi upload_id di POST /v2/messages, GET /events/{seq}/media dengan resurrection, serta POST /v2/media/download.
func TestV2MediaAPI_Kontrak(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp, MediaMaxBytes: 4096, DocumentMaxBytes: 8192}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	authMgr, _ := auth.NewManager(ctx, st)
	waMgr, err := wa.NewManager(ctx, cfg, st, nil)
	if err != nil {
		t.Fatalf("wa.NewManager gagal: %v", err)
	}
	t.Cleanup(func() { _ = waMgr.Close() })
	_, _ = waMgr.Create(ctx, "sales", 60, false)

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 2})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	exec := &apiFakeExecutor{
		ready:  map[string]bool{"sales": true},
		paired: map[string]bool{"sales": true},
	}
	upStore := media.NewUploadStore(j.DB())
	pipe := media.NewPipeline(media.Limits{
		MediaMaxBytes:    cfg.MediaMaxBytes,
		DocumentMaxBytes: cfg.DocumentMaxBytes,
		TmpDir:           cfg.TmpDir(),
	})
	waiter := media.NewRetryWaiter()
	drv := &fakeMediaDriver{}
	gov := &switchableGovernor{lvl: governor.Green}

	outSvc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{
		DisableSendJitter: true,
	})
	t.Cleanup(outSvc.Stop)
	outSvc.StartScheduler(ctx, "sales")

	key, _, err := authMgr.Create(ctx, "media-test", "")
	if err != nil {
		t.Fatalf("authMgr.Create gagal: %v", err)
	}

	h := NewHandler(authMgr, waMgr,
		WithDB(st.DB),
		WithJournal(j),
		WithOutbox(outSvc),
		WithGovernor(gov),
		WithMedia(pipe, upStore),
		withMediaDriver(drv, waiter),
	)

	// 1. POST /v2/media gambar JPEG valid -> 201 Created dengan sha256 hex lowercase & dimensi.
	jpgBytes := makeTestJPEG(320, 240)
	body, ct := newMultipartBody(t, nil, "file", "foto.jpg", jpgBytes)
	req := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/media?kind=image&filename=foto.jpg&seconds=0", bytes.NewReader(body))
	req.Header.Set("X-API-Key", key)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /v2/media diharapkan 201, didapat %d (%s)", rec.Code, rec.Body.String())
	}
	var upResp struct {
		UploadID  string `json:"upload_id"`
		Kind      string `json:"kind"`
		Mimetype  string `json:"mimetype"`
		Size      int64  `json:"size"`
		SHA256    string `json:"sha256"`
		Width     uint32 `json:"width"`
		Height    uint32 `json:"height"`
		Seconds   uint32 `json:"seconds"`
		ExpiresAt int64  `json:"expires_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &upResp); err != nil {
		t.Fatalf("decode respons 201 gagal: %v", err)
	}
	wantSHA := sha256.Sum256(jpgBytes)
	if !strings.HasPrefix(upResp.UploadID, "upl_") || upResp.Kind != "image" || upResp.Mimetype != "image/jpeg" ||
		upResp.Size != int64(len(jpgBytes)) || upResp.SHA256 != hex.EncodeToString(wantSHA[:]) ||
		upResp.Width != 320 || upResp.Height != 240 || upResp.ExpiresAt <= time.Now().Unix() {
		t.Fatalf("field respons 201 tidak sesuai kontrak: %+v", upResp)
	}

	// 2. 413 media_too_large.
	bigPNG := append(makeTestPNG(16, 16), bytes.Repeat([]byte{0}, 5000)...)
	bBig, ctBig := newMultipartBody(t, nil, "file", "besar.png", bigPNG)
	rBig := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/media?kind=image", bytes.NewReader(bBig))
	rBig.Header.Set("X-API-Key", key)
	rBig.Header.Set("Content-Type", ctBig)
	wBig := httptest.NewRecorder()
	h.ServeHTTP(wBig, rBig)
	if wBig.Code != http.StatusRequestEntityTooLarge || !strings.Contains(wBig.Body.String(), "media_too_large") {
		t.Fatalf("413 media_too_large tidak sesuai: %d %s", wBig.Code, wBig.Body.String())
	}

	// 3. 415 ptt_requires_ogg_opus dan sticker_requires_webp.
	bPTT, ctPTT := newMultipartBody(t, nil, "file", "suara.jpg", jpgBytes)
	rPTT := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/media?kind=ptt", bytes.NewReader(bPTT))
	rPTT.Header.Set("X-API-Key", key)
	rPTT.Header.Set("Content-Type", ctPTT)
	wPTT := httptest.NewRecorder()
	h.ServeHTTP(wPTT, rPTT)
	if wPTT.Code != http.StatusUnsupportedMediaType || !strings.Contains(wPTT.Body.String(), "ptt_requires_ogg_opus") {
		t.Fatalf("415 ptt_requires_ogg_opus tidak sesuai: %d %s", wPTT.Code, wPTT.Body.String())
	}

	bStk, ctStk := newMultipartBody(t, nil, "file", "stiker.png", makeTestPNG(64, 64))
	rStk := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/media?kind=sticker", bytes.NewReader(bStk))
	rStk.Header.Set("X-API-Key", key)
	rStk.Header.Set("Content-Type", ctStk)
	wStk := httptest.NewRecorder()
	h.ServeHTTP(wStk, rStk)
	if wStk.Code != http.StatusUnsupportedMediaType || !strings.Contains(wStk.Body.String(), "sticker_requires_webp") {
		t.Fatalf("415 sticker_requires_webp tidak sesuai: %d %s", wStk.Code, wStk.Body.String())
	}

	// 4. 503 memory_pressure saat governor berada pada level Red.
	gov.lvl = governor.Red
	bRed, ctRed := newMultipartBody(t, nil, "file", "ok.jpg", jpgBytes)
	rRed := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/media?kind=image", bytes.NewReader(bRed))
	rRed.Header.Set("X-API-Key", key)
	rRed.Header.Set("Content-Type", ctRed)
	wRed := httptest.NewRecorder()
	h.ServeHTTP(wRed, rRed)
	if wRed.Code != http.StatusServiceUnavailable || wRed.Header().Get("Retry-After") != "15" || !strings.Contains(wRed.Body.String(), "memory_pressure") {
		t.Fatalf("503 memory_pressure tidak sesuai: %d Retry-After=%s %s", wRed.Code, wRed.Header().Get("Retry-After"), wRed.Body.String())
	}
	gov.lvl = governor.Green

	// 5. Validasi upload_id di POST /v2/messages: 404 upload_not_found, 410 upload_expired, 400 type=ptt, dan 202 valid.
	reqNotFound := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages", strings.NewReader(`{"to":"628123456789","type":"image","media":{"upload_id":"upl_hilang"}}`))
	reqNotFound.Header.Set("X-API-Key", key)
	wNF := httptest.NewRecorder()
	h.ServeHTTP(wNF, reqNotFound)
	if wNF.Code != http.StatusNotFound || !strings.Contains(wNF.Body.String(), "upload_not_found") {
		t.Fatalf("expected 404 upload_not_found, didapat %d %s", wNF.Code, wNF.Body.String())
	}

	expRec, _ := upStore.Put(ctx, media.UploadRecord{
		InstanceID: "sales",
		Kind:       media.KindImage,
		Mimetype:   "image/jpeg",
		Size:       100,
		CreatedAt:  time.Now().Add(-25 * time.Hour).UnixMilli(),
		ExpiresAt:  time.Now().Add(-time.Hour).UnixMilli(),
	})
	reqExp := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages", strings.NewReader(`{"to":"628123456789","type":"image","media":{"upload_id":"`+expRec.ID+`"}}`))
	reqExp.Header.Set("X-API-Key", key)
	wExp := httptest.NewRecorder()
	h.ServeHTTP(wExp, reqExp)
	if wExp.Code != http.StatusGone || !strings.Contains(wExp.Body.String(), "upload_expired") {
		t.Fatalf("expected 410 upload_expired, didapat %d %s", wExp.Code, wExp.Body.String())
	}

	reqPTTType := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages", strings.NewReader(`{"to":"628123456789","type":"ptt","media":{"upload_id":"`+upResp.UploadID+`"}}`))
	reqPTTType.Header.Set("X-API-Key", key)
	wPTTType := httptest.NewRecorder()
	h.ServeHTTP(wPTTType, reqPTTType)
	if wPTTType.Code != http.StatusBadRequest {
		t.Fatalf("type 'ptt' top-level harus ditolak 400, didapat %d %s", wPTTType.Code, wPTTType.Body.String())
	}

	// Kirim 1 upload_id yang sama ke 3 penerima berbeda tanpa upload ulang ke CDN (uploadCalls tetap 1).
	beforeCalls := drv.uploadCalls
	for _, rcpt := range []string{"628123456789", "628123456790", "628123456791"} {
		rSend := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/messages", strings.NewReader(`{"to":"`+rcpt+`","type":"image","media":{"upload_id":"`+upResp.UploadID+`","caption":"multi"}}`))
		rSend.Header.Set("X-API-Key", key)
		wSend := httptest.NewRecorder()
		h.ServeHTTP(wSend, rSend)
		if wSend.Code != http.StatusAccepted {
			t.Fatalf("kirim multi-penerima dengan upload_id gagal ke %s: %d %s", rcpt, wSend.Code, wSend.Body.String())
		}
	}
	if drv.uploadCalls != beforeCalls {
		t.Fatalf("kirim 1 upload_id ke 3 penerima memicu upload ulang (%d != %d)", drv.uploadCalls, beforeCalls)
	}

	// 6. GET /v2/events/{seq}/media dengan resurrection 404 -> retry -> sukses 200 + ETag & Content-Disposition dokumen.
	mediaKey := make([]byte, 32)
	_, _ = rand.Read(mediaKey)
	docSHA := bytes.Repeat([]byte{0xab}, 32)
	docPayload, _ := json.Marshal(wa.MessageV2{
		ID:       "WAMID-EVT-1",
		From:     "628111111111@s.whatsapp.net",
		Chat:     "628111111111@s.whatsapp.net",
		ChatType: "dm",
		Type:     "document",
		Media: &wa.MediaDescriptor{
			Mimetype:      "application/pdf",
			DirectPath:    "/v/expired-path",
			MediaKey:      base64.StdEncoding.EncodeToString(mediaKey),
			FileEncSHA256: base64.StdEncoding.EncodeToString(docSHA),
			FileSHA256:    base64.StdEncoding.EncodeToString(docSHA),
			FileLength:    11,
			MediaType:     "document",
			Filename:      "../rahasia/kontrak\r\n.pdf",
		},
	})
	appRes, err := j.Append(ctx, &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "628111111111@s.whatsapp.net",
		Sender:     "628111111111@s.whatsapp.net",
		WAID:       "WAMID-EVT-1",
		DedupeKey:  "m:test-media-dl",
		TS:         time.Now().Unix(),
		ReceivedAt: time.Now().UnixMilli(),
		Payload:    docPayload,
	})
	if err != nil {
		t.Fatalf("Append event media gagal: %v", err)
	}

	drv.dlFn = func(ctx context.Context, directPath string, _, _, _ []byte, _ whatsmeow.MediaType, _ string, _ bool, f whatsmeow.File) error {
		if directPath == "/v/expired-path" {
			return whatsmeow.ErrMediaDownloadFailedWith404
		}
		_, err := f.Write([]byte("isi-pdf-ok!"))
		return err
	}
	drv.retryFn = func(ctx context.Context, info *types.MessageInfo, key []byte) error {
		notif := &waMmsRetry.MediaRetryNotification{
			StanzaID:   proto.String(string(info.ID)),
			DirectPath: proto.String("/v/new-path"),
			Result:     waMmsRetry.MediaRetryNotification_SUCCESS.Enum(),
		}
		pt, _ := proto.Marshal(notif)
		retryKey := hkdfutil.SHA256(key, nil, []byte("WhatsApp Media Retry Notification"), 32)
		iv := make([]byte, 12)
		_, _ = rand.Read(iv)
		ct, _ := gcmutil.Encrypt(retryKey, iv, pt, []byte(info.ID))
		go func() {
			waiter.Deliver(&events.MediaRetry{
				MessageID:  info.ID,
				Ciphertext: ct,
				IV:         iv,
			})
		}()
		return nil
	}

	rGetMedia := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v2/instances/sales/events/%d/media", appRes.Seq), nil)
	rGetMedia.Header.Set("X-API-Key", key)
	wGetMedia := httptest.NewRecorder()
	h.ServeHTTP(wGetMedia, rGetMedia)
	if wGetMedia.Code != http.StatusOK {
		t.Fatalf("GET /events/{seq}/media gagal: %d %s", wGetMedia.Code, wGetMedia.Body.String())
	}
	if wGetMedia.Body.String() != "isi-pdf-ok!" ||
		wGetMedia.Header().Get("ETag") != `"`+hex.EncodeToString(docSHA)+`"` ||
		!strings.Contains(wGetMedia.Header().Get("Content-Disposition"), "kontrak.pdf") {
		t.Fatalf("header/body download media v2 tidak sesuai: body=%q headers=%+v", wGetMedia.Body.String(), wGetMedia.Header())
	}

	// 7. POST /v2/instances/{id}/media/download descriptor v1.
	drv.dlFn = func(ctx context.Context, directPath string, _, _, _ []byte, _ whatsmeow.MediaType, _ string, _ bool, f whatsmeow.File) error {
		_, err := f.Write([]byte("direct-v2-dl"))
		return err
	}
	dlDescJSON := fmt.Sprintf(`{"mimetype":"image/jpeg","direct_path":"/v/ok","media_key":"%s","file_enc_sha256":"%s","file_sha256":"%s","media_type":"image"}`,
		base64.StdEncoding.EncodeToString(mediaKey), base64.StdEncoding.EncodeToString(docSHA), base64.StdEncoding.EncodeToString(docSHA))
	rV2DL := httptest.NewRequest(http.MethodPost, "/v2/instances/sales/media/download", strings.NewReader(dlDescJSON))
	rV2DL.Header.Set("X-API-Key", key)
	wV2DL := httptest.NewRecorder()
	h.ServeHTTP(wV2DL, rV2DL)
	if wV2DL.Code != http.StatusOK || wV2DL.Body.String() != "direct-v2-dl" {
		t.Fatalf("POST /v2/media/download gagal: %d %q", wV2DL.Code, wV2DL.Body.String())
	}
}

// TestV1MediaCompat memastikan /v1/messages/media tetap menerapkan aturan urutan field dan mengirim via pipeline+outbox serta /v1/media/download memakai pipeline.
func TestV1MediaCompat(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cfg := &config.Config{DataDir: tmp, MediaMaxBytes: 1 << 20, DocumentMaxBytes: 2 << 20}
	cfg.Normalize()
	_ = cfg.EnsureDirs()

	st, _ := store.Open(ctx, filepath.Join(tmp, "app.db"))
	t.Cleanup(func() { _ = st.Close() })
	authMgr, _ := auth.NewManager(ctx, st)
	waMgr, _ := wa.NewManager(ctx, cfg, st, nil)
	t.Cleanup(func() { _ = waMgr.Close() })
	_, _ = waMgr.Create(ctx, "sales", 60, false)

	j, _ := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{MaxConns: 2})
	t.Cleanup(func() { _ = j.Close() })

	var seenJob atomicBoolOrSpec
	exec := &apiFakeExecutor{
		ready:  map[string]bool{"sales": true},
		paired: map[string]bool{"sales": true},
		execFn: func(ctx context.Context, job *outbox.Job) (outbox.ExecResult, error) {
			seenJob.set(job.Kind, job.Spec.Media.UploadID)
			return outbox.ExecResult{ServerTS: time.Unix(1700000100, 0)}, nil
		},
	}
	upStore := media.NewUploadStore(j.DB())
	pipe := media.NewPipeline(media.Limits{
		MediaMaxBytes:    cfg.MediaMaxBytes,
		DocumentMaxBytes: cfg.DocumentMaxBytes,
		TmpDir:           cfg.TmpDir(),
	})
	drv := &fakeMediaDriver{}

	outSvc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{
		DisableSendJitter: true,
	})
	t.Cleanup(outSvc.Stop)
	outSvc.StartScheduler(ctx, "sales")

	key, _, _ := authMgr.Create(ctx, "v1-media", "")
	h := NewHandler(authMgr, waMgr,
		WithDB(st.DB),
		WithJournal(j),
		WithOutbox(outSvc),
		WithMedia(pipe, upStore),
		withMediaDriver(drv, media.NewRetryWaiter()),
		withV1WaitTimeout(5*time.Second),
	)

	pngBytes := makeTestPNG(24, 24)

	// 1. Aturan urutan field: part file sebelum to/type harus ditolak 400.
	badBody, badCT := newMultipartBody(t, []struct{ k, v string }{{k: "to", v: "628123456789"}}, "file", "a.png", pngBytes)
	rBad := httptest.NewRequest(http.MethodPost, "/v1/instances/sales/messages/media", bytes.NewReader(badBody))
	rBad.Header.Set("X-API-Key", key)
	rBad.Header.Set("Content-Type", badCT)
	wBad := httptest.NewRecorder()
	h.ServeHTTP(wBad, rBad)
	if wBad.Code != http.StatusBadRequest {
		t.Fatalf("part file sebelum field type wajib ditolak 400, didapat %d %s", wBad.Code, wBad.Body.String())
	}

	// 2. Upload v1 valid melewati pipeline -> UploadStore -> outbox Wait(sent).
	okBody, okCT := newMultipartBody(t, []struct{ k, v string }{
		{k: "to", v: "628123456789"},
		{k: "type", v: "image"},
		{k: "caption", v: "halo v1"},
	}, "file", "a.png", pngBytes)
	rOK := httptest.NewRequest(http.MethodPost, "/v1/instances/sales/messages/media", bytes.NewReader(okBody))
	rOK.Header.Set("X-API-Key", key)
	rOK.Header.Set("Content-Type", okCT)
	wOK := httptest.NewRecorder()
	h.ServeHTTP(wOK, rOK)
	if wOK.Code != http.StatusOK {
		t.Fatalf("POST /v1/messages/media gagal: %d %s", wOK.Code, wOK.Body.String())
	}
	var res wa.SendResult
	if err := json.Unmarshal(wOK.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode hasil v1 gagal: %v", err)
	}
	if !strings.HasPrefix(res.MessageID, "3EB0") || res.To != "628123456789@s.whatsapp.net" || res.Timestamp != 1700000100 {
		t.Fatalf("bentuk respons v1 tidak sesuai: %+v", res)
	}
	kindGot, uplGot := seenJob.get()
	if kindGot != "image" || !strings.HasPrefix(uplGot, "upl_") {
		t.Fatalf("outbox job v1 seharusnya memakai upload_id hasil pipeline, didapat kind=%q upload_id=%q", kindGot, uplGot)
	}

	// 3. POST /v1/media/download memakai pipeline.
	mediaKey := make([]byte, 32)
	docSHA := make([]byte, 32)
	dlReq := `{"mimetype":"image/png","direct_path":"/v/v1dl","media_key":"` + base64.StdEncoding.EncodeToString(mediaKey) + `","file_enc_sha256":"` + base64.StdEncoding.EncodeToString(docSHA) + `","file_sha256":"` + base64.StdEncoding.EncodeToString(docSHA) + `","media_type":"image"}`
	rDL := httptest.NewRequest(http.MethodPost, "/v1/instances/sales/media/download", strings.NewReader(dlReq))
	rDL.Header.Set("X-API-Key", key)
	wDL := httptest.NewRecorder()
	h.ServeHTTP(wDL, rDL)
	if wDL.Code != http.StatusOK || wDL.Body.String() != "media-downloaded" {
		t.Fatalf("POST /v1/media/download gagal: %d %q", wDL.Code, wDL.Body.String())
	}
}

type atomicBoolOrSpec struct {
	mu       sync.Mutex
	kind     string
	uploadID string
}

func (a *atomicBoolOrSpec) set(k, u string) {
	a.mu.Lock()
	a.kind = k
	a.uploadID = u
	a.mu.Unlock()
}

func (a *atomicBoolOrSpec) get() (string, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.kind, a.uploadID
}
