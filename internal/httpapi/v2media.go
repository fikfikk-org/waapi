package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/wa"
)

const mediaTransferDeadline = 10 * time.Minute

type v2UploadResponse struct {
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

// handleV2UploadMedia menangani unggahan multipart satu-pass dan menyimpan descriptor ke UploadStore (201 Created).
func (s *Server) handleV2UploadMedia(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.mediaPipe == nil || s.uploadStore == nil || s.mediaDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "media engine belum aktif", 0)
		return
	}

	rc := http.NewResponseController(w)
	deadline := time.Now().Add(mediaTransferDeadline)
	_ = rc.SetReadDeadline(deadline)
	_ = rc.SetWriteDeadline(deadline)

	q := r.URL.Query()
	kind, err := media.ValidateKind(q.Get("kind"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}

	var seconds uint32
	if rawSec := strings.TrimSpace(q.Get("seconds")); rawSec != "" {
		v, pErr := strconv.ParseUint(rawSec, 10, 32)
		if pErr != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "parameter seconds harus berupa bilangan bulat positif", 0)
			return
		}
		seconds = uint32(v)
	}

	filename := strings.TrimSpace(q.Get("filename"))
	mimetype := strings.TrimSpace(q.Get("mimetype"))

	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request harus berupa multipart/form-data", 0)
		return
	}

	instID := r.PathValue("id")
	for {
		part, pErr := mr.NextPart()
		if pErr == io.EOF {
			break
		}
		if pErr != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "gagal membaca multipart stream", 0)
			return
		}
		if part.FormName() != "file" {
			// Lewati part teks selain file dengan batas baca aman 8 KiB.
			_, _ = io.Copy(io.Discard, io.LimitReader(part, 8<<10))
			_ = part.Close()
			continue
		}

		if filename == "" {
			filename = part.FileName()
		}
		if mimetype == "" {
			mimetype = part.Header.Get("Content-Type")
		}

		rec, shaHex, upErr := s.uploadMediaRecord(r.Context(), instID, part, kind, media.Hint{
			Mimetype: mimetype,
			Filename: filename,
			Seconds:  seconds,
		})
		_ = part.Close()
		if upErr != nil {
			writeMappedError(w, upErr)
			return
		}

		writeJSON(w, http.StatusCreated, v2UploadResponse{
			UploadID:  rec.ID,
			Kind:      string(rec.Kind),
			Mimetype:  rec.Mimetype,
			Size:      rec.Size,
			SHA256:    shaHex,
			Width:     rec.Width,
			Height:    rec.Height,
			Seconds:   rec.Seconds,
			ExpiresAt: rec.ExpiresAt / 1000,
		})
		return
	}

	writeError(w, http.StatusBadRequest, "invalid_request", "part file tidak ditemukan di dalam form", 0)
}

func (s *Server) uploadMediaRecord(ctx context.Context, instID string, r io.Reader, kind media.Kind, hint media.Hint) (media.UploadRecord, string, error) {
	up, err := s.mediaDriver.UploaderFor(instID)
	if err != nil {
		return media.UploadRecord{}, "", err
	}
	uploaded, err := s.mediaPipe.Upload(ctx, up, r, kind, hint)
	if err != nil {
		return media.UploadRecord{}, "", err
	}

	rec, err := s.uploadStore.Put(ctx, media.UploadRecord{
		InstanceID: instID,
		Kind:       kind,
		Mimetype:   uploaded.Meta.Mimetype,
		Size:       uploaded.Meta.Size,
		Resp:       uploaded.Resp,
		Width:      uploaded.Meta.Width,
		Height:     uploaded.Meta.Height,
		Seconds:    uploaded.Meta.Seconds,
		PTT:        uploaded.Meta.PTT,
		Filename:   uploaded.Meta.Filename,
	})
	if err != nil {
		return media.UploadRecord{}, "", err
	}

	var shaHex string
	if len(uploaded.Meta.SHA256) > 0 {
		shaHex = hex.EncodeToString(uploaded.Meta.SHA256)
	} else if len(uploaded.Resp.FileSHA256) > 0 {
		shaHex = hex.EncodeToString(uploaded.Resp.FileSHA256)
	}
	return rec, shaHex, nil
}

// handleV2DownloadEventMedia mengunduh media event journal dengan mekanisme media resurrection saat 404/410 CDN.
func (s *Server) handleV2DownloadEventMedia(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.journal == nil || s.mediaPipe == nil || s.mediaDriver == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "layanan journal atau media belum aktif", 0)
		return
	}

	seq, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("seq")), 10, 64)
	if err != nil || seq <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "nomor seq event tidak valid", 0)
		return
	}

	instID := r.PathValue("id")
	ev, err := s.journal.Get(r.Context(), instID, seq)
	if err != nil {
		writeMappedError(w, err)
		return
	}

	var msg wa.MessageV2
	if ev.Type != "message" || json.Unmarshal(ev.Payload, &msg) != nil || msg.Media == nil || strings.TrimSpace(msg.Media.DirectPath) == "" {
		writeError(w, http.StatusNotFound, "event_not_found", "event tidak memiliki lampiran media", 0)
		return
	}

	pDesc, err := pipelineDescriptorFromWA(*msg.Media)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
		return
	}

	dl, sender, err := s.mediaDriver.DownloaderFor(instID)
	if err != nil {
		writeMappedError(w, err)
		return
	}

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(mediaTransferDeadline))

	hw := &downloadHeaderWriter{
		w:          w,
		attachment: pDesc.MediaType == media.KindDocument,
	}
	msgInfo := wa.ParseMessageInfoFromV2(ev.Chat, msg, ev.Sender, ev.TS)
	if s.retryWaiter != nil {
		_, err = s.mediaPipe.DownloadWithResurrection(r.Context(), dl, sender, s.retryWaiter, msgInfo, pDesc, hw)
	} else {
		_, err = s.mediaPipe.Download(r.Context(), dl, pDesc, hw)
	}
	if err != nil && !hw.wrote {
		writeMappedError(w, err)
	}
}

// handleV2DownloadMedia menangani pengunduhan media v2 berbasis descriptor v1 tanpa media resurrection.
func (s *Server) handleV2DownloadMedia(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	s.serveDescriptorDownload(w, r, inst)
}

func (s *Server) serveDescriptorDownload(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	var desc wa.MediaDescriptor
	if err := json.NewDecoder(r.Body).Decode(&desc); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "descriptor media tidak valid", 0)
		return
	}
	if strings.TrimSpace(desc.DirectPath) == "" || strings.TrimSpace(desc.MediaKey) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "direct_path dan media_key wajib diisi", 0)
		return
	}

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(mediaTransferDeadline))

	if s.mediaPipe != nil && s.mediaDriver != nil {
		pDesc, err := pipelineDescriptorFromWA(desc)
		if err != nil {
			writeMappedError(w, err)
			return
		}
		dl, _, err := s.mediaDriver.DownloaderFor(r.PathValue("id"))
		if err != nil {
			writeMappedError(w, err)
			return
		}
		hw := &downloadHeaderWriter{
			w:          w,
			attachment: pDesc.MediaType == media.KindDocument,
		}
		if _, err := s.mediaPipe.Download(r.Context(), dl, pDesc, hw); err != nil && !hw.wrote {
			writeMappedError(w, err)
		}
		return
	}

	mime := desc.Mimetype
	if mime == "" {
		mime = "application/octet-stream"
	}
	hw := &downloadHeaderWriter{
		w:          w,
		fallbackCT: mime,
	}
	if err := inst.DownloadMediaToWriter(r.Context(), desc, hw); err != nil && !hw.wrote {
		writeMappedError(w, err)
	}
}

func pipelineDescriptorFromWA(desc wa.MediaDescriptor) (media.MediaDescriptor, error) {
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

type downloadHeaderWriter struct {
	w          http.ResponseWriter
	attachment bool
	fallbackCT string
	headerDone bool
	wrote      bool
}

func (d *downloadHeaderWriter) BeforeWrite(info media.DownloadInfo) {
	if d.headerDone {
		return
	}
	d.headerDone = true
	h := d.w.Header()
	ct := info.Mimetype
	if ct == "" {
		ct = "application/octet-stream"
	}
	h.Set("Content-Type", ct)
	if info.Size >= 0 {
		h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	}
	if len(info.FileSHA256) > 0 {
		h.Set("ETag", `"`+hex.EncodeToString(info.FileSHA256)+`"`)
	}
	if d.attachment {
		name := media.SanitizeFilename(info.Filename)
		if name == "" {
			name = "attachment"
		}
		h.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, name))
	}
}

func (d *downloadHeaderWriter) Write(p []byte) (int, error) {
	if !d.headerDone && d.fallbackCT != "" {
		d.w.Header().Set("Content-Type", d.fallbackCT)
		d.headerDone = true
	}
	d.wrote = true
	return d.w.Write(p)
}
