package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/media"
	"github.com/fikfikk-org/waapi/internal/netguard"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/wa"
)

type sendTextRequest struct {
	To      string `json:"to"`
	Text    string `json:"text"`
	Typing  bool   `json:"typing"`
	ReplyTo string `json:"reply_to"`
}

type sendMediaURLRequest struct {
	To       string `json:"to"`
	Type     string `json:"type"`
	URL      string `json:"url"`
	Filename string `json:"filename"`
	Caption  string `json:"caption"`
}

// handleSendText menangani pengiriman pesan teks dengan batas body JSON 64 KB.
func (s *Server) handleSendText(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req sendTextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid atau melebihi 64KB", 0)
		return
	}
	if strings.TrimSpace(req.To) == "" || strings.TrimSpace(req.Text) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "field to dan text wajib diisi", 0)
		return
	}

	var reply *outbox.ReplyRef
	if strings.TrimSpace(req.ReplyTo) != "" {
		reply = &outbox.ReplyRef{ID: strings.TrimSpace(req.ReplyTo)}
	}
	if s.outbox != nil {
		s.sendV1ViaOutbox(w, r, r.PathValue("id"), outbox.Spec{
			Type:    "text",
			To:      req.To,
			Text:    req.Text,
			ReplyTo: reply,
			Typing:  req.Typing,
		})
		return
	}

	res, err := inst.EnqueueText(r.Context(), req.To, req.Text, req.ReplyTo, req.Typing)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSendMedia memproses multipart/form-data secara streaming menggunakan multipart.Reader.
func (s *Server) handleSendMedia(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	if s.rejectIfRed(w) {
		return
	}

	rc := http.NewResponseController(w)
	deadline := time.Now().Add(mediaTransferDeadline)
	_ = rc.SetReadDeadline(deadline)
	_ = rc.SetWriteDeadline(deadline)

	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request harus berupa multipart/form-data", 0)
		return
	}

	var (
		to, kind, caption, filename, mime string
		sent                              bool
	)

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "gagal membaca multipart stream", 0)
			return
		}
		name := part.FormName()
		if name == "file" {
			if filename == "" {
				filename = part.FileName()
			}
			mime = part.Header.Get("Content-Type")
			if strings.TrimSpace(to) == "" || strings.TrimSpace(kind) == "" {
				_ = part.Close()
				writeError(w, http.StatusBadRequest, "invalid_request", "field to dan type harus dikirim sebelum part file", 0)
				return
			}

			if s.outbox != nil && s.mediaPipe != nil && s.uploadStore != nil && s.mediaDriver != nil {
				instID := r.PathValue("id")
				if !s.outbox.Ready(instID) {
					_ = part.Close()
					writeError(w, http.StatusServiceUnavailable, "instance_not_connected", "instance sedang tidak tersambung", 0)
					return
				}
				k, kErr := media.ValidateKind(kind)
				if kErr != nil {
					_ = part.Close()
					writeMappedError(w, kErr)
					return
				}
				rec, _, upErr := s.uploadMediaRecord(r.Context(), instID, part, k, media.Hint{
					Mimetype: mime,
					Filename: filename,
				})
				_ = part.Close()
				if upErr != nil {
					writeMappedError(w, upErr)
					return
				}
				msgType := string(k)
				isPTT := false
				if k == media.KindPTT {
					msgType = "audio"
					isPTT = true
				}
				s.sendV1ViaOutbox(w, r, instID, outbox.Spec{
					Type: msgType,
					To:   to,
					Media: &outbox.MediaRef{
						UploadID: rec.ID,
						Caption:  caption,
						Filename: filename,
						PTT:      isPTT,
					},
				})
				sent = true
				return
			}

			res, sendErr := inst.EnqueueMedia(r.Context(), wa.MediaInput{
				To:        to,
				MediaType: kind,
				Caption:   caption,
				Filename:  filename,
				MimeType:  mime,
				Reader:    part,
			})
			_ = part.Close()
			if sendErr != nil {
				writeMappedError(w, sendErr)
				return
			}
			sent = true
			writeJSON(w, http.StatusOK, res)
			return
		}

		// Baca field teks kecil (maks 8 KB per field)
		valBytes, _ := io.ReadAll(io.LimitReader(part, 8<<10))
		_ = part.Close()
		val := strings.TrimSpace(string(valBytes))
		switch name {
		case "to":
			to = val
		case "type":
			kind = val
		case "caption":
			caption = val
		case "filename":
			filename = val
		}
	}

	if !sent {
		writeError(w, http.StatusBadRequest, "invalid_request", "part file tidak ditemukan di dalam form", 0)
	}
}

// handleSendMediaURL mengunduh media dari URL https eksternal (anti-SSRF) melalui pipeline lalu mengirimkannya.
func (s *Server) handleSendMediaURL(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
	if s.rejectIfRed(w) {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req sendMediaURLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid", 0)
		return
	}
	if strings.TrimSpace(req.To) == "" || strings.TrimSpace(req.Type) == "" || strings.TrimSpace(req.URL) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "field to, type, dan url wajib diisi", 0)
		return
	}
	if _, err := netguard.ValidateHTTPSURL(req.URL); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
		return
	}

	if s.outbox != nil {
		k, err := media.ValidateKind(req.Type)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
			return
		}
		msgType := string(k)
		isPTT := false
		if k == media.KindPTT {
			msgType = "audio"
			isPTT = true
		}
		s.sendV1ViaOutbox(w, r, r.PathValue("id"), outbox.Spec{
			Type: msgType,
			To:   req.To,
			Media: &outbox.MediaRef{
				URL:      req.URL,
				Caption:  req.Caption,
				Filename: req.Filename,
				PTT:      isPTT,
			},
		})
		return
	}

	client := netguard.NewClient(netguard.Policy{}, 60*time.Second)
	defer client.CloseIdleConnections()
	httpReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, req.URL, nil)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
		return
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("unduh url media: %v", err), 0)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("server media mengembalikan HTTP %d", resp.StatusCode), 0)
		return
	}

	res, err := inst.EnqueueMedia(r.Context(), wa.MediaInput{
		To:        req.To,
		MediaType: req.Type,
		Caption:   req.Caption,
		Filename:  req.Filename,
		Reader:    resp.Body,
	})
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) sendV1ViaOutbox(w http.ResponseWriter, r *http.Request, instID string, spec outbox.Spec) {
	if !s.outbox.Ready(instID) {
		writeError(w, http.StatusServiceUnavailable, "instance_not_connected", "instance sedang tidak tersambung", 0)
		return
	}

	job, _, err := s.outbox.Enqueue(r.Context(), outbox.EnqueueRequest{
		InstanceID: instID,
		IdemKey:    strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		Spec:       spec,
		Origin:     outboxOriginFromContext(r.Context(), "v1"),
	})
	if err != nil {
		writeMappedError(w, err)
		return
	}

	waitDur := s.v1WaitTimeout
	if waitDur <= 0 {
		waitDur = defaultV1WaitTimeout
	}
	waitCtx, cancel := context.WithTimeout(r.Context(), waitDur)
	defer cancel()
	doneJob, waitErr := s.outbox.Wait(waitCtx, instID, job.ID, outbox.StatusSent)
	if waitErr != nil {
		writeOutboxWaitError(w, r.Context(), instID, job, doneJob, waitErr)
		return
	}
	writeJSON(w, http.StatusOK, wa.SendResult{
		MessageID: doneJob.WAID,
		To:        doneJob.ToJID,
		Timestamp: doneJob.SentAt / 1000,
	})
}
