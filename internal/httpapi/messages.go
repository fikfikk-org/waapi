package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

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

	res, err := inst.EnqueueText(r.Context(), req.To, req.Text, req.ReplyTo, req.Typing)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleSendMedia memproses multipart/form-data secara streaming menggunakan multipart.Reader.
func (s *Server) handleSendMedia(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
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

// handleSendMediaURL mengunduh media dari URL https eksternal (anti-SSRF) secara streaming lalu mengirimkannya.
func (s *Server) handleSendMediaURL(w http.ResponseWriter, r *http.Request, inst *wa.Instance) {
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

	stream, err := wa.OpenSafeHTTPSStream(r.Context(), req.URL)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), 0)
		return
	}
	defer stream.Close()

	res, err := inst.EnqueueMedia(r.Context(), wa.MediaInput{
		To:        req.To,
		MediaType: req.Type,
		Caption:   req.Caption,
		Filename:  req.Filename,
		Reader:    stream,
	})
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

