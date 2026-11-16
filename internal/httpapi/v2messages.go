package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/wa"
)

const (
	defaultV2WaitTimeout = 30 * time.Second
	defaultV1WaitTimeout = 60 * time.Second
)

type v2SendMessageRequest struct {
	To       string           `json:"to"`
	Type     string           `json:"type"`
	Text     string           `json:"text"`
	ReplyTo  *outbox.ReplyRef `json:"reply_to"`
	Mentions []string         `json:"mentions"`
	Typing   bool             `json:"typing"`
	Media    *outbox.MediaRef `json:"media"`
	SendAt   string           `json:"send_at"`
	Priority string           `json:"priority"`
	TTL      string           `json:"ttl"`
	Wait     string           `json:"wait"`
}

type v2EnqueueResponse struct {
	ID          string `json:"id"`
	WAMessageID string `json:"wa_message_id"`
	Status      string `json:"status"`
	To          string `json:"to"`
	NotBefore   int64  `json:"not_before"`
	StatusURL   string `json:"status_url"`
}

type v2ReceiptsJSON struct {
	Delivered int `json:"delivered"`
	Read      int `json:"read"`
}

type v2ErrorJSON struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type v2MessageDetail struct {
	ID          string         `json:"id"`
	WAMessageID string         `json:"wa_message_id"`
	Instance    string         `json:"instance"`
	To          string         `json:"to"`
	Type        string         `json:"type"`
	Status      string         `json:"status"`
	Origin      string         `json:"origin"`
	Attempts    int            `json:"attempts"`
	CreatedAt   int64          `json:"created_at"`
	SentAt      *int64         `json:"sent_at"`
	DeliveredAt *int64         `json:"delivered_at"`
	ReadAt      *int64         `json:"read_at"`
	Receipts    v2ReceiptsJSON `json:"receipts"`
	Error       *v2ErrorJSON   `json:"error"`
}

func (s *Server) handleV2SendMessage(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.outbox == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "outbox belum aktif", 0)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req v2SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload JSON tidak valid atau melebihi 64KB", 0)
		return
	}

	priority, err := parseOutboxPriority(req.Priority)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_spec", err.Error(), 0)
		return
	}

	var sendAt time.Time
	if rawSA := strings.TrimSpace(req.SendAt); rawSA != "" {
		parsed, pErr := time.Parse(time.RFC3339, rawSA)
		if pErr != nil {
			writeError(w, http.StatusBadRequest, "invalid_spec", "format send_at harus RFC3339 yang valid", 0)
			return
		}
		sendAt = parsed
	}

	var ttl time.Duration
	if rawTTL := strings.TrimSpace(req.TTL); rawTTL != "" {
		parsed, dErr := time.ParseDuration(rawTTL)
		if dErr != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_spec", "durasi ttl tidak valid", 0)
			return
		}
		ttl = parsed
	}

	waitMode := strings.ToLower(strings.TrimSpace(req.Wait))
	if waitMode == "" {
		waitMode = "none"
	}
	if waitMode != "none" && waitMode != "sent" {
		writeError(w, http.StatusBadRequest, "invalid_spec", "nilai wait harus 'none' atau 'sent'", 0)
		return
	}

	instID := r.PathValue("id")
	msgType := strings.ToLower(strings.TrimSpace(req.Type))
	if req.Media != nil && strings.TrimSpace(req.Media.UploadID) != "" && s.uploadStore != nil && isCanonicalOutboxMediaType(msgType) {
		rec, uErr := s.uploadStore.Get(r.Context(), instID, strings.TrimSpace(req.Media.UploadID), time.Now())
		if uErr != nil {
			writeMappedError(w, uErr)
			return
		}
		if !wa.IsUploadKindCompatible(rec.Kind, msgType) {
			writeError(w, http.StatusBadRequest, "invalid_spec", "jenis upload tidak sesuai dengan tipe pesan", 0)
			return
		}
	}

	job, created, err := s.outbox.Enqueue(r.Context(), outbox.EnqueueRequest{
		InstanceID: instID,
		IdemKey:    strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		Spec: outbox.Spec{
			Type:     msgType,
			To:       req.To,
			Text:     req.Text,
			ReplyTo:  req.ReplyTo,
			Mentions: req.Mentions,
			Typing:   req.Typing,
			Media:    req.Media,
		},
		SendAt:   sendAt,
		Priority: priority,
		TTL:      ttl,
		Origin:   outboxOriginFromContext(r.Context(), "api"),
	})
	if err != nil {
		writeMappedError(w, err)
		return
	}

	if waitMode == "sent" {
		waitCtx, cancel := context.WithTimeout(r.Context(), defaultV2WaitTimeout)
		defer cancel()
		doneJob, waitErr := s.outbox.Wait(waitCtx, instID, job.ID, outbox.StatusSent)
		if waitErr != nil {
			writeOutboxWaitError(w, r.Context(), instID, job, doneJob, waitErr)
			return
		}
		writeJSON(w, http.StatusOK, toV2MessageDetail(doneJob))
		return
	}

	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, v2EnqueueResponse{
		ID:          job.ID,
		WAMessageID: job.WAID,
		Status:      string(job.Status),
		To:          job.ToJID,
		NotBefore:   job.NotBefore / 1000,
		StatusURL:   v2MessageStatusURL(instID, job.ID),
	})
}

func (s *Server) handleV2GetMessage(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.outbox == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "outbox belum aktif", 0)
		return
	}
	job, err := s.outbox.Get(r.Context(), r.PathValue("id"), r.PathValue("msg_id"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toV2MessageDetail(job))
}

func (s *Server) handleV2CancelMessage(w http.ResponseWriter, r *http.Request, _ *wa.Instance) {
	if s.outbox == nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "outbox belum aktif", 0)
		return
	}
	job, err := s.outbox.Cancel(r.Context(), r.PathValue("id"), r.PathValue("msg_id"))
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toV2MessageDetail(job))
}

func writeOutboxWaitError(w http.ResponseWriter, reqCtx context.Context, instID string, enqueued, latest *outbox.Job, waitErr error) {
	if reqCtx.Err() != nil && !errors.Is(waitErr, outbox.ErrWaitTimeout) {
		return
	}
	ref := latest
	if ref == nil {
		ref = enqueued
	}
	if errors.Is(waitErr, outbox.ErrWaitTimeout) {
		writeErrorDetails(w, http.StatusGatewayTimeout, "send_timeout", "batas waktu tunggu pengiriman pesan habis", 0, map[string]any{
			"id":         ref.ID,
			"status_url": v2MessageStatusURL(instID, ref.ID),
		})
		return
	}
	if errors.Is(waitErr, outbox.ErrSendFailed) {
		msg := "pesan gagal dikirim"
		if ref.ErrorMsg != "" {
			msg = ref.ErrorMsg
		} else if ref.Status == outbox.StatusExpired {
			msg = "pesan kedaluwarsa sebelum terkirim"
		}
		details := map[string]any{
			"id":     ref.ID,
			"status": string(ref.Status),
		}
		if ref.ErrorCode != "" {
			details["error_code"] = ref.ErrorCode
		}
		writeErrorDetails(w, http.StatusBadGateway, "send_failed", msg, 0, details)
		return
	}
	writeMappedError(w, waitErr)
}

func toV2MessageDetail(j *outbox.Job) v2MessageDetail {
	out := v2MessageDetail{
		ID:          j.ID,
		WAMessageID: j.WAID,
		Instance:    j.InstanceID,
		To:          j.ToJID,
		Type:        j.Kind,
		Status:      string(j.Status),
		Origin:      j.Origin,
		Attempts:    j.Attempts,
		CreatedAt:   j.CreatedAt / 1000,
		SentAt:      unixSecPtr(j.SentAt),
		DeliveredAt: unixSecPtr(j.DeliveredAt),
		ReadAt:      unixSecPtr(j.ReadAt),
		Receipts: v2ReceiptsJSON{
			Delivered: j.RcptDelivered,
			Read:      j.RcptRead,
		},
	}
	if j.ErrorCode != "" || j.ErrorMsg != "" {
		out.Error = &v2ErrorJSON{
			Code:    j.ErrorCode,
			Message: j.ErrorMsg,
		}
	}
	return out
}

func unixSecPtr(ms int64) *int64 {
	if ms <= 0 {
		return nil
	}
	sec := ms / 1000
	return &sec
}

func v2MessageStatusURL(instID, msgID string) string {
	return fmt.Sprintf("/v2/instances/%s/messages/%s", instID, msgID)
}

func parseOutboxPriority(raw string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "normal":
		return outbox.PriorityNormal, nil
	case "urgent":
		return outbox.PriorityUrgent, nil
	default:
		return 0, fmt.Errorf("priority harus 'normal' atau 'urgent'")
	}
}

func outboxOriginFromContext(ctx context.Context, kind string) outbox.Origin {
	km, _ := ctx.Value(keyMetaCtx).(*auth.KeyMeta)
	keyID := ""
	if km != nil {
		keyID = km.ID
	}
	return outbox.Origin{Kind: kind, KeyID: keyID}
}

func isCanonicalOutboxMediaType(t string) bool {
	switch t {
	case "image", "video", "audio", "document", "sticker":
		return true
	default:
		return false
	}
}
