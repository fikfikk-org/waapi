package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
)

type controlSendRequest struct {
	To      string `json:"to"`
	Text    string `json:"text"`
	ReplyTo string `json:"reply_to,omitempty"`
	Typing  bool   `json:"typing,omitempty"`
	Wait    string `json:"wait,omitempty"`
	IdemKey string `json:"idem_key,omitempty"`
}

type controlSendResponse struct {
	ID          string `json:"id"`
	WAMessageID string `json:"wa_message_id"`
	Status      string `json:"status"`
	To          string `json:"to"`
	NotBefore   int64  `json:"not_before"`
	SentAt      int64  `json:"sent_at,omitempty"`
}

func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if s.outbox == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("outbox belum aktif"))
		return
	}
	id := r.PathValue("id")
	if s.store != nil {
		if _, err := s.store.GetInstance(r.Context(), id); err != nil {
			writeStatusErr(w, err)
			return
		}
	}

	var req controlSendRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("payload JSON tidak valid"))
		return
	}

	waitMode := strings.ToLower(strings.TrimSpace(req.Wait))
	if waitMode == "" {
		waitMode = "none"
	}
	if waitMode != "none" && waitMode != "sent" {
		writeErr(w, http.StatusBadRequest, errors.New("nilai wait harus 'none' atau 'sent'"))
		return
	}

	var reply *outbox.ReplyRef
	if strings.TrimSpace(req.ReplyTo) != "" {
		reply = &outbox.ReplyRef{ID: strings.TrimSpace(req.ReplyTo)}
	}

	job, created, err := s.outbox.Enqueue(r.Context(), outbox.EnqueueRequest{
		InstanceID: id,
		IdemKey:    strings.TrimSpace(req.IdemKey),
		Spec: outbox.Spec{
			Type:    "text",
			To:      req.To,
			Text:    req.Text,
			ReplyTo: reply,
			Typing:  req.Typing,
		},
		Origin: outbox.Origin{Kind: "cli"},
	})
	if err != nil {
		writeOutboxControlErr(w, err)
		return
	}

	if waitMode == "sent" {
		waitCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		doneJob, waitErr := s.outbox.Wait(waitCtx, id, job.ID, outbox.StatusSent)
		if waitErr != nil {
			if errors.Is(waitErr, outbox.ErrWaitTimeout) {
				writeErr(w, http.StatusGatewayTimeout, waitErr)
				return
			}
			writeErr(w, http.StatusBadGateway, waitErr)
			return
		}
		writeJSON(w, http.StatusOK, controlSendResponse{
			ID:          doneJob.ID,
			WAMessageID: doneJob.WAID,
			Status:      string(doneJob.Status),
			To:          doneJob.ToJID,
			NotBefore:   doneJob.NotBefore / 1000,
			SentAt:      doneJob.SentAt / 1000,
		})
		return
	}

	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, controlSendResponse{
		ID:          job.ID,
		WAMessageID: job.WAID,
		Status:      string(job.Status),
		To:          job.ToJID,
		NotBefore:   job.NotBefore / 1000,
	})
}

func writeOutboxControlErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, errors.New("instance tidak ditemukan"))
	case errors.Is(err, outbox.ErrNotPaired):
		writeErr(w, http.StatusConflict, err)
	case errors.Is(err, outbox.ErrIdempotencyMismatch):
		writeErr(w, http.StatusUnprocessableEntity, err)
	case errors.Is(err, outbox.ErrQueueFull):
		writeErr(w, http.StatusTooManyRequests, err)
	case errors.Is(err, outbox.ErrInvalidSpec), errors.Is(err, jid.ErrInvalidRecipient):
		writeErr(w, http.StatusBadRequest, err)
	default:
		writeErr(w, http.StatusBadGateway, err)
	}
}
