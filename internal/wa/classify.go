package wa

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow"
)

// toSendError memetakan error transport whatsmeow ke *outbox.SendError untuk diklasifikasikan oleh paket outbox.
func toSendError(err error) error {
	if err == nil {
		return nil
	}
	var existing *outbox.SendError
	if errors.As(err, &existing) {
		return existing
	}

	switch {
	case errors.Is(err, whatsmeow.ErrNotConnected),
		errors.Is(err, whatsmeow.ErrNotLoggedIn):
		return &outbox.SendError{Transient: true, Code: "not_connected", Err: err}

	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, whatsmeow.ErrMessageTimedOut):
		return &outbox.SendError{Transient: true, Code: "timeout", Err: err}

	case errors.Is(err, whatsmeow.ErrIQRateOverLimit):
		return &outbox.SendError{Transient: true, Code: "rate_overlimit", MinDelay: 60 * time.Second, Err: err}

	case errors.Is(err, whatsmeow.ErrRecipientADJID),
		errors.Is(err, whatsmeow.ErrUnknownServer),
		errors.Is(err, whatsmeow.ErrBroadcastListUnsupported),
		errors.Is(err, jid.ErrInvalidRecipient):
		return &outbox.SendError{Transient: false, Code: "invalid_recipient", Err: err}

	case errors.Is(err, whatsmeow.ErrServerReturnedError):
		return &outbox.SendError{Transient: false, Code: "server_rejected", Err: err}

	case errors.Is(err, outbox.ErrMediaExpired):
		return &outbox.SendError{Transient: false, Code: "media_expired", Err: err}
	}

	var ne net.Error
	if errors.As(err, &ne) {
		return &outbox.SendError{Transient: true, Code: "timeout", Err: err}
	}

	return &outbox.SendError{Transient: true, Code: "send_failed", Err: err}
}
