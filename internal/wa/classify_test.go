package wa

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"go.mau.fi/whatsmeow"
)

type stubNetTimeoutErr struct{}

func (stubNetTimeoutErr) Error() string   { return "i/o timeout" }
func (stubNetTimeoutErr) Timeout() bool   { return true }
func (stubNetTimeoutErr) Temporary() bool { return true }

var _ net.Error = stubNetTimeoutErr{}

// TestClassify_Tabel menguji pemetaan error whatsmeow oleh wa.toSendError dan pembacaan kelasnya oleh outbox.Classify.
func TestClassify_Tabel(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		useWaMap  bool
		transient bool
		code      string
		minDelay  time.Duration
	}{
		{
			name:      "whatsmeow ErrNotConnected",
			err:       whatsmeow.ErrNotConnected,
			useWaMap:  true,
			transient: true,
			code:      "not_connected",
		},
		{
			name:      "whatsmeow ErrNotLoggedIn",
			err:       whatsmeow.ErrNotLoggedIn,
			useWaMap:  true,
			transient: true,
			code:      "not_connected",
		},
		{
			name:      "context DeadlineExceeded lewat wa",
			err:       context.DeadlineExceeded,
			useWaMap:  true,
			transient: true,
			code:      "timeout",
		},
		{
			name:      "net.Error timeout lewat wa",
			err:       stubNetTimeoutErr{},
			useWaMap:  true,
			transient: true,
			code:      "timeout",
		},
		{
			name:      "whatsmeow ErrMessageTimedOut",
			err:       whatsmeow.ErrMessageTimedOut,
			useWaMap:  true,
			transient: true,
			code:      "timeout",
		},
		{
			name:      "whatsmeow ErrIQRateOverLimit",
			err:       fmt.Errorf("gagal: %w", whatsmeow.ErrIQRateOverLimit),
			useWaMap:  true,
			transient: true,
			code:      "rate_overlimit",
			minDelay:  60 * time.Second,
		},
		{
			name:      "whatsmeow ErrRecipientADJID",
			err:       whatsmeow.ErrRecipientADJID,
			useWaMap:  true,
			transient: false,
			code:      "invalid_recipient",
		},
		{
			name:      "whatsmeow ErrUnknownServer",
			err:       whatsmeow.ErrUnknownServer,
			useWaMap:  true,
			transient: false,
			code:      "invalid_recipient",
		},
		{
			name:      "whatsmeow ErrBroadcastListUnsupported",
			err:       whatsmeow.ErrBroadcastListUnsupported,
			useWaMap:  true,
			transient: false,
			code:      "invalid_recipient",
		},
		{
			name:      "jid ErrInvalidRecipient",
			err:       jid.ErrInvalidRecipient,
			useWaMap:  true,
			transient: false,
			code:      "invalid_recipient",
		},
		{
			name:      "whatsmeow ErrServerReturnedError",
			err:       whatsmeow.ErrServerReturnedError,
			useWaMap:  true,
			transient: false,
			code:      "server_rejected",
		},
		{
			name:      "outbox ErrMediaExpired langsung",
			err:       outbox.ErrMediaExpired,
			useWaMap:  false,
			transient: false,
			code:      "media_expired",
		},
		{
			name:      "SendError eksplisit",
			err:       &outbox.SendError{Transient: true, Code: "custom_code", MinDelay: 15 * time.Second, Err: errors.New("x")},
			useWaMap:  false,
			transient: true,
			code:      "custom_code",
			minDelay:  15 * time.Second,
		},
		{
			name:      "context DeadlineExceeded langsung di outbox",
			err:       context.DeadlineExceeded,
			useWaMap:  false,
			transient: true,
			code:      "timeout",
		},
		{
			name:      "error tak dikenal",
			err:       errors.New("kesalahan acak"),
			useWaMap:  true,
			transient: true,
			code:      "send_failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.err
			if tc.useWaMap {
				in = toSendError(tc.err)
			}
			gotTransient, gotCode, gotDelay := outbox.Classify(in)
			if gotTransient != tc.transient || gotCode != tc.code || gotDelay != tc.minDelay {
				t.Fatalf("hasil klasifikasi tidak sesuai: dapat=(transient=%v, code=%q, delay=%v) ingin=(transient=%v, code=%q, delay=%v)",
					gotTransient, gotCode, gotDelay, tc.transient, tc.code, tc.minDelay)
			}
		})
	}
}
