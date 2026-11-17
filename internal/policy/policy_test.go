package policy

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
)

type fakePolicyLookup map[string]auth.KeyPolicy

func (f fakePolicyLookup) LookupPolicy(keyID string) (auth.KeyPolicy, bool) {
	p, ok := f[keyID]
	return p, ok
}

type contactTimestamps struct {
	firstIn time.Time
	lastIn  time.Time
}

type fakeContactView map[string]contactTimestamps

func (f fakeContactView) LastIn(_ context.Context, instanceID, jid string) (time.Time, bool) {
	c, ok := f[instanceID+"|"+jid]
	if !ok || c.lastIn.IsZero() {
		return time.Time{}, false
	}
	return c.lastIn, true
}

func (f fakeContactView) FirstIn(_ context.Context, instanceID, jid string) (time.Time, bool) {
	c, ok := f[instanceID+"|"+jid]
	if !ok || c.firstIn.IsZero() {
		return time.Time{}, false
	}
	return c.firstIn, true
}

// TestPolicyCheckSend_Tabel memverifikasi seluruh aturan KeyPolicy pada jalur kirim outbox serta bucket geser max_sends_per_hour di synctest.
func TestPolicyCheckSend_Tabel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Now()

		contacts := fakeContactView{
			"sales|628111222333@s.whatsapp.net": {
				firstIn: now.Add(-48 * time.Hour),
				lastIn:  now.Add(-2 * time.Hour),
			},
			"sales|628199988877@s.whatsapp.net": {
				firstIn: now.Add(-72 * time.Hour),
				lastIn:  now.Add(-30 * time.Hour),
			},
		}

		policies := fakePolicyLookup{
			"k_chat": func() auth.KeyPolicy {
				p := auth.DefaultPolicy()
				p.AllowedChats = []string{"6281*", "*@g.us"}
				return p
			}(),
			"k_window": func() auth.KeyPolicy {
				p := auth.DefaultPolicy()
				p.ReplyWindow = store.Duration(24 * time.Hour)
				return p
			}(),
			"k_fc_deny": func() auth.KeyPolicy {
				p := auth.DefaultPolicy()
				p.FirstContact = "deny"
				return p
			}(),
			"k_fc_ask": func() auth.KeyPolicy {
				p := auth.DefaultPolicy()
				p.FirstContact = "ask"
				return p
			}(),
			"k_destr_deny": func() auth.KeyPolicy {
				p := auth.DefaultPolicy()
				p.Destructive = "deny"
				return p
			}(),
			"k_destr_ask": func() auth.KeyPolicy {
				p := auth.DefaultPolicy()
				p.Destructive = "ask"
				return p
			}(),
			"k_rate": func() auth.KeyPolicy {
				p := auth.DefaultPolicy()
				p.MaxSendsPerHour = 3
				return p
			}(),
		}

		enf := New(policies, contacts)

		tests := []struct {
			name    string
			origin  outbox.Origin
			spec    outbox.Spec
			wantErr error
		}{
			{
				name:    "system_origin_bebas_policy",
				origin:  outbox.Origin{Kind: "system"},
				spec:    outbox.Spec{Type: "revoke", To: "628555000111"},
				wantErr: nil,
			},
			{
				name:    "allowed_chats_cocok_digit",
				origin:  outbox.Origin{Kind: "api", KeyID: "k_chat"},
				spec:    outbox.Spec{Type: "text", To: "628111222333", Text: "ok"},
				wantErr: nil,
			},
			{
				name:    "allowed_chats_cocok_grup",
				origin:  outbox.Origin{Kind: "api", KeyID: "k_chat"},
				spec:    outbox.Spec{Type: "text", To: "120363000111@g.us", Text: "ok"},
				wantErr: nil,
			},
			{
				name:    "allowed_chats_ditolak",
				origin:  outbox.Origin{Kind: "api", KeyID: "k_chat"},
				spec:    outbox.Spec{Type: "text", To: "628555000111", Text: "no"},
				wantErr: ErrChatNotAllowed,
			},
			{
				name:    "reply_window_dalam_jendela",
				origin:  outbox.Origin{Kind: "mcp", KeyID: "k_window"},
				spec:    outbox.Spec{Type: "text", To: "628111222333", Text: "ok"},
				wantErr: nil,
			},
			{
				name:    "reply_window_lewat_batas",
				origin:  outbox.Origin{Kind: "mcp", KeyID: "k_window"},
				spec:    outbox.Spec{Type: "text", To: "628199988877", Text: "telat"},
				wantErr: ErrOutsideReplyWindow,
			},
			{
				name:    "reply_window_kontak_asing",
				origin:  outbox.Origin{Kind: "mcp", KeyID: "k_window"},
				spec:    outbox.Spec{Type: "text", To: "628999000111", Text: "asing"},
				wantErr: ErrOutsideReplyWindow,
			},
			{
				name:    "first_contact_deny",
				origin:  outbox.Origin{Kind: "api", KeyID: "k_fc_deny"},
				spec:    outbox.Spec{Type: "text", To: "628999000111", Text: "halo"},
				wantErr: ErrFirstContactDenied,
			},
			{
				name:    "first_contact_sudah_pernah_masuk",
				origin:  outbox.Origin{Kind: "api", KeyID: "k_fc_deny"},
				spec:    outbox.Spec{Type: "text", To: "628111222333", Text: "balas"},
				wantErr: nil,
			},
			{
				name:    "first_contact_ask",
				origin:  outbox.Origin{Kind: "mcp", KeyID: "k_fc_ask"},
				spec:    outbox.Spec{Type: "text", To: "628999000111", Text: "tanya"},
				wantErr: ErrFirstContactNeedsApproval,
			},
			{
				name:    "destructive_revoke_deny",
				origin:  outbox.Origin{Kind: "api", KeyID: "k_destr_deny"},
				spec:    outbox.Spec{Type: "revoke", To: "628111222333"},
				wantErr: ErrDestructiveDenied,
			},
			{
				name:    "destructive_revoke_ask",
				origin:  outbox.Origin{Kind: "mcp", KeyID: "k_destr_ask"},
				spec:    outbox.Spec{Type: "revoke", To: "628111222333"},
				wantErr: ErrDestructiveNeedsApproval,
			},
		}

		for _, tc := range tests {
			err := enf.CheckSend(ctx, "sales", tc.origin, tc.spec)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("kasus %s: diharapkan error=%v, didapat=%v", tc.name, tc.wantErr, err)
			}
		}

		// Uji CheckDestructive untuk mutasi grup remove dan leave
		if err := enf.CheckDestructive(outbox.Origin{KeyID: "k_destr_deny"}, "remove"); !errors.Is(err, ErrDestructiveDenied) {
			t.Fatalf("CheckDestructive(remove) seharusnya ErrDestructiveDenied, didapat %v", err)
		}
		if err := enf.CheckDestructive(outbox.Origin{KeyID: "k_destr_ask"}, "leave"); !errors.Is(err, ErrDestructiveNeedsApproval) {
			t.Fatalf("CheckDestructive(leave) seharusnya ErrDestructiveNeedsApproval, didapat %v", err)
		}
		if err := enf.CheckDestructive(outbox.Origin{KeyID: "k_destr_deny"}, "add"); err != nil {
			t.Fatalf("CheckDestructive(add) tidak boleh ditolak, didapat %v", err)
		}

		// Uji jendela geser 60 menit max_sends_per_hour di dalam synctest
		for i := range 3 {
			if err := enf.CheckSend(ctx, "sales", outbox.Origin{KeyID: "k_rate"}, outbox.Spec{Type: "text", To: "628111222333", Text: "hi"}); err != nil {
				t.Fatalf("kirim ke-%d di bawah limit seharusnya lolos, didapat %v", i+1, err)
			}
		}
		if err := enf.CheckSend(ctx, "sales", outbox.Origin{KeyID: "k_rate"}, outbox.Spec{Type: "text", To: "628111222333", Text: "over"}); !errors.Is(err, ErrPolicyRate) {
			t.Fatalf("kirim ke-4 seharusnya ditolak ErrPolicyRate, didapat %v", err)
		}

		// Geser jam virtual melewati 60 menit -> bucket lama habis, boleh kirim lagi
		time.Sleep(61 * time.Minute)
		synctest.Wait()
		if err := enf.CheckSend(ctx, "sales", outbox.Origin{KeyID: "k_rate"}, outbox.Spec{Type: "text", To: "628111222333", Text: "ok-lagi"}); err != nil {
			t.Fatalf("kirim setelah 61 menit seharusnya diizinkan kembali, didapat %v", err)
		}

		// Origin.Policy memprioritaskan policy bawaan Origin (override MRTR & operator /control/mcp tanpa KeyID)
		askOnly := auth.KeyPolicy{FirstContact: "ask", Destructive: "ask"}
		if err := enf.CheckSend(ctx, "sales", outbox.Origin{Kind: "mcp", KeyID: "", Policy: &askOnly}, outbox.Spec{Type: "text", To: "628999888777", Text: "baru"}); !errors.Is(err, ErrFirstContactNeedsApproval) {
			t.Fatalf("Origin tanpa KeyID dengan Policy wajib menuntut persetujuan first_contact: %v", err)
		}
		allowOnce := auth.KeyPolicy{FirstContact: "allow", Destructive: "allow"}
		if err := enf.CheckSend(ctx, "sales", outbox.Origin{Kind: "mcp", KeyID: "k_fc_ask", Policy: &allowOnce}, outbox.Spec{Type: "text", To: "628999888777", Text: "disetujui"}); err != nil {
			t.Fatalf("override Origin.Policy FirstContact=allow gagal: %v", err)
		}
		if err := enf.CheckDestructive(outbox.Origin{Kind: "mcp", KeyID: "", Policy: &askOnly}, "remove"); !errors.Is(err, ErrDestructiveNeedsApproval) {
			t.Fatalf("CheckDestructive dengan Origin.Policy wajib menuntut persetujuan: %v", err)
		}
	})
}
