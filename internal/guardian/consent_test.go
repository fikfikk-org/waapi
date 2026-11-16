package guardian

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow/types"
)

// TestContentFanout_Normalisasi memverifikasi normalisasi teks fan-out mempertahankan angka serta mengecualikan non-teks/caption kosong.
func TestContentFanout_Normalisasi(t *testing.T) {
	if got := NormalizeContent(outbox.Spec{Type: "text", Text: "  Halo   Budi\t\n"}); got != "halo budi" {
		t.Fatalf("normalisasi spasi/kapital tidak sesuai: %q", got)
	}
	c1 := NormalizeContent(outbox.Spec{Type: "text", Text: "Kode 1234"})
	c2 := NormalizeContent(outbox.Spec{Type: "text", Text: "Kode 5678"})
	if c1 == "" || c1 == c2 {
		t.Fatalf("angka harus dipertahankan pada normalisasi fan-out: c1=%q c2=%q", c1, c2)
	}
	// Media tanpa caption, stiker, lokasi, kontak, reaksi, edit, dan revoke dikecualikan (string kosong).
	for _, sp := range []outbox.Spec{
		{Type: "image", Media: &outbox.MediaRef{UploadID: "u1"}},
		{Type: "sticker", Media: &outbox.MediaRef{UploadID: "u2"}},
		{Type: "location", Location: &outbox.Location{Lat: -6.2, Lng: 106.8}},
		{Type: "contact", Contacts: []outbox.Contact{{Name: "Ani"}}},
		{Type: "reaction", Reaction: &outbox.Reaction{MessageID: "M1", Emoji: "👍"}},
	} {
		if got := NormalizeContent(sp); got != "" {
			t.Fatalf("tipe non-teks %q tanpa caption harus menghasilkan string kosong, didapat %q", sp.Type, got)
		}
	}
	if got := NormalizeContent(outbox.Spec{Type: "image", Media: &outbox.MediaRef{Caption: " Promo  Hari Ini "}}); got != "promo hari ini" {
		t.Fatalf("caption media tidak dinormalisasi benar: %q", got)
	}
}

// TestConsent_OptOutLaluOptIn memverifikasi kata kunci opt-out, penolakan 409, bypass konfirmasi system, opt-in kembali, dan event consent di journal.
func TestConsent_OptOutLaluOptIn(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"))
	if err != nil {
		t.Fatalf("buka journal gagal: %v", err)
	}
	defer func() { _ = j.Close() }()

	enq := &fakeOutboxEnqueuer{}
	meta := &fakeMetaSource{
		instances: map[string]store.Instance{
			"sales": {
				ID:       "sales",
				JID:      "628111@s.whatsapp.net",
				PairedAt: time.Now().Add(-30 * 24 * time.Hour).Unix(),
				Settings: func() store.InstanceSettings {
					s := store.Defaults()
					s.OptoutConfirmText = "Anda telah berhenti berlangganan."
					return s
				}(),
			},
		},
	}

	g, err := New(j.DB(), Options{Settings: meta, Enqueuer: enq})
	if err != nil {
		t.Fatalf("New Guardian gagal: %v", err)
	}

	rcpt := "6281234567890@s.whatsapp.net"
	senderJID := types.NewJID("6281234567890", types.DefaultUserServer)

	// Pesan masuk "stop" -> memicu opt_out + pesan konfirmasi system + event consent di journal.
	at1 := time.Now().UnixMilli()
	evIn := &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       rcpt,
		Sender:     rcpt,
		WAID:       "W1",
		DedupeKey:  "m:opt1",
		TS:         at1 / 1000,
		ReceivedAt: at1,
		Payload:    mustJSON(t, map[string]any{"id": "W1", "from": rcpt, "sender_pn": rcpt, "type": "text", "text": "  stop  "}),
	}
	if _, err := j.Append(ctx, evIn, g.InboundHook(ctx, evIn, nil)); err != nil {
		t.Fatalf("Append pesan stop gagal: %v", err)
	}

	// Pesan biasa ke penerima opted-out harus ditolak Precheck & Decide.
	jobNormal := &outbox.Job{InstanceID: "sales", ToJID: rcpt, Kind: "text", Origin: "api", Spec: outbox.Spec{Type: "text", To: rcpt, Text: "Promo"}}
	if err := g.Precheck(ctx, jobNormal); !errors.Is(err, ErrRecipientOptedOut) {
		t.Fatalf("Precheck harus mengembalikan ErrRecipientOptedOut, didapat %v", err)
	}
	if dec := g.Decide(ctx, jobNormal, time.Now()); dec.Allow || !dec.Permanent || dec.Code != "recipient_opted_out" {
		t.Fatalf("Decide harus menolak permanen recipient_opted_out, didapat %+v", dec)
	}

	// Pesan konfirmasi system harus tetap diizinkan dan tercatat di fake enqueuer.
	if len(enq.items) != 1 || enq.items[0].text != "Anda telah berhenti berlangganan." || enq.items[0].to != rcpt {
		t.Fatalf("pesan konfirmasi opt-out tidak sesuai: %+v", enq.items)
	}
	jobSystem := &outbox.Job{InstanceID: "sales", ToJID: rcpt, Kind: "text", Origin: "system", Spec: outbox.Spec{Type: "text", To: rcpt, Text: "Anda telah berhenti berlangganan."}}
	if err := g.Precheck(ctx, jobSystem); err != nil {
		t.Fatalf("Precheck konfirmasi system seharusnya diizinkan, didapat %v", err)
	}
	if dec := g.Decide(ctx, jobSystem, time.Now()); !dec.Allow {
		t.Fatalf("Decide konfirmasi system harus diizinkan meski penerima opted_out, didapat %+v", dec)
	}

	// Pesan masuk "START" -> mengembalikan opted_out=0 + event consent opt_in.
	at2 := at1 + 2000
	evStart := &journal.Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       senderJID.String(),
		Sender:     senderJID.String(),
		WAID:       "W2",
		DedupeKey:  "m:opt2",
		TS:         at2 / 1000,
		ReceivedAt: at2,
		Payload:    mustJSON(t, map[string]any{"id": "W2", "from": rcpt, "sender_pn": rcpt, "type": "text", "text": "START"}),
	}
	if _, err := j.Append(ctx, evStart, g.InboundHook(ctx, evStart, nil)); err != nil {
		t.Fatalf("Append pesan START gagal: %v", err)
	}

	if err := g.Precheck(ctx, jobNormal); err != nil {
		t.Fatalf("Precheck setelah START seharusnya diizinkan, didapat %v", err)
	}
	if dec := g.Decide(ctx, jobNormal, time.Now()); !dec.Allow {
		t.Fatalf("Decide setelah START seharusnya diizinkan, didapat %+v", dec)
	}

	// Pastikan kedua event consent tersimpan di journal.
	res, err := j.Read(ctx, journal.Query{InstanceID: "sales", Types: []string{"consent"}, Limit: 10})
	if err != nil || len(res.Events) != 2 {
		t.Fatalf("diharapkan 2 event consent di journal, didapat %d err=%v", len(res.Events), err)
	}
	var cOut, cIn ConsentEvent
	_ = json.Unmarshal(res.Events[0].Payload, &cOut)
	_ = json.Unmarshal(res.Events[1].Payload, &cIn)
	if cOut.Action != "opt_out" || cOut.Keyword != "STOP" || cIn.Action != "opt_in" || cIn.Keyword != "START" {
		t.Fatalf("payload consent tidak sesuai: out=%+v in=%+v", cOut, cIn)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
