package wa

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// TestNormalize_Presence memastikan events.Presence dan events.ChatPresence menghasilkan event journal dengan payload persis kontrak v2.
func TestNormalize_Presence(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1700001000, 0)
	lidMap := mapLIDResolver{
		"111222333@lid": types.NewJID("628123456789", types.DefaultUserServer),
	}

	t.Run("presence-with-lid-and-last-seen", func(t *testing.T) {
		lastSeen := time.Unix(1700000900, 0)
		ev, ok := NormalizeEvent(ctx, "sales", &events.Presence{
			From:        types.NewJID("111222333", types.HiddenUserServer),
			Unavailable: true,
			LastSeen:    lastSeen,
		}, NormalizeContext{Now: now, LID: lidMap})
		if !ok || ev == nil || ev.Type != "presence" {
			t.Fatalf("NormalizeEvent presence gagal: ok=%v ev=%+v", ok, ev)
		}
		if ev.DedupeKey != "" {
			t.Fatalf("event presence harus tanpa dedupe_key, didapat %q", ev.DedupeKey)
		}

		var got PresenceV2
		if err := json.Unmarshal(ev.Payload, &got); err != nil {
			t.Fatalf("decode payload presence gagal: %v", err)
		}
		if got.From != "111222333@lid" || got.FromPN != "628123456789@s.whatsapp.net" || !got.Unavailable || got.LastSeen != 1700000900 {
			t.Fatalf("payload presence tidak sesuai: %+v", got)
		}
	})

	t.Run("presence-zero-last-seen", func(t *testing.T) {
		ev, ok := NormalizeEvent(ctx, "sales", &events.Presence{
			From:        types.NewJID("628555666777", types.DefaultUserServer),
			Unavailable: false,
		}, NormalizeContext{Now: now})
		if !ok || ev == nil {
			t.Fatalf("NormalizeEvent presence online gagal")
		}
		var got PresenceV2
		_ = json.Unmarshal(ev.Payload, &got)
		if got.From != "628555666777@s.whatsapp.net" || got.Unavailable || got.LastSeen != 0 {
			t.Fatalf("payload presence online tidak sesuai: %+v", got)
		}
	})

	t.Run("chat-presence-recording", func(t *testing.T) {
		ev, ok := NormalizeEvent(ctx, "sales", &events.ChatPresence{
			MessageSource: types.MessageSource{
				Chat:   types.NewJID("120363012345", types.GroupServer),
				Sender: types.NewJID("628123456789", types.DefaultUserServer),
			},
			State: types.ChatPresenceComposing,
			Media: types.ChatPresenceMediaAudio,
		}, NormalizeContext{Now: now})
		if !ok || ev == nil || ev.Type != "chat_presence" {
			t.Fatalf("NormalizeEvent chat_presence gagal: ok=%v ev=%+v", ok, ev)
		}
		if ev.DedupeKey != "" {
			t.Fatalf("event chat_presence harus tanpa dedupe_key, didapat %q", ev.DedupeKey)
		}
		var got ChatPresenceV2
		if err := json.Unmarshal(ev.Payload, &got); err != nil {
			t.Fatalf("decode payload chat_presence gagal: %v", err)
		}
		if got.Chat != "120363012345@g.us" || got.Sender != "628123456789@s.whatsapp.net" || got.State != "composing" || got.Media != "audio" {
			t.Fatalf("payload chat_presence tidak sesuai: %+v", got)
		}
	})
}
