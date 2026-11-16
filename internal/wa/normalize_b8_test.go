package wa

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// TestNormalize_GroupInfo memverifikasi normalisasi events.GroupInfo multi-perubahan, JoinedGroup, CallOffer, dan CallTerminate.
func TestNormalize_GroupInfo(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1794360200, 0)
	ts := time.Unix(1794360190, 0)
	grp := types.NewJID("12036301122334455", types.GroupServer)
	actorLID := types.NewJID("9988776655", types.HiddenUserServer)
	actorPN := types.NewJID("628111222333", types.DefaultUserServer)
	member1 := types.NewJID("628123450001", types.DefaultUserServer)
	member2 := types.NewJID("628123450002", types.DefaultUserServer)
	lidMap := mapLIDResolver{
		actorLID.String(): actorPN,
	}

	t.Run("multi-change-group-info", func(t *testing.T) {
		invite := "https://chat.whatsapp.com/ABC123"
		raw := &events.GroupInfo{
			JID:                    grp,
			Sender:                 &actorLID,
			SenderPN:               &actorPN,
			Timestamp:              ts,
			JoinReason:             "invite",
			Join:                   []types.JID{member1},
			Leave:                  []types.JID{member2},
			Promote:                []types.JID{member1},
			Demote:                 []types.JID{member2},
			Name:                   &types.GroupName{Name: "Grup Uji B8"},
			Topic:                  &types.GroupTopic{Topic: "Deskripsi baru"},
			Announce:               &types.GroupAnnounce{IsAnnounce: true},
			Locked:                 &types.GroupLocked{IsLocked: true},
			Ephemeral:              &types.GroupEphemeral{IsEphemeral: true, DisappearingTimer: 86400},
			NewInviteLink:          &invite,
			MembershipApprovalMode: &types.GroupMembershipApprovalMode{IsJoinApprovalRequired: true},
			Delete:                 &types.GroupDelete{Deleted: true, DeleteReason: "closed"},
		}
		ev, ok := NormalizeEvent(ctx, "sales", raw, NormalizeContext{Now: now, LID: lidMap})
		if !ok || ev == nil || ev.Type != "group" {
			t.Fatalf("NormalizeEvent GroupInfo gagal: ok=%v ev=%+v", ok, ev)
		}
		if ev.Chat != grp.String() || ev.Sender != actorLID.String() || ev.TS != ts.Unix() || ev.DedupeKey != "" {
			t.Fatalf("metadata journal GroupInfo tidak sesuai: %+v", ev)
		}

		var got GroupEventV2
		if err := json.Unmarshal(ev.Payload, &got); err != nil {
			t.Fatalf("decode GroupEventV2 gagal: %v", err)
		}
		if got.Group != grp.String() || got.Actor != actorLID.String() || got.ActorPN != actorPN.String() || got.Timestamp != ts.Unix() || got.JoinReason != "invite" {
			t.Fatalf("header payload GroupEventV2 tidak sesuai: %+v", got)
		}
		if len(got.Changes) != 12 {
			t.Fatalf("diharapkan 12 item changes dalam satu event, didapat %d (%+v)", len(got.Changes), got.Changes)
		}
		byAction := make(map[string]GroupChangeV2, len(got.Changes))
		for _, ch := range got.Changes {
			byAction[ch.Action] = ch
		}
		if byAction["join"].Participants[0] != member1.String() ||
			byAction["leave"].Participants[0] != member2.String() ||
			byAction["promote"].Participants[0] != member1.String() ||
			byAction["demote"].Participants[0] != member2.String() ||
			byAction["name"].Value != "Grup Uji B8" ||
			byAction["description"].Value != "Deskripsi baru" ||
			byAction["announce"].Value != "true" ||
			byAction["locked"].Value != "true" ||
			byAction["ephemeral"].Value != "86400" ||
			byAction["invite_link"].Value != invite ||
			byAction["membership_approval"].Value != "true" ||
			byAction["delete"].Value != "closed" {
			t.Fatalf("isi changes tidak sesuai kontrak: %+v", byAction)
		}
	})

	t.Run("joined-group", func(t *testing.T) {
		raw := &events.JoinedGroup{
			Sender:   &actorLID,
			SenderPN: &actorPN,
			GroupInfo: types.GroupInfo{
				JID:       grp,
				GroupName: types.GroupName{Name: "Komunitas Baru"},
			},
		}
		ev, ok := NormalizeEvent(ctx, "sales", raw, NormalizeContext{Now: now, LID: lidMap})
		if !ok || ev == nil || ev.Type != "group" {
			t.Fatalf("NormalizeEvent JoinedGroup gagal: ok=%v ev=%+v", ok, ev)
		}
		var got GroupEventV2
		if err := json.Unmarshal(ev.Payload, &got); err != nil {
			t.Fatalf("decode JoinedGroup gagal: %v", err)
		}
		if got.Group != grp.String() || got.Name != "Komunitas Baru" || len(got.Changes) != 1 || got.Changes[0].Action != "joined" {
			t.Fatalf("payload JoinedGroup tidak sesuai: %+v", got)
		}
	})

	t.Run("call-offer-dan-terminate", func(t *testing.T) {
		offer := &events.CallOffer{
			BasicCallMeta: types.BasicCallMeta{
				From:           actorLID,
				CallCreator:    actorLID,
				CallCreatorAlt: actorPN,
				CallID:         "CALL-001",
				Timestamp:      ts,
			},
		}
		evOffer, ok := NormalizeEvent(ctx, "sales", offer, NormalizeContext{Now: now, LID: lidMap})
		if !ok || evOffer == nil || evOffer.Type != "call" || evOffer.DedupeKey != "c:CALL-001:offer" {
			t.Fatalf("normalisasi CallOffer gagal: ok=%v ev=%+v", ok, evOffer)
		}
		var gotOffer CallEventV2
		if err := json.Unmarshal(evOffer.Payload, &gotOffer); err != nil {
			t.Fatalf("decode CallEventV2 gagal: %v", err)
		}
		if gotOffer.CallID != "CALL-001" || gotOffer.From != actorLID.String() || gotOffer.FromPN != actorPN.String() || gotOffer.Action != "offer" {
			t.Fatalf("payload CallOffer tidak sesuai: %+v", gotOffer)
		}

		term := &events.CallTerminate{
			BasicCallMeta: types.BasicCallMeta{
				From:      actorPN,
				CallID:    "CALL-001",
				Timestamp: ts,
			},
			Reason: "timeout",
		}
		evTerm, ok := NormalizeEvent(ctx, "sales", term, NormalizeContext{Now: now, LID: lidMap})
		if !ok || evTerm == nil || evTerm.Type != "call" || evTerm.DedupeKey != "c:CALL-001:terminate" {
			t.Fatalf("normalisasi CallTerminate gagal: ok=%v ev=%+v", ok, evTerm)
		}
		var gotTerm CallEventV2
		if err := json.Unmarshal(evTerm.Payload, &gotTerm); err != nil {
			t.Fatalf("decode CallTerminate gagal: %v", err)
		}
		if gotTerm.Action != "terminate" || gotTerm.Reason != "timeout" || gotTerm.FromPN != actorPN.String() {
			t.Fatalf("payload CallTerminate tidak sesuai: %+v", gotTerm)
		}
	})
}
