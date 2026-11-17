package hooks

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/fikfikk-org/waapi/internal/ai"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
)

// matchesHandoffKeyword memeriksa apakah teks pesan mengandung kata kunci handoff secara case-insensitive pada batas kata.
func matchesHandoffKeyword(text string, keywords []string) bool {
	if strings.TrimSpace(text) == "" || len(keywords) == 0 {
		return false
	}
	lowerText := strings.ToLower(text)
	for _, rawKW := range keywords {
		kw := strings.ToLower(strings.TrimSpace(rawKW))
		if kw == "" {
			continue
		}
		if containsWordBoundary(lowerText, kw) {
			return true
		}
	}
	return false
}

func containsWordBoundary(lowerText, lowerKW string) bool {
	start := 0
	for {
		idx := strings.Index(lowerText[start:], lowerKW)
		if idx < 0 {
			return false
		}
		pos := start + idx
		endPos := pos + len(lowerKW)

		leftOK := pos == 0
		if !leftOK {
			prevRune, _ := lastRuneBefore(lowerText, pos)
			leftOK = !isWordChar(prevRune)
		}
		rightOK := endPos >= len(lowerText)
		if !rightOK {
			nextRune, _ := firstRuneFrom(lowerText, endPos)
			rightOK = !isWordChar(nextRune)
		}
		if leftOK && rightOK {
			return true
		}
		_, step := firstRuneFrom(lowerText, pos)
		if step <= 0 {
			step = 1
		}
		start = pos + step
	}
}

func firstRuneFrom(s string, pos int) (rune, int) {
	for i, r := range s[pos:] {
		_ = i
		return r, len(string(r))
	}
	return 0, 0
}

func lastRuneBefore(s string, pos int) (rune, int) {
	var last rune
	var width int
	for _, r := range s[:pos] {
		last = r
		width = len(string(r))
	}
	return last, width
}

func isWordChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func aiIdemKey(hookID string, seq int64, aiActionOrdinal int) string {
	if aiActionOrdinal == 0 {
		return fmt.Sprintf("ai:%s:%d", hookID, seq)
	}
	return fmt.Sprintf("ai:%s:%d:%d", hookID, seq, aiActionOrdinal)
}

func (e *Engine) executeAIAction(
	ctx context.Context,
	instanceID string,
	c *Compiled,
	a Action,
	ev journal.Event,
	view EventView,
	aiActionOrdinal int,
) (usedFallback bool, err error) {
	if view.Chat == "" {
		return false, errors.New("event tidak memiliki target chat")
	}
	if e.outbox == nil {
		return false, errors.New("service outbox belum siap")
	}
	if e.ai == nil {
		return false, errors.New("client AI tidak aktif")
	}

	ctxCount := a.ContextMessages
	if ctxCount <= 0 {
		ctxCount = DefaultAIContextMessages
	}
	maxTok := a.MaxTokens
	if maxTok <= 0 {
		maxTok = DefaultAIMaxTokens
	}

	aiRule := ai.Rule{
		System:          a.System,
		ContextMessages: ctxCount,
		MaxTokens:       maxTok,
		Temperature:     a.EffectiveTemperature(),
		Model:           a.Model,
		FallbackText:    a.FallbackText,
	}

	var history []ctxstore.Message
	if e.ctxStore != nil && ctxCount > 0 {
		// Ambil ctxCount+1 pesan terbaru agar jika pesan pemicu sudah tersimpan lewat RecordHook dapat disaring tanpa mengurangi kapasitas.
		if page, mErr := e.ctxStore.Messages(ctx, instanceID, view.Chat, ctxCount+1, ""); mErr == nil {
			history = deduplicateTriggerHistory(page.Messages, view.WAID, ctxCount)
		}
	}

	senderName := firstNonEmpty(view.PushName, view.SenderPN, view.Sender)
	trigger := ai.Trigger{
		Chat:       view.Chat,
		SenderName: senderName,
		Text:       view.Text,
		IsGroup:    view.ChatType == "group",
	}

	msgs := ai.BuildMessages(aiRule, history, trigger)
	replyText, compErr := e.ai.Complete(ctx, aiRule, msgs)
	idemKey := aiIdemKey(c.Rule.ID, ev.Seq, aiActionOrdinal)
	origin := outbox.Origin{Kind: "ai", KeyID: c.Rule.ID}

	if compErr != nil {
		fb := strings.TrimSpace(a.FallbackText)
		if fb == "" {
			return false, compErr
		}
		if _, _, enqErr := e.outbox.Enqueue(ctx, outbox.EnqueueRequest{
			InstanceID: instanceID,
			IdemKey:    idemKey,
			Spec: outbox.Spec{
				Type: "text",
				To:   view.Chat,
				Text: fb,
			},
			Origin: origin,
		}); enqErr != nil {
			return true, enqErr
		}
		return true, nil
	}

	if _, _, enqErr := e.outbox.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: instanceID,
		IdemKey:    idemKey,
		Spec: outbox.Spec{
			Type: "text",
			To:   view.Chat,
			Text: replyText,
		},
		Origin: origin,
	}); enqErr != nil {
		return false, enqErr
	}
	return false, nil
}

func (e *Engine) sendAIFallbackUnderPressure(ctx context.Context, instanceID string, c *Compiled, seq int64, view EventView) {
	if e.outbox == nil || view.Chat == "" {
		return
	}
	aiOrd := 0
	for _, ca := range c.Actions {
		if ca.raw.Action != "ai_reply" {
			continue
		}
		fb := strings.TrimSpace(ca.raw.FallbackText)
		if fb != "" {
			_, _, _ = e.outbox.Enqueue(ctx, outbox.EnqueueRequest{
				InstanceID: instanceID,
				IdemKey:    aiIdemKey(c.Rule.ID, seq, aiOrd),
				Spec: outbox.Spec{
					Type: "text",
					To:   view.Chat,
					Text: fb,
				},
				Origin: outbox.Origin{Kind: "ai", KeyID: c.Rule.ID},
			})
		}
		aiOrd++
	}
}

func deduplicateTriggerHistory(items []ctxstore.Message, triggerWAID string, limit int) []ctxstore.Message {
	if len(items) == 0 {
		return nil
	}
	if triggerWAID != "" && items[len(items)-1].ID == triggerWAID {
		items = items[:len(items)-1]
	} else if triggerWAID != "" {
		filtered := make([]ctxstore.Message, 0, len(items))
		for _, it := range items {
			if it.ID == triggerWAID {
				continue
			}
			filtered = append(filtered, it)
		}
		items = filtered
	}
	if len(items) > limit && limit > 0 {
		items = items[len(items)-limit:]
	}
	return items
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
