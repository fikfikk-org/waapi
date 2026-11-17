package hooks

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/wa"
)

// EventView menyediakan proyeksi datar seluruh field event yang dievaluasi rule hooks.
type EventView struct {
	Type         string
	Chat         string
	ChatType     string
	Sender       string
	SenderPN     string
	FromMe       bool
	PushName     string
	MsgType      string
	Text         string
	MentionsMe   bool
	IsReply      bool
	QuotedFromMe bool
	AgeSec       float64
	Offline      bool
	MediaKind    string
	MediaSize    float64
	HasMediaSize bool
	GroupAction  string
	CallAction   string
	PollName     string

	WAID       string
	ReplySender string
}

// EventViewFromJournal mengekstrak EventView dari satu baris *journal.Event beserta payload JSON-nya.
func EventViewFromJournal(ev journal.Event, selfJID string, now time.Time) (EventView, error) {
	if now.IsZero() {
		now = time.Now()
	}
	age := 0.0
	if ev.ReceivedAt > 0 {
		diff := now.Sub(time.UnixMilli(ev.ReceivedAt)).Seconds()
		if diff > 0 {
			age = diff
		}
	}

	v := EventView{
		Type:    ev.Type,
		Chat:    ev.Chat,
		Sender:  ev.Sender,
		AgeSec:  age,
		Offline: (ev.Flags & journal.FlagOffline) != 0,
		WAID:    ev.WAID,
	}

	if len(ev.Payload) == 0 {
		return v, nil
	}

	switch ev.Type {
	case "message":
		var m wa.MessageV2
		if err := json.Unmarshal(ev.Payload, &m); err != nil {
			return EventView{}, err
		}
		if v.Chat == "" {
			v.Chat = m.Chat
		}
		if v.Sender == "" {
			v.Sender = m.From
		}
		if v.WAID == "" {
			v.WAID = m.ID
		}
		v.ReplySender = m.From
		v.ChatType = m.ChatType
		v.SenderPN = m.SenderPN
		v.FromMe = m.IsFromMe
		v.PushName = m.PushName
		v.MsgType = m.Type
		v.Text = m.Text
		v.Offline = v.Offline || m.Flags.Offline
		if m.Quoted != nil {
			v.IsReply = true
			if sameJIDUserOrExact(m.Quoted.Participant, selfJID) {
				v.QuotedFromMe = true
			}
		}
		for _, mj := range m.Mentions {
			if sameJIDUserOrExact(mj, selfJID) {
				v.MentionsMe = true
				break
			}
		}
		if m.Media != nil {
			if m.Media.MediaType == "audio" && m.Media.PTT {
				v.MediaKind = "ptt"
			} else {
				v.MediaKind = m.Media.MediaType
			}
			v.MediaSize = float64(m.Media.FileLength)
			v.HasMediaSize = true
		}
		if m.Poll != nil {
			v.PollName = m.Poll.Name
		}

	case "group":
		var g wa.GroupEventV2
		if err := json.Unmarshal(ev.Payload, &g); err != nil {
			return EventView{}, err
		}
		if v.Chat == "" {
			v.Chat = g.Group
		}
		v.ChatType = "group"
		if v.Sender == "" {
			v.Sender = g.Actor
		}
		v.SenderPN = g.ActorPN
		if len(g.Changes) > 0 {
			v.GroupAction = g.Changes[0].Action
		}

	case "call":
		var c wa.CallEventV2
		if err := json.Unmarshal(ev.Payload, &c); err != nil {
			return EventView{}, err
		}
		if v.Chat == "" {
			v.Chat = c.From
		}
		if c.Group != "" {
			v.ChatType = "group"
		} else {
			v.ChatType = "dm"
		}
		if v.Sender == "" {
			v.Sender = c.From
		}
		v.SenderPN = c.FromPN
		v.CallAction = c.Action

	case "poll_vote":
		var pv struct {
			Chat    string `json:"chat"`
			Voter   string `json:"voter"`
			VoterPN string `json:"voter_pn"`
		}
		if err := json.Unmarshal(ev.Payload, &pv); err == nil {
			if v.Chat == "" {
				v.Chat = pv.Chat
			}
			if v.Sender == "" {
				v.Sender = pv.Voter
			}
			v.SenderPN = pv.VoterPN
		}
	}

	return v, nil
}

// EventViewFromEnvelope mengubah payload JSON envelope webhook v2 menjadi EventView untuk dry-run CLI/REST.
func EventViewFromEnvelope(rawEnvelope []byte, instanceID, selfJID string, now time.Time) (journal.Event, EventView, error) {
	var env struct {
		ID        string          `json:"id"`
		Event     string          `json:"event"`
		Instance  string          `json:"instance"`
		Timestamp int64           `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rawEnvelope, &env); err != nil {
		return journal.Event{}, EventView{}, err
	}
	evType := strings.TrimSpace(env.Event)
	if evType == "" {
		evType = "message"
	}
	inst := env.Instance
	if inst == "" {
		inst = instanceID
	}
	recAt := now.UnixMilli()
	if env.Timestamp > 0 {
		recAt = env.Timestamp * 1000
	}
	ev := journal.Event{
		InstanceID: inst,
		Type:       evType,
		TS:         now.Unix(),
		ReceivedAt: recAt,
		Payload:    env.Data,
	}
	view, err := EventViewFromJournal(ev, selfJID, now)
	if err != nil {
		return journal.Event{}, EventView{}, err
	}
	ev.Chat = view.Chat
	ev.Sender = view.Sender
	ev.WAID = view.WAID
	return ev, view, nil
}

func sameJIDUserOrExact(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	ua, _, _ := strings.Cut(a, "@")
	ub, _, _ := strings.Cut(b, "@")
	ua, _, _ = strings.Cut(ua, ":")
	ub, _, _ = strings.Cut(ub, ":")
	return ua != "" && ua == ub
}

// Match mengevaluasi AST kondisi rule terkompilasi terhadap EventView.
func (c *Compiled) Match(v EventView) bool {
	if c == nil {
		return false
	}
	if !c.HasWhen {
		return true
	}
	return evalNode(&c.When, v)
}

// MatchesEventType memeriksa apakah rule berlangganan pada tipe event evType.
func (c *Compiled) MatchesEventType(evType string) bool {
	if c == nil {
		return false
	}
	_, ok := c.OnTypes[evType]
	return ok
}

func evalNode(n *compiledCond, v EventView) bool {
	switch n.kind {
	case condAll:
		for i := range n.children {
			if !evalNode(&n.children[i], v) {
				return false
			}
		}
		return true
	case condAny:
		for i := range n.children {
			if evalNode(&n.children[i], v) {
				return true
			}
		}
		return false
	case condNot:
		if len(n.children) == 0 {
			return false
		}
		return !evalNode(&n.children[0], v)
	case condLeaf:
		return evalLeaf(n, v)
	default:
		return false
	}
}

type valKind uint8

const (
	valUnset valKind = iota
	valStr
	valNum
	valBool
)

func readField(fk fieldKind, v EventView) (kind valKind, s string, f float64, b bool) {
	switch fk {
	case fieldType:
		return strOrUnset(v.Type)
	case fieldChat:
		return strOrUnset(v.Chat)
	case fieldChatType:
		return strOrUnset(v.ChatType)
	case fieldSender:
		return strOrUnset(v.Sender)
	case fieldSenderPN:
		return strOrUnset(v.SenderPN)
	case fieldFromMe:
		return valBool, "", 0, v.FromMe
	case fieldPushName:
		return strOrUnset(v.PushName)
	case fieldMsgType:
		return strOrUnset(v.MsgType)
	case fieldText:
		return strOrUnset(v.Text)
	case fieldMentionsMe:
		return valBool, "", 0, v.MentionsMe
	case fieldIsReply:
		return valBool, "", 0, v.IsReply
	case fieldQuotedFromMe:
		return valBool, "", 0, v.QuotedFromMe
	case fieldAgeSec:
		return valNum, "", v.AgeSec, false
	case fieldOffline:
		return valBool, "", 0, v.Offline
	case fieldMediaKind:
		return strOrUnset(v.MediaKind)
	case fieldMediaSize:
		if !v.HasMediaSize {
			return valUnset, "", 0, false
		}
		return valNum, "", v.MediaSize, false
	case fieldGroupAction:
		return strOrUnset(v.GroupAction)
	case fieldCallAction:
		return strOrUnset(v.CallAction)
	case fieldPollName:
		return strOrUnset(v.PollName)
	default:
		return valUnset, "", 0, false
	}
}

func strOrUnset(s string) (valKind, string, float64, bool) {
	if s == "" {
		return valUnset, "", 0, false
	}
	return valStr, s, 0, false
}

func evalLeaf(n *compiledCond, v EventView) bool {
	fk, strVal, numVal, boolVal := readField(n.field, v)

	switch n.op {
	case opExists:
		exists := fk != valUnset
		return exists == n.wantSet

	case opEq, opNe:
		var eq bool
		switch fk {
		case valStr:
			eq = !n.hasBool && !n.hasNum && strVal == n.strVal
		case valBool:
			eq = n.hasBool && boolVal == n.boolVal
		case valNum:
			eq = n.hasNum && numVal == n.numVal
		case valUnset:
			// Untuk eq/ne string kosong terhadap field string kosong dianggap match bila n.strVal == ""
			eq = !n.hasBool && !n.hasNum && n.strVal == ""
		}
		if n.op == opNe {
			return !eq
		}
		return eq

	case opIn, opNin:
		matched := false
		switch fk {
		case valStr:
			_, matched = n.inStr[strVal]
		case valNum:
			for _, item := range n.inNum {
				if item == numVal {
					matched = true
					break
				}
			}
		case valBool:
			for _, item := range n.inBool {
				if item == boolVal {
					matched = true
					break
				}
			}
		}
		if n.op == opNin {
			return !matched
		}
		return matched

	case opContains:
		return fk == valStr && strings.Contains(strVal, n.strVal)
	case opIContains:
		return fk == valStr && containsCaseFold(strVal, n.strVal)
	case opPrefix:
		return fk == valStr && strings.HasPrefix(strVal, n.strVal)
	case opSuffix:
		return fk == valStr && strings.HasSuffix(strVal, n.strVal)
	case opMatches:
		return fk == valStr && n.re != nil && n.re.MatchString(strVal)
	case opGt:
		return fk == valNum && numVal > n.numVal
	case opGte:
		return fk == valNum && numVal >= n.numVal
	case opLt:
		return fk == valNum && numVal < n.numVal
	case opLte:
		return fk == valNum && numVal <= n.numVal
	default:
		return false
	}
}

// containsCaseFold mencari needle (sudah huruf kecil saat kompilasi) tanpa alokasi untuk input ASCII.
func containsCaseFold(haystack, lowerNeedle string) bool {
	if lowerNeedle == "" {
		return true
	}
	if len(lowerNeedle) > len(haystack) {
		return false
	}
	for i := 0; i < len(haystack); i++ {
		if haystack[i] >= 0x80 {
			return strings.Contains(strings.ToLower(haystack), lowerNeedle)
		}
	}
	for i := 0; i < len(lowerNeedle); i++ {
		if lowerNeedle[i] >= 0x80 {
			return strings.Contains(strings.ToLower(haystack), lowerNeedle)
		}
	}
	maxStart := len(haystack) - len(lowerNeedle)
	for i := 0; i <= maxStart; i++ {
		match := true
		for j := 0; j < len(lowerNeedle); j++ {
			b := haystack[i+j]
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if b != lowerNeedle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
