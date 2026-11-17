package hooks

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/netguard"
)

var hookIDRegex = regexp.MustCompile(`^[a-z0-9_-]{1,40}$`)

// CompileOptions mengendalikan hak istimewa saat mengompilasi rule hook.
type CompileOptions struct {
	Trusted bool // true khusus rule dari control plane (CLI/UDS); false memaksa batas SSRF REST publik
}

type fieldKind uint8

const (
	fieldInvalid fieldKind = iota
	fieldType
	fieldChat
	fieldChatType
	fieldSender
	fieldSenderPN
	fieldFromMe
	fieldPushName
	fieldMsgType
	fieldText
	fieldMentionsMe
	fieldIsReply
	fieldQuotedFromMe
	fieldAgeSec
	fieldOffline
	fieldMediaKind
	fieldMediaSize
	fieldGroupAction
	fieldCallAction
	fieldPollName
)

var validFields = map[string]fieldKind{
	"type":           fieldType,
	"chat":           fieldChat,
	"chat_type":      fieldChatType,
	"sender":         fieldSender,
	"sender_pn":      fieldSenderPN,
	"from_me":        fieldFromMe,
	"push_name":      fieldPushName,
	"msg_type":       fieldMsgType,
	"text":           fieldText,
	"mentions_me":    fieldMentionsMe,
	"is_reply":       fieldIsReply,
	"quoted_from_me": fieldQuotedFromMe,
	"age_sec":        fieldAgeSec,
	"offline":        fieldOffline,
	"media_kind":     fieldMediaKind,
	"media_size":     fieldMediaSize,
	"group_action":   fieldGroupAction,
	"call_action":    fieldCallAction,
	"poll_name":      fieldPollName,
}

type opKind uint8

const (
	opInvalid opKind = iota
	opEq
	opNe
	opIn
	opNin
	opContains
	opIContains
	opPrefix
	opSuffix
	opMatches
	opExists
	opGt
	opGte
	opLt
	opLte
)

var validOps = map[string]opKind{
	"eq":        opEq,
	"ne":        opNe,
	"in":        opIn,
	"nin":       opNin,
	"contains":  opContains,
	"icontains": opIContains,
	"prefix":    opPrefix,
	"suffix":    opSuffix,
	"matches":   opMatches,
	"exists":    opExists,
	"gt":        opGt,
	"gte":       opGte,
	"lt":        opLt,
	"lte":       opLte,
}

var validEventTypes = map[string]struct{}{
	"message":       {},
	"receipt":       {},
	"connection":    {},
	"pairing":       {},
	"outbox":        {},
	"poll_vote":     {},
	"health":        {},
	"group":         {},
	"call":          {},
	"presence":      {},
	"chat_presence": {},
	"undecryptable": {},
	"consent":       {},
}

type condKind uint8

const (
	condAll condKind = iota + 1
	condAny
	condNot
	condLeaf
)

type compiledCond struct {
	kind     condKind
	children []compiledCond
	field    fieldKind
	op       opKind
	strVal   string
	numVal   float64
	hasNum   bool
	boolVal  bool
	hasBool  bool
	re       *regexp.Regexp
	inStr    map[string]struct{}
	inNum    []float64
	inBool   []bool
	wantSet  bool // untuk operator exists
}

type activeHoursWindow struct {
	enabled  bool
	startMin int
	endMin   int
}

type compiledThrottle struct {
	enabled  bool
	capacity int
	window   time.Duration
	spec     string
}

type compiledAction struct {
	raw       Action
	timeout   time.Duration
	toHasTmpl bool
}

// Compiled menyimpan rule hasil validasi beserta AST kondisi & regex terkompilasi agar evaluasi cepat dan nol-alokasi.
type Compiled struct {
	Rule         Rule
	Priority     int
	Enabled      bool
	RespectTake  bool
	OnTypes      map[string]struct{}
	HasWhen      bool
	When         compiledCond
	Actions      []compiledAction
	Heavy        bool
	CanSuppress  bool
	StaticSends  int
	Throttle     compiledThrottle
	ActiveWindow activeHoursWindow
}

// ParseRuleJSON membaca representasi JSON menjadi Rule dengan aman tanpa panic.
func ParseRuleJSON(data []byte) (Rule, error) {
	if len(data) == 0 {
		return Rule{}, errors.New("spesifikasi JSON rule kosong")
	}
	var r Rule
	if err := json.Unmarshal(data, &r); err != nil {
		return Rule{}, fmt.Errorf("parse JSON rule tidak valid: %w", err)
	}
	return r, nil
}

// Compile memvalidasi dan mengompilasi Rule menggunakan nilai r.Trusted apa adanya.
func Compile(r Rule) (*Compiled, error) {
	return CompileWithOptions(r, CompileOptions{Trusted: r.Trusted})
}

// CompileWithOptions memvalidasi seluruh field, operator, kedalaman kondisi, regex, serta keamanan URL dan mengembalikan *Compiled.
func CompileWithOptions(r Rule, opts CompileOptions) (*Compiled, error) {
	if !hookIDRegex.MatchString(r.ID) {
		return nil, fmt.Errorf("id: harus berupa slug [a-z0-9_-] sepanjang 1-40 karakter")
	}
	if len(r.On) == 0 {
		return nil, fmt.Errorf("on: wajib berisi minimal satu tipe event")
	}
	onSet := make(map[string]struct{}, len(r.On))
	for i, evType := range r.On {
		clean := strings.TrimSpace(evType)
		if _, ok := validEventTypes[clean]; !ok {
			return nil, fmt.Errorf("on[%d]: tipe event %q tidak dikenal", i, evType)
		}
		onSet[clean] = struct{}{}
	}

	rCopy := r
	rCopy.Trusted = opts.Trusted

	comp := &Compiled{
		Rule:        rCopy,
		Priority:    r.EffectivePriority(),
		Enabled:     r.IsEnabled(),
		RespectTake: r.RespectsTakeover(),
		OnTypes:     onSet,
	}

	if r.When != nil {
		totalConds := 0
		root, err := compileCondNode(r.When, 1, "when", &totalConds)
		if err != nil {
			return nil, err
		}
		comp.HasWhen = true
		comp.When = root
	}

	if len(r.Do) == 0 {
		return nil, fmt.Errorf("do: wajib berisi minimal 1 aksi")
	}
	if len(r.Do) > MaxActionsPerRule {
		return nil, fmt.Errorf("do: jumlah aksi %d melebihi batas maksimal %d", len(r.Do), MaxActionsPerRule)
	}

	comp.Actions = make([]compiledAction, 0, len(r.Do))
	for i, a := range r.Do {
		ca, heavy, suppresses, sends, err := compileAction(i, a, opts.Trusted)
		if err != nil {
			return nil, err
		}
		if heavy {
			comp.Heavy = true
		}
		if suppresses {
			comp.CanSuppress = true
		}
		comp.StaticSends += sends
		comp.Actions = append(comp.Actions, ca)
	}

	if r.Throttle != nil && strings.TrimSpace(r.Throttle.PerChat) != "" {
		n, win, err := parseThrottleRate(r.Throttle.PerChat)
		if err != nil {
			return nil, fmt.Errorf("throttle.per_chat: %w", err)
		}
		comp.Throttle = compiledThrottle{
			enabled:  true,
			capacity: n,
			window:   win,
			spec:     strings.TrimSpace(r.Throttle.PerChat),
		}
	}

	if strings.TrimSpace(r.ActiveHours) != "" {
		win, err := parseActiveHours(r.ActiveHours)
		if err != nil {
			return nil, fmt.Errorf("active_hours: %w", err)
		}
		comp.ActiveWindow = win
	}

	return comp, nil
}

func compileCondNode(c *Cond, depth int, path string, totalConds *int) (compiledCond, error) {
	if c == nil {
		return compiledCond{}, fmt.Errorf("%s: node kondisi tidak boleh nil", path)
	}
	if depth > MaxConditionDepth {
		return compiledCond{}, fmt.Errorf("%s: kedalaman kondisi melebihi batas maksimal %d", path, MaxConditionDepth)
	}
	*totalConds++
	if *totalConds > MaxConditionsPerRule {
		return compiledCond{}, fmt.Errorf("when: jumlah kondisi di %s melebihi batas maksimal %d", path, MaxConditionsPerRule)
	}

	hasAll := len(c.All) > 0
	hasAny := len(c.Any) > 0
	hasNot := c.Not != nil
	hasLeaf := strings.TrimSpace(c.Field) != "" || strings.TrimSpace(c.Op) != ""

	combos := 0
	if hasAll {
		combos++
	}
	if hasAny {
		combos++
	}
	if hasNot {
		combos++
	}
	if hasLeaf {
		combos++
	}
	if combos != 1 {
		return compiledCond{}, fmt.Errorf("%s: kondisi harus berisi tepat satu dari all, any, not, atau predikat field/op", path)
	}

	if hasAll {
		children := make([]compiledCond, 0, len(c.All))
		for i := range c.All {
			ch, err := compileCondNode(&c.All[i], depth+1, fmt.Sprintf("%s.all[%d]", path, i), totalConds)
			if err != nil {
				return compiledCond{}, err
			}
			children = append(children, ch)
		}
		return compiledCond{kind: condAll, children: children}, nil
	}
	if hasAny {
		children := make([]compiledCond, 0, len(c.Any))
		for i := range c.Any {
			ch, err := compileCondNode(&c.Any[i], depth+1, fmt.Sprintf("%s.any[%d]", path, i), totalConds)
			if err != nil {
				return compiledCond{}, err
			}
			children = append(children, ch)
		}
		return compiledCond{kind: condAny, children: children}, nil
	}
	if hasNot {
		ch, err := compileCondNode(c.Not, depth+1, path+".not", totalConds)
		if err != nil {
			return compiledCond{}, err
		}
		return compiledCond{kind: condNot, children: []compiledCond{ch}}, nil
	}

	fk, ok := validFields[c.Field]
	if !ok {
		return compiledCond{}, fmt.Errorf("%s.field: field %q tidak dikenal", path, c.Field)
	}
	okOp := validOps[c.Op]
	if okOp == opInvalid {
		return compiledCond{}, fmt.Errorf("%s.op: operator %q tidak dikenal", path, c.Op)
	}

	leaf := compiledCond{kind: condLeaf, field: fk, op: okOp}
	valPath := path + ".value"

	switch okOp {
	case opExists:
		if c.Value == nil {
			leaf.wantSet = true
		} else {
			b, isBool := c.Value.(bool)
			if !isBool {
				return compiledCond{}, fmt.Errorf("%s: nilai operator exists harus berupa boolean", valPath)
			}
			leaf.wantSet = b
		}
	case opMatches:
		pat, isStr := c.Value.(string)
		if !isStr || pat == "" {
			return compiledCond{}, fmt.Errorf("%s: nilai regex untuk matches wajib string non-kosong", valPath)
		}
		if len(pat) > MaxRegexLen {
			return compiledCond{}, fmt.Errorf("%s: panjang regex %d melebihi batas %d karakter", valPath, len(pat), MaxRegexLen)
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return compiledCond{}, fmt.Errorf("%s: regex tidak valid: %w", valPath, err)
		}
		leaf.re = re
	case opContains, opIContains, opPrefix, opSuffix:
		s, isStr := c.Value.(string)
		if !isStr {
			return compiledCond{}, fmt.Errorf("%s: nilai operator string wajib berupa string", valPath)
		}
		if okOp == opIContains {
			s = strings.ToLower(s)
		}
		leaf.strVal = s
	case opGt, opGte, opLt, opLte:
		num, ok := toFiniteFloat(c.Value)
		if !ok {
			return compiledCond{}, fmt.Errorf("%s: nilai operator perbandingan numerik harus berupa angka hingga", valPath)
		}
		leaf.numVal = num
		leaf.hasNum = true
	case opIn, opNin:
		items, ok := extractSlice(c.Value)
		if !ok || len(items) == 0 {
			return compiledCond{}, fmt.Errorf("%s: nilai operator %s wajib berupa array non-kosong", valPath, c.Op)
		}
		leaf.inStr = make(map[string]struct{}, len(items))
		for idx, item := range items {
			switch tv := item.(type) {
			case string:
				leaf.inStr[tv] = struct{}{}
			case bool:
				leaf.inBool = append(leaf.inBool, tv)
			default:
				if num, okNum := toFiniteFloat(tv); okNum {
					leaf.inNum = append(leaf.inNum, num)
				} else {
					return compiledCond{}, fmt.Errorf("%s[%d]: elemen array harus string, angka, atau boolean", valPath, idx)
				}
			}
		}
	case opEq, opNe:
		switch tv := c.Value.(type) {
		case string:
			leaf.strVal = tv
		case bool:
			leaf.boolVal = tv
			leaf.hasBool = true
		default:
			if num, okNum := toFiniteFloat(tv); okNum {
				leaf.numVal = num
				leaf.hasNum = true
			} else {
				return compiledCond{}, fmt.Errorf("%s: nilai untuk %s harus string, angka, atau boolean", valPath, c.Op)
			}
		}
	}

	return leaf, nil
}

func compileAction(idx int, a Action, trusted bool) (ca compiledAction, heavy, suppresses bool, staticSends int, err error) {
	prefix := fmt.Sprintf("do[%d]", idx)
	ca.raw = a
	switch a.Action {
	case "reply":
		if err := validateTextBody(a.Text); err != nil {
			return ca, false, false, 0, fmt.Errorf("%s.text: %w", prefix, err)
		}
		return ca, true, false, 1, nil
	case "send":
		toTrim := strings.TrimSpace(a.To)
		if toTrim == "" {
			return ca, false, false, 0, fmt.Errorf("%s.to: target penerima wajib diisi", prefix)
		}
		if strings.Contains(toTrim, "{{") {
			ca.toHasTmpl = true
		} else if _, pErr := jid.ParseRecipient(toTrim); pErr != nil {
			return ca, false, false, 0, fmt.Errorf("%s.to: format nomor/JID tidak valid: %w", prefix, pErr)
		}
		if err := validateTextBody(a.Text); err != nil {
			return ca, false, false, 0, fmt.Errorf("%s.text: %w", prefix, err)
		}
		return ca, true, false, 1, nil
	case "react":
		em := strings.TrimSpace(a.Emoji)
		if em == "" {
			return ca, false, false, 0, fmt.Errorf("%s.emoji: emoji wajib diisi", prefix)
		}
		if utf8.RuneCountInString(em) > 16 {
			return ca, false, false, 0, fmt.Errorf("%s.emoji: terlalu panjang", prefix)
		}
		return ca, true, false, 1, nil
	case "mark_read":
		return ca, false, false, 0, nil
	case "suppress_webhook":
		return ca, false, true, 0, nil
	case "http":
		if strings.TrimSpace(a.URL) == "" {
			return ca, false, false, 0, fmt.Errorf("%s.url: URL endpoint wajib diisi", prefix)
		}
		if _, urlErr := netguard.ValidateHookURL(a.URL, trusted); urlErr != nil {
			return ca, false, false, 0, fmt.Errorf("%s.url: %w", prefix, urlErr)
		}
		if a.TimeoutMs < 0 || a.TimeoutMs > MaxActionTimeoutMs {
			return ca, false, false, 0, fmt.Errorf("%s.timeout_ms: harus berada antara 0 sampai %d ms", prefix, MaxActionTimeoutMs)
		}
		toMs := a.TimeoutMs
		if toMs == 0 {
			toMs = DefaultActionTimeoutMs
		}
		ca.timeout = time.Duration(toMs) * time.Millisecond
		switch a.Include {
		case "", "event", "conversation":
		default:
			return ca, false, false, 0, fmt.Errorf("%s.include: harus event atau conversation, didapat %q", prefix, a.Include)
		}
		return ca, true, a.ExpectReply, 0, nil
	default:
		return ca, false, false, 0, fmt.Errorf("%s.action: aksi %q tidak dikenal", prefix, a.Action)
	}
}

func validateTextBody(txt string) error {
	if strings.TrimSpace(txt) == "" {
		return errors.New("teks wajib diisi")
	}
	if len(txt) > 60000 {
		return errors.New("panjang teks melebihi batas 60000 byte")
	}
	return nil
}

func extractSlice(v any) ([]any, bool) {
	switch s := v.(type) {
	case []any:
		return s, true
	case []string:
		out := make([]any, len(s))
		for i, item := range s {
			out[i] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func toFiniteFloat(v any) (float64, bool) {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case float32:
		f = float64(n)
	case int:
		f = float64(n)
	case int64:
		f = float64(n)
	case json.Number:
		parsed, err := n.Float64()
		if err != nil {
			return 0, false
		}
		f = parsed
	default:
		return 0, false
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

func parseThrottleRate(spec string) (int, time.Duration, error) {
	parts := strings.SplitN(strings.TrimSpace(spec), "/", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("harus berformat N/durasi (mis. 1/6h), didapat %q", spec)
	}
	n, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || n < 1 || n > 10000 {
		return 0, 0, fmt.Errorf("jumlah N harus antara 1 sampai 10000, didapat %q", parts[0])
	}
	durRaw := strings.TrimSpace(parts[1])
	var dur time.Duration
	if strings.HasSuffix(durRaw, "d") && len(durRaw) > 1 {
		days, dErr := strconv.Atoi(strings.TrimSuffix(durRaw, "d"))
		if dErr != nil || days < 1 {
			return 0, 0, fmt.Errorf("durasi tidak valid %q", durRaw)
		}
		dur = time.Duration(days) * 24 * time.Hour
	} else {
		dur, err = time.ParseDuration(durRaw)
		if err != nil || dur <= 0 {
			return 0, 0, fmt.Errorf("durasi tidak valid %q", durRaw)
		}
	}
	return n, dur, nil
}

func parseActiveHours(raw string) (activeHoursWindow, error) {
	s := strings.TrimSpace(raw)
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return activeHoursWindow{}, errors.New("harus berformat HH:MM-HH:MM")
	}
	start, err := parseClockHHMM(parts[0])
	if err != nil {
		return activeHoursWindow{}, err
	}
	end, err := parseClockHHMM(parts[1])
	if err != nil {
		return activeHoursWindow{}, err
	}
	return activeHoursWindow{enabled: true, startMin: start, endMin: end}, nil
}

func parseClockHHMM(raw string) (int, error) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) != 2 {
		return 0, fmt.Errorf("waktu %q harus berformat HH:MM", raw)
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, fmt.Errorf("jam atau menit tidak valid pada %q", raw)
	}
	return h*60 + m, nil
}

func (w activeHoursWindow) allows(t time.Time) bool {
	if !w.enabled {
		return true
	}
	cur := t.Hour()*60 + t.Minute()
	if w.startMin == w.endMin {
		return true
	}
	if w.startMin < w.endMin {
		return cur >= w.startMin && cur < w.endMin
	}
	return cur >= w.startMin || cur < w.endMin
}
