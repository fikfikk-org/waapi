package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const subscriptionChatMsgLimit = 50

func (s *Server) registerResources(srv *mcp.Server) {
	srv.AddResourceTemplate(&mcp.ResourceTemplate{
		Name:        "chat_transcript",
		Title:       "Transkrip Chat WhatsApp",
		Description: UntrustedBanner + " Mengembalikan 50 pesan terakhir sebuah chat dalam format markdown.",
		URITemplate: "waapi://instances/{instance}/chats/{+chat}",
		MIMEType:    "text/markdown",
	}, s.handleReadChatResource)

	srv.AddResourceTemplate(&mcp.ResourceTemplate{
		Name:        "instance_health",
		Title:       "Kesehatan Akun Instance",
		Description: "Laporan kesehatan Account Health Guardian dalam format JSON.",
		URITemplate: "waapi://instances/{instance}/health",
		MIMEType:    "application/json",
	}, s.handleReadHealthResource)
}

func (s *Server) handleReadChatResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	inst, kind, chat, ok := parseResourceURI(req.Params.URI)
	if !ok || kind != "chats" {
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	if _, err := s.authorizeResource(ctx, req.GetExtra(), inst, auth.ScopeMessagesRead); err != nil {
		return nil, err
	}
	if s.deps.CtxStore == nil {
		return nil, ctxstore.ErrDisabled
	}

	page, err := s.deps.CtxStore.Messages(ctx, inst, chat, subscriptionChatMsgLimit, "")
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	b.WriteString(UntrustedBanner)
	b.WriteString("\n\n# Transkrip ")
	b.WriteString(chat)
	b.WriteString(" (")
	b.WriteString(inst)
	b.WriteString(")\n\n")
	for _, m := range page.Messages {
		who := m.From
		if m.FromMe {
			who = "me"
		}
		fmt.Fprintf(&b, "- [%d] **%s** (%s, %s): %s\n", m.Timestamp, who, m.Type, m.ID, m.Text)
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      req.Params.URI,
			MIMEType: "text/markdown",
			Text:     b.String(),
		}},
	}, nil
}

func (s *Server) handleReadHealthResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	inst, kind, _, ok := parseResourceURI(req.Params.URI)
	if !ok || kind != "health" {
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	if _, err := s.authorizeResource(ctx, req.GetExtra(), inst, auth.ScopeHealthRead); err != nil {
		return nil, err
	}
	if s.deps.Health == nil {
		return nil, errors.New("layanan health tidak tersedia")
	}
	rep, err := s.deps.Health(ctx, inst)
	if err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      req.Params.URI,
			MIMEType: "application/json",
			Text:     string(raw),
		}},
	}, nil
}

func (s *Server) authorizeResource(ctx context.Context, extra *mcp.RequestExtra, inst string, scope auth.Scope) (*auth.KeyMeta, error) {
	km, _ := auth.KeyMetaFromContext(ctx)
	if km == nil && extra != nil {
		km, _ = s.resolvePrincipal(ctx, extra)
	}
	if km == nil {
		return nil, errors.New("unauthorized")
	}
	if !km.Scopes.Has(scope) {
		return nil, fmt.Errorf("scope_denied: butuh scope %s", scope)
	}
	if !km.AllowedInstance(inst) {
		return nil, errors.New("forbidden_instance: API key tidak memiliki akses ke instance ini")
	}
	return km, nil
}

// ActiveSubscriptionCount mengembalikan jumlah URI resource yang sedang aktif disubscribe.
func (s *Server) ActiveSubscriptionCount() int {
	if s == nil {
		return 0
	}
	s.subMu.Lock()
	defer s.subMu.Unlock()
	return len(s.subs)
}

func (s *Server) handleSubscribe(ctx context.Context, req *mcp.SubscribeRequest) error {
	if req == nil || req.Params == nil {
		return errors.New("parameter subscribe kosong")
	}
	inst, kind, chat, ok := parseResourceURI(req.Params.URI)
	if !ok {
		return mcp.ResourceNotFoundError(req.Params.URI)
	}
	needScope := auth.ScopeHealthRead
	if kind == "chats" {
		needScope = auth.ScopeMessagesRead
	}
	if _, err := s.authorizeResource(ctx, req.GetExtra(), inst, needScope); err != nil {
		return err
	}
	canonicalURI := formatResourceURI(inst, kind, chat)

	s.subMu.Lock()
	defer s.subMu.Unlock()
	if s.subs == nil {
		s.subs = make(map[string]int)
	}
	if _, exists := s.subs[canonicalURI]; !exists && len(s.subs) >= maxSubscribedURIsCap {
		return fmt.Errorf("batas maksimal %d subscription resource tercapai", maxSubscribedURIsCap)
	}
	s.subs[canonicalURI]++
	if s.subs[canonicalURI] == 1 {
		s.ensureSubscriptionLoopLocked()
		s.signalSubscriptionWakeLocked()
	}
	return nil
}

func (s *Server) handleUnsubscribe(_ context.Context, req *mcp.UnsubscribeRequest) error {
	if req == nil || req.Params == nil {
		return nil
	}
	inst, kind, chat, ok := parseResourceURI(req.Params.URI)
	if !ok {
		return nil
	}
	canonicalURI := formatResourceURI(inst, kind, chat)

	s.subMu.Lock()
	var cancel context.CancelFunc
	var done chan struct{}
	if s.subs != nil {
		if n := s.subs[canonicalURI]; n <= 1 {
			delete(s.subs, canonicalURI)
		} else {
			s.subs[canonicalURI] = n - 1
		}
		if len(s.subs) == 0 {
			s.subs = nil
			cancel = s.subCancel
			done = s.subDone
			s.subCancel = nil
			s.subDone = nil
		} else {
			s.signalSubscriptionWakeLocked()
		}
	}
	s.subMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}

func (s *Server) ensureSubscriptionLoopLocked() {
	if s.subCancel != nil || s.deps.Journal == nil {
		return
	}
	subCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	if s.subWake == nil {
		s.subWake = make(chan struct{}, 1)
	}
	s.subCancel = cancel
	s.subDone = done
	go s.runSubscriptionLoop(subCtx, done)
}

func (s *Server) signalSubscriptionWakeLocked() {
	if s.subWake == nil {
		return
	}
	select {
	case s.subWake <- struct{}{}:
	default:
	}
}

func (s *Server) runSubscriptionLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	if s.deps.Journal == nil {
		return
	}

	active := make(map[string]*instSub)
	defer func() {
		for _, sub := range active {
			sub.unsub()
		}
	}()

	syncInstances := func() {
		s.subMu.Lock()
		needed := make(map[string]struct{})
		for uri := range s.subs {
			if inst, _, _, ok := parseResourceURI(uri); ok {
				needed[inst] = struct{}{}
			}
		}
		s.subMu.Unlock()

		for inst, sub := range active {
			if _, keep := needed[inst]; !keep {
				sub.unsub()
				delete(active, inst)
			}
		}
		for inst := range needed {
			if _, exists := active[inst]; !exists {
				head, _ := s.deps.Journal.HeadSeq(ctx, inst)
				ch, unsub := s.deps.Journal.Subscribe(inst)
				active[inst] = &instSub{ch: ch, unsub: unsub, lastSeq: head}
			}
		}
	}

	syncInstances()
	for {
		cases := make([]reflect.SelectCase, 0, len(active)+2)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.subWake)})
		instOrder := make([]string, 0, len(active))
		for inst, sub := range active {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sub.ch)})
			instOrder = append(instOrder, inst)
		}

		chosen, _, _ := reflect.Select(cases)
		switch chosen {
		case 0:
			return
		case 1:
			syncInstances()
		default:
			inst := instOrder[chosen-2]
			sub := active[inst]
			if sub == nil {
				continue
			}
			s.flushInstanceNotifications(ctx, inst, sub)
		}
	}
}

type instSub struct {
	ch      <-chan struct{}
	unsub   func()
	lastSeq int64
}

func (s *Server) flushInstanceNotifications(ctx context.Context, inst string, sub *instSub) {
	res, err := s.deps.Journal.Read(ctx, journal.Query{
		InstanceID: inst,
		AfterSeq:   sub.lastSeq,
		Types:      []string{"message", "health"},
		Limit:      100,
	})
	if err != nil {
		return
	}
	if res.ScannedTo > sub.lastSeq {
		sub.lastSeq = res.ScannedTo
	}
	if s.mcpSrv == nil || len(res.Events) == 0 {
		return
	}

	s.subMu.Lock()
	subsCopy := make(map[string]struct{}, len(s.subs))
	for k := range s.subs {
		subsCopy[k] = struct{}{}
	}
	s.subMu.Unlock()

	notified := make(map[string]struct{})
	for _, ev := range res.Events {
		var uri string
		switch ev.Type {
		case "message":
			uri = formatResourceURI(inst, "chats", ev.Chat)
		case "health":
			uri = formatResourceURI(inst, "health", "")
		}
		if uri == "" {
			continue
		}
		if _, subscribed := subsCopy[uri]; !subscribed {
			continue
		}
		if _, already := notified[uri]; already {
			continue
		}
		notified[uri] = struct{}{}
		if err := s.mcpSrv.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: uri}); err != nil {
			slogError("gagal mengirim notifikasi resource MCP", "uri", uri, "err", err)
		}
	}
}

func parseResourceURI(rawURI string) (inst, kind, chat string, ok bool) {
	u, err := url.Parse(strings.TrimSpace(rawURI))
	if err != nil || u.Scheme != "waapi" || u.Host != "instances" {
		return "", "", "", false
	}
	trimmed := strings.Trim(u.Path, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 2 && parts[0] != "" && parts[1] == "health" {
		return parts[0], "health", "", true
	}
	if len(parts) == 3 && parts[0] != "" && parts[1] == "chats" && parts[2] != "" {
		rawChat, unescErr := url.PathUnescape(parts[2])
		if unescErr != nil {
			rawChat = parts[2]
		}
		canon, err := normalizeChatOrGroup(rawChat)
		if err != nil {
			return "", "", "", false
		}
		return parts[0], "chats", canon, true
	}
	return "", "", "", false
}

func formatResourceURI(inst, kind, chat string) string {
	if kind == "health" {
		return "waapi://instances/" + inst + "/health"
	}
	return "waapi://instances/" + inst + "/chats/" + chat
}

func normalizeChatOrGroup(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("chat kosong")
	}
	if strings.HasSuffix(trimmed, "@lid") {
		return trimmed, nil
	}
	parsed, err := jid.ParseRecipient(trimmed)
	if err != nil {
		return "", err
	}
	return parsed.ToNonAD().String(), nil
}
