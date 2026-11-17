package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/fikfikk-org/waapi/internal/auth"
	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/governor"
	"github.com/fikfikk-org/waapi/internal/guardian"
	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/policy"
	"github.com/fikfikk-org/waapi/internal/wa"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.mau.fi/whatsmeow/types"
)

type toolRule struct {
	name        string
	scope       auth.Scope
	readOnly    bool
	destructive bool
}

var mcpToolRules = []toolRule{
	{name: "list_instances", scope: auth.ScopeInstancesRead, readOnly: true},
	{name: "get_health", scope: auth.ScopeHealthRead, readOnly: true},
	{name: "list_chats", scope: auth.ScopeMessagesRead, readOnly: true},
	{name: "get_conversation", scope: auth.ScopeMessagesRead, readOnly: true},
	{name: "search_messages", scope: auth.ScopeMessagesRead, readOnly: true},
	{name: "get_events", scope: auth.ScopeEventsRead, readOnly: true},
	{name: "check_numbers", scope: auth.ScopeMessagesRead, readOnly: true},
	{name: "get_message_status", scope: auth.ScopeMessagesRead, readOnly: true},
	{name: "list_groups", scope: auth.ScopeGroupsRead, readOnly: true},
	{name: "get_group_info", scope: auth.ScopeGroupsRead, readOnly: true},
	{name: "send_message", scope: auth.ScopeMessagesSend},
	{name: "react_message", scope: auth.ScopeMessagesSend},
	{name: "mark_read", scope: auth.ScopeMessagesSend},
	{name: "revoke_message", scope: auth.ScopeMessagesSend, destructive: true},
	{name: "manage_group_participants", scope: auth.ScopeGroupsWrite, destructive: true},
}

var mcpToolRuleMap = func() map[string]toolRule {
	m := make(map[string]toolRule, len(mcpToolRules))
	for _, r := range mcpToolRules {
		m[r.name] = r
	}
	return m
}()

func isToolAllowed(km *auth.KeyMeta, name string) bool {
	if km == nil {
		return false
	}
	rule, ok := mcpToolRuleMap[name]
	if !ok {
		return false
	}
	if !km.Scopes.Has(rule.scope) {
		return false
	}
	if len(km.Policy.Tools) == 0 {
		// Set aman default: seluruh readOnly + send_message, react_message, mark_read (bukan tool destruktif).
		return !rule.destructive
	}
	for _, allowed := range km.Policy.Tools {
		if allowed == name {
			return true
		}
	}
	return false
}

func boolPtr(v bool) *bool { return &v }

func (s *Server) registerTools(srv *mcp.Server) {
	// 1. list_instances
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "list_instances",
		Description: "Daftar instance WhatsApp beserta status koneksi, JID, dan ringkasan kesehatan.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolListInstances)

	// 2. get_health
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "get_health",
		Description: "Laporan lengkap Account Health Guardian untuk sebuah instance.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolGetHealth)

	// 3. list_chats
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "list_chats",
		Description: "Daftar percakapan terbaru dari context store opt-in milik instance.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolListChats)

	// 4. get_conversation
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "get_conversation",
		Description: UntrustedBanner + " Membaca riwayat pesan dalam sebuah chat.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolGetConversation)

	// 5. search_messages
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "search_messages",
		Description: UntrustedBanner + " Mencari pesan di context store menggunakan FTS5 BM25.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolSearchMessages)

	// 6. get_events
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "get_events",
		Description: UntrustedBanner + " Membaca event masuk dari journal v2 mulai setelah cursor seq.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolGetEvents)

	// 7. check_numbers
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "check_numbers",
		Description: "Memeriksa apakah daftar nomor (maks 20) terdaftar di WhatsApp.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true},
	}, s.toolCheckNumbers)

	// 8. get_message_status
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "get_message_status",
		Description: "Memeriksa status pesan keluar di durable outbox.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolGetMessageStatus)

	// 9. list_groups
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "list_groups",
		Description: "Daftar ringkasan grup WhatsApp yang diikuti oleh instance.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolListGroups)

	// 10. get_group_info
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "get_group_info",
		Description: "Detail metadata dan daftar peserta sebuah grup WhatsApp.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, s.toolGetGroupInfo)

	// 11. send_message
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "send_message",
		Description: "Mengantrekan pengiriman pesan WhatsApp via outbox dengan penegakan Guardian, KeyPolicy, dan persetujuan manusia.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPtr(false), OpenWorldHint: boolPtr(true)},
	}, s.toolSendMessage)

	// 12. react_message
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "react_message",
		Description: "Mengirim atau menghapus reaksi emoji terhadap suatu pesan lewat outbox.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPtr(false), IdempotentHint: true},
	}, s.toolReactMessage)

	// 13. mark_read
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "mark_read",
		Description: "Menandai satu atau lebih pesan chat telah dibaca sekaligus mereset counter unread.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPtr(false), IdempotentHint: true},
	}, s.toolMarkRead)

	// 14. revoke_message
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "revoke_message",
		Description: "Menarik/menghapus pesan terkirim untuk semua peserta (aksi destruktif tunduk KeyPolicy.destructive).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPtr(true)},
	}, s.toolRevokeMessage)

	// 15. manage_group_participants
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "manage_group_participants",
		Description: "Mengubah keanggotaan grup WhatsApp (add, remove, promote, demote) dengan penegakan KeyPolicy dan GroupGovernor.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPtr(true)},
	}, s.toolManageGroupParticipants)
}

func (s *Server) toolPrincipalAndAccess(ctx context.Context, req *mcp.CallToolRequest, inst string) (*auth.KeyMeta, error) {
	var extra *mcp.RequestExtra
	if req != nil {
		extra = req.GetExtra()
	}
	km, ok := auth.KeyMetaFromContext(ctx)
	if !ok || km == nil {
		km, ok = s.resolvePrincipal(ctx, extra)
	}
	if !ok || km == nil {
		return nil, errors.New("unauthorized: principal tidak tersedia")
	}
	if inst != "" && !km.AllowedInstance(inst) {
		return nil, errors.New("forbidden_instance: API key tidak memiliki akses ke instance ini")
	}
	return km, nil
}

// --- Input/Output Structs & Handlers ---

type emptyIn struct{}

type instanceSummaryOut struct {
	ID     string            `json:"id"`
	Status string            `json:"status"`
	JID    string            `json:"jid"`
	Health *wa.HealthSummary `json:"health,omitempty"`
}

type listInstancesOut struct {
	Instances []instanceSummaryOut `json:"instances"`
}

func (s *Server) toolListInstances(ctx context.Context, req *mcp.CallToolRequest, _ emptyIn) (*mcp.CallToolResult, listInstancesOut, error) {
	km, err := s.toolPrincipalAndAccess(ctx, req, "")
	if err != nil {
		return nil, listInstancesOut{}, err
	}
	if s.deps.Instances == nil {
		return nil, listInstancesOut{}, errors.New("not_ready: manajer instance belum siap")
	}
	all, err := s.deps.Instances.ListStatuses(ctx)
	if err != nil {
		return nil, listInstancesOut{}, err
	}
	items := make([]instanceSummaryOut, 0, len(all))
	for _, st := range all {
		if !km.AllowedInstance(st.ID) {
			continue
		}
		items = append(items, instanceSummaryOut{
			ID:     st.ID,
			Status: string(st.Status),
			JID:    st.JID,
			Health: st.Health,
		})
	}
	return nil, listInstancesOut{Instances: items}, nil
}

type instanceOnlyIn struct {
	Instance string `json:"instance" jsonschema:"ID instance WhatsApp"`
}

func (s *Server) toolGetHealth(ctx context.Context, req *mcp.CallToolRequest, in instanceOnlyIn) (*mcp.CallToolResult, guardian.HealthReport, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, guardian.HealthReport{}, err
	}
	if s.deps.Health == nil {
		return nil, guardian.HealthReport{}, errors.New("not_ready: layanan health belum siap")
	}
	rep, err := s.deps.Health(ctx, in.Instance)
	if err != nil {
		return nil, guardian.HealthReport{}, mapDomainToolError(err)
	}
	if rep.Factors == nil {
		rep.Factors = []guardian.Factor{}
	}
	return nil, rep, nil
}

type listChatsIn struct {
	Instance string `json:"instance" jsonschema:"ID instance WhatsApp"`
	Limit    int    `json:"limit,omitempty" jsonschema:"Jumlah maksimal chat (1..50, default 50)"`
	Before   string `json:"before,omitempty" jsonschema:"Cursor paginasi before"`
}

type listChatsOut struct {
	Chats      []ctxstore.ChatItem `json:"chats"`
	NextBefore *string             `json:"next_before"`
}

func (s *Server) toolListChats(ctx context.Context, req *mcp.CallToolRequest, in listChatsIn) (*mcp.CallToolResult, listChatsOut, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, listChatsOut{}, err
	}
	if s.deps.CtxStore == nil {
		return nil, listChatsOut{}, errors.New("context_store_disabled: context store belum tersedia")
	}
	limit := clampPositiveMax(in.Limit, 50, 50)
	page, err := s.deps.CtxStore.Chats(ctx, in.Instance, limit, in.Before)
	if err != nil {
		return nil, listChatsOut{}, mapDomainToolError(err)
	}
	if page.Chats == nil {
		page.Chats = []ctxstore.ChatItem{}
	}
	return nil, listChatsOut{Chats: page.Chats, NextBefore: page.NextBefore}, nil
}

type getConversationIn struct {
	Instance string `json:"instance" jsonschema:"ID instance WhatsApp"`
	Chat     string `json:"chat" jsonschema:"Nomor atau JID chat tujuan"`
	Limit    int    `json:"limit,omitempty" jsonschema:"Jumlah pesan (1..50, default 50)"`
	Before   string `json:"before,omitempty" jsonschema:"Cursor paginasi before"`
}

type getConversationOut struct {
	Untrusted  bool                   `json:"untrusted"`
	Messages   []ctxstore.MessageItem `json:"messages"`
	NextBefore *string                `json:"next_before"`
}

func (s *Server) toolGetConversation(ctx context.Context, req *mcp.CallToolRequest, in getConversationIn) (*mcp.CallToolResult, getConversationOut, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, getConversationOut{}, err
	}
	if s.deps.CtxStore == nil {
		return nil, getConversationOut{}, errors.New("context_store_disabled: context store belum tersedia")
	}
	chatCanon, err := normalizeChatOrGroup(in.Chat)
	if err != nil {
		return nil, getConversationOut{}, fmt.Errorf("invalid_number: %w", err)
	}
	limit := clampPositiveMax(in.Limit, 50, 50)
	page, err := s.deps.CtxStore.Messages(ctx, in.Instance, chatCanon, limit, in.Before)
	if err != nil {
		return nil, getConversationOut{}, mapDomainToolError(err)
	}
	if page.Messages == nil {
		page.Messages = []ctxstore.MessageItem{}
	}
	return untrustedToolResult(getConversationOut{
		Untrusted:  true,
		Messages:   page.Messages,
		NextBefore: page.NextBefore,
	})
}

type searchMessagesIn struct {
	Instance string `json:"instance" jsonschema:"ID instance WhatsApp"`
	Query    string `json:"query" jsonschema:"Kata kunci pencarian FTS5"`
	Chat     string `json:"chat,omitempty" jsonschema:"Filter opsional JID atau nomor chat"`
	Limit    int    `json:"limit,omitempty" jsonschema:"Jumlah hasil maksimal (1..20, default 20)"`
}

type searchMessagesOut struct {
	Untrusted bool                 `json:"untrusted"`
	Hits      []ctxstore.SearchHit `json:"hits"`
}

func (s *Server) toolSearchMessages(ctx context.Context, req *mcp.CallToolRequest, in searchMessagesIn) (*mcp.CallToolResult, searchMessagesOut, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, searchMessagesOut{}, err
	}
	if s.deps.Governor != nil && s.deps.Governor.Level() == governor.Red {
		return nil, searchMessagesOut{}, errors.New("memory_pressure: pencarian dinonaktifkan sementara pada level memori Red")
	}
	if s.deps.CtxStore == nil {
		return nil, searchMessagesOut{}, errors.New("context_store_disabled: context store belum tersedia")
	}
	var chatFilter string
	if strings.TrimSpace(in.Chat) != "" {
		c, err := normalizeChatOrGroup(in.Chat)
		if err != nil {
			return nil, searchMessagesOut{}, fmt.Errorf("invalid_number: %w", err)
		}
		chatFilter = c
	}
	limit := clampPositiveMax(in.Limit, 20, 20)
	res, err := s.deps.CtxStore.Search(ctx, in.Instance, in.Query, chatFilter, limit, false)
	if err != nil {
		return nil, searchMessagesOut{}, mapDomainToolError(err)
	}
	if res.Hits == nil {
		res.Hits = []ctxstore.SearchHit{}
	}
	return untrustedToolResult(searchMessagesOut{
		Untrusted: true,
		Hits:      res.Hits,
	})
}

type getEventsIn struct {
	Instance string   `json:"instance" jsonschema:"ID instance WhatsApp"`
	After    int64    `json:"after,omitempty" jsonschema:"Ambil event setelah nomor seq ini"`
	Types    []string `json:"types,omitempty" jsonschema:"Filter tipe event opsional"`
	Limit    int      `json:"limit,omitempty" jsonschema:"Batas event (1..100, default 50)"`
}

type eventView struct {
	Seq       int64           `json:"seq"`
	Type      string          `json:"type"`
	Chat      string          `json:"chat,omitempty"`
	Sender    string          `json:"sender,omitempty"`
	WAID      string          `json:"wa_id,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

type getEventsOut struct {
	Untrusted bool        `json:"untrusted"`
	Events    []eventView `json:"events"`
	NextAfter int64       `json:"next_after"`
	HasMore   bool        `json:"has_more"`
}

func (s *Server) toolGetEvents(ctx context.Context, req *mcp.CallToolRequest, in getEventsIn) (*mcp.CallToolResult, getEventsOut, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, getEventsOut{}, err
	}
	if s.deps.Journal == nil {
		return nil, getEventsOut{}, errors.New("not_ready: journal tidak tersedia")
	}
	limit := clampPositiveMax(in.Limit, 50, 100)
	res, err := s.deps.Journal.Read(ctx, journal.Query{
		InstanceID: in.Instance,
		AfterSeq:   in.After,
		Types:      in.Types,
		Limit:      limit,
	})
	if err != nil {
		return nil, getEventsOut{}, mapDomainToolError(err)
	}
	items := make([]eventView, 0, len(res.Events))
	for _, ev := range res.Events {
		items = append(items, eventView{
			Seq:       ev.Seq,
			Type:      ev.Type,
			Chat:      ev.Chat,
			Sender:    ev.Sender,
			WAID:      ev.WAID,
			Timestamp: ev.TS,
			Data:      ev.Payload,
		})
	}
	return untrustedToolResult(getEventsOut{
		Untrusted: true,
		Events:    items,
		NextAfter: res.ScannedTo,
		HasMore:   res.HasMore,
	})
}

type checkNumbersIn struct {
	Instance string   `json:"instance" jsonschema:"ID instance WhatsApp"`
	Numbers  []string `json:"numbers" jsonschema:"Daftar nomor telepon (1..20)"`
}

type checkNumbersOut struct {
	Results []wa.NumberCheckResult `json:"results"`
}

func (s *Server) toolCheckNumbers(ctx context.Context, req *mcp.CallToolRequest, in checkNumbersIn) (*mcp.CallToolResult, checkNumbersOut, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, checkNumbersOut{}, err
	}
	if s.deps.WA == nil {
		return nil, checkNumbersOut{}, errors.New("not_ready: operasi WA belum siap")
	}
	if len(in.Numbers) == 0 || len(in.Numbers) > 20 {
		return nil, checkNumbersOut{}, errors.New("invalid_request: numbers harus berisi 1 sampai 20 nomor")
	}
	results, err := s.deps.WA.CheckNumbers(ctx, in.Instance, in.Numbers)
	if err != nil {
		return nil, checkNumbersOut{}, mapDomainToolError(err)
	}
	if results == nil {
		results = []wa.NumberCheckResult{}
	}
	return nil, checkNumbersOut{Results: results}, nil
}

type getMessageStatusIn struct {
	Instance  string `json:"instance" jsonschema:"ID instance WhatsApp"`
	MessageID string `json:"message_id" jsonschema:"ID job pesan outbox (msg_...)"`
}

type messageStatusOut struct {
	ID          string `json:"id"`
	WAMessageID string `json:"wa_message_id"`
	To          string `json:"to"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	Attempts    int    `json:"attempts"`
	ErrorCode   string `json:"error_code,omitempty"`
}

func (s *Server) toolGetMessageStatus(ctx context.Context, req *mcp.CallToolRequest, in getMessageStatusIn) (*mcp.CallToolResult, messageStatusOut, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, messageStatusOut{}, err
	}
	if s.deps.Outbox == nil {
		return nil, messageStatusOut{}, errors.New("not_ready: outbox belum siap")
	}
	job, err := s.deps.Outbox.Get(ctx, in.Instance, in.MessageID)
	if err != nil {
		return nil, messageStatusOut{}, mapDomainToolError(err)
	}
	return nil, messageStatusOut{
		ID:          job.ID,
		WAMessageID: job.WAID,
		To:          job.ToJID,
		Type:        job.Kind,
		Status:      string(job.Status),
		Attempts:    job.Attempts,
		ErrorCode:   job.ErrorCode,
	}, nil
}

type listGroupsOut struct {
	Groups []wa.GroupSummary `json:"groups"`
}

func (s *Server) toolListGroups(ctx context.Context, req *mcp.CallToolRequest, in instanceOnlyIn) (*mcp.CallToolResult, listGroupsOut, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, listGroupsOut{}, err
	}
	if s.deps.WA == nil {
		return nil, listGroupsOut{}, errors.New("not_ready: operasi grup belum siap")
	}
	groups, err := s.deps.WA.ListGroups(ctx, in.Instance)
	if err != nil {
		return nil, listGroupsOut{}, mapDomainToolError(err)
	}
	if groups == nil {
		groups = []wa.GroupSummary{}
	}
	return nil, listGroupsOut{Groups: groups}, nil
}

type getGroupInfoIn struct {
	Instance string `json:"instance" jsonschema:"ID instance WhatsApp"`
	Group    string `json:"group" jsonschema:"JID grup (akhiran @g.us)"`
}

func (s *Server) toolGetGroupInfo(ctx context.Context, req *mcp.CallToolRequest, in getGroupInfoIn) (*mcp.CallToolResult, wa.GroupDetail, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, wa.GroupDetail{}, err
	}
	if s.deps.WA == nil {
		return nil, wa.GroupDetail{}, errors.New("not_ready: operasi grup belum siap")
	}
	gj, err := jid.ParseRecipient(in.Group)
	if err != nil || gj.Server != types.GroupServer {
		return nil, wa.GroupDetail{}, errors.New("invalid_number: JID grup harus berakhiran @g.us")
	}
	detail, err := s.deps.WA.GetGroup(ctx, in.Instance, gj)
	if err != nil {
		return nil, wa.GroupDetail{}, mapDomainToolError(err)
	}
	if detail.Participants == nil {
		detail.Participants = []wa.GroupParticipantView{}
	}
	return nil, detail, nil
}

type sendMessageIn struct {
	Instance       string           `json:"instance" jsonschema:"ID instance WhatsApp"`
	To             string           `json:"to" jsonschema:"Nomor telepon atau JID tujuan"`
	Type           string           `json:"type" jsonschema:"Tipe pesan: text, location, contact, poll, image_url, atau document_url"`
	Text           string           `json:"text,omitempty" jsonschema:"Isi pesan teks atau caption"`
	URL            string           `json:"url,omitempty" jsonschema:"URL HTTPS untuk tipe image_url atau document_url"`
	Filename       string           `json:"filename,omitempty" jsonschema:"Nama file opsional untuk document_url"`
	Location       *outbox.Location `json:"location,omitempty" jsonschema:"Payload lokasi untuk tipe location"`
	Contacts       []outbox.Contact `json:"contacts,omitempty" jsonschema:"Daftar kontak untuk tipe contact"`
	Poll           *outbox.PollSpec `json:"poll,omitempty" jsonschema:"Payload polling untuk tipe poll"`
	ReplyTo        *outbox.ReplyRef `json:"reply_to,omitempty" jsonschema:"Referensi pesan balasan"`
	Mentions       []string         `json:"mentions,omitempty" jsonschema:"Daftar JID yang di-mention"`
	IdempotencyKey string           `json:"idempotency_key,omitempty" jsonschema:"Kunci idempotency opsional"`
}

type enqueueOut struct {
	ID          string `json:"id"`
	WAMessageID string `json:"wa_message_id"`
	Status      string `json:"status"`
	To          string `json:"to"`
}

func (s *Server) toolSendMessage(ctx context.Context, req *mcp.CallToolRequest, in sendMessageIn) (*mcp.CallToolResult, enqueueOut, error) {
	km, err := s.toolPrincipalAndAccess(ctx, req, in.Instance)
	if err != nil {
		return nil, enqueueOut{}, err
	}
	if s.deps.Outbox == nil {
		return nil, enqueueOut{}, errors.New("not_ready: outbox belum siap")
	}
	spec, preview, err := buildMCPSendSpec(in)
	if err != nil {
		return nil, enqueueOut{}, err
	}
	canonTo, err := normalizeChatOrGroup(spec.To)
	if err != nil {
		return nil, enqueueOut{}, fmt.Errorf("invalid_number: %w", err)
	}
	specHash := HashSpec("send_message", in.Instance, canonTo, spec)

	pol := km.Policy
	hasRetry, approved, retryErr := checkApprovalRetry(req, s.signer, km.ID, in.Instance, canonTo, specHash)
	if retryErr != nil {
		return nil, enqueueOut{}, retryErr
	}
	if hasRetry && approved {
		pol.FirstContact = "allow"
	}

	job, _, err := s.deps.Outbox.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: in.Instance,
		IdemKey:    strings.TrimSpace(in.IdempotencyKey),
		Spec:       spec,
		Origin:     outbox.Origin{Kind: "mcp", KeyID: km.ID, Policy: &pol},
	})
	if err != nil {
		if errors.Is(err, policy.ErrFirstContactNeedsApproval) && !hasRetry {
			if !clientSupportsElicitation(req) {
				return nil, enqueueOut{}, errors.New("first_contact_requires_approval: pengiriman ke kontak baru butuh persetujuan manusia; ubah KeyPolicy.first_contact atau gunakan klien MCP yang mendukung elicitation")
			}
			tok, signErr := s.signer.Sign(km.ID, in.Instance, canonTo, specHash)
			if signErr != nil {
				return nil, enqueueOut{}, signErr
			}
			return approvalInputResult(firstContactApprovalPrompt(canonTo, preview), tok), enqueueOut{}, nil
		}
		return nil, enqueueOut{}, mapDomainToolError(err)
	}
	return nil, enqueueOut{
		ID:          job.ID,
		WAMessageID: job.WAID,
		Status:      string(job.Status),
		To:          job.ToJID,
	}, nil
}

func buildMCPSendSpec(in sendMessageIn) (outbox.Spec, string, error) {
	msgType := strings.ToLower(strings.TrimSpace(in.Type))
	spec := outbox.Spec{
		To:       strings.TrimSpace(in.To),
		ReplyTo:  in.ReplyTo,
		Mentions: in.Mentions,
	}
	var preview string
	switch msgType {
	case "text":
		spec.Type = "text"
		spec.Text = in.Text
		preview = in.Text
	case "location":
		spec.Type = "location"
		spec.Location = in.Location
		if in.Location != nil {
			preview = fmt.Sprintf("lokasi %s (%.4f,%.4f)", in.Location.Name, in.Location.Lat, in.Location.Lng)
		}
	case "contact":
		spec.Type = "contact"
		spec.Contacts = in.Contacts
		if len(in.Contacts) > 0 {
			preview = "kontak " + in.Contacts[0].Name
		}
	case "poll":
		spec.Type = "poll"
		spec.Poll = in.Poll
		if in.Poll != nil {
			preview = "polling " + in.Poll.Name
		}
	case "image_url":
		spec.Type = "image"
		spec.Media = &outbox.MediaRef{URL: strings.TrimSpace(in.URL), Caption: in.Text}
		preview = "[gambar] " + in.Text
	case "document_url":
		spec.Type = "document"
		spec.Media = &outbox.MediaRef{URL: strings.TrimSpace(in.URL), Caption: in.Text, Filename: in.Filename}
		preview = "[dokumen] " + in.Filename
	default:
		return outbox.Spec{}, "", fmt.Errorf("invalid_spec: type MCP %q tidak didukung (gunakan text, location, contact, poll, image_url, atau document_url)", in.Type)
	}
	return spec, preview, nil
}

type reactMessageIn struct {
	Instance  string `json:"instance" jsonschema:"ID instance WhatsApp"`
	Chat      string `json:"chat" jsonschema:"Nomor atau JID chat target"`
	MessageID string `json:"message_id" jsonschema:"ID pesan WhatsApp target reaksi"`
	Sender    string `json:"sender,omitempty" jsonschema:"JID pengirim asli pada grup (opsional)"`
	Emoji     string `json:"emoji" jsonschema:"Emoji reaksi atau string kosong untuk menghapus reaksi"`
}

func (s *Server) toolReactMessage(ctx context.Context, req *mcp.CallToolRequest, in reactMessageIn) (*mcp.CallToolResult, enqueueOut, error) {
	km, err := s.toolPrincipalAndAccess(ctx, req, in.Instance)
	if err != nil {
		return nil, enqueueOut{}, err
	}
	if s.deps.Outbox == nil {
		return nil, enqueueOut{}, errors.New("not_ready: outbox belum siap")
	}
	pol := km.Policy
	job, _, err := s.deps.Outbox.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: in.Instance,
		Spec: outbox.Spec{
			Type:     "reaction",
			To:       in.Chat,
			Reaction: &outbox.Reaction{MessageID: in.MessageID, Sender: in.Sender, Emoji: in.Emoji},
		},
		Origin: outbox.Origin{Kind: "mcp", KeyID: km.ID, Policy: &pol},
	})
	if err != nil {
		return nil, enqueueOut{}, mapDomainToolError(err)
	}
	return nil, enqueueOut{ID: job.ID, WAMessageID: job.WAID, Status: string(job.Status), To: job.ToJID}, nil
}

type markReadIn struct {
	Instance   string   `json:"instance" jsonschema:"ID instance WhatsApp"`
	Chat       string   `json:"chat" jsonschema:"Nomor atau JID chat"`
	MessageIDs []string `json:"message_ids" jsonschema:"Daftar ID pesan (1..50)"`
	Sender     string   `json:"sender,omitempty" jsonschema:"JID pengirim pada grup (opsional)"`
}

type markReadOut struct {
	OK    bool `json:"ok"`
	Count int  `json:"count"`
}

func (s *Server) toolMarkRead(ctx context.Context, req *mcp.CallToolRequest, in markReadIn) (*mcp.CallToolResult, markReadOut, error) {
	if _, err := s.toolPrincipalAndAccess(ctx, req, in.Instance); err != nil {
		return nil, markReadOut{}, err
	}
	if s.deps.WA == nil {
		return nil, markReadOut{}, errors.New("not_ready: operasi WA belum siap")
	}
	chatJID, err := jid.ParseRecipient(in.Chat)
	if err != nil {
		return nil, markReadOut{}, fmt.Errorf("invalid_number: %w", err)
	}
	if len(in.MessageIDs) == 0 || len(in.MessageIDs) > 50 {
		return nil, markReadOut{}, errors.New("invalid_request: message_ids harus berisi 1 sampai 50 ID")
	}
	var senderJID types.JID
	if strings.TrimSpace(in.Sender) != "" {
		parsed, sErr := jid.ParseRecipient(in.Sender)
		if sErr != nil {
			return nil, markReadOut{}, errors.New("invalid_request: sender tidak valid")
		}
		senderJID = parsed
	}
	ids := make([]types.MessageID, 0, len(in.MessageIDs))
	for _, raw := range in.MessageIDs {
		if t := strings.TrimSpace(raw); t != "" {
			ids = append(ids, types.MessageID(t))
		}
	}
	if len(ids) == 0 {
		return nil, markReadOut{}, errors.New("invalid_request: message_ids tidak boleh kosong")
	}
	if err := s.deps.WA.MarkRead(ctx, in.Instance, chatJID, senderJID, ids, false); err != nil {
		return nil, markReadOut{}, mapDomainToolError(err)
	}
	if s.deps.CtxStore != nil {
		_ = s.deps.CtxStore.MarkChatRead(ctx, in.Instance, chatJID.String())
	}
	return nil, markReadOut{OK: true, Count: len(ids)}, nil
}

type revokeMessageIn struct {
	Instance  string `json:"instance" jsonschema:"ID instance WhatsApp"`
	Chat      string `json:"chat" jsonschema:"Nomor atau JID chat"`
	MessageID string `json:"message_id" jsonschema:"ID pesan yang akan dihapus"`
	Sender    string `json:"sender,omitempty" jsonschema:"JID pengirim asli (opsional)"`
}

func (s *Server) toolRevokeMessage(ctx context.Context, req *mcp.CallToolRequest, in revokeMessageIn) (*mcp.CallToolResult, enqueueOut, error) {
	km, err := s.toolPrincipalAndAccess(ctx, req, in.Instance)
	if err != nil {
		return nil, enqueueOut{}, err
	}
	if s.deps.Outbox == nil {
		return nil, enqueueOut{}, errors.New("not_ready: outbox belum siap")
	}
	canonChat, err := normalizeChatOrGroup(in.Chat)
	if err != nil {
		return nil, enqueueOut{}, fmt.Errorf("invalid_number: %w", err)
	}
	specHash := HashSpec("revoke_message", in.Instance, canonChat, in.MessageID, in.Sender)

	pol := km.Policy
	hasRetry, approved, retryErr := checkApprovalRetry(req, s.signer, km.ID, in.Instance, canonChat, specHash)
	if retryErr != nil {
		return nil, enqueueOut{}, retryErr
	}
	if hasRetry && approved {
		pol.Destructive = "allow"
	}

	job, _, err := s.deps.Outbox.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: in.Instance,
		Spec: outbox.Spec{
			Type:   "revoke",
			To:     in.Chat,
			Revoke: &outbox.Revoke{MessageID: in.MessageID, Sender: in.Sender},
		},
		Origin: outbox.Origin{Kind: "mcp", KeyID: km.ID, Policy: &pol},
	})
	if err != nil {
		if errors.Is(err, policy.ErrDestructiveNeedsApproval) && !hasRetry {
			if !clientSupportsElicitation(req) {
				return nil, enqueueOut{}, errors.New("destructive_requires_approval: revoke pesan butuh persetujuan manusia; ubah KeyPolicy.destructive atau gunakan klien MCP dengan elicitation")
			}
			tok, signErr := s.signer.Sign(km.ID, in.Instance, canonChat, specHash)
			if signErr != nil {
				return nil, enqueueOut{}, signErr
			}
			return approvalInputResult(destructiveApprovalPrompt("revoke_message", canonChat, "hapus pesan "+in.MessageID), tok), enqueueOut{}, nil
		}
		return nil, enqueueOut{}, mapDomainToolError(err)
	}
	return nil, enqueueOut{ID: job.ID, WAMessageID: job.WAID, Status: string(job.Status), To: job.ToJID}, nil
}

type manageGroupParticipantsIn struct {
	Instance     string   `json:"instance" jsonschema:"ID instance WhatsApp"`
	Group        string   `json:"group" jsonschema:"JID grup WhatsApp (@g.us)"`
	Action       string   `json:"action" jsonschema:"Aksi keanggotaan: add, remove, promote, atau demote"`
	Participants []string `json:"participants" jsonschema:"Daftar nomor atau JID peserta (1..50)"`
}

type manageGroupParticipantsOut struct {
	Results []wa.ParticipantResult `json:"results"`
}

func (s *Server) toolManageGroupParticipants(ctx context.Context, req *mcp.CallToolRequest, in manageGroupParticipantsIn) (*mcp.CallToolResult, manageGroupParticipantsOut, error) {
	km, err := s.toolPrincipalAndAccess(ctx, req, in.Instance)
	if err != nil {
		return nil, manageGroupParticipantsOut{}, err
	}
	if s.deps.WA == nil {
		return nil, manageGroupParticipantsOut{}, errors.New("not_ready: operasi grup belum siap")
	}
	gj, err := jid.ParseRecipient(in.Group)
	if err != nil || gj.Server != types.GroupServer {
		return nil, manageGroupParticipantsOut{}, errors.New("invalid_number: JID grup harus berakhiran @g.us")
	}
	action := strings.ToLower(strings.TrimSpace(in.Action))
	switch action {
	case "add", "remove", "promote", "demote":
	default:
		return nil, manageGroupParticipantsOut{}, errors.New("invalid_request: action harus add, remove, promote, atau demote")
	}
	if len(in.Participants) == 0 || len(in.Participants) > 50 {
		return nil, manageGroupParticipantsOut{}, errors.New("invalid_request: participants harus berisi 1 sampai 50 nomor/JID")
	}

	specHash := HashSpec("manage_group_participants", in.Instance, gj.String(), action, in.Participants)
	pol := km.Policy
	hasRetry, approved, retryErr := checkApprovalRetry(req, s.signer, km.ID, in.Instance, gj.String(), specHash)
	if retryErr != nil {
		return nil, manageGroupParticipantsOut{}, retryErr
	}
	if hasRetry && approved {
		pol.Destructive = "allow"
	}

	if s.deps.Policy != nil {
		if dErr := s.deps.Policy.CheckDestructive(outbox.Origin{Kind: "mcp", KeyID: km.ID, Policy: &pol}, action); dErr != nil {
			if errors.Is(dErr, policy.ErrDestructiveNeedsApproval) && !hasRetry {
				if !clientSupportsElicitation(req) {
					return nil, manageGroupParticipantsOut{}, errors.New("destructive_requires_approval: aksi remove peserta butuh persetujuan manusia; ubah KeyPolicy.destructive atau gunakan klien MCP dengan elicitation")
				}
				tok, signErr := s.signer.Sign(km.ID, in.Instance, gj.String(), specHash)
				if signErr != nil {
					return nil, manageGroupParticipantsOut{}, signErr
				}
				return approvalInputResult(destructiveApprovalPrompt("remove_participants", gj.String(), strings.Join(in.Participants, ", ")), tok), manageGroupParticipantsOut{}, nil
			}
			return nil, manageGroupParticipantsOut{}, mapDomainToolError(dErr)
		}
	}

	res, err := s.deps.WA.UpdateGroupParticipants(ctx, in.Instance, gj, wa.UpdateParticipantsRequest{
		Action:       action,
		Participants: in.Participants,
	})
	if err != nil {
		return nil, manageGroupParticipantsOut{}, mapDomainToolError(err)
	}
	if res == nil {
		res = []wa.ParticipantResult{}
	}
	return nil, manageGroupParticipantsOut{Results: res}, nil
}

func clampPositiveMax(v, def, max int) int {
	if v <= 0 {
		return def
	}
	if v > max {
		return max
	}
	return v
}

type codedErr interface {
	ErrorCode() string
}

func mapDomainToolError(err error) error {
	if err == nil {
		return nil
	}
	var ce codedErr
	if errors.As(err, &ce) && ce.ErrorCode() != "" {
		return fmt.Errorf("%s: %s", ce.ErrorCode(), err.Error())
	}
	switch {
	case errors.Is(err, ctxstore.ErrDisabled):
		return fmt.Errorf("context_store_disabled: %s", err.Error())
	case errors.Is(err, policy.ErrOutsideReplyWindow):
		return fmt.Errorf("outside_reply_window: %s", err.Error())
	case errors.Is(err, policy.ErrFirstContactNeedsApproval):
		return fmt.Errorf("first_contact_requires_approval: %s", err.Error())
	case errors.Is(err, policy.ErrFirstContactDenied):
		return fmt.Errorf("first_contact_denied: %s", err.Error())
	case errors.Is(err, policy.ErrChatNotAllowed):
		return fmt.Errorf("chat_not_allowed: %s", err.Error())
	case errors.Is(err, policy.ErrPolicyRate):
		return fmt.Errorf("policy_rate_limited: %s", err.Error())
	case errors.Is(err, policy.ErrDestructiveDenied):
		return fmt.Errorf("destructive_denied: %s", err.Error())
	case errors.Is(err, policy.ErrDestructiveNeedsApproval):
		return fmt.Errorf("destructive_requires_approval: %s", err.Error())
	default:
		return err
	}
}
