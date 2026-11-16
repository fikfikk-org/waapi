package wa

// MessageV2 adalah representasi kanonik pesan WhatsApp yang disimpan di payload JSON journal v2.
type MessageV2 struct {
	ID             string           `json:"id"`
	From           string           `json:"from"`
	SenderPN       string           `json:"sender_pn,omitempty"`
	SenderLID      string           `json:"sender_lid,omitempty"`
	Chat           string           `json:"chat"`
	ChatRaw        string           `json:"chat_raw,omitempty"`
	ChatType       string           `json:"chat_type"`
	IsGroup        bool             `json:"is_group"`
	IsFromMe       bool             `json:"is_from_me"`
	PushName       string           `json:"push_name,omitempty"`
	AddressingMode string           `json:"addressing_mode,omitempty"`
	Type           string           `json:"type"`
	Timestamp      int64            `json:"timestamp"`
	Text           string           `json:"text,omitempty"`
	Truncated      bool             `json:"truncated,omitempty"`
	Media          *MediaDescriptor `json:"media,omitempty"`
	Location       *LocationInfo    `json:"location,omitempty"`
	Contacts       []ContactInfo    `json:"contacts,omitempty"`
	Poll           *PollInfo        `json:"poll,omitempty"`
	Reaction       *ReactionInfo    `json:"reaction,omitempty"`
	Edit           *EditInfo        `json:"edit,omitempty"`
	Revoke         *RevokeInfo      `json:"revoke,omitempty"`
	Quoted         *QuotedPayload   `json:"quoted,omitempty"`
	Mentions       []string         `json:"mentions,omitempty"`
	Flags          MsgFlags         `json:"flags,omitempty"`
}

// LocationInfo menyimpan koordinat dan deskripsi pesan lokasi atau live location.
type LocationInfo struct {
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	Name    string  `json:"name,omitempty"`
	Address string  `json:"address,omitempty"`
	URL     string  `json:"url,omitempty"`
}

// ContactInfo menyimpan nama tampilan dan vCard pada pesan kontak tunggal maupun array.
type ContactInfo struct {
	DisplayName string `json:"display_name,omitempty"`
	Vcard       string `json:"vcard,omitempty"`
}

// PollInfo menyimpan pertanyaan dan daftar opsi dari pembuatan jajak pendapat.
type PollInfo struct {
	Name       string   `json:"name"`
	Options    []string `json:"options"`
	Selectable uint32   `json:"selectable,omitempty"`
}

// ReactionInfo menyimpan detail reaksi emoji terhadap sebuah pesan target.
type ReactionInfo struct {
	TargetID    string `json:"target_id"`
	Emoji       string `json:"emoji,omitempty"`
	Removed     bool   `json:"removed,omitempty"`
	Participant string `json:"participant,omitempty"`
}

// EditInfo menyimpan ID pesan target dan teks revisinya dari ProtocolMessage MESSAGE_EDIT.
type EditInfo struct {
	TargetID string `json:"target_id"`
	Text     string `json:"text,omitempty"`
}

// RevokeInfo menyimpan ID pesan target yang dihapus (revoke) oleh pengirim.
type RevokeInfo struct {
	TargetID string `json:"target_id"`
}

// QuotedPayload menyimpan ringkasan pesan yang dikutip (reply) di payload journal.
type QuotedPayload struct {
	ID          string `json:"id"`
	Participant string `json:"participant,omitempty"`
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
}

// MsgFlags menyimpan bendera tambahan pada pesan masuk.
type MsgFlags struct {
	Forwarded bool `json:"forwarded,omitempty"`
	Offline   bool `json:"offline,omitempty"`
	Ephemeral bool `json:"ephemeral,omitempty"`
	ViewOnce  bool `json:"view_once,omitempty"`
}

// ReceiptV2 adalah representasi kanonik tanda terima (delivery/read/played) di journal v2.
type ReceiptV2 struct {
	Type       string   `json:"type"`
	Chat       string   `json:"chat"`
	ChatRaw    string   `json:"chat_raw,omitempty"`
	Sender     string   `json:"sender,omitempty"`
	SenderPN   string   `json:"sender_pn,omitempty"`
	MessageIDs []string `json:"message_ids"`
	Timestamp  int64    `json:"timestamp"`
	IsGroup    bool     `json:"is_group,omitempty"`
}

// UndecryptableV2 adalah representasi kanonik kegagalan dekripsi pesan masuk di journal v2.
type UndecryptableV2 struct {
	ID              string `json:"id"`
	Chat            string `json:"chat"`
	ChatRaw         string `json:"chat_raw,omitempty"`
	From            string `json:"from"`
	SenderPN        string `json:"sender_pn,omitempty"`
	Timestamp       int64  `json:"timestamp"`
	IsUnavailable   bool   `json:"is_unavailable,omitempty"`
	DecryptFailMode string `json:"decrypt_fail_mode,omitempty"`
}

// PresenceV2 adalah representasi event status online/terakhir-dilihat pengguna di journal v2.
type PresenceV2 struct {
	From        string `json:"from"`
	FromPN      string `json:"from_pn,omitempty"`
	Unavailable bool   `json:"unavailable"`
	LastSeen    int64  `json:"last_seen,omitempty"`
}

// ChatPresenceV2 adalah representasi event status mengetik/merekam di suatu chat pada journal v2.
type ChatPresenceV2 struct {
	Chat   string `json:"chat"`
	Sender string `json:"sender"`
	State  string `json:"state"`
	Media  string `json:"media,omitempty"`
}

// GroupChangeV2 merepresentasikan satu perubahan metadata atau keanggotaan dalam event grup v2.
type GroupChangeV2 struct {
	Action       string   `json:"action"`
	Participants []string `json:"participants,omitempty"`
	Value        string   `json:"value,omitempty"`
}

// GroupEventV2 adalah representasi kanonik event perubahan grup atau bergabung ke grup pada journal v2.
type GroupEventV2 struct {
	Group      string          `json:"group"`
	Actor      string          `json:"actor,omitempty"`
	ActorPN    string          `json:"actor_pn,omitempty"`
	Timestamp  int64           `json:"timestamp"`
	Changes    []GroupChangeV2 `json:"changes"`
	JoinReason string          `json:"join_reason,omitempty"`
	Name       string          `json:"name,omitempty"`
}

// CallEventV2 adalah representasi kanonik event panggilan masuk, penolakan otomatis, atau pemutusan panggilan.
type CallEventV2 struct {
	CallID string `json:"call_id"`
	From   string `json:"from"`
	FromPN string `json:"from_pn,omitempty"`
	Group  string `json:"group,omitempty"`
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
}
