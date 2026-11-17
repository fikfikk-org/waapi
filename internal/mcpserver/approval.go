package mcpserver

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	approvalTTL        = 5 * time.Minute
	maxApprovalPreview = 200
)

var (
	errApprovalRejected = errors.New("ditolak oleh manusia")
	errApprovalInvalid  = errors.New("token persetujuan tidak valid")
	errApprovalExpired  = errors.New("token persetujuan telah kedaluwarsa")
)

type approvalPayload struct {
	KID  string `json:"kid"`
	Inst string `json:"inst"`
	To   string `json:"to"`
	Hash string `json:"h"`
	Exp  int64  `json:"exp"`
}

// ApprovalSigner menandatangani dan memverifikasi token Multi Round-Trip Requests (HMAC-SHA256, TTL 5 menit).
type ApprovalSigner struct {
	secret []byte
}

// NewApprovalSigner membuat penanda tangan token persetujuan baru (mengisi secret acak 32 byte bila kosong).
func NewApprovalSigner(secret []byte) *ApprovalSigner {
	sec := make([]byte, 32)
	if len(secret) >= 32 {
		copy(sec, secret[:32])
	} else {
		_, _ = rand.Read(sec)
	}
	return &ApprovalSigner{secret: sec}
}

// HashSpec menghasilkan ringkasan SHA-256 terpotong 16 byte (32 hex) untuk mengikat argumen persetujuan.
func HashSpec(parts ...any) string {
	raw, _ := json.Marshal(parts)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:16])
}

// Sign menerbitkan token persetujuan bertanda tangan HMAC dengan masa berlaku 5 menit.
func (s *ApprovalSigner) Sign(kid, inst, to, specHash string) (string, error) {
	if s == nil || len(s.secret) == 0 {
		return "", errors.New("approval signer belum siap")
	}
	payload := approvalPayload{
		KID:  kid,
		Inst: inst,
		To:   to,
		Hash: specHash,
		Exp:  time.Now().Add(approvalTTL).Unix(),
	}
	rawJSON, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(rawJSON)
	mac := s.computeMAC(encoded)
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac), nil
}

// Verify memvalidasi integritas HMAC, kecocokan principal/instance/target/hash, dan masa kedaluwarsa token.
func (s *ApprovalSigner) Verify(token, kid, inst, to, specHash string) error {
	if s == nil || len(s.secret) == 0 {
		return errApprovalInvalid
	}
	encoded, sigB64, ok := strings.Cut(strings.TrimSpace(token), ".")
	if !ok || encoded == "" || sigB64 == "" {
		return errApprovalInvalid
	}
	gotSig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return errApprovalInvalid
	}
	wantSig := s.computeMAC(encoded)
	if !hmac.Equal(gotSig, wantSig) {
		return errApprovalInvalid
	}
	rawJSON, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return errApprovalInvalid
	}
	var payload approvalPayload
	if err := json.Unmarshal(rawJSON, &payload); err != nil {
		return errApprovalInvalid
	}
	if time.Now().Unix() > payload.Exp {
		return errApprovalExpired
	}
	if payload.KID != kid || payload.Inst != inst || payload.To != to || payload.Hash != specHash {
		return errApprovalInvalid
	}
	return nil
}

func (s *ApprovalSigner) computeMAC(encoded string) []byte {
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(encoded))
	return mac.Sum(nil)
}

func approvalInputResult(prompt, token string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		InputRequests: mcp.InputRequestMap{
			"approval": &mcp.ElicitParams{
				Message: prompt,
				RequestedSchema: &jsonschema.Schema{
					Type: "object",
					Properties: map[string]*jsonschema.Schema{
						"approve": {Type: "boolean"},
					},
					Required: []string{"approve"},
				},
			},
		},
		RequestState: token,
	}
}

func checkApprovalRetry(req *mcp.CallToolRequest, signer *ApprovalSigner, kid, inst, target, specHash string) (hasRetry, approved bool, err error) {
	if req == nil || req.Params == nil {
		return false, false, nil
	}
	if req.Params.RequestState == "" && len(req.Params.InputResponses) == 0 {
		return false, false, nil
	}
	if err := signer.Verify(req.Params.RequestState, kid, inst, target, specHash); err != nil {
		return true, false, err
	}
	respAny, ok := req.Params.InputResponses["approval"]
	if !ok || respAny == nil {
		return true, false, errApprovalInvalid
	}
	er, ok := respAny.(*mcp.ElicitResult)
	if !ok || er == nil {
		return true, false, errApprovalInvalid
	}
	if !strings.EqualFold(er.Action, "accept") {
		return true, false, errApprovalRejected
	}
	val, _ := er.Content["approve"].(bool)
	if !val {
		return true, false, errApprovalRejected
	}
	return true, true, nil
}

func clientSupportsElicitation(req *mcp.CallToolRequest) bool {
	if req == nil {
		return false
	}
	caps := req.ClientCapabilities()
	return caps != nil && caps.Elicitation != nil
}

func truncatePreview(s string, max int) string {
	s = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(strings.TrimSpace(s))
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	var count int
	for idx := range s {
		if count == max {
			return s[:idx]
		}
		count++
	}
	return s
}

func firstContactApprovalPrompt(to, preview string) string {
	return fmt.Sprintf("Agen meminta izin mengirim pesan ke kontak baru %s: %s. Setujui?", to, truncatePreview(preview, maxApprovalPreview))
}

func destructiveApprovalPrompt(action, target, summary string) string {
	return fmt.Sprintf("Agen meminta izin aksi destruktif %s ke %s: %s. Setujui?", action, target, truncatePreview(summary, maxApprovalPreview))
}
