package ai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/ctxstore"
)

// TestAIClient_BentukRequest memverifikasi request HTTP Chat Completions OpenAI tanpa tools beserta header bearer dan pembacaan usage.
func TestAIClient_BentukRequest(t *testing.T) {
	var (
		gotAuth    string
		gotPath    string
		gotRawBody []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotRawBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices": [{"message": {"role": "assistant", "content": "  Halo, toko buka 08.00-17.00 WIB.  "}}],
			"usage": {"prompt_tokens": 25, "completion_tokens": 11}
		}`))
	}))
	defer srv.Close()

	cli := NewClient(Config{
		BaseURL:        srv.URL + "/v1/",
		APIKey:         "sk-uji-rahasia-12345",
		Model:          "gpt-4o-mini",
		Timeout:        5 * time.Second,
		MaxConcurrency: 2,
	})
	if cli == nil {
		t.Fatalf("client AI seharusnya terbentuk saat BaseURL terisi")
	}

	reply, err := cli.Complete(context.Background(), Rule{
		MaxTokens:   250,
		Temperature: 0.25,
	}, []ChatMessage{
		{Role: "system", Content: "Anda CS."},
		{Role: "user", Content: "Jam buka?"},
	})
	if err != nil {
		t.Fatalf("Complete gagal: %v", err)
	}
	if reply != "Halo, toko buka 08.00-17.00 WIB." {
		t.Fatalf("balasan AI tidak ter-trim dengan benar: %q", reply)
	}
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("path endpoint salah: %q", gotPath)
	}
	if gotAuth != "Bearer sk-uji-rahasia-12345" {
		t.Fatalf("header Authorization salah: %q", gotAuth)
	}

	var parsed map[string]any
	if err := json.Unmarshal(gotRawBody, &parsed); err != nil {
		t.Fatalf("body JSON tidak valid: %v", err)
	}
	if parsed["model"] != "gpt-4o-mini" || parsed["stream"] != false {
		t.Fatalf("model/stream tidak sesuai: %+v", parsed)
	}
	if int(parsed["max_tokens"].(float64)) != 250 {
		t.Fatalf("max_tokens salah: %+v", parsed["max_tokens"])
	}
	if _, hasTools := parsed["tools"]; hasTools {
		t.Fatalf("model AI dilarang diberi tools: %s", string(gotRawBody))
	}
	if _, hasToolChoice := parsed["tool_choice"]; hasToolChoice {
		t.Fatalf("model AI dilarang diberi tool_choice: %s", string(gotRawBody))
	}

	// Saat server error, API key tidak boleh pernah bocor ke pesan error.
	srvErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid api key"}`))
	}))
	defer srvErr.Close()
	cliErr := NewClient(Config{BaseURL: srvErr.URL, APIKey: "sk-rahasia-jangan-bocor", Model: "m"})
	_, cErr := cliErr.Complete(context.Background(), Rule{}, []ChatMessage{{Role: "user", Content: "hi"}})
	if cErr == nil || strings.Contains(cErr.Error(), "sk-rahasia-jangan-bocor") {
		t.Fatalf("error harus ada tanpa membocorkan API key: %v", cErr)
	}
}

// TestPrompt_PeranDanBudget memverifikasi pemetaan role user/assistant, awalan pengirim grup, pemotongan 1000/8000, serta sufiks keamanan.
func TestPrompt_PeranDanBudget(t *testing.T) {
	// Bangun 10 pesan riwayat masing-masing ~950 karakter -> harus dipangkas dari pesan tertua agar total <= 8000 karakter.
	history := make([]ctxstore.Message, 0, 10)
	for i := 0; i < 10; i++ {
		prefix := "OLD_"
		if i >= 5 {
			prefix = "NEW_"
		}
		txt := prefix + strings.Repeat(string(rune('A'+i)), 940)
		history = append(history, ctxstore.Message{
			ID:     "H_" + string(rune('0'+i)),
			From:   "6281110001@s.whatsapp.net",
			FromMe: i%2 == 1,
			Text:   txt,
		})
	}
	// Pesan >1000 karakter harus dipotong menjadi 1000 karakter.
	history = append(history, ctxstore.Message{
		ID:     "LONG",
		From:   "6281110002@s.whatsapp.net",
		FromMe: false,
		Text:   strings.Repeat("X", 1500),
	})

	msgs := BuildMessages(
		Rule{System: "Anda CS Toko X.", ContextMessages: 20},
		history,
		Trigger{Chat: "120363011@g.us", SenderName: "Budi", Text: "Apakah stok tersedia?", IsGroup: true},
	)

	if len(msgs) < 3 {
		t.Fatalf("pesan minimal berisi system + trigger, didapat %d", len(msgs))
	}
	if msgs[0].Role != "system" || !strings.HasPrefix(msgs[0].Content, "Anda CS Toko X.") || !strings.Contains(msgs[0].Content, SecurityPromptSuffix) {
		t.Fatalf("system prompt atau sufiks keamanan tidak sesuai: %+v", msgs[0])
	}

	last := msgs[len(msgs)-1]
	if last.Role != "user" || last.Content != "[Budi]: Apakah stok tersedia?" {
		t.Fatalf("format trigger pesan grup tidak sesuai: %+v", last)
	}

	// Hitung total rune dan verifikasi pesan tertua (OLD_0 / AAAA) terbuang, sedangkan pesan terbaru & LONG ada & <=1000 rune.
	totalRunes := 0
	var sawOldZero, sawGroupPrefix, sawBotAssistant bool
	for _, m := range msgs {
		rc := len([]rune(m.Content))
		totalRunes += rc
		if m.Role != "system" && rc > MaxMessageRunes {
			t.Fatalf("pesan individu melebihi %d karakter: %d", MaxMessageRunes, rc)
		}
		if strings.Contains(m.Content, "OLD_AAAAA") {
			sawOldZero = true
		}
		if m.Role == "user" && strings.HasPrefix(m.Content, "[6281110002]: ") {
			sawGroupPrefix = true
		}
		if m.Role == "assistant" {
			sawBotAssistant = true
		}
	}
	if sawOldZero {
		t.Fatalf("pesan riwayat tertua seharusnya terbuang saat melebihi anggaran 8000 karakter")
	}
	if !sawGroupPrefix || !sawBotAssistant {
		t.Fatalf("awalan grup (%v) atau role assistant from_me (%v) tidak ditemukan", sawGroupPrefix, sawBotAssistant)
	}
	if totalRunes > MaxTotalPromptRunes {
		t.Fatalf("total karakter prompt %d melebihi batas %d", totalRunes, MaxTotalPromptRunes)
	}
}
