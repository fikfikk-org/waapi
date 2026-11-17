package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/netguard"
	"github.com/fikfikk-org/waapi/internal/obs"
)

const (
	defaultAITimeout        = 20 * time.Second
	defaultAIMaxConcurrency = 2
	defaultAIMaxTokens      = 300
	maxAIResponseBytes      = 256 << 10 // 256 KiB batas baca respons provider AI
)

// ErrEmptyResponse dikembalikan ketika endpoint AI merespons HTTP 2xx namun isi pesan balasan kosong.
var ErrEmptyResponse = errors.New("balasan AI kosong")

// Config menampung konfigurasi provider Chat Completions kompatibel OpenAI.
type Config struct {
	BaseURL        string
	APIKey         string
	Model          string
	Timeout        time.Duration
	MaxConcurrency int
	HTTPClient     *http.Client // opsional khusus pengujian synctest/httptest
}

// ChatRequest adalah payload POST {WAAPI_AI_BASE_URL}/chat/completions tanpa kemampuan tools.
type ChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Temperature float64       `json:"temperature"`
	Stream      bool          `json:"stream"`
}

// ChatUsage merekam statistik pemakaian token opsional dari respons provider.
type ChatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// ChatResponse memetakan potongan respons Chat Completions yang dibutuhkan waapi.
type ChatResponse struct {
	Choices []struct {
		Message ChatMessage `json:"message"`
	} `json:"choices"`
	Usage *ChatUsage `json:"usage,omitempty"`
}

// Client mengeksekusi permintaan Chat Completions dengan pool koneksi privat-allowed & batas konkurensi.
type Client struct {
	cfg  Config
	http *http.Client
	sem  chan struct{}
}

// NewClient mengembalikan nil tanpa alokasi apapun jika BaseURL kosong, atau Client siap pakai jika terisi.
func NewClient(cfg Config) *Client {
	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		return nil
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultAITimeout
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = defaultAIMaxConcurrency
	}
	cfg.BaseURL = base
	cfg.Model = strings.TrimSpace(cfg.Model)

	httpCli := cfg.HTTPClient
	if httpCli == nil {
		// Base URL AI dikonfigurasi operator (bisa Ollama lokal 127.0.0.1:11434), sehingga AllowPrivate diizinkan.
		httpCli = &http.Client{
			Timeout: cfg.Timeout,
			Transport: netguard.NewTransport(netguard.Policy{
				AllowPrivate:          true,
				ResponseHeaderTimeout: cfg.Timeout,
			}),
		}
	}

	return &Client{
		cfg:  cfg,
		http: httpCli,
		sem:  make(chan struct{}, cfg.MaxConcurrency),
	}
}

// Complete mengirim prompt ke endpoint Chat Completions, mencatat metrik latensi/token, dan membatasi balasan 4096 karakter.
func (c *Client) Complete(ctx context.Context, r Rule, msgs []ChatMessage) (string, error) {
	if c == nil {
		return "", errors.New("client AI tidak aktif")
	}

	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		obs.MetricAIRequestsTotal.With("timeout").Inc()
		return "", fmt.Errorf("menunggu antrean AI dibatalkan: %w", ctx.Err())
	}

	model := strings.TrimSpace(r.Model)
	if model == "" {
		model = c.cfg.Model
	}
	if model == "" {
		obs.MetricAIRequestsTotal.With("error").Inc()
		return "", errors.New("model AI belum dikonfigurasi")
	}

	maxTok := r.MaxTokens
	if maxTok <= 0 {
		maxTok = defaultAIMaxTokens
	}
	if maxTok > 1024 {
		maxTok = 1024
	}

	payload := ChatRequest{
		Model:       model,
		Messages:    msgs,
		MaxTokens:   maxTok,
		Temperature: r.Temperature,
		Stream:      false,
	}
	rawBody, err := json.Marshal(payload)
	if err != nil {
		obs.MetricAIRequestsTotal.With("error").Inc()
		return "", fmt.Errorf("marshal request AI: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	endpoint := c.cfg.BaseURL + "/chat/completions"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(rawBody))
	if err != nil {
		obs.MetricAIRequestsTotal.With("error").Inc()
		return "", fmt.Errorf("bentuk request AI gagal")
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}

	start := time.Now()
	resp, err := c.http.Do(req)
	obs.MetricAILatencySeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		if reqCtx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			obs.MetricAIRequestsTotal.With("timeout").Inc()
			return "", fmt.Errorf("panggilan AI melebihi batas waktu %s", c.cfg.Timeout)
		}
		obs.MetricAIRequestsTotal.With("error").Inc()
		// Jangan pernah menempelkan *url.Error atau token secret ke error yang dikembalikan/dicatat.
		return "", errors.New("gagal menghubungi endpoint AI")
	}
	defer resp.Body.Close()

	respRaw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxAIResponseBytes))
	if readErr != nil {
		obs.MetricAIRequestsTotal.With("error").Inc()
		return "", errors.New("gagal membaca respons endpoint AI")
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		obs.MetricAIRequestsTotal.With("rate_limit").Inc()
		return "", errors.New("provider AI mengembalikan status 429 rate_limit")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		obs.MetricAIRequestsTotal.With("error").Inc()
		return "", fmt.Errorf("provider AI mengembalikan status HTTP %d", resp.StatusCode)
	}

	var parsed ChatResponse
	if err := json.Unmarshal(respRaw, &parsed); err != nil {
		obs.MetricAIRequestsTotal.With("error").Inc()
		return "", fmt.Errorf("parse JSON respons AI gagal: %w", err)
	}
	if parsed.Usage != nil {
		if parsed.Usage.PromptTokens > 0 {
			obs.MetricAITokensTotal.With("prompt").Add(float64(parsed.Usage.PromptTokens))
		}
		if parsed.Usage.CompletionTokens > 0 {
			obs.MetricAITokensTotal.With("completion").Add(float64(parsed.Usage.CompletionTokens))
		}
	}
	if len(parsed.Choices) == 0 {
		obs.MetricAIRequestsTotal.With("empty").Inc()
		return "", ErrEmptyResponse
	}

	reply := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if reply == "" {
		obs.MetricAIRequestsTotal.With("empty").Inc()
		return "", ErrEmptyResponse
	}
	reply = truncateRunes(reply, MaxReplyRunes)
	obs.MetricAIRequestsTotal.With("ok").Inc()
	return reply, nil
}
