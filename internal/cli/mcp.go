package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/fikfikk-org/waapi/internal/control"
)

const (
	defaultMCPMaxInflight = 8
	maxMCPStdioLineBytes  = 1 << 20 // 1 MiB batas per baris JSON-RPC stdin
)

// runMCP menjalankan bridge stdio<->UDS menuju /control/mcp pada daemon lokal.
func runMCP(ctx context.Context, ctrl *control.Client, args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	keyID := fs.String("key", "", "ID API key opsional untuk mewarisi scope & policy MCP")
	maxInflight := fs.Int("max-inflight", defaultMCPMaxInflight, "Batas request MCP bersamaan lewat control.sock")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *maxInflight < 1 {
		return errors.New("nilai --max-inflight harus >= 1")
	}

	httpClient := ctrl.HTTPClient()
	if httpClient == nil {
		return errors.New("HTTP client control socket tidak tersedia")
	}
	return runMCPBridge(ctx, os.Stdin, os.Stdout, httpClient, "http://control/control/mcp", strings.TrimSpace(*keyID), *maxInflight)
}

type mcpRPCEnvelope struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
}

type mcpMetaParams struct {
	ProtocolVersion string            `json:"protocolVersion,omitempty"`
	Name            string            `json:"name,omitempty"`
	URI             string            `json:"uri,omitempty"`
	Meta            map[string]any    `json:"_meta,omitempty"`
}

type mcpInitResultEnvelope struct {
	Result struct {
		ProtocolVersion string `json:"protocolVersion,omitempty"`
	} `json:"result"`
}

type mcpBridgeState struct {
	mu           sync.Mutex
	sessionID    string
	protoVersion string
}

func (s *mcpBridgeState) headersSnapshot(reqProto string) (sessionID, proto string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reqProto != "" {
		proto = reqProto
	} else {
		proto = s.protoVersion
	}
	return s.sessionID, proto
}

func (s *mcpBridgeState) updateFromInitRequest(reqProto string) {
	if reqProto == "" {
		return
	}
	s.mu.Lock()
	s.protoVersion = reqProto
	s.mu.Unlock()
}

func (s *mcpBridgeState) updateFromResponse(resp *http.Response, body []byte) {
	sid := strings.TrimSpace(resp.Header.Get("Mcp-Session-Id"))
	var initRes mcpInitResultEnvelope
	if len(body) > 0 {
		_ = json.Unmarshal(body, &initRes)
	}
	proto := strings.TrimSpace(initRes.Result.ProtocolVersion)
	if sid == "" && proto == "" {
		return
	}
	s.mu.Lock()
	if sid != "" {
		s.sessionID = sid
	}
	if proto != "" {
		s.protoVersion = proto
	}
	s.mu.Unlock()
}

// runMCPBridge menjembatani baris JSON-RPC dari stdin ke endpoint HTTP/UDS dan menulis respons ke stdout.
func runMCPBridge(ctx context.Context, r io.Reader, w io.Writer, client *http.Client, baseURL, keyID string, maxInflight int) error {
	if client == nil {
		return errors.New("HTTP client bridge tidak boleh nil")
	}
	if maxInflight < 1 {
		maxInflight = defaultMCPMaxInflight
	}

	bridgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	scanner := bufio.NewScanner(r)
	// Batasi buffer line tepat 1 MiB agar baris tak wajar gagal secara eksplisit dan aman memori.
	scanner.Buffer(make([]byte, 64*1024), maxMCPStdioLineBytes)

	sem := make(chan struct{}, maxInflight)
	state := &mcpBridgeState{}
	var outMu sync.Mutex
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var writeErr error

	setFatalErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if writeErr == nil {
			writeErr = err
		}
		errMu.Unlock()
		cancel()
	}

	writeLine := func(line []byte) error {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			return nil
		}
		outMu.Lock()
		defer outMu.Unlock()
		if _, err := w.Write(trimmed); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
		return nil
	}

	dispatch := func(lineBytes []byte, env mcpRPCEnvelope) error {
		err := forwardMCPLine(bridgeCtx, client, baseURL, keyID, lineBytes, env, state, writeLine)
		if err != nil {
			// Error transport lokal dicatat ke stderr (stdout khusus JSON-RPC); hentikan bridge jika writer gagal.
			var wErr *bridgeWriteError
			if errors.As(err, &wErr) {
				setFatalErr(wErr.err)
				return wErr.err
			}
			fmt.Fprintf(os.Stderr, "waapi mcp: gagal meneruskan %s: %v\n", env.Method, err)
		}
		return nil
	}

	for scanner.Scan() {
		if bridgeCtx.Err() != nil {
			break
		}
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		lineCopy := make([]byte, len(raw))
		copy(lineCopy, raw)

		var env mcpRPCEnvelope
		_ = json.Unmarshal(lineCopy, &env)

		// Jalankan initialize & notifikasi handshake berurutan agar session ID & versi protokol konsisten.
		if env.Method == "initialize" || strings.HasPrefix(env.Method, "notifications/") {
			if err := dispatch(lineCopy, env); err != nil {
				break
			}
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-bridgeCtx.Done():
			wg.Wait()
			return nil
		}

		wg.Add(1)
		go func(payload []byte, meta mcpRPCEnvelope) {
			defer func() {
				<-sem
				wg.Done()
			}()
			_ = dispatch(payload, meta)
		}(lineCopy, env)
	}

	scanErr := scanner.Err()
	if scanErr != nil {
		// Error pembacaan atau baris melebihi 1 MiB membatalkan request berjalan dan mengembalikan error.
		cancel()
		wg.Wait()
		if errors.Is(scanErr, bufio.ErrTooLong) {
			return fmt.Errorf("baris JSON-RPC stdin melebihi batas %d byte", maxMCPStdioLineBytes)
		}
		return fmt.Errorf("baca stdin mcp: %w", scanErr)
	}

	// Saat stdin mencapai EOF normal, tunggu respons in-flight lalu batalkan sisa konteks.
	wg.Wait()
	cancel()

	errMu.Lock()
	defer errMu.Unlock()
	if writeErr != nil {
		return fmt.Errorf("tulis stdout mcp: %w", writeErr)
	}
	return nil
}

type bridgeWriteError struct {
	err error
}

func (e *bridgeWriteError) Error() string { return e.err.Error() }

func forwardMCPLine(
	ctx context.Context,
	client *http.Client,
	baseURL, keyID string,
	raw []byte,
	env mcpRPCEnvelope,
	state *mcpBridgeState,
	writeLine func([]byte) error,
) error {
	var params mcpMetaParams
	if len(env.Params) > 0 {
		_ = json.Unmarshal(env.Params, &params)
	}

	reqProto := extractRequestProtocolVersion(env.Method, params)
	if env.Method == "initialize" {
		state.updateFromInitRequest(params.ProtocolVersion)
	}
	sessionID, proto := state.headersSnapshot(reqProto)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if proto != "" {
		req.Header.Set("MCP-Protocol-Version", proto)
	}
	if env.Method != "" {
		req.Header.Set("Mcp-Method", env.Method)
	}
	if name := extractMCPTargetName(env.Method, params); name != "" {
		req.Header.Set("Mcp-Name", encodeMCPHeaderName(name))
	}
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}
	if keyID != "" {
		req.Header.Set("X-Waapi-Key-Id", keyID)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// HTTP 202 Accepted (notifikasi/respons klien): simpan session header jika ada, tanpa output stdout.
	if resp.StatusCode == http.StatusAccepted {
		state.updateFromResponse(resp, nil)
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil
	}

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.HasPrefix(ct, "text/event-stream") {
		state.updateFromResponse(resp, nil)
		return streamSSEResponse(resp.Body, state, resp, writeLine)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	state.updateFromResponse(resp, body)
	if len(bytes.TrimSpace(body)) == 0 {
		if resp.StatusCode >= 400 {
			return fmt.Errorf("status HTTP %d tanpa body", resp.StatusCode)
		}
		return nil
	}
	if err := writeLine(body); err != nil {
		return &bridgeWriteError{err: err}
	}
	return nil
}

func streamSSEResponse(r io.Reader, state *mcpBridgeState, resp *http.Response, writeLine func([]byte) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 16*1024), 4<<20)

	var dataLines [][]byte
	flushEvent := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		joined := bytes.Join(dataLines, []byte("\n"))
		dataLines = dataLines[:0]
		trimmed := bytes.TrimSpace(joined)
		if len(trimmed) == 0 {
			return nil
		}
		state.updateFromResponse(resp, trimmed)
		// Pastikan payload dikirim sebagai satu baris JSON-RPC utuh di stdout.
		var compact bytes.Buffer
		if err := json.Compact(&compact, trimmed); err == nil {
			if err := writeLine(compact.Bytes()); err != nil {
				return &bridgeWriteError{err: err}
			}
			return nil
		}
		if err := writeLine(trimmed); err != nil {
			return &bridgeWriteError{err: err}
		}
		return nil
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			if err := flushEvent(); err != nil {
				return err
			}
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			val := bytes.TrimSpace(line[len("data:"):])
			cp := make([]byte, len(val))
			copy(cp, val)
			dataLines = append(dataLines, cp)
		}
	}
	if err := flushEvent(); err != nil {
		return err
	}
	return scanner.Err()
}

func extractRequestProtocolVersion(method string, params mcpMetaParams) string {
	if params.Meta != nil {
		if v, ok := params.Meta["io.modelcontextprotocol/protocolVersion"].(string); ok {
			if v = strings.TrimSpace(v); v != "" {
				return v
			}
		}
	}
	if method == "initialize" {
		return strings.TrimSpace(params.ProtocolVersion)
	}
	return ""
}

func extractMCPTargetName(method string, params mcpMetaParams) string {
	switch method {
	case "tools/call", "prompts/get":
		return strings.TrimSpace(params.Name)
	case "resources/read":
		return strings.TrimSpace(params.URI)
	default:
		if strings.TrimSpace(params.Name) != "" {
			return strings.TrimSpace(params.Name)
		}
		return ""
	}
}

func encodeMCPHeaderName(s string) string {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b < 0x20 || b >= 0x7f {
			return "=?base64?" + base64.StdEncoding.EncodeToString([]byte(s)) + "?="
		}
	}
	return s
}
