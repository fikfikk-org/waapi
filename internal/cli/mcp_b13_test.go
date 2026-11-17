package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStdioBridge_RoundTrip memverifikasi bridge stdio->UDS meneruskan header MCP 2026-07-28, JSON, SSE, dan mengabaikan 202.
func TestStdioBridge_RoundTrip(t *testing.T) {
	var mu sync.Mutex
	type seenReq struct {
		method    string
		proto     string
		mcpMethod string
		mcpName   string
		keyID     string
		sessionID string
	}
	var seen []seenReq

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, seenReq{
			method:    r.Method,
			proto:     r.Header.Get("MCP-Protocol-Version"),
			mcpMethod: r.Header.Get("Mcp-Method"),
			mcpName:   r.Header.Get("Mcp-Name"),
			keyID:     r.Header.Get("X-Waapi-Key-Id"),
			sessionID: r.Header.Get("Mcp-Session-Id"),
		})
		mu.Unlock()

		var req struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &req)

		switch req.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Mcp-Session-Id", "sess-legacy-1")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25"}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/call":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"ok\":true}}\n\n"))
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":3,"result":{"tools":[]}}`))
		}
	}))
	defer ts.Close()

	inReader, inWriter := io.Pipe()
	var outBuf bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- runMCPBridge(context.Background(), inReader, &outBuf, ts.Client(), ts.URL, "k_bridge_1", 4)
	}()

	msgs := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_instances","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}`,
	}, "\n") + "\n"
	_, _ = io.WriteString(inWriter, msgs)
	_ = inWriter.Close()

	if err := <-errCh; err != nil {
		t.Fatalf("runMCPBridge gagal: %v", err)
	}

	outLines := strings.Split(strings.TrimSpace(outBuf.String()), "\n")
	// 4 input, 1 bernilai 202 (notifications/initialized) -> harus tepat 3 baris output
	if len(outLines) != 3 {
		t.Fatalf("diharapkan 3 baris output (202 diabaikan), didapat %d: %q", len(outLines), outBuf.String())
	}
	var gotCallOut, gotListOut bool
	for _, line := range outLines {
		if strings.Contains(line, `"id":2`) && strings.Contains(line, `"ok":true`) {
			gotCallOut = true
		}
		if strings.Contains(line, `"id":3`) && strings.Contains(line, `"tools":[]`) {
			gotListOut = true
		}
	}
	if !gotCallOut || !gotListOut {
		t.Fatalf("respons JSON/SSE untuk id=2 dan id=3 tidak lengkap: %q", outBuf.String())
	}
	if len(seen) != 4 {
		t.Fatalf("server seharusnya menerima 4 POST, didapat %d", len(seen))
	}
	if seen[0].mcpMethod != "initialize" || seen[0].proto != "2025-11-25" {
		t.Fatalf("request pertama harus initialize dengan proto 2025-11-25: %+v", seen[0])
	}
	if seen[1].mcpMethod != "notifications/initialized" || seen[1].sessionID != "sess-legacy-1" {
		t.Fatalf("Mcp-Session-Id dari initialize harus diteruskan pada notifications/initialized: %+v", seen[1])
	}
	var seenCall bool
	for _, req := range seen {
		if req.mcpMethod == "tools/call" {
			seenCall = true
			if req.proto != "2026-07-28" || req.mcpName != "list_instances" || req.keyID != "k_bridge_1" {
				t.Fatalf("header HTTP tools/call tidak sesuai: %+v", req)
			}
		}
	}
	if !seenCall {
		t.Fatalf("server tidak menerima tools/call: %+v", seen)
	}
}

// TestStdioBridge_ConcurrencyDanBatasBaris memastikan maxInflight berjalan paralel dan baris >1 MiB ditolak.
func TestStdioBridge_ConcurrencyDanBatasBaris(t *testing.T) {
	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	unblockFirst := make(chan struct{})
	firstStarted := make(chan struct{})
	var once sync.Once

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inFlight.Add(1)
		for {
			prev := maxSeen.Load()
			if cur <= prev || maxSeen.CompareAndSwap(prev, cur) {
				break
			}
		}
		defer inFlight.Add(-1)

		var req struct {
			ID any `json:"id"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &req)
		if idNum, ok := req.ID.(float64); ok && idNum == 10 {
			once.Do(func() { close(firstStarted) })
			<-unblockFirst
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"ok":true}}`))
	}))
	defer ts.Close()

	inReader, inWriter := io.Pipe()
	var outBuf bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- runMCPBridge(context.Background(), inReader, &outBuf, ts.Client(), ts.URL, "", 2)
	}()

	_, _ = io.WriteString(inWriter, `{"jsonrpc":"2.0","id":10,"method":"tools/list","params":{}}`+"\n")
	<-firstStarted
	_, _ = io.WriteString(inWriter, `{"jsonrpc":"2.0","id":11,"method":"tools/list","params":{}}`+"\n")
	// Tunggu sejenak agar id=11 berjalan bersamaan dengan id=10 yang masih tertahan.
	deadline := time.Now().Add(time.Second)
	for maxSeen.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	close(unblockFirst)
	_ = inWriter.Close()
	if err := <-errCh; err != nil {
		t.Fatalf("bridge concurrency gagal: %v", err)
	}
	if maxSeen.Load() < 2 {
		t.Fatalf("diharapkan >=2 request berjalan paralel dengan maxInflight=2, didapat %d", maxSeen.Load())
	}

	// Baris melebihi 1 MiB harus ditolak dengan error.
	oversize := strings.Repeat("a", maxMCPStdioLineBytes+128) + "\n"
	var out2 bytes.Buffer
	if err := runMCPBridge(context.Background(), strings.NewReader(oversize), &out2, ts.Client(), ts.URL, "", 2); err == nil {
		t.Fatalf("baris >1 MiB wajib ditolak dengan error")
	}
}
