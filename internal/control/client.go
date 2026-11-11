package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
)

// Sentinel error internal control plane.
var (
	ErrInstanceNotFound     = errors.New("instance tidak ditemukan")
	ErrStreamingUnsupported = errors.New("streaming tidak didukung oleh response writer")
)

// Client adalah HTTP client yang selalu terhubung ke Unix Domain Socket daemon.
type Client struct {
	sock string
	http *http.Client
}

// NewClient membuat Client baru yang mengarah ke path socket control.sock.
func NewClient(sock string) *Client {
	return &Client{
		sock: sock,
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", sock)
				},
			},
			// Tanpa timeout global agar aliran pairing QR tidak terputus; gunakan context per perintah
			Timeout: 0,
		},
	}
}

// DoJSON mengirim request JSON ke control socket dan men-decode hasilnya ke out.
func (c *Client) DoJSON(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://control"+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("daemon tidak merespons di %s (pastikan service waapi sedang berjalan): %w", c.sock, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errBody struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		if errBody.Error != "" {
			return errors.New(errBody.Error)
		}
		return fmt.Errorf("control error: HTTP %d", resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// OpenPairStream membuka koneksi streaming NDJSON untuk proses pairing instance.
func (c *Client) OpenPairStream(ctx context.Context, id, phone string) (*http.Response, error) {
	b, _ := json.Marshal(pairReq{Phone: phone})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://control/control/instances/"+id+"/pair", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("daemon tidak merespons di %s (pastikan service waapi sedang berjalan): %w", c.sock, err)
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		var errBody struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		if errBody.Error != "" {
			return nil, errors.New(errBody.Error)
		}
		return nil, fmt.Errorf("pair gagal: HTTP %d", resp.StatusCode)
	}
	return resp, nil
}

