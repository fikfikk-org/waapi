package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

// TargetResolver mengambil konfigurasi webhook terkini milik sebuah instance.
type TargetResolver func(ctx context.Context, instanceID string) (url, secret, eventsCSV string, err error)

// Envelope adalah struktur JSON standar yang dikirim ke endpoint webhook penerima.
type Envelope struct {
	Event     string `json:"event"`
	Instance  string `json:"instance"`
	Timestamp int64  `json:"timestamp"`
	Data      any    `json:"data"`
}

// Dispatcher dipertahankan sebagai lapisan tipis kompatibilitas konstruktor v1 yang hanya menyediakan TargetCache dan SendTest.
// Pengiriman event keluar sepenuhnya ditangani oleh Service berbasis journal v2.
type Dispatcher struct {
	store   *store.Store
	resolve TargetResolver
	cache   *TargetCache
	client  *http.Client
}

// NewDispatcher membuat lapisan tipis Dispatcher tanpa worker atau poller v1.
func NewDispatcher(st *store.Store, resolve TargetResolver, _ int) *Dispatcher {
	return &Dispatcher{
		store:   st,
		resolve: resolve,
		client:  defaultWebhookHTTPClient(),
	}
}

// SetTargetCache memasang cache target memori agar SendTest dan resolver tidak selalu menyentuh app.db.
func (d *Dispatcher) SetTargetCache(tc *TargetCache) {
	if d == nil {
		return
	}
	d.cache = tc
}

// Start dipertahankan sebagai no-op untuk kompatibilitas wiring konstruktor v1.
func (d *Dispatcher) Start(context.Context) {}

// Stop dipertahankan sebagai no-op untuk kompatibilitas wiring konstruktor v1.
func (d *Dispatcher) Stop() {}

// SendTest mengirim satu event percobaan secara sinkron memakai konfigurasi target dan format terkini.
func (d *Dispatcher) SendTest(ctx context.Context, instanceID string) (int, error) {
	if d == nil {
		return 0, fmt.Errorf("dispatcher webhook belum aktif")
	}
	target, err := d.resolveTarget(ctx, instanceID)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(target.URL) == "" {
		return 0, fmt.Errorf("webhook_url belum diset pada instance %s", instanceID)
	}
	now := time.Now()
	body, err := json.Marshal(Envelope{
		Event:     "test",
		Instance:  instanceID,
		Timestamp: now.Unix(),
		Data:      map[string]any{"message": "webhook test dari waapi"},
	})
	if err != nil {
		return 0, err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	status, _, _, sendErr := sendWebhookHTTP(reqCtx, d.client, target, instanceID, "test", 0, 1, now, body)
	return status, sendErr
}

func (d *Dispatcher) resolveTarget(ctx context.Context, instanceID string) (Target, error) {
	if d.cache != nil {
		if t, ok := d.cache.Get(instanceID); ok {
			return normalizeTarget(t), nil
		}
	}
	if d.store != nil {
		inst, err := d.store.GetInstance(ctx, instanceID)
		if err == nil && inst != nil {
			return normalizeTarget(Target{
				URL:         inst.WebhookURL,
				Secret:      inst.WebhookSecret,
				EventsCSV:   inst.WebhookEvents,
				Format:      inst.Settings.WebhookFormat,
				Lanes:       inst.Settings.WebhookLanes,
				MaxAttempts: inst.Settings.WebhookMaxAttempts,
				Paused:      inst.Settings.WebhookPaused,
			}), nil
		}
	}
	if d.resolve == nil {
		return Target{}, nil
	}
	u, sec, csv, err := d.resolve(ctx, instanceID)
	if err != nil {
		return Target{}, err
	}
	return normalizeTarget(Target{URL: u, Secret: sec, EventsCSV: csv}), nil
}
