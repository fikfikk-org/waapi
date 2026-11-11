package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

const (
	maxPayloadBytes = 64 << 10 // batas maksimum payload webhook 64 KB
	maxAttempts     = 8        // maksimal percobaan ulang sebelum dead-letter
	retentionSec    = 24 * 3600
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

type job struct {
	outboxID   int64
	instanceID string
	event      string
	payload    []byte
	attempts   int
}

// Dispatcher mengelola worker pool bounded dan outbox poller di SQLite.
type Dispatcher struct {
	store   *store.Store
	resolve TargetResolver
	client  *http.Client
	ch      chan job
	workers int
	wg      sync.WaitGroup
}

// NewDispatcher membuat Dispatcher dengan antrian memori terikat (512 item).
func NewDispatcher(st *store.Store, resolve TargetResolver, workers int) *Dispatcher {
	if workers <= 0 {
		workers = 4
	}
	return &Dispatcher{
		store:   st,
		resolve: resolve,
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        16,
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		ch:      make(chan job, 512),
		workers: workers,
	}
}

// Start menyalakan worker pengirim dan poller outbox.
func (d *Dispatcher) Start(ctx context.Context) {
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.workerLoop(ctx)
		}()
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.pollerLoop(ctx)
	}()
}

// Stop menunggu seluruh worker selesai dan memindahkan sisa item di channel ke outbox disk.
func (d *Dispatcher) Stop() {
	for {
		select {
		case j := <-d.ch:
			if j.outboxID == 0 {
				_ = d.store.EnqueueOutbox(context.Background(), j.instanceID, j.event, j.payload, j.attempts, time.Now().Unix())
			}
		default:
			d.wg.Wait()
			return
		}
	}
}

// Enqueue mengemas event dan mengirimkannya secara non-blocking ke channel worker atau spill ke disk.
func (d *Dispatcher) Enqueue(instanceID, event string, data any) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	url, _, eventsCSV, err := d.resolve(ctx, instanceID)
	if err != nil || strings.TrimSpace(url) == "" {
		return
	}
	if !eventAllowed(eventsCSV, event) {
		return
	}

	env := Envelope{
		Event:     event,
		Instance:  instanceID,
		Timestamp: time.Now().Unix(),
		Data:      data,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		slog.Warn("gagal marshal payload webhook", "instance", instanceID, "err", err)
		return
	}
	if len(raw) > maxPayloadBytes {
		// Potong payload besar agar tidak melanggar budget memori/disk
		env.Data = map[string]any{"truncated": true, "note": "payload melebihi 64KB"}
		raw, _ = json.Marshal(env)
	}

	j := job{
		instanceID: instanceID,
		event:      event,
		payload:    raw,
		attempts:   0,
	}
	select {
	case d.ch <- j:
	default:
		// Antrian memori penuh -> langsung spill ke outbox disk
		if err := d.store.EnqueueOutbox(ctx, instanceID, event, raw, 0, time.Now().Unix()); err != nil {
			slog.Warn("gagal spill webhook ke outbox", "instance", instanceID, "err", err)
		}
	}
}

// SendTest mengirim satu event percobaan secara sinkron untuk perintah CLI webhook test.
func (d *Dispatcher) SendTest(ctx context.Context, instanceID string) (int, error) {
	url, secret, _, err := d.resolve(ctx, instanceID)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(url) == "" {
		return 0, fmt.Errorf("webhook_url belum diset pada instance %s", instanceID)
	}
	now := time.Now().Unix()
	body, err := json.Marshal(Envelope{
		Event:     "test",
		Instance:  instanceID,
		Timestamp: now,
		Data:      map[string]any{"message": "webhook test dari waapi"},
	})
	if err != nil {
		return 0, err
	}
	return d.deliverHTTP(ctx, url, secret, instanceID, "test", now, body)
}

func (d *Dispatcher) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-d.ch:
			d.handleJob(ctx, j)
		}
	}
}

func (d *Dispatcher) handleJob(ctx context.Context, j job) {
	url, secret, _, err := d.resolve(ctx, j.instanceID)
	if err != nil || strings.TrimSpace(url) == "" {
		if j.outboxID != 0 {
			_ = d.store.DeleteOutbox(ctx, j.outboxID)
		}
		return
	}

	now := time.Now().Unix()
	code, err := d.deliverHTTP(ctx, url, secret, j.instanceID, j.event, now, j.payload)
	if err == nil && code >= 200 && code < 300 {
		if j.outboxID != 0 {
			_ = d.store.DeleteOutbox(ctx, j.outboxID)
		}
		return
	}

	attempts := j.attempts + 1
	if attempts >= maxAttempts {
		slog.Warn("webhook dead-letter setelah batas retry", "instance", j.instanceID, "event", j.event, "status", code, "err", err)
		if j.outboxID != 0 {
			_ = d.store.DeleteOutbox(ctx, j.outboxID)
		}
		return
	}

	nextRetry := now + backoffSeconds(attempts)
	if j.outboxID == 0 {
		_ = d.store.EnqueueOutbox(ctx, j.instanceID, j.event, j.payload, attempts, nextRetry)
	} else {
		_ = d.store.RescheduleOutbox(ctx, j.outboxID, attempts, nextRetry)
	}
}

func (d *Dispatcher) deliverHTTP(ctx context.Context, url, secret, instanceID, event string, ts int64, body []byte) (int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "waapi-webhook/1.0")
	req.Header.Set("X-WA-Event", event)
	req.Header.Set("X-WA-Instance", instanceID)
	req.Header.Set("X-WA-Timestamp", strconv.FormatInt(ts, 10))
	if secret != "" {
		req.Header.Set("X-WA-Signature", Sign(secret, ts, body))
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, nil
}

func (d *Dispatcher) pollerLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().Unix()
			_ = d.store.PruneOutbox(ctx, maxAttempts, now-retentionSec)
			items, err := d.store.ClaimDueOutbox(ctx, now, 32)
			if err != nil {
				continue
			}
			for _, it := range items {
				// Geser next_retry_at sementara agar tidak di-claim ganda oleh siklus poller berikutnya
				_ = d.store.RescheduleOutbox(ctx, it.ID, it.Attempts, now+30)
				select {
				case d.ch <- job{
					outboxID:   it.ID,
					instanceID: it.InstanceID,
					event:      it.Event,
					payload:    it.Payload,
					attempts:   it.Attempts,
				}:
				default:
				}
			}
		}
	}
}

// backoffSeconds menghitung min(2^attempts * 5s, 30m) dengan jitter +-20%.
func backoffSeconds(attempts int) int64 {
	shift := attempts
	if shift > 9 {
		shift = 9
	}
	base := int64(5) * (int64(1) << shift)
	if base > 1800 {
		base = 1800
	}
	jitter := (rand.Float64()*0.4 - 0.2) * float64(base)
	return base + int64(jitter)
}

func eventAllowed(csv, event string) bool {
	if strings.TrimSpace(csv) == "" || csv == "*" {
		return true
	}
	for _, p := range strings.Split(csv, ",") {
		if strings.EqualFold(strings.TrimSpace(p), event) {
			return true
		}
	}
	return false
}

