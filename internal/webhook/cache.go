package webhook

import (
	"context"
	"sync"

	"github.com/fikfikk-org/waapi/internal/store"
)

// Target menyimpan konfigurasi pengiriman webhook ter-cache untuk satu instance.
type Target struct {
	URL         string
	Secret      string
	EventsCSV   string
	Format      string
	Lanes       int
	MaxAttempts int
	Paused      string
}

func normalizeTarget(t Target) Target {
	if t.Lanes <= 0 {
		t.Lanes = 4
	}
	if t.MaxAttempts <= 0 {
		t.MaxAttempts = 12
	}
	if t.Format == "" {
		t.Format = "waapi"
	}
	return t
}

// TargetCache menyediakan lookup target webhook di memori agar hot path bebas query app.db.
type TargetCache struct {
	mu      sync.RWMutex
	targets map[string]Target
}

// NewTargetCache membuat TargetCache kosong yang aman diakses secara konkuren.
func NewTargetCache() *TargetCache {
	return &TargetCache{
		targets: make(map[string]Target),
	}
}

// Load membaca seluruh konfigurasi instance dari store ke dalam cache memori.
func (tc *TargetCache) Load(ctx context.Context, st *store.Store) error {
	if tc == nil || st == nil {
		return nil
	}
	list, err := st.ListInstances(ctx)
	if err != nil {
		return err
	}
	next := make(map[string]Target, len(list))
	for i := range list {
		inst := list[i]
		next[inst.ID] = normalizeTarget(Target{
			URL:         inst.WebhookURL,
			Secret:      inst.WebhookSecret,
			EventsCSV:   inst.WebhookEvents,
			Format:      inst.Settings.WebhookFormat,
			Lanes:       inst.Settings.WebhookLanes,
			MaxAttempts: inst.Settings.WebhookMaxAttempts,
			Paused:      inst.Settings.WebhookPaused,
		})
	}
	tc.mu.Lock()
	tc.targets = next
	tc.mu.Unlock()
	return nil
}

// Get mengambil target webhook instance dari memori tanpa I/O.
func (tc *TargetCache) Get(instanceID string) (Target, bool) {
	if tc == nil {
		return Target{}, false
	}
	tc.mu.RLock()
	t, ok := tc.targets[instanceID]
	tc.mu.RUnlock()
	return t, ok
}

// Set memperbarui atau menyisipkan konfigurasi target webhook untuk satu instance.
func (tc *TargetCache) Set(instanceID string, t Target) {
	if tc == nil || instanceID == "" {
		return
	}
	t = normalizeTarget(t)
	tc.mu.Lock()
	if tc.targets == nil {
		tc.targets = make(map[string]Target)
	}
	tc.targets[instanceID] = t
	tc.mu.Unlock()
}

// SetFromInstance menyalin kolom webhook dan format dari store.Instance ke dalam cache.
func (tc *TargetCache) SetFromInstance(inst *store.Instance) {
	if tc == nil || inst == nil {
		return
	}
	tc.Set(inst.ID, Target{
		URL:         inst.WebhookURL,
		Secret:      inst.WebhookSecret,
		EventsCSV:   inst.WebhookEvents,
		Format:      inst.Settings.WebhookFormat,
		Lanes:       inst.Settings.WebhookLanes,
		MaxAttempts: inst.Settings.WebhookMaxAttempts,
		Paused:      inst.Settings.WebhookPaused,
	})
}

// SetPaused memperbarui status pause target di cache memori bila instance sudah ada.
func (tc *TargetCache) SetPaused(instanceID, reason string) {
	if tc == nil || instanceID == "" {
		return
	}
	tc.mu.Lock()
	if t, ok := tc.targets[instanceID]; ok {
		t.Paused = reason
		tc.targets[instanceID] = t
	}
	tc.mu.Unlock()
}

// Delete menghapus konfigurasi target webhook sebuah instance dari cache.
func (tc *TargetCache) Delete(instanceID string) {
	if tc == nil || instanceID == "" {
		return
	}
	tc.mu.Lock()
	delete(tc.targets, instanceID)
	tc.mu.Unlock()
}

// Resolve mengembalikan (url, secret, eventsCSV, nil) dari cache memori.
func (tc *TargetCache) Resolve(instanceID string) (url, secret, eventsCSV string, err error) {
	t, ok := tc.Get(instanceID)
	if !ok {
		return "", "", "", nil
	}
	return t.URL, t.Secret, t.EventsCSV, nil
}

// Resolver mengembalikan TargetResolver yang mengutamakan cache dan jatuh ke fallback bila belum ada di cache.
func (tc *TargetCache) Resolver(fallback TargetResolver) TargetResolver {
	return func(ctx context.Context, instanceID string) (string, string, string, error) {
		if t, ok := tc.Get(instanceID); ok {
			return t.URL, t.Secret, t.EventsCSV, nil
		}
		if fallback != nil {
			return fallback(ctx, instanceID)
		}
		return "", "", "", nil
	}
}
