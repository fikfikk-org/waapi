package outbox_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/ctxstore"
	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/outbox"
	"github.com/fikfikk-org/waapi/internal/store"
)

type obContextSettings struct {
	mu      sync.RWMutex
	enabled bool
}

func (s *obContextSettings) ContextConfig(_ context.Context, _ string) (bool, int, time.Duration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled, 50, 168 * time.Hour, nil
}

// TestOutbox_RecordOutboundSaatSent memastikan outbox mencatat pesan keluar, menerapkan edit/revoke saat sent, dan melewati reaction.
func TestOutbox_RecordOutboundSaatSent(t *testing.T) {
	ctx := context.Background()
	j, err := journal.Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{MaxConns: 2, TxLock: "immediate"})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	cfg := &obContextSettings{enabled: true}
	cs := ctxstore.New(j.DB(), cfg)
	exec := &fakeExecutor{ready: true, paired: true}

	svc := outbox.NewService(outbox.NewSQLiteStore(j.DB()), j, exec, nil, nil, outbox.Options{
		ContextStore:      cs,
		DisableSendJitter: true,
	})
	svcCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	svc.StartScheduler(svcCtx, "sales")
	defer svc.Stop()
	svc.NotifyReady("sales")

	to := "6281234567890"
	chat := to + "@s.whatsapp.net"

	// 1. Kirim teks -> StatusSent -> masuk context store
	job1, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Origin:     outbox.Origin{Kind: "api", KeyID: "k1"},
		Spec:       outbox.Spec{Type: "text", To: to, Text: "halo pelanggan lama"},
	})
	if err != nil {
		t.Fatal(err)
	}
	done1, err := svc.Wait(ctx, "sales", job1.ID, outbox.StatusSent)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Kirim reaction -> StatusSent -> tidak boleh masuk sebagai baris pesan di context store
	jobReact, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Origin:     outbox.Origin{Kind: "api", KeyID: "k1"},
		Spec:       outbox.Spec{Type: "reaction", To: to, Reaction: &outbox.Reaction{MessageID: done1.WAID, Emoji: "❤️"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Wait(ctx, "sales", jobReact.ID, outbox.StatusSent); err != nil {
		t.Fatal(err)
	}

	// 3. Kirim edit terhadap job1 -> memperbarui teks dan FTS5 tanpa menambah baris
	jobEdit, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Origin:     outbox.Origin{Kind: "api", KeyID: "k1"},
		Spec:       outbox.Spec{Type: "edit", To: to, Edit: &outbox.Edit{MessageID: done1.WAID, Text: "halo pelanggan terbaru"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Wait(ctx, "sales", jobEdit.ID, outbox.StatusSent); err != nil {
		t.Fatal(err)
	}

	page, err := cs.Messages(ctx, "sales", chat, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != done1.WAID || page.Messages[0].Text != "halo pelanggan terbaru" {
		t.Fatalf("hasil RecordOutbound + edit tidak sesuai: %+v", page.Messages)
	}

	// 4. Kirim revoke terhadap job1 -> menghapus pesan dari context store & FTS5
	jobRev, _, err := svc.Enqueue(ctx, outbox.EnqueueRequest{
		InstanceID: "sales",
		Origin:     outbox.Origin{Kind: "api", KeyID: "k1"},
		Spec:       outbox.Spec{Type: "revoke", To: to, Revoke: &outbox.Revoke{MessageID: done1.WAID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Wait(ctx, "sales", jobRev.ID, outbox.StatusSent); err != nil {
		t.Fatal(err)
	}

	afterRev, err := cs.Messages(ctx, "sales", chat, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(afterRev.Messages) != 0 {
		t.Fatalf("pesan outbound yang di-revoke wajib dihapus dari context store: %+v", afterRev.Messages)
	}
}
