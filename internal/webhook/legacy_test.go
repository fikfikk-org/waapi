package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

// TestMigrateLegacyOutbox memverifikasi migrasi baris webhook_outbox v1 ke journal.db secara batch dan idempoten.
func TestMigrateLegacyOutbox(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	st, err := store.Open(ctx, filepath.Join(tmp, "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	defer st.Close()

	if err := st.CreateInstance(ctx, store.Instance{
		ID:            "sales",
		WebhookSecret: "whsec_legacy",
		RatePerMin:    20,
	}); err != nil {
		t.Fatalf("CreateInstance gagal: %v", err)
	}

	j, err := journal.Open(ctx, filepath.Join(tmp, "journal.db"), store.DBOptions{
		MaxConns:    2,
		CacheKiB:    512,
		IdleTimeout: 30 * time.Second,
		TxLock:      "immediate",
	})
	if err != nil {
		t.Fatalf("journal.Open gagal: %v", err)
	}
	defer j.Close()

	// Sisipkan 520 baris (> ukuran batch 500) ke webhook_outbox memakai format envelope v1.
	const totalRows = 520
	for i := 1; i <= totalRows; i++ {
		envRaw, _ := json.Marshal(Envelope{
			Event:     "message",
			Instance:  "sales",
			Timestamp: 1794360000 + int64(i),
			Data: map[string]any{
				"id":   fmt.Sprintf("M-%d", i),
				"chat": "6281111111111@s.whatsapp.net",
				"text": fmt.Sprintf("pesan legacy %d", i),
			},
		})
		if err := st.EnqueueOutbox(ctx, "sales", "message", envRaw, 2, time.Now().Unix()); err != nil {
			t.Fatalf("EnqueueOutbox ke-%d gagal: %v", i, err)
		}
	}

	// Jalankan migrasi pertama: semua 520 baris harus pindah ke journal dan outbox kosong.
	migrated, err := MigrateLegacyOutbox(ctx, st, j)
	if err != nil {
		t.Fatalf("MigrateLegacyOutbox gagal: %v", err)
	}
	if migrated != totalRows {
		t.Fatalf("jumlah baris termigrasi diharapkan %d, didapat %d", totalRows, migrated)
	}

	remain, err := st.CountOutbox(ctx)
	if err != nil {
		t.Fatalf("CountOutbox gagal: %v", err)
	}
	if remain != 0 {
		t.Fatalf("webhook_outbox harus kosong setelah migrasi, tersisa %d", remain)
	}

	// Periksa bahwa data dari envelope v1 sudah terekstrak rapi dan dedupe_key memakai "legacy:<id>".
	res1, err := j.Read(ctx, journal.Query{InstanceID: "sales", Limit: 500})
	if err != nil {
		t.Fatalf("journal.Read halaman 1 gagal: %v", err)
	}
	res2, err := j.Read(ctx, journal.Query{InstanceID: "sales", AfterSeq: res1.ScannedTo, Limit: 500})
	if err != nil {
		t.Fatalf("journal.Read halaman 2 gagal: %v", err)
	}
	gotCount := len(res1.Events) + len(res2.Events)
	if gotCount != totalRows {
		t.Fatalf("jumlah event di journal diharapkan %d, didapat %d", totalRows, gotCount)
	}
	first := res1.Events[0]
	if first.DedupeKey != "legacy:1" || first.Chat != "6281111111111@s.whatsapp.net" {
		t.Fatalf("atribut event legacy pertama tidak sesuai: %+v", first)
	}
	var payloadMap map[string]any
	if err := json.Unmarshal(first.Payload, &payloadMap); err != nil {
		t.Fatalf("payload hasil ekstraksi bukan JSON valid: %v", err)
	}
	if _, hasEnvelopeData := payloadMap["data"]; hasEnvelopeData {
		t.Fatalf("payload journal seharusnya berisi field data hasil ekstraksi, bukan envelope ganda: %s", string(first.Payload))
	}
	if payloadMap["text"] != "pesan legacy 1" {
		t.Fatalf("isi payload tidak sesuai: %+v", payloadMap)
	}

	// Sifat idempoten: masukkan baris dengan id yang sama atau panggil ulang -> tidak menggandakan di journal.
	envDup, _ := json.Marshal(Envelope{
		Event:     "message",
		Instance:  "sales",
		Timestamp: 1794360001,
		Data:      map[string]any{"id": "M-1", "text": "duplikat"},
	})
	if _, err := st.DB.ExecContext(ctx,
		`INSERT INTO webhook_outbox (id, instance_id, event, payload, attempts, next_retry_at, created_at) VALUES (1, 'sales', 'message', ?, 0, 0, 1794360001)`,
		envDup,
	); err != nil {
		t.Fatalf("sisip ulang baris id=1 untuk uji idempoten gagal: %v", err)
	}
	if _, err := MigrateLegacyOutbox(ctx, st, j); err != nil {
		t.Fatalf("MigrateLegacyOutbox kedua gagal: %v", err)
	}
	if _, err := MigrateLegacyOutbox(ctx, st, j); err != nil {
		t.Fatalf("MigrateLegacyOutbox ketiga gagal: %v", err)
	}

	var journalRows int
	if err := j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE instance_id = 'sales'`).Scan(&journalRows); err != nil {
		t.Fatalf("hitung events journal gagal: %v", err)
	}
	if journalRows != totalRows {
		t.Fatalf("migrasi ulang tidak boleh menggandakan event journal: diharapkan %d, didapat %d", totalRows, journalRows)
	}

	// Pastikan tabel webhook_outbox tidak dihapus sesuai larangan Batch 4.
	var tblName string
	if err := st.DB.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name='webhook_outbox'`).Scan(&tblName); err != nil {
		t.Fatalf("tabel webhook_outbox harus tetap ada: %v", err)
	}
}
