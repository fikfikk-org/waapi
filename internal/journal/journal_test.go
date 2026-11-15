package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

func openTestJournal(t *testing.T) *Journal {
	t.Helper()
	ctx := context.Background()
	j, err := Open(ctx, filepath.Join(t.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    512,
		IdleTimeout: 60 * time.Second,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	})
	if err != nil {
		t.Fatalf("Open journal gagal: %v", err)
	}
	t.Cleanup(func() {
		_ = j.Close()
	})
	return j
}

// TestJournal_AppendDedupe memastikan dedupe_key identik mengembalikan Duplicate=true tanpa memanggil TxHook kedua kali.
func TestJournal_AppendDedupe(t *testing.T) {
	ctx := context.Background()
	j := openTestJournal(t)

	var hookCalls int
	hook := func(ctx context.Context, tx *sql.Tx, seq int64) error {
		hookCalls++
		return nil
	}

	ev1 := &Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "6281111111111@s.whatsapp.net",
		Sender:     "6281111111111@s.whatsapp.net",
		WAID:       "WAMID-001",
		DedupeKey:  "m:123@lid:123@lid:WAMID-001",
		TS:         1794360100,
		Payload:    []byte(`{"id":"WAMID-001"}`),
	}
	res1, err := j.Append(ctx, ev1, hook)
	if err != nil {
		t.Fatalf("Append pertama gagal: %v", err)
	}
	if res1.Duplicate || res1.Seq <= 0 {
		t.Fatalf("Append pertama harus non-duplikat dengan seq > 0, didapat %+v", res1)
	}

	ev2 := &Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "6281111111111@s.whatsapp.net",
		Sender:     "6281111111111@s.whatsapp.net",
		WAID:       "WAMID-001",
		DedupeKey:  "m:123@lid:123@lid:WAMID-001",
		TS:         1794360100,
		Payload:    []byte(`{"id":"WAMID-001"}`),
	}
	res2, err := j.Append(ctx, ev2, hook)
	if err != nil {
		t.Fatalf("Append kedua gagal: %v", err)
	}
	if !res2.Duplicate || res2.Seq != res1.Seq {
		t.Fatalf("Append kedua harus Duplicate=true dan Seq=%d, didapat %+v", res1.Seq, res2)
	}
	if hookCalls != 1 {
		t.Fatalf("TxHook diharapkan dipanggil tepat 1 kali, didapat %d kali", hookCalls)
	}

	var count int
	if err := j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE instance_id = 'sales'`).Scan(&count); err != nil {
		t.Fatalf("hitung baris events gagal: %v", err)
	}
	if count != 1 {
		t.Fatalf("jumlah baris di events harus tetap 1, didapat %d", count)
	}
}

// TestJournal_SeqTidakDipakaiUlang memastikan AUTOINCREMENT menjaga seq tetap naik meski seluruh baris dihapus.
func TestJournal_SeqTidakDipakaiUlang(t *testing.T) {
	ctx := context.Background()
	j := openTestJournal(t)

	for i := 1; i <= 10; i++ {
		res, err := j.Append(ctx, &Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       "6281@s.whatsapp.net",
			WAID:       fmt.Sprintf("ID-%d", i),
			DedupeKey:  fmt.Sprintf("m:c:s:ID-%d", i),
			TS:         1794360100 + int64(i),
			Payload:    []byte(`{"text":"hi"}`),
		})
		if err != nil {
			t.Fatalf("Append ke-%d gagal: %v", i, err)
		}
		if res.Seq != int64(i) {
			t.Fatalf("seq ke-%d diharapkan %d, didapat %d", i, i, res.Seq)
		}
	}

	if _, err := j.DB().ExecContext(ctx, `DELETE FROM events`); err != nil {
		t.Fatalf("hapus semua baris gagal: %v", err)
	}

	res11, err := j.Append(ctx, &Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "6281@s.whatsapp.net",
		WAID:       "ID-11",
		DedupeKey:  "m:c:s:ID-11",
		TS:         1794360111,
		Payload:    []byte(`{"text":"setelah hapus"}`),
	})
	if err != nil {
		t.Fatalf("Append ke-11 gagal: %v", err)
	}
	if res11.Seq != 11 {
		t.Fatalf("seq setelah hapus semua harus 11 (bukan 1), didapat %d", res11.Seq)
	}
}

// TestJournal_ReadFilterLaneDanMaxBytes memastikan Read memfilter type, partisi lane, dan berhenti di MaxBytes.
func TestJournal_ReadFilterLaneDanMaxBytes(t *testing.T) {
	ctx := context.Background()
	j := openTestJournal(t)

	payload120 := []byte(`{"id":"x","pad":"` + strings.Repeat("a", 104) + `"}`)

	// Buat dua chat yang jatuh ke lane berbeda pada Lanes=4
	var chatLane0, chatLane1 string
	for i := 0; i < 1000 && (chatLane0 == "" || chatLane1 == ""); i++ {
		c := fmt.Sprintf("628123456%04d@s.whatsapp.net", i)
		p := PartForChat(c) % 4
		if p == 0 && chatLane0 == "" {
			chatLane0 = c
		}
		if p == 1 && chatLane1 == "" {
			chatLane1 = c
		}
	}
	if chatLane0 == "" || chatLane1 == "" {
		t.Fatal("gagal menemukan sampel chat untuk dua lane berbeda")
	}

	// Masukkan campuran receipt dan message di kedua lane
	for i := 0; i < 6; i++ {
		chat := chatLane0
		if i%2 == 1 {
			chat = chatLane1
		}
		typ := "message"
		if i == 0 {
			typ = "receipt"
		}
		if _, err := j.Append(ctx, &Event{
			InstanceID: "sales",
			Type:       typ,
			Chat:       chat,
			TS:         1794360100 + int64(i),
			Payload:    payload120,
		}); err != nil {
			t.Fatalf("Append ke-%d gagal: %v", i, err)
		}
	}

	// Uji filter Types=["message"] dan Lane=0 dari Lanes=4 -> seharusnya hanya event indeks 2 dan 4 (seq 3 dan 5)
	resLane0, err := j.Read(ctx, Query{
		InstanceID: "sales",
		AfterSeq:   0,
		Types:      []string{"message"},
		Lane:       0,
		Lanes:      4,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Read lane 0 gagal: %v", err)
	}
	if len(resLane0.Events) != 2 {
		t.Fatalf("diharapkan 2 event message pada lane 0, didapat %d", len(resLane0.Events))
	}
	for _, ev := range resLane0.Events {
		if ev.Type != "message" || ev.Chat != chatLane0 {
			t.Fatalf("event tidak cocok filter: %+v", ev)
		}
	}
	if resLane0.ScannedTo != 6 {
		t.Fatalf("ScannedTo ketika pemindaian habis harus head=6, didapat %d", resLane0.ScannedTo)
	}

	// Uji MaxBytes: 120 * 2 = 240, 3 event = 360 -> MaxBytes=260 harus berhenti di 2 event
	resMax, err := j.Read(ctx, Query{
		InstanceID: "sales",
		AfterSeq:   0,
		Limit:      100,
		MaxBytes:   260,
	})
	if err != nil {
		t.Fatalf("Read MaxBytes gagal: %v", err)
	}
	if len(resMax.Events) != 2 {
		t.Fatalf("MaxBytes=260 diharapkan mengembalikan 2 event, didapat %d", len(resMax.Events))
	}
	if resMax.ScannedTo != resMax.Events[1].Seq {
		t.Fatalf("ScannedTo saat berhenti karena MaxBytes harus %d, didapat %d", resMax.Events[1].Seq, resMax.ScannedTo)
	}
}

// TestJournal_ReadScannedTo memastikan ScannedTo maju ke head saat filter kosong agar MinCursor ikut maju.
func TestJournal_ReadScannedTo(t *testing.T) {
	ctx := context.Background()
	j := openTestJournal(t)

	for i := 0; i < 100; i++ {
		if _, err := j.Append(ctx, &Event{
			InstanceID: "sales",
			Type:       "receipt",
			Chat:       "6281@s.whatsapp.net",
			TS:         1794360100 + int64(i),
			Payload:    []byte(`{"type":"read"}`),
		}); err != nil {
			t.Fatalf("Append receipt %d gagal: %v", i, err)
		}
	}

	res, err := j.Read(ctx, Query{
		InstanceID: "sales",
		AfterSeq:   0,
		Types:      []string{"message"},
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("Read gagal: %v", err)
	}
	if len(res.Events) != 0 {
		t.Fatalf("diharapkan 0 event cocok filter message, didapat %d", len(res.Events))
	}
	if res.ScannedTo != 100 {
		t.Fatalf("ScannedTo diharapkan 100 (head), didapat %d", res.ScannedTo)
	}

	if err := j.CommitCursor(ctx, "webhook", "sales", 0, res.ScannedTo); err != nil {
		t.Fatalf("CommitCursor gagal: %v", err)
	}
	minSeq, err := j.MinCursor(ctx, "sales")
	if err != nil {
		t.Fatalf("MinCursor gagal: %v", err)
	}
	if minSeq != 100 {
		t.Fatalf("MinCursor setelah commit ScannedTo harus 100, didapat %d", minSeq)
	}

	// Konsumen lain di lane berbeda belum commit -> MinCursor harus tetap rendah sampai ikut commit ScannedTo
	if err := j.CommitCursor(ctx, "webhook", "sales", 1, res.ScannedTo); err != nil {
		t.Fatalf("CommitCursor lane 1 gagal: %v", err)
	}
	cur, err := j.Cursor(ctx, "webhook", "sales", 1)
	if err != nil || cur != 100 {
		t.Fatalf("Cursor lane 1 diharapkan 100, didapat %d, err=%v", cur, err)
	}
}

// TestJournal_TxHookAtomik memastikan kegagalan TxHook me-rollback seluruh transaksi Append termasuk baris event.
func TestJournal_TxHookAtomik(t *testing.T) {
	ctx := context.Background()
	j := openTestJournal(t)

	errHook := errors.New("simulasi gagal observer")
	_, err := j.Append(ctx, &Event{
		InstanceID: "sales",
		Type:       "message",
		Chat:       "6281@s.whatsapp.net",
		WAID:       "MSG-ATOMIC",
		DedupeKey:  "m:c:s:MSG-ATOMIC",
		TS:         1794360100,
		Payload:    []byte(`{"id":"MSG-ATOMIC"}`),
	}, func(ctx context.Context, tx *sql.Tx, seq int64) error {
		// Pastikan inside-tx sudah melihat seq dan table cursors dapat ditulis dalam tx yang sama
		if seq <= 0 {
			return fmt.Errorf("seq di TxHook harus > 0, didapat %d", seq)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO cursors(consumer, instance_id, lane, seq, enabled, updated_at) VALUES ('obs', 'sales', 0, ?, 1, 1)`, seq); err != nil {
			return err
		}
		return errHook
	})
	if !errors.Is(err, errHook) {
		t.Fatalf("Append harus mengembalikan errHook, didapat %v", err)
	}

	var evCount, curCount int
	_ = j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&evCount)
	_ = j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM cursors`).Scan(&curCount)
	if evCount != 0 || curCount != 0 {
		t.Fatalf("rollback atomik gagal: events=%d, cursors=%d (keduanya harus 0)", evCount, curCount)
	}
}

// TestJournal_PenulisKonkuren memastikan 8 goroutine penulis bersamaan dan pruner berjalan tanpa SQLITE_BUSY.
func TestJournal_PenulisKonkuren(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	j := openTestJournal(t)

	const workers = 8
	const perWorker = 300

	pruneStop := make(chan struct{})
	pruneDone := make(chan struct{})
	errCh := make(chan error, workers*perWorker+16)

	go func() {
		defer close(pruneDone)
		for {
			select {
			case <-pruneStop:
				return
			default:
				if _, err := j.Prune(ctx, PrunePolicy{
					Retention:    72 * time.Hour,
					ReplayWindow: time.Hour,
					MaxRows:      50000,
				}, time.Now()); err != nil {
					errCh <- fmt.Errorf("prune error: %w", err)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				waid := fmt.Sprintf("W%d-%d", workerID, i)
				_, err := j.Append(ctx, &Event{
					InstanceID: "sales",
					Type:       "message",
					Chat:       fmt.Sprintf("6281%d@s.whatsapp.net", workerID),
					WAID:       waid,
					DedupeKey:  "m:c:s:" + waid,
					TS:         1794360100,
					Payload:    []byte(`{"id":"` + waid + `"}`),
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(pruneStop)
	<-pruneDone
	close(errCh)

	for err := range errCh {
		t.Fatalf("terjadi error saat penulisan konkuren (kemungkinan SQLITE_BUSY): %v", err)
	}

	var total int
	if err := j.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE instance_id = 'sales'`).Scan(&total); err != nil {
		t.Fatalf("hitung baris gagal: %v", err)
	}
	if total != workers*perWorker {
		t.Fatalf("jumlah baris diharapkan %d, didapat %d", workers*perWorker, total)
	}
}

// TestJournal_SubscribeDigabung memastikan sinyal Subscribe berkapasitas 1 digabung tanpa memblokir Append.
func TestJournal_SubscribeDigabung(t *testing.T) {
	ctx := context.Background()
	j := openTestJournal(t)

	ch, unsub := j.Subscribe("sales")
	defer unsub()

	for i := 0; i < 100; i++ {
		_, err := j.Append(ctx, &Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       "6281@s.whatsapp.net",
			TS:         1794360100 + int64(i),
			Payload:    []byte(`{"text":"coalesce"}`),
		})
		if err != nil {
			t.Fatalf("Append ke-%d gagal: %v", i, err)
		}
	}

	select {
	case <-ch:
	default:
		t.Fatal("subscriber harus menerima minimal 1 sinyal setelah 100 Append")
	}

	// Kapasitas channel 1 sehingga setelah dibaca sekali harus kosong (digabung)
	select {
	case <-ch:
		t.Fatal("sinyal subscriber seharusnya tergabung dalam channel berkapasitas 1")
	default:
	}

	// Uji MarshalEnvelope juga sekalian memvalidasi struktur §4.1.2
	ev := Event{
		Seq:        1201,
		InstanceID: "sales",
		Type:       "message",
		TS:         1794360100,
		Payload:    []byte(`{"id":"M1","text":"halo"}`),
	}
	raw, err := ev.MarshalEnvelope()
	if err != nil {
		t.Fatalf("MarshalEnvelope gagal: %v", err)
	}
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal envelope gagal: %v", err)
	}
	if env["v"] != float64(2) || env["id"] != "1201" || env["event"] != "message" || env["instance"] != "sales" {
		t.Fatalf("header envelope v2 tidak sesuai: %v", env)
	}
}

// BenchmarkJournal_Append mengukur latensi dan alokasi operasi Append tunggal pada journal.db.
func BenchmarkJournal_Append(b *testing.B) {
	ctx := context.Background()
	j, err := Open(ctx, filepath.Join(b.TempDir(), "journal.db"), store.DBOptions{
		MaxConns:    3,
		CacheKiB:    1024,
		IdleTimeout: 60 * time.Second,
		TxLock:      "immediate",
		Sync:        "NORMAL",
	})
	if err != nil {
		b.Fatalf("Open journal gagal: %v", err)
	}
	defer j.Close()

	payload := []byte(`{"id":"BENCH","chat":"6281234567890@s.whatsapp.net","chat_type":"dm","is_group":false,"from":"6281234567890@s.whatsapp.net","from_me":false,"timestamp":1794360100,"type":"text","text":"benchmark payload","truncated":false,"flags":{"ephemeral":false,"view_once":false,"forwarded":false,"offline":false}}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := j.Append(ctx, &Event{
			InstanceID: "sales",
			Type:       "message",
			Chat:       "6281234567890@s.whatsapp.net",
			Sender:     "6281234567890@s.whatsapp.net",
			WAID:       fmt.Sprintf("B-%d", i),
			DedupeKey:  fmt.Sprintf("m:c:s:B-%d", i),
			TS:         1794360100,
			Payload:    payload,
		})
		if err != nil {
			b.Fatalf("Append gagal pada iterasi %d: %v", i, err)
		}
	}
}
