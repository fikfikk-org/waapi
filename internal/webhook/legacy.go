package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
)

const legacyBatchSize = 500

// MigrateLegacyOutbox memindahkan sisa antrian webhook_outbox v1 ke journal.db dalam batch 500 baris secara idempoten.
func MigrateLegacyOutbox(ctx context.Context, st *store.Store, j *journal.Journal) (int, error) {
	if st == nil || st.DB == nil || j == nil {
		return 0, nil
	}

	migrated := 0
	for {
		if ctx.Err() != nil {
			return migrated, ctx.Err()
		}

		rows, err := st.DB.QueryContext(ctx, `
			SELECT id, instance_id, event, payload, created_at
			FROM webhook_outbox
			ORDER BY id ASC
			LIMIT ?`, legacyBatchSize,
		)
		if err != nil {
			return migrated, fmt.Errorf("baca batch webhook_outbox: %w", err)
		}

		type legacyRow struct {
			id         int64
			instanceID string
			event      string
			payload    []byte
			createdAt  int64
		}
		var batch []legacyRow
		for rows.Next() {
			var r legacyRow
			if err := rows.Scan(&r.id, &r.instanceID, &r.event, &r.payload, &r.createdAt); err != nil {
				_ = rows.Close()
				return migrated, fmt.Errorf("scan baris webhook_outbox: %w", err)
			}
			batch = append(batch, r)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return migrated, fmt.Errorf("iterasi baris webhook_outbox: %w", err)
		}
		_ = rows.Close()

		if len(batch) == 0 {
			return migrated, nil
		}

		migratedIDs := make([]int64, 0, len(batch))
		for _, r := range batch {
			ts, dataBytes, chat := extractLegacyEnvelope(r.payload, r.createdAt)
			ev := &journal.Event{
				InstanceID: r.instanceID,
				Type:       r.event,
				Chat:       chat,
				DedupeKey:  fmt.Sprintf("legacy:%d", r.id),
				TS:         ts,
				Part:       journal.PartForChat(chat),
				Payload:    dataBytes,
			}
			res, err := j.Append(ctx, ev)
			if err != nil {
				return migrated, fmt.Errorf("append event legacy id=%d: %w", r.id, err)
			}
			migratedIDs = append(migratedIDs, r.id)
			if !res.Duplicate {
				migrated++
			}
		}

		if err := deleteLegacyOutboxIDs(ctx, st, migratedIDs); err != nil {
			return migrated, err
		}
		if len(batch) < legacyBatchSize {
			return migrated, nil
		}
	}
}

func extractLegacyEnvelope(raw []byte, fallbackTS int64) (ts int64, data []byte, chat string) {
	ts = fallbackTS
	var env struct {
		Timestamp int64           `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
	}
	trimmed := strings.TrimSpace(string(raw))
	if json.Unmarshal([]byte(trimmed), &env) == nil && len(env.Data) > 0 && strings.TrimSpace(string(env.Data)) != "null" {
		if env.Timestamp > 0 {
			ts = env.Timestamp
		}
		data = env.Data
	} else if json.Valid([]byte(trimmed)) {
		data = []byte(trimmed)
	} else {
		data, _ = json.Marshal(map[string]string{"raw": string(raw)})
	}

	var meta struct {
		Chat string `json:"chat"`
	}
	if json.Unmarshal(data, &meta) == nil && meta.Chat != "" {
		chat = meta.Chat
	}

	if len(data) > journal.MaxPayloadBytes {
		data, _ = json.Marshal(map[string]any{
			"chat":      chat,
			"truncated": true,
			"note":      "payload legacy melebihi 64 KiB",
		})
	}
	return ts, data, chat
}

func deleteLegacyOutboxIDs(ctx context.Context, st *store.Store, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := st.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mulai transaksi hapus legacy outbox: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `DELETE FROM webhook_outbox WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("siapkan delete legacy outbox: %w", err)
	}
	defer stmt.Close()

	for _, id := range ids {
		if _, err := stmt.ExecContext(ctx, id); err != nil {
			return fmt.Errorf("hapus baris legacy outbox id=%d: %w", id, err)
		}
	}
	return tx.Commit()
}
