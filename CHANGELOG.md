# Changelog

Seluruh perubahan penting pada `waapi` dicatat di berkas ini.

## [2.0.0] — Target Rilis V2 (di branch integrasi `v2-dev`)

### Fitur Baru (Batch 0–16)
- **Fondasi & Toolchain Go 1.27 (`CGO_ENABLED=0`)**: dukungan profil memori `micro`, `lean` (default), dan `classic`, `InstanceSettings` bertipe (`waapi edit --set`), logger `whatsmeow` ke `slog` ber-rate-limit, serta backup otomatis pra-v2 (`backups/pre-v2-*`).
- **Observability Tanpa Dependensi**: `waapi doctor --deep` (procfs, `smaps_rollup`, cgroup v2, PSI, THP, `sqlite.DBStatus`), profiling `pprof` via `control.sock` (termasuk profil `goroutineleak`), `/readyz`, serta eksposisi `/metrics` Prometheus opsional (`WAAPI_METRICS_ADDR`).
- **Self-Healing Supervisor**: state machine koneksi eksplisit (`idle` -> `connecting` -> `online` / `backoff` / `replaced` / `banned_temp` / `outdated`), backoff eksponensial + circuit breaker, deteksi socket mati (`WAAPI_KEEPALIVE_MAX_FAIL`, `WAAPI_TCP_USER_TIMEOUT`), `waapi recycle <id>`, serta `sd_notify` + watchdog systemd.
- **Zero-Loss Inbound Journal (`journal.db`)**: `SynchronousAck` + ack sinkron pasca transaksi `journal.db` (`InboundObserver` / `TxHook`), dedupe berbasis alamat mentah, resolusi `@lid` ↔ nomor telepon (`sender_pn`), retensi berbatas + pruner, dan Pull API (`GET /v2/instances/{id}/events`).
- **Webhook v2, Standard Webhooks & SSE**: pengiriman berurutan per chat memakai cursor per lane (`webhook_lanes`), *Dead-Letter Queue* (`waapi webhook dlq ls|replay|purge`), auto-pause `410 Gone` + `waapi webhook resume`, mode tanda tangan `standard`, serta SSE stream (`Last-Event-ID`, kuota `WAAPI_SSE_MAX`).
- **Durable Outbox**: `POST /v2/instances/{id}/messages` (`202 Accepted`), idempotency key, `wa_message_id` pra-kirim, pemulihan lease crash, penjadwalan `send_at`, batas `TTL` & kuota antrean, serta `GET`/`DELETE /v2/.../messages/{msg_id}`.
- **Single-Pass Media Engine v2**: pipeline 1 temp file (`O_TMPFILE` + drop-behind `4 MiB`), reuse `upload_id` (`POST /v2/instances/{id}/media`), proteksi SSRF [`internal/netguard`](internal/netguard/netguard.go), dan *Media Resurrection* (`404/410` CDN retry).
- **Rich Messaging, Grup, Kontak & Panggilan**: teks ber-reply/mention, reaksi, edit, tarik pesan (revoke), lokasi, kartu kontak vCard, polling ber-tally SHA-256 terdekripsi, mark-read/presence, CRUD grup lengkap, profil kontak, serta auto-reject panggilan.
- **Account Health Guardian**: skor kesehatan 0–100, *consent ledger* (STOP/START otomatis), *quiet hours*, *warm-up*, *content fan-out cap*, kuota *first-contact*, dan CLI/REST `health`.
- **Memory Governor & Hardening**: level `Green`/`Yellow`/`Red` berbasis memori anonim & PSI, admission control `503 memory_pressure`, `GOGC`/`FreeOSMemory`/`PRAGMA shrink_memory`, unit systemd hardened, dan `scripts/burnin.sh`.
- **Security v2**: header `Authorization: Bearer`, `X-Request-Id` & masking error `5xx`, scope granular + `KeyPolicy`, masa berlaku & rotasi API key grace period, `audit_log` + UID peer `SO_PEERCRED`, socket kontrol atomik `0660`, verifikasi update Ed25519 *fail-closed*, dan `waapi backup [--out dir]`.
- **Context Store + FTS5 (Opt-in)**: penyimpanan ringkasan percakapan & pencarian FTS5 BM25 opsional per instance (`context_store`, mati secara default), sinkron edit/revoke, dan CLI `waapi context`.
- **Agent-Native MCP Server (`2026-07-28`)**: server MCP stateless di `POST /mcp` dan bridge stdio `waapi mcp` (`control.sock`), 15 tool tersaring `KeyPolicy`, persetujuan MRTR (`approval_token` HMAC-SHA256 5m), dan 2 resource langganan.
- **Reactive Hooks & Native AI Responder**: rule deklaratif (`when`/`do`/`throttle`/`active_hours`), stale guard & human takeover, `Webhook Gate`, CLI/REST CRUD + dry-run `test`, dan aksi `ai_reply` Chat Completions tanpa tool calling (`WAAPI_AI_*`, `ai_handoff_keywords`, `fallback_text`).

## [1.0.2]
- Rilis stabil v1: gateway WhatsApp headless multi-instance berbasis `app.db` + `wa.db`, pengiriman sinkron `/v1/*`, pairing QR/kode 8 digit, serta webhook HMAC dasar.
