# Dokumentasi API — wa-api

> Status: terimplementasi (`internal/httpapi`, `internal/control`, `internal/cli`). Base URL: `http://127.0.0.1:8080` (diekspos via reverse proxy TLS).
> Autentikasi: header `X-API-Key: wak_...` (bukan Sanctum). Key dibuat lewat CLI `waapi key add --name <nama>`.

## Format Error Umum

| HTTP | code | Arti |
|---|---|---|
| 400 | invalid_request / invalid_number / invalid_spec | Payload, nomor, atau spesifikasi pesan outbox tidak valid |
| 401 | unauthorized | Key tidak ada, salah, atau dicabut |
| 403 | forbidden_instance | Key tidak punya akses ke instance |
| 404 | instance_not_found / message_not_found | Instance atau pesan outbox tidak ditemukan |
| 409 | instance_not_paired / message_not_cancellable | Instance belum di-pairing atau status pesan bukan `queued` saat pembatalan |
| 410 | media_gone | Media referensi outbox sudah kedaluwarsa |
| 413 | media_too_large | Media melebihi batas |
| 422 | idempotency_key_reused | Header `Idempotency-Key` dipakai ulang untuk tujuan atau isi pesan berbeda |
| 429 | queue_full | Antrian kirim penuh (ada `retry_after` & header `Retry-After`) |
| 429 | sse_limit | Batas koneksi SSE konkuren tercapai (disertai header `Retry-After: 10`) |
| 502 | send_failed | Pengiriman gagal permanen (`failed`/`expired`/`cancelled`) saat `wait=sent` |
| 503 | not_ready | Daemon tidak siap (DB gagal atau tekanan memori level `Red`) |
| 503 | instance_not_connected | Instance belum/tidak tersambung |
| 504 | send_timeout | WhatsApp tidak merespons dalam batas tunggu (v2 menyertakan `details: { id, status_url }`) |

## GET /healthz

| Field | Detail |
|---|---|
| Method + Endpoint | GET /healthz |
| Auth Required | No |
| Headers | - |
| Query Params | - |
| Request Body | - |
| Response | 200 `status: ok` |
| Notes | Tidak membocorkan info instance |

## GET /readyz

| Field | Detail |
|---|---|
| Method + Endpoint | GET /readyz |
| Auth Required | No |
| Headers | - |
| Query Params | - |
| Request Body | - |
| Response | 200 `{"status":"ready"}` · 503 `not_ready` |
| Notes | Menguji `SELECT 1` ke `app.db` (timeout 500 ms) dan memastikan `governor.Source.Level() != Red`; error tidak memaparkan detail internal |

## GET /metrics (Listener Terpisah Opsional)

| Field | Detail |
|---|---|
| Method + Endpoint | GET /metrics (pada `WAAPI_METRICS_ADDR`, mis. `127.0.0.1:9108`) |
| Auth Required | No (listener terpisah; disarankan loopback) |
| Headers | Accept: text/plain (opsional) |
| Query Params | - |
| Request Body | - |
| Response | 200 `text/plain; version=0.0.4; charset=utf-8` berisi metrik berawalan `waapi_` |
| Notes | Mati secara default bila `WAAPI_METRICS_ADDR` kosong; memicu log WARN saat start bila alamat bukan loopback; hanya melayani `/metrics` |

## GET /v1/instances

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v1/instances |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 array instance (id, status, state, state_since, reconnect_attempts, next_retry_in, ban, jid, queue_depth, rate_per_min) |
| Notes | Hanya instance sesuai scope key; tidak membuat instance runtime baru di memori |

## GET /v1/instances/{id}

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v1/instances/{id} |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 id, status, state, state_since, reconnect_attempts, next_retry_in, ban, jid, push_name, queue_depth, rate_per_min, settings, paired_at, connected_since · 403 · 404 |
| Notes | - |

## POST /v1/instances/{id}/pair

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/pair |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: application/json (opsional) |
| Query Params | - |
| Request Body | phone (string, opsional, 8-15 digit tanpa awalan 0 untuk mode kode 8-digit; kosongkan untuk mode QR) |
| Response | 200 instance, method, state, pairing_code, qr_raw, qr_image_url, expires_in, session_expires_in · 400 · 409 · 504 |
| Notes | Idempoten untuk method+phone yang sama; sesi berjalan di latar belakang dengan TTL 170 detik |

## GET /v1/instances/{id}/pair

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v1/instances/{id}/pair |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 snapshot sesi pairing terakhir (state: starting, qr_refreshed, code_generated, connected, timeout, cancelled, error) · 404 |
| Notes | Digunakan oleh dashboard web untuk polling status pairing tiap 2-3 detik |

## DELETE /v1/instances/{id}/pair

| Field | Detail |
|---|---|
| Method + Endpoint | DELETE /v1/instances/{id}/pair |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 snapshot dengan state `cancelled` · 404 jika tidak ada sesi aktif |
| Notes | Memutus koneksi websocket pairing sementara dan melepas semaphore |

## GET /v1/instances/{id}/qr.png

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v1/instances/{id}/qr.png |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | scale (integer, opsional, default 6, rentang 2-16) |
| Request Body | - |
| Response | 200 binary `image/png` (`Cache-Control: no-store`, header `X-QR-Expires-In`) · 404 jika QR tidak aktif |
| Notes | Wajib di-proxy oleh backend dashboard (seperti Laravel) agar X-API-Key tidak bocor ke browser |

## POST /v1/instances/{id}/messages/text

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/messages/text |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: application/json, Idempotency-Key (opsional) |
| Query Params | - |
| Request Body | to (string, wajib, nomor E.164 tanpa + atau JID) · text (string, wajib) · typing (bool, opsional) · reply_to (string, opsional, message id) |
| Response | 200 message_id, to, timestamp (Unix detik) · 422 · 429 · 502 · 503 · 504 |
| Notes | Body maks 64 KB; dikirim sinkron via Durable Outbox (menunggu status `sent` hingga 60 detik atau `504 send_timeout`) dan mendukung `Idempotency-Key` |

## POST /v1/instances/{id}/messages/media

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/messages/media |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: multipart/form-data |
| Query Params | - |
| Request Body | to (wajib) · type (wajib: image/video/audio/document) · caption (opsional) · filename (opsional) · file (binary, wajib, harus part terakhir) |
| Response | 200 message_id, to, timestamp · 413 · 429 · 503 |
| Notes | Di-stream ke file sementara, RAM konstan; maks 16 MB / 64 MB dokumen |

## POST /v1/instances/{id}/messages/media-url

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/messages/media-url |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | to (wajib) · type (wajib) · url (wajib, https) · filename (opsional) · caption (opsional) |
| Response | 200 message_id, to, timestamp · 400 · 413 |
| Notes | Hanya https, IP privat diblokir (anti-SSRF), timeout unduh 60 detik |

## POST /v1/instances/{id}/contacts/check

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/contacts/check |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | numbers (array string, wajib, maks 20) |
| Response | 200 results[] (number, exists, jid) |
| Notes | Hasil di-cache 24 jam untuk mengurangi risiko ban |

## POST /v1/instances/{id}/media/download

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/media/download |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | Objek `media` dari payload webhook: direct_path, media_key, file_enc_sha256, file_sha256, file_length, media_type, mimetype |
| Response | 200 stream biner dengan Content-Type asli |
| Notes | Media tidak disimpan; diunduh on-demand lalu file sementara dihapus |

## GET /v2/instances/{id}/events

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/events |
| Auth Required | Yes (X-API-Key, scope `events:read`) |
| Headers | X-API-Key |
| Query Params | `after` (int64, opsional, default 0), `limit` (int, opsional, default 100, maks 500), `types` (string csv, opsional, mis. `message,receipt`) |
| Request Body | - |
| Response | 200 `{ events, next_after, has_more }` (400 jika query tidak valid, 403 jika scope/instance ditolak, 503 jika journal belum aktif) |
| Notes | Setiap item pada `events` memakai envelope v2 (`v`, `id`, `event`, `instance`, `timestamp`, `data`); `next_after` mengikuti `ScannedTo` agar pagination tetap maju melewati event yang terfilter |

## GET /v2/instances/{id}/events/stream

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/events/stream |
| Auth Required | Yes (X-API-Key, scope `events:read`) |
| Headers | X-API-Key, Last-Event-ID (opsional, diprioritaskan di atas query `after`) |
| Query Params | `after` (int64 >= 0, opsional; bila kosong bersama `Last-Event-ID` mulai dari head seq terkini), `types` (string csv, opsional) |
| Request Body | - |
| Response | 200 `text/event-stream` (`id: <seq>`, `event: <type>`, `data: <envelope v2 JSON>`; heartbeat `: ping` tiap 25 detik) · 400 · 403 · 404 · 429 `sse_limit` · 503 |
| Notes | Kuota koneksi SSE dibatasi `WAAPI_SSE_MAX` (header `Retry-After: 10` saat penuh); write deadline per frame 30 detik via `http.ResponseController` untuk memutus klien lambat tanpa mematikan stream idle |

## POST /v2/instances/{id}/messages

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/messages |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: application/json, Idempotency-Key (opsional) |
| Query Params | - |
| Request Body | to (string, wajib) · type (string, saat ini `text`) · text (string, wajib untuk `text`) · reply_to (`{ id, sender? }`, opsional) · mentions (`[]string`, opsional) · typing (bool, opsional) · send_at (RFC3339, opsional) · priority (`normal` \| `urgent`, default `normal`) · ttl (Go duration, opsional) · wait (`none` \| `sent`, default `none`) |
| Response | `wait=none`: 202 (baru) / 200 (idempoten hit) `{ id, wa_message_id, status, to, not_before, status_url }` · `wait=sent`: 200 objek detail pesan · 400 · 409 · 422 · 429 · 502 · 504 |
| Notes | Seluruh timestamp JSON berformat Unix detik. Saat `wait=sent` (batas 30 detik), gagal mengembalikan `502 send_failed` dan timeout mengembalikan `504 send_timeout` berisi `details: { id, status_url }` |

## GET /v2/instances/{id}/messages/{msg_id}

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/messages/{msg_id} |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 `{ id, wa_message_id, instance, to, type, status, origin, attempts, created_at, sent_at, delivered_at, read_at, receipts: { delivered, read }, error }` · 404 `message_not_found` · 503 |
| Notes | Seluruh timestamp (`created_at`, `sent_at`, `delivered_at`, `read_at`) bernilai Unix detik (`null` bila belum terjadi) |

## DELETE /v2/instances/{id}/messages/{msg_id}

| Field | Detail |
|---|---|
| Method + Endpoint | DELETE /v2/instances/{id}/messages/{msg_id} |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 detail pesan berstatus `cancelled` · 404 `message_not_found` · 409 `message_not_cancellable` · 503 |
| Notes | Hanya dapat membatalkan pesan outbox yang masih berstatus `queued` |

## Webhook Keluar (dikirim daemon ke backend)

| Field | Detail |
|---|---|
| Method + Endpoint | POST ke `webhook_url` instance (`User-Agent: waapi-webhook/2.0`) |
| Headers | Mode `waapi`: `X-WA-Event`, `X-WA-Instance`, `X-WA-Timestamp`, `X-WA-Event-Id`, `X-WA-Delivery-Attempt`, `X-WA-Signature` (`sha256=` HMAC `timestamp.body`). Mode `standard`: `webhook-id`, `webhook-timestamp`, `webhook-signature` (`v1,<base64>` Standard Webhooks) |
| Body | Envelope v2 (`v: 2`, `id`, `event`, `instance`, `timestamp`, `data`) dibaca bertahap dari `journal.db` |
| Event | message, receipt, undecryptable, connection, pairing, outbox (dan `test` untuk `waapi webhook test`). Payload `outbox`: `{ id, wa_message_id, to, type, status, attempts, timestamp, error? }` (`status`: `sent\|failed\|expired`, `timestamp` Unix detik) |
| Notes | Pengiriman berurutan per `lane = part % webhook_lanes` (default 4 lane). Jadwal retry `0, 5s, 15s, 45s, 2m, 5m, 15m, 30m, 1h...` (±20% jitter) hingga `webhook_max_attempts` (default 12), menghormati `Retry-After` (maks 1h); `410 Gone` mem-pause target (`gone`); `4xx` lainnya maks 3 percobaan lalu dipindah ke `webhook_dead` (DLQ). Target dibaca dari `TargetCache` memori |

## Control Plane (CLI only, Unix socket)

Tidak diekspos via HTTP publik; hanya diakses perintah `wa-api` lewat `control.sock` (perm 0660).
- `PATCH /control/instances/{id}` menerima field tambahan `settings` (objek JSON parsial untuk di-merge ke `InstanceSettings`; diakses dari CLI lewat `waapi edit <id> --set key=value`) serta `webhook_from` (`now` atau `retained`, diakses lewat `waapi webhook set <id> <url> [--from now|retained]`).
- `POST /control/instances/{id}/webhook/resume` menghapus status `webhook_paused` dan menyalakan ulang deliverer dari cursor tersimpan (`waapi webhook resume <id>`).
- `GET /control/instances/{id}/webhook/dlq` (`?limit=N`), `POST /control/instances/{id}/webhook/dlq/replay` (`{ all, seq, seqs }`), dan `DELETE /control/instances/{id}/webhook/dlq` mengelola dead-letter queue (`waapi webhook dlq <id> <ls|replay|purge>`).
- `POST /control/instances/{id}/messages` mengantrekan pesan teks ke Durable Outbox dari kontrol lokal `{ to, text, reply_to?, typing?, wait?, idem_key? }` (`wait`: `none` atau `sent` maks 30 detik), diakses melalui CLI `waapi send <id> <to> <text> [--wait] [--idem KEY] [--typing] [--json]`.
- `GET /control/doctor?deep=1` mengembalikan `DeepReport` (blok proses `procfs`/`smaps_rollup`, `runtime/metrics`, cgroup v2 & PSI, THP mode, `DBStatus` SQLite per DB termasuk `journal.db`, ringkasan per-instance `webhook`: `head_seq`, `min_cursor`, `lag`, `oldest_undelivered_sec`, `dlq`, `paused`, `dropped_undelivered`, metadata build, dan jumlah `goroutineleak`), diakses via `waapi doctor --deep [--json]`.
- `GET /control/instances/{id}/events` mengambil atau long-poll (`?after=N&limit=N&types=csv&wait=N`), dan `GET /control/instances/{id}/events/stream` melakukan SSE stream event journal v2 per instance via `waapi events <id> [--after N] [--types csv] [--limit N] [--follow] [--json]`.
- `GET /control/debug/pprof/*` menyediakan endpoint `net/http/pprof` (`heap`, `goroutine`, `goroutineleak`, `profile`, `cmdline`, `symbol`, `trace`), diakses via `waapi debug pprof <profil> [-o file] [--seconds N]`.
- `POST /control/instances/{id}/recycle` menghentikan supervisor, melepas client lama, memuat ulang device `sqlstore`, dan menyalakan ulang supervisor (`waapi recycle <id>`). Ditolak HTTP 400 bila belum dipairing.

## Scheduled Tasks

Belum ada `routes/web.php` / `Kernel.php` (bukan proyek Laravel). Tugas berkala dan daemon loop internal:

| Tugas | Frekuensi | Tujuan | Efek samping |
|---|---|---|---|
| Migrasi `webhook_outbox` legacy | Sekali saat startup | Pindahkan backlog v1 ke `journal.db` | Kosongkan baris `webhook_outbox` (tabel tetap dipertahankan) |
| Scheduler outbox per instance | Berbasis sinyal & timer `not_before` | Klaim dan kirim pesan teks dari tabel `outbox` sesuai token rate limit saat instance tersambung | Perbarui status `sending`/`sent`/`failed`/`expired`, `wa_message_id`, serta tulis event `outbox` ke `journal.db` |
| Outbox lease recovery | Saat startup & tiap 60 detik | Kembalikan pekerjaan `sending` yang lease-nya kedaluwarsa (crash/kill) | Ubah status kembali ke `queued` dan bangunkan scheduler instance |
| Webhook lane deliverer | Berbasis notif (`Subscribe`) + fallback idle | Kirim envelope v2 per lane dari `journal.db` ke `webhook_url` | Commit cursor, pause saat `410`, catat gagal permanen ke `webhook_dead` |
| Flush last_used_at | Tiap 60 detik | Simpan waktu pakai API key | Write batch ke app.db |
| Journal pruner | Tiap 5 menit (+ tugas per jam) | Pangkas baris `journal.db` sesuai replay window, retensi, batas baris, dan jalankan pemangkasan final `outbox` tiap jam | Hapus baris lama (kecuali `webhook_dead`), bersihkan baris final `outbox` kadaluwarsa, & inkremen `dropped_undelivered_total` |
| Temp sweeper | Saat startup | Hapus file media sementara > 1 jam | Hapus file di `tmp/` |
| Systemd watchdog loop | Tiap `WATCHDOG_USEC/2` (bila disetel) | Kirim `WATCHDOG=1` ke `$NOTIFY_SOCKET` setelah `SELECT 1` ke `app.db` lulus <= 500 ms | Menunda heartbeat jika DB macet agar systemd dapat me-restart service |

