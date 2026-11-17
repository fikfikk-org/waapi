# Dokumentasi API — wa-api

> Status: terimplementasi (`internal/httpapi`, `internal/control`, `internal/cli`). Base URL: `http://127.0.0.1:8080` (diekspos via reverse proxy TLS).
> Autentikasi: header `X-API-Key: wak_...` atau `Authorization: Bearer wak_...` (bukan Sanctum). Key dibuat lewat CLI `waapi key add --name <nama>`. Setiap respons membawa header `X-Request-Id`.

## Format Error Umum

| HTTP | code | Arti |
|---|---|---|
| 400 | invalid_request / invalid_number / invalid_spec | Payload, nomor, URL non-HTTPS/SSRF, atau spesifikasi pesan outbox tidak valid |
| 401 | unauthorized / key_expired | Key tidak ada, salah, dicabut, atau melewati `expires_at` |
| 403 | forbidden_instance / scope_denied / chat_not_allowed / outside_reply_window / first_contact_denied / first_contact_requires_approval / destructive_denied / destructive_requires_approval | Key tidak punya akses instance, scope kurang, atau melanggar aturan `KeyPolicy` pengiriman / aksi destruktif |
| 404 | instance_not_found / message_not_found / event_not_found / upload_not_found | Instance, pesan outbox, event journal, atau descriptor upload tidak ditemukan |
| 409 | instance_not_paired / message_not_cancellable / context_store_disabled | Instance belum di-pairing, status pesan bukan `queued` saat pembatalan, atau context store belum diaktifkan |
| 410 | upload_expired / media_gone | Descriptor `upload_id` kedaluwarsa (>24 jam) atau media sudah tidak tersedia (termasuk setelah media resurrection) |
| 413 | media_too_large | Media melebihi batas (16 MiB media, 64 MiB dokumen, 1 MiB sticker) |
| 415 | unsupported_media / ptt_requires_ogg_opus / sticker_requires_webp | Format media tidak didukung (mis. GIF sebagai `image`), PTT wajib OGG Opus, sticker wajib WebP |
| 422 | idempotency_key_reused | Header `Idempotency-Key` dipakai ulang untuk tujuan atau isi pesan berbeda |
| 429 | queue_full / sse_limit / rate_limited / policy_rate_limited / guardian_limited | Antrian kirim penuh, kuota SSE penuh, rate limit per API key terlampaui (`Retry-After: 1`), kuota per jam `KeyPolicy` habis, atau batas Guardian |
| 502 | send_failed / upstream_error / pairing_failed | Kesalahan upstream/pengiriman gagal permanen dengan pesan generik aman (detail asli dicatat hanya di log internal) |
| 503 | not_ready / memory_pressure | Daemon tidak siap (DB gagal atau tekanan memori level `Red`; endpoint media menyertakan `Retry-After: 15`) |
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
| Response | 200 `text/plain; version=0.0.4; charset=utf-8` berisi metrik berawalan `waapi_` (termasuk `waapi_memory_governor_level`: `0` green, `1` yellow, `2` red) |
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
| Response | 200 id, status, state, state_since, reconnect_attempts, next_retry_in, ban, health (`{ state, score }`), jid, push_name, queue_depth, rate_per_min, settings, paired_at, connected_since · 403 · 404 |
| Notes | - |

## POST /v1/instances/{id}/pair

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/pair |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: application/json (opsional) |
| Query Params | - |
| Request Body | phone (string, opsional, 8-15 digit tanpa awalan 0 untuk mode kode 8-digit; kosongkan untuk mode QR) |
| Response | 200 instance, method, state, pairing_code, qr_raw, qr_image_url, expires_in, session_expires_in · 400 · 409 · 503 `memory_pressure` · 504 |
| Notes | Idempoten untuk method+phone yang sama; sesi berjalan di latar belakang dengan TTL 170 detik; ditolak `503 memory_pressure` (`Retry-After: 15`) saat level governor `Red` |

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
| Auth Required | Yes (X-API-Key, scope `messages:send`) |
| Headers | X-API-Key, Content-Type: multipart/form-data, Idempotency-Key (opsional) |
| Query Params | - |
| Request Body | to (wajib) · type (wajib: image/video/audio/ptt/document/sticker) · caption (opsional) · filename (opsional) · file (binary, wajib, harus part terakhir setelah `to` & `type`) |
| Response | 200 message_id, to, timestamp · 413 · 415 · 429 · 502 · 503 `memory_pressure` / `instance_not_connected` · 504 |
| Notes | Mengunggah stream lewat pipeline satu-pass -> `UploadStore` -> `outbox.Enqueue` + `Wait(sent, 60s)` (fallback ke `EnqueueMedia` bila outbox tidak terpasang); deadline baca/tulis 10 menit |

## POST /v1/instances/{id}/messages/media-url

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/messages/media-url |
| Auth Required | Yes (X-API-Key, scope `messages:send`) |
| Headers | X-API-Key, Content-Type: application/json, Idempotency-Key (opsional) |
| Query Params | - |
| Request Body | to (wajib) · type (wajib) · url (wajib, https) · filename (opsional) · caption (opsional) |
| Response | 200 message_id, to, timestamp · 400 · 413 · 502 · 503 `memory_pressure` · 504 |
| Notes | Hanya https, IP privat/terlarang diblokir `netguard` secara permanen tanpa retry, timeout unduh 60 detik; dieksekusi via durable outbox + `Wait(60s)` |

## POST /v1/instances/{id}/contacts/check

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/contacts/check |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | numbers (array string, wajib, maks 20) |
| Response | 200 results[] (number, exists, jid) |
| Notes | Hasil di-cache pada `LRU` 2048 entri (TTL 24 jam) dan request konkuren batch uncached terurut digabung lewat `singleflight` untuk mencegah duplikasi query ke server |

## POST /v1/instances/{id}/media/download

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v1/instances/{id}/media/download |
| Auth Required | Yes (X-API-Key, scope `messages:read`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | Objek `media` dari payload webhook: direct_path, media_key, file_enc_sha256, file_sha256, file_length, media_type, mimetype |
| Response | 200 stream biner dengan `Content-Type`, `Content-Length`, `ETag`, dan `Content-Disposition` (dokumen) · 503 `memory_pressure` |
| Notes | Media tidak disimpan; diunduh on-demand ke file temp anonim via `Pipeline.Download` tanpa resurrection lalu file langsung dihapus saat close |

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
| Response | 200 `text/event-stream` (`id: <seq>`, `event: <type>`, `data: <envelope v2 JSON>`; heartbeat `: ping` tiap 25 detik) · 400 · 403 · 404 · 429 `sse_limit` · 503 `memory_pressure` / `journal_unavailable` |
| Notes | Kuota koneksi SSE dibatasi `WAAPI_SSE_MAX` (header `Retry-After: 10` saat penuh); koneksi baru ditolak `503 memory_pressure` (`Retry-After: 15`) saat governor level `Red`; write deadline per frame 30 detik via `http.ResponseController` |

## POST /v2/instances/{id}/media

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/media |
| Auth Required | Yes (X-API-Key, scope `media:write`) |
| Headers | X-API-Key, Content-Type: multipart/form-data |
| Query Params | `kind` (wajib: `image`, `video`, `audio`, `ptt`, `document`, `sticker`) · `filename` (opsional) · `mimetype` (opsional) · `seconds` (uint32, opsional) |
| Request Body | Multipart part `file` (stream biner) |
| Response | 201 `{ upload_id, kind, mimetype, size, sha256, width, height, seconds, expires_at }` · 400 · 413 · 415 · 503 `memory_pressure` |
| Notes | Upload satu-pass (temp anonim + drop-behind per 4 MiB); `sha256` hex lowercase, `expires_at` Unix detik (+24 jam); deadline baca/tulis 10 menit |

## GET /v2/instances/{id}/events/{seq}/media

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/events/{seq}/media |
| Auth Required | Yes (X-API-Key, scope `messages:read`) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 stream biner (`Content-Type`, `Content-Length`, `ETag: "<hex>"`, `Content-Disposition` untuk dokumen) · 404 `event_not_found` · 410 `media_gone` · 503 `memory_pressure` |
| Notes | Jika CDN merespons 404/410, otomatis menjalankan media resurrection (`SendMediaRetryReceipt` + tunggu `MediaRetry` maks 30s) lalu mengunduh ulang dengan `DirectPath` baru |

## POST /v2/instances/{id}/media/download

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/media/download |
| Auth Required | Yes (X-API-Key, scope `messages:read`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | Descriptor media v1 (`direct_path`, `media_key`, `file_enc_sha256`, `file_sha256`, `file_length`, `media_type`, `mimetype`, `filename?`) |
| Response | 200 stream biner (`Content-Type`, `Content-Length`, `ETag`, `Content-Disposition` untuk dokumen) · 400 · 503 `memory_pressure` |
| Notes | Alias v2 resmi untuk descriptor v1 tanpa media resurrection |

## POST /v2/instances/{id}/messages

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/messages |
| Auth Required | Yes (X-API-Key, scope `messages:send`) |
| Headers | X-API-Key, Content-Type: application/json, Idempotency-Key (opsional) |
| Query Params | - |
| Request Body | to (string, wajib) · type (`text` \| `image` \| `video` \| `audio` \| `document` \| `sticker` \| `location` \| `contact` \| `poll` \| `reaction` \| `edit` \| `revoke`) · text (1–60.000 byte untuk `text`) · media (`{ upload_id \| url, ... }`) · location (`{ lat, lng, name?, address? }`) · contacts (`[{ name, phones?, vcard? }]`, 1–10 kontak) · poll (`{ name, options, selectable }`, 2–12 opsi unik) · reaction (`{ message_id, sender?, emoji }`) · edit (`{ message_id, text }`) · revoke (`{ message_id, sender? }`) · reply_to (`{ id, sender? }`, opsional) · mentions (`[]string`, opsional) · typing (bool, opsional) · send_at (RFC3339, opsional) · priority (`normal` \| `urgent`, default `normal`) · ttl (Go duration, opsional) · wait (`none` \| `sent`, default `none`) |
| Response | `wait=none`: 202 (baru) / 200 (idempoten hit) `{ id, wa_message_id, status, to, not_before, status_url }` · `wait=sent`: 200 objek detail pesan · 400 · 404 `upload_not_found` · 409 · 410 `upload_expired` · 422 · 429 · 502 · 503 `memory_pressure` (bila `media.url` diisi saat `Red`) · 504 |
| Notes | `upload_id` divalidasi langsung di handler HTTP dan diperiksa ulang saat eksekusi (`media_expired`); pada level governor `Red`, hanya enqueue dengan `media.url` non-kosong yang ditolak `503 memory_pressure` sementara pesan teks & `upload_id` tetap diizinkan |

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

## POST /v2/instances/{id}/chats/{jid}/read

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/chats/{jid}/read |
| Auth Required | Yes (X-API-Key, scope `messages:send`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | message_ids (`[]string`, 1–50 ID, wajib) · sender (string, opsional untuk pesan grup) · played (bool, opsional, untuk `ReceiptTypePlayed`) |
| Response | 204 No Content · 400 `invalid_request` · 503 `instance_not_connected` |
| Notes | Memanggil `cli.MarkRead` dengan `time.Now()` serta `types.ReceiptTypePlayed` bila `played: true` |

## POST /v2/instances/{id}/presence

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/presence |
| Auth Required | Yes (X-API-Key, scope `messages:send`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | state (`available` \| `unavailable`, wajib) |
| Response | 204 No Content · 400 `invalid_request` · 503 `instance_not_connected` |
| Notes | Diperlukan `available` agar server WhatsApp mengirim update presence lawan bicara |

## POST /v2/instances/{id}/chats/{jid}/presence

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/chats/{jid}/presence |
| Auth Required | Yes (X-API-Key, scope `messages:send`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | state (`composing` \| `recording` \| `paused`, wajib) |
| Response | 204 No Content · 400 `invalid_request` · 503 `instance_not_connected` |
| Notes | `recording` memetakan ke `ChatPresenceComposing` dengan media audio |

## POST /v2/instances/{id}/presence/subscribe

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/presence/subscribe |
| Auth Required | Yes (X-API-Key, scope `messages:read`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | jid (string nomor atau JID target, wajib) |
| Response | 204 No Content · 400 `invalid_request` (termasuk jika melebihi batas 100 JID) · 503 `instance_not_connected` |
| Notes | Target disimpan di RAM (maks 100 JID unik/instance) dan otomatis di-subscribe ulang saat supervisor `online` |

## GET /v2/instances/{id}/polls/{wa_id}

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/polls/{wa_id} |
| Auth Required | Yes (X-API-Key, scope `messages:read`) |
| Headers | X-API-Key |
| Query Params | `chat` (string JID/nomor chat, **wajib**) |
| Request Body | - |
| Response | 200 `{ poll_id, chat, name, selectable, options: [{ name, votes, voters }], total_voters }` · 400 `invalid_request` · 404 `message_not_found` |
| Notes | Daftar `voters` dibatasi maks 100 per opsi; pemilih yang mencabut pilihan (`selected: []`) tidak dihitung di `total_voters` |

## GET /v2/instances/{id}/groups

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/groups |
| Auth Required | Yes (X-API-Key, scope `groups:read`) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 `{ groups: [{ jid, name, size, is_admin, announce, locked, created_at }] }` · 503 `instance_not_connected` |
| Notes | Ringkasan grup tanpa daftar peserta di-cache selama 60 detik per instance (`LRU` 128 entri) |

## POST /v2/instances/{id}/groups

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/groups |
| Auth Required | Yes (X-API-Key, scope `groups:write`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | `name` (string wajib 1..25 karakter), `participants` (array nomor/JID opsional, 0..50 peserta) |
| Response | 201 detail grup (identik skema `GET /groups/{jid}`) · 400 `invalid_request` · 403/429 dari `GroupGovernor` / WhatsApp |
| Notes | Grup kosong (0 peserta) diperbolehkan; validasi input dan pengecekan `GroupGovernor.AllowGroupCreate` dijalankan sebelum memanggil WhatsApp |

## GET /v2/instances/{id}/groups/{jid}

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/groups/{jid} |
| Auth Required | Yes (X-API-Key, scope `groups:read`) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 `{ jid, name, description, owner, created_at, announce, locked, join_approval, ephemeral_seconds, participants: [{ jid, pn?, is_admin, is_super_admin }], participants_truncated? }` · 403 `group_forbidden` · 404 `group_not_found` |
| Notes | `{jid}` wajib berakhiran `@g.us`; bila jumlah anggota > 1024, `participants` dipotong pada 1024 entri dengan `participants_truncated: true` |

## PATCH /v2/instances/{id}/groups/{jid}

| Field | Detail |
|---|---|
| Method + Endpoint | PATCH /v2/instances/{id}/groups/{jid} |
| Auth Required | Yes (X-API-Key, scope `groups:write`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | Opsional parsial (minimal satu): `name` (1..25 karakter), `description`, `announce` (bool), `locked` (bool), `join_approval` (bool) |
| Response | 200 detail grup lengkap terbaru (identik `GET /groups/{jid}`) · 400 `invalid_request` (body `{}` / nama invalid) · Error dengan `error.details.applied: [...]` jika gagal di tengah |
| Notes | Mutasi dijalankan berurutan (`name` -> `description` -> `announce` -> `locked` -> `join_approval`); bila gagal di tengah, `error.details.applied` mencatat field yang sudah sukses tembus ke WhatsApp |

## POST /v2/instances/{id}/groups/{jid}/participants

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/groups/{jid}/participants |
| Auth Required | Yes (X-API-Key, scope `groups:write`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | `action` (`add\|remove\|promote\|demote`), `participants` (array nomor/JID, 1..50 peserta) |
| Response | 200 `{ results: [{ jid, status }] }` (`ok\|privacy_requires_invite\|already_member\|not_on_whatsapp\|error_<kode>`) · 400 `invalid_request` · 403/429 dari `GroupGovernor` / IQError |
| Notes | `GroupGovernor.AllowParticipantAdd` diperiksa khusus saat `action == "add"` dan `PolicyChecker.CheckDestructive` diperiksa saat `action == "remove"` sebelum memanggil WhatsApp |

## GET /v2/instances/{id}/groups/{jid}/invite-link

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/groups/{jid}/invite-link |
| Auth Required | Yes (X-API-Key, scope `groups:read`, atau `groups:write` bila `reset=true`) |
| Headers | X-API-Key |
| Query Params | `reset` (bool opsional, default `false`) |
| Request Body | - |
| Response | 200 `{ link: "https://chat.whatsapp.com/..." }` · 403 `group_forbidden` · 404 `group_not_found` |
| Notes | Saat `reset=true`, tautan lama dicabut dan tautan undangan baru dibuat oleh server WhatsApp |

## POST /v2/instances/{id}/groups/join

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/groups/join |
| Auth Required | Yes (X-API-Key, scope `groups:write`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | Salah satu wajib: `code` (string kode undangan) atau `link` (URL `https://chat.whatsapp.com/...`) |
| Response | 200 `{ jid }` · 400 `invalid_request` · 502 `upstream_error` |
| Notes | Prefix URL dan query string diekstrak menjadi kode undangan serta menginvalidasi cache daftar grup instance |

## POST /v2/instances/{id}/groups/{jid}/leave

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/groups/{jid}/leave |
| Auth Required | Yes (X-API-Key, scope `groups:write`) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 204 No Content · 403 `group_forbidden` · 404 `group_not_found` |
| Notes | Mengeluarkan akun instance dari grup dan menghapus entri ringkasan grup instance di cache |

## GET /v2/instances/{id}/groups/{jid}/requests

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/groups/{jid}/requests |
| Auth Required | Yes (X-API-Key, scope `groups:read`) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 `{ requests: [{ jid, requested_at }] }` (`requested_at` Unix detik) · 403 `group_forbidden` · 404 `group_not_found` |
| Notes | Mengambil daftar permintaan bergabung ke grup yang menunggu persetujuan admin |

## POST /v2/instances/{id}/groups/{jid}/requests

| Field | Detail |
|---|---|
| Method + Endpoint | POST /v2/instances/{id}/groups/{jid}/requests |
| Auth Required | Yes (X-API-Key, scope `groups:write`) |
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | `action` (`approve\|reject`), `participants` (array nomor/JID, 1..50 peserta) |
| Response | 200 `{ results: [{ jid, status }] }` · 400 `invalid_request` · 403 `group_forbidden` |
| Notes | Pemetaan field `status` per peserta identik dengan `POST /groups/{jid}/participants` |

## GET /v2/instances/{id}/contacts/{jid}

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/contacts/{jid} |
| Auth Required | Yes (X-API-Key, scope `messages:read`) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 `{ jid, pn, lid, push_name, full_name, business_name, about, picture: { url, id } \| null, verified_name }` · 400 `invalid_number`/`invalid_request` · 503 `instance_not_connected` |
| Notes | Profil di-cache di `LRU` 256 entri (TTL 1 jam); input `@lid` diresolusi ke PN terlebih dahulu via `Store.LIDs`; bila foto profil tidak disetel atau dibatasi privasi (401/404), endpoint tetap mengembalikan 200 dengan `picture: null`; `verified_name` bertipe string |

## GET /v2/instances/{id}/health

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/health |
| Auth Required | Yes (X-API-Key, scope `health:read`) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 `{ instance, state, score, profile, shadow, factors: [{ code, impact, detail }], counters: { sent_24h, first_contact_24h, delivered_24h, replied_24h }, limits: { daily_cap, daily_remaining, first_contact_cap, first_contact_remaining, warmup_factor, quiet_hours }, ban: { active, last_code?, last_at?, until } }` · 404 `instance_not_found` |
| Notes | `factors` diurutkan menurut penalti `impact` terberat lebih dulu (mis. `-60`, `-22`, `+8`); selama shadow mode aktif (`shadow: true`), aturan 3–7 `Decide` dan kuota grup hanya dicatat ke `waapi_guardian_shadow_total` |

## GET /v2/instances/{id}/settings

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/settings |
| Auth Required | Yes (`X-API-Key` / `Bearer`, scope `settings:admin`) |
| Headers | `X-API-Key` atau `Authorization: Bearer wak_...` |
| Query Params | - |
| Request Body | - |
| Response | 200 langsung objek `store.InstanceSettings` di level atas JSON · 401 · 403 `scope_denied` · 404 |
| Notes | - |

## PATCH /v2/instances/{id}/settings

| Field | Detail |
|---|---|
| Method + Endpoint | PATCH /v2/instances/{id}/settings |
| Auth Required | Yes (`X-API-Key` / `Bearer`, scope `settings:admin`) |
| Headers | `X-API-Key` atau `Authorization: Bearer wak_...`, Content-Type: application/json |
| Query Params | - |
| Request Body | Patch JSON parsial terhadap field `InstanceSettings` (field asing atau nilai tidak valid ditolak `400`) |
| Response | 200 langsung objek `store.InstanceSettings` terbaru · 400 `invalid_request` / `invalid_spec` · 403 `scope_denied` · 404 |
| Notes | Menerapkan `InstanceSettings.Merge`, menyinkronkan cache target/instance di memori, dan mencatat entri `settings.update` ke `audit_log` |

## GET /v2/instances/{id}/chats

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/chats |
| Auth Required | Yes (`X-API-Key` / `Bearer`, scope `messages:read`) |
| Headers | `X-API-Key` atau `Authorization: Bearer wak_...` |
| Query Params | `limit` (int, default `50`, maks `100`), `before` (opsional, cursor `<ts>:<chat>` atau Unix detik `<ts>`) |
| Request Body | - |
| Response | 200 `{ chats: [{ chat, chat_type, name, last_ts, last_preview, unread }], next_before }` (`next_before` string cursor atau `null`) · 400 `invalid_request` · 409 `context_store_disabled` |
| Notes | Diurutkan menurun berdasar `last_ts`; `chat_type` diturunkan dari domain JID (`dm\|group\|newsletter\|broadcast\|status`) |

## GET /v2/instances/{id}/chats/{jid}/messages

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/chats/{jid}/messages |
| Auth Required | Yes (`X-API-Key` / `Bearer`, scope `messages:read`) |
| Headers | `X-API-Key` atau `Authorization: Bearer wak_...` |
| Query Params | `limit` (int, default `20`, maks `100`), `before` (opsional, cursor `<ts>:<id>` atau Unix detik `<ts>`) |
| Request Body | - |
| Response | 200 `{ messages: [{ id, from, from_me, timestamp, type, text, quoted_id }], next_before }` (terurut naik kronologis dalam halaman) · 400 · 409 `context_store_disabled` |
| Notes | Tidak menyimpan media; pesan `reaction`/`status@broadcast` dilewati, pesan `revoke` terhapus beserta FTS5 |

## GET /v2/instances/{id}/search

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v2/instances/{id}/search |
| Auth Required | Yes (`X-API-Key` / `Bearer`, scope `messages:read`) |
| Headers | `X-API-Key` atau `Authorization: Bearer wak_...` |
| Query Params | `q` (string wajib), `chat` (opsional, filter JID/nomor), `limit` (int, default `20`, maks `100`), `prefix` (bool, default `false`) |
| Request Body | - |
| Response | 200 `{ hits: [{ chat, id, from, from_me, timestamp, type, snippet, rank }] }` · 400 `invalid_request` · 409 `context_store_disabled` |
| Notes | Pencarian FTS5 (`unicode61 remove_diacritics 2`) BM25; seluruh token dibungkus kutip ganda aman sintaks, `prefix=true` menambah `*` di token terakhir |

## POST /mcp

| Field | Detail |
|---|---|
| Method + Endpoint | POST /mcp (beserta `GET` / `DELETE` Streamable HTTP MCP 2026-07-28) |
| Auth Required | Yes (`Authorization: Bearer` / `X-API-Key`, scope `mcp`, dibatasi `instance_id` key bila terisi) |
| Headers | `Authorization: Bearer wak_...` atau `X-API-Key`, `MCP-Protocol-Version` (`2026-07-28` / `2025-11-25`), `Mcp-Method`, `Mcp-Name`, `Mcp-Session-Id`, `Origin` (bila dikirim, wajib cocok `WAAPI_MCP_ALLOWED_ORIGINS`) |
| Query Params | - |
| Request Body | JSON-RPC 2.0 MCP (maks `1 MiB`), termasuk `initialize`, `server/discover`, `tools/list`, `tools/call`, `resources/list`, `resources/templates/list`, `resources/read`, `resources/subscribe`, `resources/unsubscribe`, `subscriptions/listen` |
| Response | `200` (`application/json` atau `text/event-stream`) · `202` notifikasi · `403 origin_forbidden` / `scope_denied` · `404 mcp_disabled` · `429 mcp_overloaded` (semafor `8`) |
| Notes | Lazy init pada request pertama; `X-Waapi-Principal` eksternal dihapus middleware; 15 tool terfilter per `KeyPolicy.Tools`; elisitasi MRTR / JSON-RPC error `-32042` + `approval_token` HMAC-SHA256 (`5m`) untuk `first_contact=ask` dan `destructive=ask`; langganan resource maks `32` URI global (`0` subscriber = `0` goroutine) |

## Webhook Keluar (dikirim daemon ke backend)

| Field | Detail |
|---|---|
| Method + Endpoint | POST ke `webhook_url` instance (`User-Agent: waapi-webhook/2.0`) |
| Headers | Mode `waapi`: `X-WA-Event`, `X-WA-Instance`, `X-WA-Timestamp`, `X-WA-Event-Id`, `X-WA-Delivery-Attempt`, `X-WA-Signature` (`sha256=` HMAC `timestamp.body`). Mode `standard`: `webhook-id`, `webhook-timestamp`, `webhook-signature` (`v1,<base64>` Standard Webhooks) |
| Body | Envelope v2 (`v: 2`, `id`, `event`, `instance`, `timestamp`, `data`) dibaca bertahap dari `journal.db` |
| Event | message, receipt, undecryptable, connection, pairing, outbox, poll_vote, presence, chat_presence, group, call, health, consent (dan `test` untuk `waapi webhook test`). Default untuk instance baru: `message,receipt,connection,pairing,outbox,poll_vote,health`. Payload `outbox`: `{ id, wa_message_id, to, type, status, attempts, timestamp, error? }` (`status`: `sent\|failed\|expired`, `timestamp` Unix detik). Payload `poll_vote`: `{ poll_id, chat, voter, voter_pn?, selected, selected_hashes, poll_known, timestamp }`. Payload `presence`: `{ from, from_pn?, unavailable, last_seen? }`. Payload `chat_presence`: `{ chat, sender, state, media? }`. Payload `group`: `{ group, actor?, actor_pn?, timestamp, changes: [{ action, participants?, value? }], join_reason?, name? }`. Payload `call`: `{ call_id, from, from_pn?, group?, action, reason? }` (`c:<call_id>:<offer\|rejected\|terminate>`). Payload `health`: `{ state, prev_state, score, profile, shadow, reason?, ban_code?, ban_until? }`. Payload `consent`: `{ jid, action, keyword }` |
| Notes | Pengiriman berurutan per `lane = part % webhook_lanes` (default 4 lane). Jadwal retry `0, 5s, 15s, 45s, 2m, 5m, 15m, 30m, 1h...` (±20% jitter) hingga `webhook_max_attempts` (default 12), menghormati `Retry-After` (maks 1h); `410 Gone` mem-pause target (`gone`); `4xx` lainnya maks 3 percobaan lalu dipindah ke `webhook_dead` (DLQ). Target dibaca dari `TargetCache` memori |

## Control Plane (CLI only, Unix socket)

Tidak diekspos via HTTP publik; hanya diakses perintah `wa-api` lewat `control.sock` (perm 0660).
- `PATCH /control/instances/{id}` menerima field tambahan `settings` (objek JSON parsial untuk di-merge ke `InstanceSettings`; diakses dari CLI lewat `waapi edit <id> --set key=value`) serta `webhook_from` (`now` atau `retained`, diakses lewat `waapi webhook set <id> <url> [--from now|retained]`).
- `POST /control/instances/{id}/webhook/resume` menghapus status `webhook_paused` dan menyalakan ulang deliverer dari cursor tersimpan (`waapi webhook resume <id>`).
- `GET /control/instances/{id}/webhook/dlq` (`?limit=N`), `POST /control/instances/{id}/webhook/dlq/replay` (`{ all, seq, seqs }`), dan `DELETE /control/instances/{id}/webhook/dlq` mengelola dead-letter queue (`waapi webhook dlq <id> <ls|replay|purge>`).
- `POST /control/instances/{id}/messages` mengantrekan pesan teks ke Durable Outbox dari kontrol lokal `{ to, text, reply_to?, typing?, wait?, idem_key? }` (`wait`: `none` atau `sent` maks 30 detik), diakses melalui CLI `waapi send <id> <to> <text> [--wait] [--idem KEY] [--typing] [--json]`.
- `GET /control/doctor?deep=1` mengembalikan `DeepReport` (blok proses `procfs`/`smaps_rollup`, `runtime/metrics`, cgroup v2 & PSI, THP mode, `DBStatus` SQLite per DB termasuk `journal.db`, ringkasan per-instance `webhook`: `head_seq`, `min_cursor`, `lag`, `oldest_undelivered_sec`, `dlq`, `paused`, `dropped_undelivered`, field `governor_level`, metadata build, dan jumlah `goroutineleak`), diakses via `waapi doctor --deep [--json]` (menampilkan saran `GODEBUG=disablethp=1` bila THP `always`). `GET /control/doctor` standar juga menyertakan `governor_level`.
- `GET /control/instances/{id}/events` mengambil atau long-poll (`?after=N&limit=N&types=csv&wait=N`), dan `GET /control/instances/{id}/events/stream` melakukan SSE stream event journal v2 per instance via `waapi events <id> [--after N] [--types csv] [--limit N] [--follow] [--json]`.
- `GET /control/debug/pprof/*` menyediakan endpoint `net/http/pprof` (`heap`, `goroutine`, `goroutineleak`, `profile`, `cmdline`, `symbol`, `trace`), diakses via `waapi debug pprof <profil> [-o file] [--seconds N]`.
- `POST /control/instances/{id}/recycle` menghentikan supervisor, melepas client lama, memuat ulang device `sqlstore`, dan menyalakan ulang supervisor (`waapi recycle <id>`). Ditolak HTTP 400 bila belum dipairing.
- `GET /control/instances/{id}/health` mengembalikan `HealthReport` Guardian lengkap untuk sebuah instance (`waapi health <id> [--json]`).
- `POST /control/keys` mendukung atribut v2 (`scopes`, `expires_in`/`expires_at`, `rate_per_sec`, `policy`), `GET /control/keys/{id}` menampilkan detail metadata key tanpa rahasia (`waapi key show <id>`), dan `POST /control/keys/{id}/rotate` menerbitkan key pengganti sekaligus menetapkan masa grace key lama (`waapi key rotate <id> [--grace 24h]`).
- `GET /control/audit` (`?since=24h&action=...&limit=100`) membaca entri `audit_log` beserta actor UID (`SO_PEERCRED` Linux) / key ID (`waapi audit`).
- `POST /control/backup` membuat backup atomik `VACUUM INTO` untuk `app.db`, `journal.db`, dan `wa.db` ke `backups/db-<versi>-<timestamp>/` (`0600`, retensi 3 backup terbaru); dipanggil otomatis oleh `waapi update` bila daemon hidup. Socket `control.sock` dibuat atomik lewat temp socket -> `chmod 0660` -> `rename`.
- `POST /control/instances/{id}/context` (`{ enabled, purge? }`) mengaktifkan/mematikan context store sekaligus opsional mem-purge data instance, dan `POST /control/instances/{id}/context/purge` (`{ chat? }`) membersihkan satu chat atau seluruh instance (`waapi context <id> on|off [--purge]|purge [--chat jid]`, mencatat `audit_log`).
- `/control/mcp` (`POST`/`GET`/`DELETE`) mengekspos server MCP Streamable HTTP ke `control.sock` dengan dukungan header `X-Waapi-Key-Id` opsional atau default policy operator lokal (`first_contact: ask`, `destructive: ask`); diakses dari CLI melalui bridge stdio `waapi mcp [--key KEY_ID] [--max-inflight 8]` (batas baris `1 MiB`, meneruskan `MCP-Protocol-Version`/`Mcp-Method`/`Mcp-Name`/`Mcp-Session-Id`).

## Scheduled Tasks

Belum ada `routes/web.php` / `Kernel.php` (bukan proyek Laravel). Tugas berkala dan daemon loop internal:

| Tugas | Frekuensi | Tujuan | Efek samping |
|---|---|---|---|
| Migrasi `webhook_outbox` legacy | Sekali saat startup | Pindahkan backlog v1 ke `journal.db` | Kosongkan baris `webhook_outbox` (tabel tetap dipertahankan) |
| Scheduler outbox per instance | Berbasis sinyal & timer `not_before` | Klaim dan kirim semua tipe pesan v2 (`text`, media, `location`, `contact`, `poll`, `reaction`, `edit`, `revoke`) serta unduh `media.url` | Simpan `polls` keluar sebelum `SendMessage`, perbarui status outbox, dan tulis event `outbox` ke `journal.db` |
| Re-subscribe presence | Saat supervisor instance masuk state `online` | Berlangganan ulang hingga 100 JID presence yang terdaftar di RAM instance | Memanggil `cli.SubscribePresence` per JID agar pembaruan status tetap mengalir pasca-reconnect |
| Outbox lease recovery | Saat startup & tiap 60 detik | Kembalikan pekerjaan `sending` yang lease-nya kedaluwarsa (crash/kill) | Ubah status kembali ke `queued` dan bangunkan scheduler instance |
| Webhook lane deliverer | Berbasis notif (`Subscribe`) + fallback idle | Kirim envelope v2 per lane dari `journal.db` ke `webhook_url` | Commit cursor, pause saat `410`, catat gagal permanen ke `webhook_dead` |
| Flush last_used_at | Tiap 60 detik | Simpan waktu pakai API key | Write batch ke app.db |
| Journal pruner | Tiap 5 menit (+ tugas per jam) | Pangkas baris `journal.db` sesuai replay window, retensi, batas baris, pangkas context store (`context_per_chat` & `context_max_age`), dan jalankan pemangkasan final `outbox`, `uploads`, bucket Guardian, serta `audit_log` (> 90 hari) tiap jam | Hapus baris lama (kecuali `webhook_dead`), pangkas chat tersentuh & pesan context kedaluwarsa per chunk `1000`, bersihkan baris final `outbox`, descriptor `uploads` > 24h, `guardian_hourly` > 8 hari, `guardian_fanout` > 2 hari, `audit_log` > 90 hari, serta inkremen `dropped_undelivered_total` |
| Guardian bucket flusher | Tiap 30 detik & saat shutdown | Sinkronkan delta ring counter 25 jam dari RAM ke tabel `guardian_hourly` di `journal.db` | Menjaga hitungan skor dan kuota tetap akurat setelah restart tanpa write DB per pesan |
| Temp sweeper | Saat startup | Hapus file media sementara > 1 jam | Hapus file di `tmp/` |
| Systemd watchdog loop | Tiap `WATCHDOG_USEC/2` (bila disetel) | Kirim `WATCHDOG=1` ke `$NOTIFY_SOCKET` setelah `SELECT 1` ke `app.db` lulus <= 500 ms | Menunda heartbeat jika DB macet agar systemd dapat me-restart service |
| Memory Governor loop | Tiap 5 detik | Sampel memori anonim cgroup/procfs + PSI dengan hysteresis bertahap | Atur `GOGC` (`25`/`BaseGOGC`), jalankan `PRAGMA shrink_memory` (`Yellow`), `FreeOSMemory()` (`Red`), pacing webhook `200 ms`, dan auto-recycle round-robin saat `Red >= 10m` & `WAAPI_AUTO_RECYCLE=on` |
| Scheduled recycle harian | Tiap 30 detik cek `recycle_at` lokal | Refresh client `wa.Manager.Recycle` sekali sehari di luar jam sibuk | Dilewati untuk hari itu bila ada job `sending` atau aktivitas pesan 5 menit terakhir |
| Boot binary pageout | 90 detik setelah start (bila `WAAPI_PAGEOUT_AFTER_BOOT` `cold` \| `pageout`) | Terapkan `SYS_MADVISE` pada mapping `r-xp`/`r--p` binary Linux (mati secara default) | Tandai halaman teks/rodata binary agar siap di-reclaim kernel |
| Fanout subscription MCP | On-demand selama URI terlanggan > `0` (`0` subscriber = `0` goroutine) | Pantau notifikasi `journal.Subscribe` dan teruskan ke sesi MCP | Mengalirkan pembaruan resource `waapi://instances/{instance}/chats/{+chat}` & `.../health` (maks `32` URI global) |

