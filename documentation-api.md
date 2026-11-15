# Dokumentasi API — wa-api

> Status: terimplementasi (`internal/httpapi`, `internal/control`, `internal/cli`). Base URL: `http://127.0.0.1:8080` (diekspos via reverse proxy TLS).
> Autentikasi: header `X-API-Key: wak_...` (bukan Sanctum). Key dibuat lewat CLI `waapi key add --name <nama>`.

## Format Error Umum

| HTTP | code | Arti |
|---|---|---|
| 400 | invalid_request / invalid_number | Payload atau nomor tidak valid |
| 401 | unauthorized | Key tidak ada, salah, atau dicabut |
| 403 | forbidden_instance | Key tidak punya akses ke instance |
| 404 | instance_not_found | Instance tidak ada |
| 413 | media_too_large | Media melebihi batas |
| 429 | queue_full | Antrian kirim penuh (ada `retry_after`) |
| 503 | not_ready | Daemon tidak siap (DB gagal atau tekanan memori level `Red`) |
| 503 | instance_not_connected | Instance belum/tidak tersambung |
| 504 | send_timeout | WhatsApp tidak merespons |

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
| Headers | X-API-Key, Content-Type: application/json |
| Query Params | - |
| Request Body | to (string, wajib, nomor E.164 tanpa + atau JID) · text (string, wajib) · typing (bool, opsional) · reply_to (string, opsional, message id) |
| Response | 200 message_id, to, timestamp · 429 · 503 · 504 |
| Notes | Body maks 64 KB; dikirim lewat antrian ber-rate-limit per instance |

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

## Webhook Keluar (dikirim daemon ke backend)

| Field | Detail |
|---|---|
| Method + Endpoint | POST ke `webhook_url` instance |
| Headers | X-WA-Event, X-WA-Instance, X-WA-Timestamp, X-WA-Signature (`sha256=` HMAC dari `timestamp.body`) |
| Body | event, instance, timestamp, data (message/receipt/pairing atau connection aditif: status, state, prev_state, jid, reason, attempt, next_retry_in, circuit, ban) |
| Event | message, receipt, connection, pairing |
| Notes | Retry maks 8 kali dengan backoff; penerima wajib membalas 2xx dalam 10 detik dan menolak timestamp lebih dari 5 menit |

## Control Plane (CLI only, Unix socket)

Tidak diekspos via HTTP publik; hanya diakses perintah `wa-api` lewat `control.sock` (perm 0660).
- `PATCH /control/instances/{id}` menerima field tambahan `settings` (objek JSON parsial untuk di-merge ke `InstanceSettings`; diakses dari CLI lewat `waapi edit <id> --set key=value`).
- `GET /control/doctor?deep=1` mengembalikan `DeepReport` (blok proses `procfs`/`smaps_rollup`, `runtime/metrics`, cgroup v2 & PSI, THP mode, `DBStatus` SQLite per DB, metadata build, dan jumlah `goroutineleak`), diakses via `waapi doctor --deep [--json]`.
- `GET /control/debug/pprof/*` menyediakan endpoint `net/http/pprof` (`heap`, `goroutine`, `goroutineleak`, `profile`, `cmdline`, `symbol`, `trace`), diakses via `waapi debug pprof <profil> [-o file] [--seconds N]`.
- `POST /control/instances/{id}/recycle` menghentikan supervisor, melepas client lama, memuat ulang device `sqlstore`, dan menyalakan ulang supervisor (`waapi recycle <id>`). Ditolak HTTP 400 bila belum dipairing.

## Scheduled Tasks

Belum ada `routes/web.php` / `Kernel.php` (bukan proyek Laravel). Tugas berkala internal daemon:

| Tugas | Frekuensi | Tujuan | Efek samping |
|---|---|---|---|
| Outbox poller | Tiap 5 detik | Kirim ulang webhook gagal | Hapus baris > 8 percobaan / > 24 jam |
| Flush last_used_at | Tiap 60 detik | Simpan waktu pakai API key | Write batch ke app.db |
| Temp sweeper | Saat startup | Hapus file media sementara > 1 jam | Hapus file di `tmp/` |
| Systemd watchdog loop | Tiap `WATCHDOG_USEC/2` (bila disetel) | Kirim `WATCHDOG=1` ke `$NOTIFY_SOCKET` setelah `SELECT 1` ke `app.db` lulus <= 500 ms | Menunda heartbeat jika DB macet agar systemd dapat me-restart service |

