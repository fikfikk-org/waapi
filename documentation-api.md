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

## GET /v1/instances

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v1/instances |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 array instance (id, status, jid, queue_depth, rate_per_min) |
| Notes | Hanya instance sesuai scope key |

## GET /v1/instances/{id}

| Field | Detail |
|---|---|
| Method + Endpoint | GET /v1/instances/{id} |
| Auth Required | Yes (X-API-Key) |
| Headers | X-API-Key |
| Query Params | - |
| Request Body | - |
| Response | 200 id, status, jid, push_name, queue_depth, rate_per_min, connected_since · 403 · 404 |
| Notes | - |

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
| Body | event, instance, timestamp, data (id, from, chat, is_group, push_name, type, text, media) |
| Event | message, receipt, connection, pairing |
| Notes | Retry maks 8 kali dengan backoff; penerima wajib membalas 2xx dalam 10 detik dan menolak timestamp lebih dari 5 menit |

## Control Plane (CLI only, Unix socket)

Tidak diekspos via HTTP publik; hanya diakses perintah `wa-api` lewat `control.sock` (perm 0600).

## Scheduled Tasks

Belum ada `routes/web.php` / `Kernel.php` (bukan proyek Laravel). Tugas berkala internal daemon:

| Tugas | Frekuensi | Tujuan | Efek samping |
|---|---|---|---|
| Outbox poller | Tiap 5 detik | Kirim ulang webhook gagal | Hapus baris > 8 percobaan / > 24 jam |
| Flush last_used_at | Tiap 60 detik | Simpan waktu pakai API key | Write batch ke app.db |
| Temp sweeper | Saat startup | Hapus file media sementara > 1 jam | Hapus file di `tmp/` |

