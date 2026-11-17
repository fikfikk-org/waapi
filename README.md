# waapi — Ultra-Lightweight Headless WhatsApp API Gateway (v2.0.0)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/fikfikk-org/waapi)](https://github.com/fikfikk-org/waapi/releases)

Private WhatsApp Multi-Device API Gateway dalam **1 binary statis Go** (`go 1.27`, `CGO_ENABLED=0`, `whatsmeow` + `modernc.org/sqlite`) tanpa admin panel web dan tanpa layanan eksternal wajib. Seluruh backlog masuk maupun keluar disimpan di disk (`journal.db`), sementara memori kerja daemon dikendalikan oleh **Memory Governor** (`micro` / `lean` / `classic`, default `lean`: `GOMEMLIMIT=28MiB`, `MemoryMax=80M`).

> [!NOTE]
> **Perubahan penting v2.0.0 dibanding v1.0.2:** `waapi` kini mencatat event masuk & status pesan keluar secara sementara di `journal.db` untuk menjamin pengiriman nol-hilang, urutan webhook per chat, dan Durable Outbox. **Context Store (FTS5)** tetap **mati secara default** (`context_store: false`) untuk mempertahankan prinsip minimal retensi percakapan. Detail migrasi ada di [docs/MIGRATION-V2.md](docs/MIGRATION-V2.md).

---

## Daftar Isi

1. [Instalasi di Server Linux (systemd)](#1-instalasi-di-server-linux-systemd)
2. [Panduan Cepat: 5 Langkah Menuju Pesan Pertama](#2-panduan-cepat-5-langkah-menuju-pesan-pertama)
3. [Fitur Utama v2.0.0](#3-fitur-utama-v200)
4. [Contoh Pemanggilan REST API (v2 & Kompatibilitas v1)](#4-contoh-pemanggilan-rest-api-v2--kompatibilitas-v1)
5. [Webhook v2, Standard Webhooks & Event Stream](#5-webhook-v2-standard-webhooks--event-stream)
6. [Referensi Perintah CLI (`waapi`)](#6-referensi-perintah-cli-waapi)
7. [Konfigurasi Environment Variable & Profil Memori](#7-konfigurasi-environment-variable--profil-memori)
8. [Menjalankan di Lokal & Backup / Update](#8-menjalankan-di-lokal--backup--update)
9. [Dokumentasi Lanjutan](#9-dokumentasi-lanjutan)

---

## 1. Instalasi di Server Linux (systemd)

### Opsi A — One-Liner Installer (Direkomendasikan)

Mengunduh binary statis (`amd64` / `arm64`), membuat user `waapi`, menyiapkan `/var/lib/waapi`, serta memasang service `systemd` (`Type=notify` + `WatchdogSec=30s`, profil default `lean`):

```bash
curl -fsSL https://raw.githubusercontent.com/fikfikk-org/waapi/main/deploy/install.sh | sudo WAAPI_PROFILE=lean bash
```

Pilih `WAAPI_PROFILE=micro` untuk VPS ≤ 512 MB (`MemoryMax=64M`), `lean` sebagai default (`MemoryMax=80M`), atau `classic` untuk beban lebih tinggi (`MemoryMax=100M`).

```bash
source /etc/profile.d/waapi.sh
```

### Opsi B — Build dari Source di Server

Membutuhkan **Go 1.27+**:

```bash
git clone https://github.com/fikfikk-org/waapi.git
cd waapi
make build-linux          # atau make build-linux-arm64
sudo bash deploy/install.sh
source /etc/profile.d/waapi.sh
```

---

## 2. Panduan Cepat: 5 Langkah Menuju Pesan Pertama

### Langkah 1 — Pastikan daemon aktif & sehat

```bash
waapi status
```

Untuk inspeksi memori lengkap (RSS, `Pss_Anon`, cgroup v2, PSI, THP, ukuran database, lag webhook, dan level Memory Governor):

```bash
waapi doctor --deep
```

### Langkah 2 — Buat instance WhatsApp baru

```bash
waapi add sales --rate 20
```

### Langkah 3 — Tautkan (Pairing) nomor WhatsApp

**Metode A — Scan QR Code langsung di terminal:**
```bash
waapi pair sales
```

**Metode B — Menggunakan 8-Digit Pairing Code (tanpa kamera):**
```bash
waapi pair sales --phone 6281234567890
```

Pastikan status sudah `connected`:
```bash
waapi list
```

### Langkah 4 — Buat API Key

API key baru secara default menerima scope minimal yang aman (`instances:read,messages:send,messages:read,media:write,events:read,health:read`). Tambahkan `--scopes` bila memerlukan izin tambahan (`pair`, `groups:read`, `groups:write`, `settings:admin`, `hooks:admin`, `mcp`, atau `*`):

```bash
waapi key add --name backend-prod --instance sales
```

> **Penting:** Salin nilai `Key` (`wak_...`) segera karena tidak pernah ditampilkan ulang. Anda dapat mengirim API key menggunakan header `X-API-Key: wak_...` maupun `Authorization: Bearer wak_...`.

### Langkah 5 — Kirim pesan pertama melalui Durable Outbox (v2)

```bash
curl -X POST http://127.0.0.1:8080/v2/instances/sales/messages \
  -H "Authorization: Bearer wak_..." \
  -H "Idempotency-Key: order-1024-init" \
  -H "Content-Type: application/json" \
  -d '{
    "to": "6289876543210",
    "type": "text",
    "text": "Halo! Pesanan #1024 sedang diproses.",
    "typing": true
  }'
```

Respons (`202 Accepted`):
```json
{
  "id": "msg_01J...",
  "wa_message_id": "3EB0...",
  "status": "queued",
  "to": "6289876543210@s.whatsapp.net",
  "not_before": 1794360012,
  "status_url": "/v2/instances/sales/messages/msg_01J..."
}
```

> Endpoint `/v1/instances/{id}/messages/text` tetap didukung penuh untuk klien v1 yang mengharapkan respons sinkron (`wait` hingga 60 detik).

---

## 3. Fitur Utama v2.0.0

- **Zero-Loss Inbound Journal:** Memakai `SynchronousAck` + penyimpanan ke `journal.db` sebelum mengirim ack ke server WhatsApp, lengkap dengan dedupe key & resolusi `@lid` (`sender_pn`).
- **Webhook v2 & Pull/SSE API:** Pengiriman berurutan per chat (`webhook_lanes`), tanda tangan `waapi` atau **Standard Webhooks** (`whsec_` + `v1,base64`), DLQ (`waapi webhook dlq`), Pull API, dan SSE (`Last-Event-ID`).
- **Durable Outbox & Rich Messaging:** Pesan teks, media (upload sekali -> pakai ulang `upload_id`), *voice note* PTT OGG/Opus, stiker WebP, lokasi, kartu kontak vCard, polling bertally SHA-256 terdekripsi, reaksi, edit, tarik pesan (*revoke*), *quoted reply* penuh, dan penjadwalan `send_at`.
- **Self-Healing Supervisor & Media Resurrection:** Deteksi socket half-open (`75s` keepalive + `TCP_USER_TIMEOUT` `45s`), backoff + circuit breaker, `waapi recycle`, serta pemulihan media CDN `404/410` otomatis lewat `SendMediaRetryReceipt`.
- **Account Health Guardian & Memory Governor:** Skor kesehatan 0–100, *consent ledger* (`STOP`/`BERHENTI` otomatis), *quiet hours*, *warm-up* nomor baru, pembatasan fan-out/first-contact, serta pengendalian tekanan memori (`Green`/`Yellow`/`Red`, admission `503 memory_pressure`).
- **Security v2:** Scope per API key, expiry & rotasi grace period, `KeyPolicy`, `audit_log` + `SO_PEERCRED`, proteksi SSRF [`internal/netguard`](internal/netguard/netguard.go), socket atomik `0660`, dan verifikasi update Ed25519 *fail-closed*.
- **Opsional Zero-Cost Saat Mati:**
  - **Context Store + FTS5 BM25** (`context_store=true` per instance).
  - **Agent-Native MCP Server** (`2026-07-28`, stdio `waapi mcp` dan `POST /mcp`, lazy init).
  - **Reactive Hooks deklaratif & AI Responder** (`ai_reply` kompatibel OpenAI/Ollama tanpa tool calling, *human takeover*, kata kunci handoff).

---

## 4. Contoh Pemanggilan REST API (v2 & Kompatibilitas v1)

Dokumentasi endpoint lengkap tersedia di [documentation-api.md](documentation-api.md).

### A. Upload Media Sekali -> Kirim Banyak Penerima (v2)

```bash
# 1. Upload streaming (mendukung ?kind=image|video|audio|ptt|document|sticker)
curl -X POST "http://127.0.0.1:8080/v2/instances/sales/media?kind=document&filename=katalog.pdf" \
  -H "X-API-Key: wak_..." \
  --data-binary "@./katalog.pdf"
# Menghasilkan: { "upload_id": "upl_...", "expires_at": ... }

# 2. Kirim menggunakan upload_id (bisa dipakai ulang dalam 24 jam)
curl -X POST http://127.0.0.1:8080/v2/instances/sales/messages \
  -H "X-API-Key: wak_..." \
  -H "Content-Type: application/json" \
  -d '{
    "to": "6289876543210",
    "type": "document",
    "media": { "upload_id": "upl_...", "caption": "Katalog terbaru" }
  }'
```

### B. Cek Status Pengiriman Outbox & Batalkan Pesan Terjadwal

```bash
curl http://127.0.0.1:8080/v2/instances/sales/messages/msg_01J... \
  -H "X-API-Key: wak_..."

curl -X DELETE http://127.0.0.1:8080/v2/instances/sales/messages/msg_01J... \
  -H "X-API-Key: wak_..."
```

### C. Kirim Polling, Reaksi & Penandaan Terbaca

```bash
curl -X POST http://127.0.0.1:8080/v2/instances/sales/messages \
  -H "X-API-Key: wak_..." \
  -H "Content-Type: application/json" \
  -d '{
    "to": "1203630123456789@g.us",
    "type": "poll",
    "poll": { "name": "Jadwal rilis?", "options": ["Besok", "Lusa"], "selectable": 1 }
  }'
```

### D. Manajemen Grup, Kesehatan Guardian & Pull/SSE Events

```bash
curl http://127.0.0.1:8080/v2/instances/sales/health \
  -H "X-API-Key: wak_..."

curl "http://127.0.0.1:8080/v2/instances/sales/events?after=0&limit=50&wait=20" \
  -H "X-API-Key: wak_..."
```

---

## 5. Webhook v2, Standard Webhooks & Event Stream

Pasang webhook (secara default mulai dari posisi `seq` terbaru via `--from now`, atau `--from retained` untuk memutar backlog tersimpan):

```bash
waapi webhook set sales https://backend.anda.com/webhook/wa --from now
# Opsional aktifkan Standard Webhooks:
waapi edit sales --set webhook_format=standard
```

Pelajari struktur Envelope v2, verifikasi tanda tangan Go/PHP/Node/Python, penanganan `410 Gone`, dan antrean DLQ di [docs/WEBHOOKS.md](docs/WEBHOOKS.md).

---

## 6. Referensi Perintah CLI (`waapi`)

| Perintah | Kegunaan |
|---|---|
| `waapi list` / `add <id>` / `show <id>` / `edit <id>` | Kelola instance (`--rate`, `--webhook`, `--events`, `--autostart`, `--rotate-secret`, `--set key=val`) |
| `waapi pair <id> [--phone N]` | Tautkan nomor WhatsApp (QR ASCII / 8-digit pairing code) |
| `waapi restart` / `recycle` / `stop` / `logout` / `rm` | Kendalikan koneksi dan bersihkan cache internal instance (`recycle`) |
| `waapi key list\|add\|show\|rotate\|rm` | Kelola API key beserta `--scopes`, `--expires`, `--rate`, `--policy`, & grace rotation |
| `waapi audit [--since 24h] [--action pola]` | Tinjau jejak audit aksi administratif |
| `waapi webhook set\|unset\|resume\|dlq\|test` | Konfigurasi target webhook, lanjutkan pasca `410 Gone`, kelola DLQ |
| `waapi events <id> [--after N] [--follow]` | Baca event journal atau ikuti stream SSE lewat control plane |
| `waapi send <id> <to> <text> [--wait] [--idem KEY]` | Kirim pesan teks lewat Durable Outbox dari terminal |
| `waapi health <id> [--json]` | Inspeksi skor, state, kuota harian, & faktor Account Health Guardian |
| `waapi context <id> on\|off\|purge [--chat JID]` | Nyalakan/matikan Context Store FTS5 opt-in atau hapus riwayat chat |
| `waapi hook ls\|add\|rm\|enable\|disable\|logs\|test` | Kelola dan simulasi dry-run rule Reactive Hooks (membuat rule `trusted=true`) |
| `waapi mcp [--key KEY_ID] [--max-inflight 8]` | Bridge stdio JSON-RPC MCP menuju `control.sock` |
| `waapi backup [--out dir] [--json]` | Buat snapshot `VACUUM INTO` (`app.db`, `journal.db`, `wa.db`) |
| `waapi status` / `waapi doctor [--deep] [--json]` | Cek kesehatan daemon, anatomi RAM `procfs`/`cgroup`/`DBStatus` |
| `waapi debug pprof <profil> [-o file]` | Ambil profil `heap`, `goroutine`, `goroutineleak`, atau `profile` |
| `waapi update` / `rollback` / `versions` | Pembaruan biner bertanda tangan Ed25519 beserta pre-backup DB otomatis |

---

## 7. Konfigurasi Environment Variable & Profil Memori

| Variabel | Default | Keterangan |
|---|---|---|
| `WAAPI_PROFILE` | `lean` | Profil memori: `micro`, `lean`, atau `classic` |
| `WAAPI_DATA_DIR` | `~/.wa-api` (`/var/lib/waapi` di unit systemd) | Lokasi `app.db`, `journal.db`, `wa.db`, `control.sock`, `backups/`, `tmp/` |
| `WAAPI_HTTP_ADDR` | `127.0.0.1:8080` | Alamat bind HTTP publik (letakkan di belakang Caddy/Nginx TLS) |
| `WAAPI_METRICS_ADDR` | *(kosong / nonaktif)* | Bind listener Prometheus `/metrics` terpisah (mis. `127.0.0.1:9108`) |
| `WAAPI_LOG_LEVEL` / `WAAPI_LOG_FORMAT` / `WAAPI_WA_LOG_LEVEL` | `info` / `text` / `warn` | Kontrol verbosity slog & filter/rate-limit log whatsmeow |
| `WAAPI_MEMORY_BUDGET_MIB` | Sesuai profil (`16` / `22` / `64`) | Batas memori anonim acuan **Memory Governor** |
| `WAAPI_AUTO_RECYCLE` / `WAAPI_PAGEOUT_AFTER_BOOT` | `off` / `off` | Mitigasi tekanan memori lanjut di host Linux |
| `WAAPI_KEEPALIVE_MAX_FAIL` / `WAAPI_TCP_USER_TIMEOUT` | `75s` / `45s` | Ambang deteksi koneksi setengah mati (half-open) |
| `WAAPI_JOURNAL_RETENTION` / `WAAPI_JOURNAL_REPLAY_WINDOW` / `WAAPI_JOURNAL_MAX_ROWS` / `WAAPI_JOURNAL_SYNC` | `72h` / `1h` / `50000` / `normal` | Retensi event journal, batas baris per instance, mode sync WAL |
| `WAAPI_SSE_MAX` | Sesuai profil (`4` / `8` / `32`) | Kuota maksimum koneksi SSE publik serentak |
| `WAAPI_OUTBOX_TTL` / `WAAPI_OUTBOX_MAX_QUEUED` | `24h` / `1000` | TTL & batas maksimum antrean per instance di Durable Outbox |
| `WAAPI_MEDIA_DROPBEHIND` | `on` | Reclaim page cache tiap 4 MiB saat streaming media |
| `WAAPI_MCP` / `WAAPI_MCP_ALLOWED_ORIGINS` | `on` / *(kosong)* | Aktifkan/matikan server MCP & daftar header `Origin` yang diizinkan |
| `WAAPI_AI_BASE_URL`, `WAAPI_AI_API_KEY`, `WAAPI_AI_MODEL` | *(kosong / mati)* | Endpoint Chat Completions kompatibel OpenAI / Ollama lokal (API key hanya di env/RAM) |
| `WAAPI_AI_TIMEOUT` / `WAAPI_AI_MAX_CONCURRENCY` | `20s` / `2` | Timeout & batas panggilan AI paralel |

---

## 8. Menjalankan di Lokal & Backup / Update

Jalankan daemon lokal tanpa `systemd`:

```bash
WAAPI_DATA_DIR=./data WAAPI_HTTP_ADDR=127.0.0.1:8080 go run ./cmd/waapi serve
```

Backup manual yang aman dengan sandbox `systemd`:

```bash
waapi backup --out ~/waapi-backup-harian
```

---

## 9. Dokumentasi Lanjutan

- [ARCHITECTURE.md](ARCHITECTURE.md) — Arsitektur V2 (3 DB SQLite, jalur masuk/keluar, supervisor, Guardian, Memory Governor)
- [documentation.md](documentation.md) — Aturan bisnis lengkap, diagram status entitas & siklus hidup
- [documentation-api.md](documentation-api.md) — Referensi teknis seluruh endpoint HTTP v1/v2, Control Plane, & tugas latar
- [docs/WEBHOOKS.md](docs/WEBHOOKS.md) — Envelope v2, tanda tangan HMAC, contoh verifikasi Go/PHP/Node/Python, & DLQ
- [docs/MCP.md](docs/MCP.md) — Server MCP `2026-07-28`, bridge `waapi mcp`, `KeyPolicy`, & persetujuan MRTR
- [docs/HOOKS.md](docs/HOOKS.md) — 5 resep Reactive Hooks deklaratif & konfigurasi `ai_reply`
- [docs/MIGRATION-V2.md](docs/MIGRATION-V2.md) — Panduan upgrade v1.0.2 -> v2.0.0, rollback aman, & kebijakan penyimpanan
- [docs/PAIRING-API-SPEC.md](docs/PAIRING-API-SPEC.md) — Integrasi pairing QR & 8-digit untuk dashboard web
- [SECURITY.md](SECURITY.md) — Kebijakan keamanan & rilis bertanda tangan Ed25519
- [CHANGELOG.md](CHANGELOG.md) — Catatan perubahan versi
- [bench/burnin-v2.0.0.md](bench/burnin-v2.0.0.md) — Hasil pengukuran terukur lokal & daftar uji burn-in ARM64
