# waapi — Ultra-Lightweight Headless WhatsApp API Gateway

Private WhatsApp Multi-Device API Gateway dalam **1 binary statis Go** (`whatsmeow` + pure-Go SQLite) tanpa admin panel web. Seluruh manajemen instance, pairing QR/kode 8 digit, webhook, dan API key dikendalikan langsung lewat terminal (`waapi`), dengan konsumsi RAM **< 100 MB** (`GOMEMLIMIT=80MiB`, `MemoryMax=100M`).

---

## Daftar Isi

1. [Instalasi di Server Linux (systemd)](#1-instalasi-di-server-linux-systemd)
2. [Panduan Cepat: Dari Install Sampai Kirim Pesan Pertama](#2-panduan-cepat-dari-install-sampai-kirim-pesan-pertama)
3. [Contoh Lengkap Pemanggilan REST API](#3-contoh-lengkap-pemanggilan-rest-api)
4. [Menerima Pesan Masuk via Webhook](#4-menerima-pesan-masuk-via-webhook)
5. [Referensi Perintah CLI (`waapi`)](#5-referensi-perintah-cli-waapi)
6. [Konfigurasi Environment Variable](#6-konfigurasi-environment-variable)
7. [Menjalankan di Lokal (Tanpa systemd)](#7-menjalankan-di-lokal-tanpa-systemd)
8. [Dokumentasi Lanjutan](#8-dokumentasi-lanjutan)

---

## 1. Instalasi di Server Linux (systemd)

### Opsi A — One-Liner Installer (Direkomendasikan)

Mengunduh binary statis sesuai arsitektur server (`amd64` / `arm64`), membuat user & direktori `/var/lib/waapi`, serta menyalakan service `systemd` dengan batas RAM 100 MB:

```bash
curl -fsSL https://raw.githubusercontent.com/fikfikk-org/waapi/main/deploy/install.sh | sudo bash
```

> **Catatan:** Setelah instalasi pertama, jalankan `source /etc/profile.d/waapi.sh` (atau login ulang SSH) agar perintah `waapi` dapat langsung dijalankan tanpa `sudo`.

### Opsi B — Build dari Source di VPS

Jika ingin mengompilasi sendiri (membutuhkan Go 1.25+):

```bash
git clone https://github.com/fikfikk-org/waapi.git
cd waapi
make build-linux
sudo bash deploy/install.sh
source /etc/profile.d/waapi.sh
```

---

## 2. Panduan Cepat: Dari Install Sampai Kirim Pesan Pertama

Ikuti **5 langkah berurutan** berikut di terminal server Anda:

### Langkah 1 — Pastikan daemon berjalan

```bash
waapi status
```

Output yang diharapkan:
```text
Daemon        running (pid 1423, uptime 1m)
RSS           24.6 MB   (hard target < 100 MB)
Go heap       8.4 MB alloc / 14.1 MB sys   (GOMEMLIMIT 80MiB, GOGC 50)
...
```

### Langkah 2 — Buat instance WhatsApp baru

Misalnya kita beri nama instance `sales` dengan batas kirim 20 pesan/menit:

```bash
waapi add sales --rate 20
```

### Langkah 3 — Tautkan (Pairing) nomor WhatsApp

Pilih **salah satu** metode di bawah ini:

**Metode A — Scan QR Code langsung di terminal:**
```bash
waapi pair sales
```
Buka WhatsApp di HP → **Perangkat tertaut** → **Tautkan perangkat** → arahkan kamera ke QR yang muncul di terminal.

**Metode B — Menggunakan 8-Digit Pairing Code (tanpa kamera):**
```bash
waapi pair sales --phone 6281234567890
```
Terminal akan menampilkan kode seperti `K7QD-2MXA`. Masukkan kode tersebut di notifikasi WhatsApp HP Anda.

Pastikan status instance sudah `connected`:
```bash
waapi list
```
```text
ID      STATUS      JID                        RATE/MIN   QUEUE   WEBHOOK
sales   connected   6281234567890:12@s...      20         0       -
```

### Langkah 4 — Buat API Key

Buat API key untuk aplikasi backend Anda. Anda bisa membatasinya ke satu instance (`--instance sales`) atau mengosongkan flag `--instance` agar bisa mengakses semua instance:

```bash
waapi key add --name backend-prod --instance sales
```

Output:
```text
✔ API key dibuat (simpan sekarang, tidak akan ditampilkan lagi)

  ID        k_7f3a9c
  Name      backend-prod
  Scope     instance: sales
  Key       wak_Q2x9pL0vR7mN4sT8yB1cD6eF3gH5jK2lZ0aW9uV7
```
> **Penting:** Salin nilai `Key` (`wak_...`) karena hanya ditampilkan satu kali saat pembuatan.

### Langkah 5 — Kirim pesan pertama Anda!

Ganti `wak_...` dengan API key dari Langkah 4 dan `6289876543210` dengan nomor tujuan (format internasional tanpa `+`):

```bash
curl -X POST http://127.0.0.1:8080/v1/instances/sales/messages/text \
  -H "X-API-Key: wak_Q2x9pL0vR7mN4sT8yB1cD6eF3gH5jK2lZ0aW9uV7" \
  -H "Content-Type: application/json" \
  -d '{
    "to": "6289876543210",
    "text": "Halo! Ini pesan pertama dari waapi.",
    "typing": true
  }'
```

Response sukses (`200 OK`):
```json
{
  "message_id": "3EB0A1B2C3D4E5F6",
  "to": "6289876543210@s.whatsapp.net",
  "timestamp": 1794360012
}
```

---

## 3. Contoh Lengkap Pemanggilan REST API

Semua request ke `/v1/*` wajib menyertakan header `X-API-Key: wak_...`.
Nomor tujuan (`to`) dapat berupa nomor internasional (`6281234567890`) atau JID grup (`1203630123456789@g.us`).

### A. Kirim Pesan Teks (atau Balas Pesan)

```bash
curl -X POST http://127.0.0.1:8080/v1/instances/sales/messages/text \
  -H "X-API-Key: wak_..." \
  -H "Content-Type: application/json" \
  -d '{
    "to": "6289876543210",
    "text": "Pesanan #1024 sudah dikirim.",
    "typing": true,
    "reply_to": ""
  }'
```

### B. Kirim Gambar / Dokumen / Video / Audio (Upload File)

> **Aturan penting:** Karena `waapi` membaca stream secara langsung tanpa memuat file ke RAM, field `to` dan `type` (`image` | `video` | `audio` | `document`) **wajib diletakkan sebelum** `-F "file=@..."`.

```bash
curl -X POST http://127.0.0.1:8080/v1/instances/sales/messages/media \
  -H "X-API-Key: wak_..." \
  -F "to=6289876543210" \
  -F "type=document" \
  -F "filename=invoice-1024.pdf" \
  -F "caption=Berikut invoice pesanan Anda" \
  -F "file=@./invoice-1024.pdf"
```

### C. Kirim Media dari URL (`https://`)

```bash
curl -X POST http://127.0.0.1:8080/v1/instances/sales/messages/media-url \
  -H "X-API-Key: wak_..." \
  -H "Content-Type: application/json" \
  -d '{
    "to": "6289876543210",
    "type": "image",
    "url": "https://example.com/produk.jpg",
    "caption": "Foto produk"
  }'
```

### D. Cek Apakah Nomor Terdaftar di WhatsApp

Maksimal 20 nomor per request (hasil di-cache selama 24 jam):

```bash
curl -X POST http://127.0.0.1:8080/v1/instances/sales/contacts/check \
  -H "X-API-Key: wak_..." \
  -H "Content-Type: application/json" \
  -d '{
    "numbers": ["6281234567890", "6289999999999"]
  }'
```

### E. Cek Status Instance via API

```bash
curl http://127.0.0.1:8080/v1/instances/sales \
  -H "X-API-Key: wak_..."
```

---

## 4. Menerima Pesan Masuk via Webhook

### Pasang URL Webhook pada Instance

```bash
waapi webhook set sales https://api.domain-anda.com/webhook/wa --events message,receipt,connection
```

### Uji Pengiriman Webhook

```bash
waapi webhook test sales
```

### Contoh Payload Webhook (`POST`)

Setiap request webhook menyertakan header:
- `X-WA-Event`: `message` | `receipt` | `connection`
- `X-WA-Instance`: ID instance (mis. `sales`)
- `X-WA-Timestamp`: Unix epoch detik
- `X-WA-Signature`: `sha256=<hex HMAC-SHA256(webhook_secret, timestamp + "." + raw_body)>`

```json
{
  "event": "message",
  "instance": "sales",
  "timestamp": 1794360100,
  "data": {
    "id": "3EB0F00D...",
    "from": "6281234567890@s.whatsapp.net",
    "chat": "6281234567890@s.whatsapp.net",
    "is_group": false,
    "from_me": false,
    "push_name": "Budi",
    "type": "text",
    "text": "Halo, apakah stok masih ada?"
  }
}
```

Jika pesan masuk berupa media (`image`, `video`, `audio`, `document`), field `data.media` akan berisi *descriptor* yang dapat diunduh secara *on-demand* (streaming) melalui endpoint `POST /v1/instances/{id}/media/download` dengan mengirimkan JSON `data.media` sebagai request body.

---

## 5. Referensi Perintah CLI (`waapi`)

```text
PERINTAH INSTANCE:
  waapi list                                   Daftar semua instance WhatsApp
  waapi add <id> [--rate 20] [--no-autostart]  Buat instance baru
  waapi show <id>                              Detail status satu instance
  waapi edit <id> [opsi]                       Ubah rate limit / webhook / autostart instance
  waapi pair <id> [--phone 62812xxx]           Tautkan nomor via QR ASCII atau 8-digit kode
  waapi restart <id>                           Sambungkan ulang instance
  waapi stop <id>                              Hentikan koneksi instance dan matikan autostart
  waapi logout <id>                            Unlink perangkat dari WhatsApp
  waapi rm <id> [--yes]                        Logout dan hapus instance beserta datanya

OPSI EDIT INSTANCE (waapi edit <id>):
  --rate <n>              Batas kirim pesan per menit (contoh: --rate 30)
  --webhook <url>         URL tujuan webhook (kosongkan "" untuk menghapus)
  --events <csv>          Filter event: message,receipt,connection
  --autostart <bool>      true atau false
  --rotate-secret         Putar ulang secret HMAC webhook

MANAJEMEN API KEY:
  waapi key list                               Daftar seluruh API key
  waapi key add --name <nama> [--instance <id>] Buat API key baru (ditampilkan sekali)
  waapi key rm <key-id>                        Cabut (revoke) API key

MANAJEMEN WEBHOOK:
  waapi webhook set <id> <url> [--events ...]  Pasang URL webhook pada instance
  waapi webhook unset <id>                     Hapus URL webhook dari instance
  waapi webhook test <id>                      Kirim event uji coba ke webhook

DAEMON, UPDATE & DIAGNOSTIK:
  waapi status | waapi doctor                  Cek pemakaian RAM (RSS/Heap), DB, & kesehatan daemon
  waapi logs [<id>]                            Pantau log service systemd (journalctl)
  waapi update --check                         Cek apakah ada versi terbaru di GitHub Releases
  sudo waapi update | sudo waapi update:latest Update ke versi terbaru (otomatis backup versi lama)
  sudo waapi update v1.0.0                     Pasang/downgrade ke versi spesifik
  sudo waapi rollback [v1.0.0]                 Kembalikan ke backup lokal terakhir atau versi tertentu
  waapi versions                               Lihat daftar rilis GitHub & backup lokal
  sudo waapi uninstall [--purge]               Hapus service & binary (--purge untuk hapus data)
  waapi serve                                  Jalankan background daemon
  waapi version                                Tampilkan versi binary
```

### Contoh Cek Update & Rollback

Session WhatsApp (`wa.db`) dan API key (`app.db`) **tidak pernah terhapus** saat update maupun rollback:

```bash
# 1. Cek apakah ada versi baru di GitHub Releases
waapi update --check

# 2. Lihat daftar versi di GitHub & backup lokal yang tersimpan di /var/lib/waapi/backups
waapi versions

# 3. Update ke versi terbaru (otomatis mem-backup binary lama & verifikasi SHA-256)
sudo waapi update:latest

# 4. Rollback instan ke versi sebelumnya (atau sebutkan versi: sudo waapi rollback v1.0.0)
sudo waapi rollback
```

---

## 6. Konfigurasi Environment Variable

Secara default dikonfigurasi di `/etc/systemd/system/waapi.service`:

| Variabel | Default | Keterangan |
|---|---|---|
| `WAAPI_DATA_DIR` | `/var/lib/waapi` (`~/.wa-api` jika lokal) | Lokasi `app.db`, `wa.db`, `control.sock`, dan `tmp/` |
| `WAAPI_HTTP_ADDR` | `127.0.0.1:8080` | Alamat bind HTTP REST API |
| `WAAPI_MEMORY_LIMIT_MIB` | `80` | Fallback soft memory limit jika `GOMEMLIMIT` kosong |
| `WAAPI_MAX_CONNS` | `64` | Batas koneksi HTTP publik konkuren |
| `WAAPI_MEDIA_MAX_MIB` | `16` | Batas ukuran upload gambar/video/audio (MiB) |
| `WAAPI_DOCUMENT_MAX_MIB` | `64` | Batas ukuran upload dokumen (MiB) |
| `WAAPI_WEBHOOK_WORKERS` | `4` | Jumlah goroutine worker pengirim webhook |

---

## 7. Menjalankan di Lokal (Tanpa systemd)

Untuk pengembangan di komputer lokal (Linux / macOS / Windows):

```bash
# Terminal 1 — Jalankan daemon
go build -o bin/waapi ./cmd/waapi
./bin/waapi serve

# Terminal 2 — Kelola via CLI
./bin/waapi status
./bin/waapi add dev
./bin/waapi pair dev
./bin/waapi key add --name local-test --instance dev
```

---

## 8. Dokumentasi Lanjutan

- [ARCHITECTURE.md](ARCHITECTURE.md) — Analisis first-principles, alokasi budget memori < 100 MB, pragma SQLite, dan diagram alur sistem.
- [documentation.md](documentation.md) — Siklus hidup entitas, transisi status instance, dan batasan bisnis.
- [documentation-api.md](documentation-api.md) — Tabel spesifikasi lengkap seluruh endpoint REST API dan payload webhook.
