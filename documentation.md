# Dokumentasi Business Logic — wa-api

> Status: terimplementasi (`cmd/waapi`, `internal/*`). Sumber: [ARCHITECTURE.md](ARCHITECTURE.md).

## Lifecycle Entitas

```mermaid
flowchart TD
    subgraph Instance
        C["created"] -->|"instance pair"| P["pairing"]
        P -->|"pairing sukses"| CN["connected"]
        P -->|"timeout / batal"| C
        CN -->|"koneksi putus"| D["disconnected"]
        D -->|"reconnect otomatis"| CN
        CN -->|"logout / unlink dari HP"| L["logged_out"]
        D -->|"logout"| L
        L -->|"pair ulang"| P
        CN -->|"instance stop"| DS["disabled"]
        DS -->|"instance restart"| CN
    end
    subgraph Supervisor
        SI["idle"] -->|"Start()"| SC["connecting"]
        PA["PairSession sukses"] -->|"Adopt()"| SO["online"]
        SC -->|"events.Connected / AlreadyConnected"| SO
        SC -->|"gagal / ConnectTimeout 30s"| SB["backoff"]
        SO -->|"KeepAliveTimeout &lt; KeepAliveMaxFail"| SD["degraded"]
        SD -->|"KeepAliveRestored"| SO
        SD -->|"KeepAliveTimeout &gt;= KeepAliveMaxFail"| SB
        SO -->|"events.Disconnected"| SB
        SB -->|"timer backoff (+ circuit 15m &gt;= 10x)"| SC
        SO -->|"TemporaryBan"| SBT["banned_temp"]
        SBT -->|"Expire + jitter 30-120s"| SC
        SO -->|"StreamReplaced"| SR["replaced"]
        SO -->|"LoggedOut reason lain"| SLO["logged_out"]
        SO -->|"LoggedOut 406"| SBN["banned"]
        SO -->|"ClientOutdated"| SOUT["outdated"]
        SC -->|"Stop()"| SST["stopped"]
        SB -->|"Stop()"| SST
    end
    subgraph ApiKey
        KA["active"] -->|"key revoke"| KR["revoked"]
    end
    subgraph WebhookEvent
        E1["dibuat"] -->|"kirim 2xx"| E2["terkirim"]
        E1 -->|"gagal / antrian penuh"| E3["outbox"]
        E3 -->|"retry 2xx"| E2
        E3 -->|"lebih dari 8 percobaan / umur lebih dari 24 jam"| E4["dead-letter (dihapus)"]
    end
```

## Aturan per Entitas

### Instance

| Field | Rule | Catatan |
|---|---|---|
| id | Unik, slug huruf kecil | Dipakai di URL API |
| jid | Unik, terisi setelah pairing | Kosong sebelum pairing |
| status | Salah satu dari 6 status | Lihat tabel transisi |
| webhook_url | Opsional, wajib https di produksi | |
| webhook_secret | Dibuat otomatis | Untuk tanda tangan HMAC |
| rate_per_min | Default 20 | Batas kirim per instance |
| auto_start | Default aktif | Instance dinyalakan saat daemon start |
| settings | JSON bertipe per instance | Menyimpan konfigurasi lanjutan (timezone, guardian_profile, dll) |
| paired_at | Unix timestamp opsional | Waktu pairing nomor aktif; di-reset saat ganti nomor atau logout |

### API Key

| Field | Rule | Catatan |
|---|---|---|
| key | Ditampilkan sekali saat dibuat | Hanya hash yang disimpan |
| instance_id | Kosong = akses semua instance | Scope per instance |
| revoked_at | Terisi = key tidak berlaku | Efek instan |
| last_used_at | Diperbarui berkala (60 detik) | Bukan per request |

### Webhook Outbox

| Field | Rule | Catatan |
|---|---|---|
| attempts | Maks 8 | Lebih dari itu dihapus |
| next_retry_at | Backoff eksponensial 5 detik s/d 30 menit + jitter | |
| payload | Maks 64 KB | Teks panjang dipotong |

### Inbound Journal (`journal.db`)

| Field | Rule | Catatan |
|---|---|---|
| seq | Nomor urut monotonik (`AUTOINCREMENT`), tidak dipakai ulang setelah prune | Menjadi `id` string pada envelope v2 |
| dedupe_key | Unik per `(instance_id, dedupe_key)` memakai alamat wire mentah sebelum kanonikalisasi LID | Idempotensi retry dari WhatsApp |
| chat | JID chat kanonik (diubah ke `@s.whatsapp.net` jika LID tersedia lokal) | Pembentuk partisi `part = fnv32a(chat) % 64` |
| payload | JSON normalisasi (`text` maks 16 KiB, `quoted.text` maks 1 KiB, total maks 64 KiB) | Tidak pernah membuang `id`, `chat`, `from`, `type`, `timestamp` |
| flags | Bitmask `1` (`offline`), `2` (`truncated`) | Menandai pesan offline sync dan pemotongan konten |
| cursors | Menyimpan posisi `seq` terakhir konsumen per `(consumer, instance_id, lane)` | Menjadi batas aman penghapusan event pada pruner |

## Transisi Status Instance

| Dari | Ke | Pemicu |
|---|---|---|
| created | pairing | Operator menjalankan pairing |
| pairing | connected | Scan QR / input kode berhasil |
| pairing | created | QR kedaluwarsa / dibatalkan |
| connected | disconnected | Koneksi jaringan putus |
| disconnected | connected | Reconnect otomatis berhasil |
| connected, disconnected | logged_out | Logout via CLI atau perangkat di-unlink dari HP |
| semua | disabled | Operator menghentikan instance |
| logged_out | pairing | Pairing ulang |

## Batasan Bisnis & Trigger

| Aturan | Detail |
|---|---|
| Tanpa riwayat chat | Pesan masuk hanya diteruskan ke webhook, tidak disimpan |
| Tanpa penyimpanan media | Media hanya transit di file sementara lalu dihapus |
| Pairing serial | Hanya satu instance boleh pairing dalam satu waktu (via CLI maupun REST API `/pair`, TTL otomatis 170 detik) |
| Batas kirim | Per instance sesuai rate_per_min, dengan jeda acak |
| Antrian kirim penuh | Request ditolak, klien diminta mencoba lagi |
| Instance tidak tersambung | Request kirim ditolak |
| Ukuran media | 16 MB (gambar/video/audio), 64 MB (dokumen) |
| Check number | Maks 20 nomor per request, hasil di-cache 24 jam |
| Admin | Hanya lewat terminal server (CLI), tidak ada panel web |
| Kapasitas | 3–6 instance aktif per proses agar RAM di bawah 100 MB |
| Upgrade skema v1 ke v2 | Backup `app.db` dan `wa.db` dibuat sekali ke folder `backups/pre-v2-*` sebelum migrasi dijalankan |
| Kesiapan layanan (`/readyz`) | Daemon dianggap siap hanya bila `app.db` merespons dalam 500 ms dan level tekanan memori governor bukan `Red` |
| Observabilitas memori | Pengukuran metrik memakai `runtime/metrics`, `procfs`, cgroup v2 PSI/stat, THP, dan `sqlite.DBStatus` tanpa memicu stop-the-world (`ReadMemStats`); profiling `pprof` hanya dibuka via `control.sock` |
| Listener metrik Prometheus | Nonaktif secara default (0 goroutine tambahan); aktif hanya bila `WAAPI_METRICS_ADDR` diisi dan memberi peringatan log jika di-bind ke non-loopback |
| Supervisor koneksi self-healing | Satu goroutine supervisor per instance aktif pemilik tunggal reconnect (`EnableAutoReconnect=false`, `InitialAutoReconnect=false`), backoff equal jitter + circuit breaker setelah 10 kegagalan beruntun, deteksi socket mati via keepalive (`WAAPI_KEEPALIVE_MAX_FAIL` 75s) & `TCP_USER_TIMEOUT` (`WAAPI_TCP_USER_TIMEOUT` 45s), `AutoStartAll` berjeda 2s + jitter 0-1s, serta `Status`/`ListStatuses` murni read-only tanpa membuat instance |
| Recycle instance & systemd watchdog | Operator dapat menyegarkan penuh client dan cache `sqlstore` instance terpasang via `waapi recycle <id>`; daemon mengirim `READY=1`, `WATCHDOG=1` berkala bila `SELECT 1 app.db <= 500ms`, dan `STOPPING=1` ke systemd |
| Inbound journal effectively-once | Ack ke WhatsApp hanya dikirim setelah transaksi `journal.db` (beserta `TxHook` observer) sukses commit (`SynchronousAck`, `EnableDecryptedEventBuffer`, `UseRetryMessageStore`); hot path handler berjalan tanpa query `app.db` maupun I/O jaringan |
| Cache target & settings di memori | `TargetCache` webhook dan salinan `InstanceSettings` disinkronkan di memori pada `Create`/`Update`/`Delete` sehingga hot path bebas query `app.db` |
| Retensi & pemangkasan `journal.db` | Pruner menghapus event terkirim melewati `JournalReplayWindow` (1 jam), event belum terkirim melewati `JournalRetention` (72 jam) dengan increment metrik `dropped_undelivered_total`, serta menjaga maksimal `JournalMaxRows` (50.000) baris per instance |

