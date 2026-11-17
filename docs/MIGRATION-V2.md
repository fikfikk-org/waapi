# Panduan Migrasi v1.0.2 → v2.0.0 (`docs/MIGRATION-V2.md`)

Panduan langkah operator untuk melakukan upgrade dari `waapi v1.0.2` ke `v2.0.0`, memahami perubahan perilaku v2, serta prosedur rollback aman bila diperlukan.

---

## 1. Perubahan Perilaku & Kompatibilitas

| Area | `v1.0.2` | `v2.0.0` | Aksi Operator / Klien |
|---|---|---|---|
| Endpoint `/v1/*` | Kirim sinkron langsung | Tetap kompatibel; pengiriman teks & media masuk lewat Durable Outbox dengan timeout tunggu sinkron `60s` | Tidak perlu ubah request dasar |
| `504 send_timeout` | Jarang | Dapat muncul bila antrean penuh saat tunggu `60s`; pesan tetap di antrean (`details.id`, `details.status_url`) | Cek `status_url` sebelum retry, atau sertakan header `Idempotency-Key` |
| Envelope & urutan webhook | `event`, `instance`, `timestamp`, `data`; urutan tidak dijamin | Tambahan `v: 2`, `id` (`seq`), urutan terjamin per chat (`lane`), serta header `X-WA-Event-Id` / `X-WA-Delivery-Attempt` | Opsional: pakai `id` untuk dedupe; gunakan `sender_pn` bila perlu nomor telepon dari pengirim `@lid` |
| API key & scope | Key v1 penuh per instance | Key v1 otomatis diberi scope `*`. Key baru v2 mendapat scope default terbatas (tanpa `pair`, `settings:admin`, `hooks:admin`, `mcp`) | Dashboard pairing perlu key dengan `--scopes` menyertakan `pair` |
| Database di `$WAAPI_DATA_DIR` | `app.db`, `wa.db` | Ditambah `journal.db` (events, cursors, outbox, uploads, guardian, polls, context store opsional) | Pastikan disk memiliki ruang memadai |
| Penyimpanan sementara pesan | Tidak menyimpan pesan di luar `webhook_outbox` RAM/DB | `journal.db` menyimpan event masuk selama `WAAPI_JOURNAL_REPLAY_WINDOW` (`1h` pasca terkirim) dan belum terkirim hingga `WAAPI_JOURNAL_RETENTION` (`72h`); `outbox` menyimpan riwayat keluar hingga `7 hari` setelah status final. **Context Store FTS5 tetap mati secara default** (`context_store: false`) | Sesuaikan variabel `WAAPI_JOURNAL_*` atau gunakan `waapi context <id> purge` bila kebijakan data menuntut retensi lebih pendek |
| Unit `systemd` & `control.sock` | `Type=simple`, `control.sock` `0660`, `MemoryMax=100M` | `Type=notify` + `WatchdogSec=30s`, profil `WAAPI_PROFILE=lean` (`GOMEMLIMIT=28MiB`, `MemoryMax=80M`), socket dibuat atomik `0660` | Jalankan ulang `deploy/install.sh` untuk memperbarui unit |

---

## 2. Langkah Upgrade Aman

1. **Periksa rilis & buat backup manual pra-upgrade (disarankan):**
   ```bash
   waapi update --check
   waapi backup --out ~/waapi-backup-pra-v2
   ```
   > Jika binary yang terpasang saat ini masih **v1.0.2**, `waapi v1` belum memiliki perintah `waapi backup` maupun verifikasi tanda tangan Ed25519 otomatis; salin `app.db` & `wa.db` dengan `sqlite3 /var/lib/waapi/wa.db "VACUUM INTO '...'"` sebelum upgrade dan verifikasi `sha256sum` dengan `checksums.txt` dari GitHub Releases.

2. **Perbarui binary:**
   ```bash
   sudo waapi update v2.0.0 --insecure-no-verify   # catatan: wajib --insecure-no-verify hanya bila dijalankan dari binary v1 tanpa public key tertanam
   ```
   *(Jika menjalankan dari instalasi manual: unduh `waapi-linux-amd64` / `waapi-linux-arm64`, verifikasi `sha256sum` & `checksums.txt.sig`, pasang ke `/usr/local/bin/waapi`.)*

3. **Backup & migrasi otomatis saat boot pertama V2:**
   - Saat daemon v2 pertama kali mendeteksi `app.db` dengan `user_version <= 2`, daemon otomatis mem-backup `app.db` dan `wa.db` menggunakan `VACUUM INTO` ke `$WAAPI_DATA_DIR/backups/pre-v2-<timestamp>/` **sebelum** menjalankan migrasi v2.
   - Instance lama mempertahankan daftar `webhook_events` miliknya. Untuk menerima tipe event v2 baru (`outbox`, `poll_vote`, `health`, `group`, `call`, dll.), perbarui lewat `waapi edit <id> --events ...`.

4. **Perbarui unit `systemd` ke profil v2 (`lean` default):**
   ```bash
   curl -fsSL https://raw.githubusercontent.com/fikfikk-org/waapi/main/deploy/install.sh | sudo WAAPI_PROFILE=lean bash
   ```
   *(Unit lama disimpan sebagai `waapi.service.bak` apabila tersedia.)*

5. **Verifikasi kesehatan sistem:**
   ```bash
   waapi doctor --deep
   waapi show sales
   waapi health sales
   waapi webhook test sales
   ```
   Pastikan state koneksi `online`, Guardian berstatus `healthy` (dengan grace shadow mode 7 hari aktif untuk instance tertaut baru migrasi), serta penerima webhook menerima event uji dengan header baru.

---

## 3. Rollback ke `v1.0.2`

1. Pastikan seluruh event backlog webhook di `journal.db` sudah terkirim (`lag: 0` di `waapi doctor --deep`) sebelum mematikan v2.
2. Jalankan rollback binary:
   ```bash
   sudo waapi rollback v1.0.2
   ```
   - `v1.0.2` dapat membaca `app.db` hasil v2 karena query v1 menyebutkan nama kolom secara eksplisit dan mengabaikan `user_version` yang lebih tinggi.
   - `journal.db` diabaikan oleh `v1.0.2` (tidak merusak start v1).

> [!CAUTION]
> **Jangan** memulihkan berkas `backups/pre-v2-*/wa.db` lama apabila instance sudah sempat mengirim atau menerima pesan di v2.0.0. Memutar mundur state Signal di `wa.db` menyebabkan desinkronisasi ratchet enkripsi (*UndecryptableMessage*); jika `wa.db` tidak kompatibel dengan v1, lebih aman melakukan `waapi logout` lalu pairing ulang.
