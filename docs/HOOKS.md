# Panduan Reactive Hooks & AI Responder (`docs/HOOKS.md`)

`waapi` mengevaluasi **Reactive Hooks** deklaratif secara berurutan per instance di dalam daemon tanpa runtime script eksternal. Setiap aksi kirim masuk ke `outbox.Enqueue` (`Origin{Kind: "hook"}` atau `Origin{Kind: "ai"}`) dengan `IdemKey` deterministik (`hook:<id>:<seq>:<n>` atau `ai:<id>:<seq>`).

---

## 1. Penjaga Bawaan (Built-in Guards)

1. **Stale guard:** Event dengan umur > `hooks_max_age` (default `120s`) atau flag `offline: true` dilewati (`skipped_stale`).
2. **Human takeover:** Pesan keluar manual dari HP (`from_me: true` non-outbox) otomatis mem-pause otomasi chat selama `takeover_pause` (default `30m`, dicatat `skipped_takeover`), kecuali rule menyetel `"respect_takeover": false`.
3. **Throttle & batas kirim:** Mendukung `throttle.per_chat` (mis. `"1/6h"`, `"3/24h"`) serta batas global `30` kiriman hooks/menit per instance.
4. **Memory pressure (`governor.Red`):** Aksi berat (kirim outbox / panggilan HTTP) dilewati sebagai `skipped_pressure` (pada `ai_reply`, `fallback_text` tetap dikirim bila terisi), sedangkan rule ringan `suppress_webhook` / `mark_read` tetap berjalan.
5. **Keamanan URL `http`:**
   - Rule dari REST API (`POST`/`PUT /v2/instances/{id}/hooks`) selalu memaksa `trusted: false` (`https://` publik non-privat saja).
   - Rule dari CLI/control plane (`waapi hook add`) berstatus `trusted: true` dan boleh memanggil `http://127.0.0.1` / `localhost` / `::1`, namun tetap memblokir IP LAN/CGNAT/link-local (`10.x`, `172.16.x`, `192.168.x`, `169.254.x`).

---

## 2. Lima Resep Siap Pakai

### Resep 1 — FAQ Otomatis (Harga & Alamat) + Tandai Terbaca

```json
{
  "id": "faq-jam-buka",
  "name": "Balas pertanyaan jam operasional",
  "priority": 10,
  "on": ["message"],
  "when": {
    "all": [
      { "field": "chat_type", "op": "eq", "value": "dm" },
      { "field": "text", "op": "matches", "value": "(?i)\\b(jam buka|buka jam|jadwal)\\b" }
    ]
  },
  "throttle": { "per_chat": "1/1h" },
  "do": [
    { "action": "mark_read" },
    { "action": "reply", "text": "Halo {{push_name}}, kami buka Senin-Sabtu 08:00-17:00 WIB.", "quote": true, "typing": true }
  ],
  "stop": true
}
```

### Resep 2 — Auto-Responder Luar Jam Operasional

`active_hours` mengevaluasi zona waktu instance (`timezone`, default `"Asia/Jakarta"`).

```json
{
  "id": "auto-tutup-malam",
  "on": ["message"],
  "active_hours": "17:30-07:59",
  "when": {
    "field": "chat_type", "op": "eq", "value": "dm"
  },
  "throttle": { "per_chat": "1/6h" },
  "do": [
    { "action": "reply", "text": "Terima kasih {{push_name}}. Pesan Anda tercatat pada {{date}} {{time}} dan akan dibalas admin pukul 08:00." }
  ]
}
```

### Resep 3 — Sambutan Anggota Grup Baru (`group_action: "join"`)

Pastikan `webhook_events` / event `group` diproduksi untuk event grup (`normalize.go` menghasilkan `Action: "join"` saat peserta baru bergabung ke grup).

```json
{
  "id": "sambutan-grup",
  "on": ["group"],
  "when": {
    "all": [
      { "field": "chat", "op": "eq", "value": "1203630123456789@g.us" },
      { "field": "group_action", "op": "eq", "value": "join" }
    ]
  },
  "respect_takeover": false,
  "do": [
    { "action": "reply", "text": "Selamat datang di grup! Silakan baca deskripsi grup untuk tata tertib." }
  ]
}
```

### Resep 4 — Eskalasi Kata Kunci Urgen ke Grup Admin (dan Redam Webhook)

```json
{
  "id": "eskalasi-komplain",
  "priority": 5,
  "on": ["message"],
  "when": {
    "all": [
      { "field": "chat_type", "op": "eq", "value": "dm" },
      { "field": "text", "op": "icontains", "value": "refund" }
    ]
  },
  "do": [
    { "action": "react", "emoji": "🙏" },
    { "action": "send", "to": "1203630999888777@g.us", "text": "[ESKALASI {{date}} {{time}}] Dari {{push_name}} ({{sender_pn}} / {{chat}}): {{text}}" },
    { "action": "suppress_webhook" }
  ]
}
```

### Resep 5 — Function-Call `http` ke Backend Lokal + Reply AI (`ai_reply` opsional)

Saat aksi `http` memiliki `"expect_reply": true`, server tujuan menerima POST envelope JSON bertanda tangan (`X-WA-Signature` atau `webhook-signature` + `X-WA-Hook-Run`) dan dapat membalas `{"reply": "...", "react": "👍", "suppress_webhook": true}`. `Webhook Gate` menunggu hooks selesai (fail-open 10 detik) sebelum memutuskan pengiriman webhook reguler.

```json
{
  "id": "tanya-stok-lokal",
  "on": ["message"],
  "when": {
    "all": [
      { "field": "chat_type", "op": "eq", "value": "dm" },
      { "field": "text", "op": "prefix", "value": "!stok " }
    ]
  },
  "do": [
    {
      "action": "http",
      "url": "http://127.0.0.1:9000/hooks/cek-stok",
      "timeout_ms": 3000,
      "include": "event",
      "expect_reply": true
    }
  ],
  "stop": true
}
```

Untuk balasan LLM generatif (memerlukan `WAAPI_AI_BASE_URL`), ganti blok `do` memakai aksi `ai_reply`:

```json
{
  "action": "ai_reply",
  "system": "Anda asisten CS toko. Jawab singkat dalam bahasa pelanggan.",
  "context_messages": 10,
  "max_tokens": 300,
  "temperature": 0.3,
  "fallback_text": "Maaf, sistem sedang sibuk. Admin akan segera membalas pesan Anda."
}
```

---

## 3. Cara Menguji & Memasang via CLI atau REST

```bash
# 1. Uji coba aman (dry-run tanpa mengirim pesan atau I/O HTTP)
waapi hook test sales --file faq-jam-buka.json --event sample-event.json

# 2. Pasang dari control plane (mendapat status trusted=true untuk http://127.0.0.1)
waapi hook add sales --file tanya-stok-lokal.json

# 3. Kelola rule & pantau riwayat eksekusi
waapi hook ls sales
waapi hook disable sales faq-jam-buka
waapi hook enable sales faq-jam-buka
waapi hook logs sales --hook faq-jam-buka --since 1h
waapi hook rm sales faq-jam-buka
```

Atau lewat REST API publik (membutuhkan scope `hooks:admin`, selalu `trusted=false`):

```bash
curl -X POST http://127.0.0.1:8080/v2/instances/sales/hooks \
  -H "X-API-Key: wak_..." \
  -H "Content-Type: application/json" \
  -d @faq-jam-buka.json
```
