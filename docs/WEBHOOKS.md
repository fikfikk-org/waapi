# Panduan Webhook v2 (`docs/WEBHOOKS.md`)

`waapi` v2 mengirim seluruh event secara **berurutan per chat**, langsung dari `journal.db` (O(1) terhadap RAM), dengan dua pilihan format tanda tangan HMAC dan *Dead-Letter Queue* (DLQ).

---

## 1. Pengiriman per Lane & Jaminan Urutan

- Setiap instance memakai `webhook_lanes` worker (default `4` untuk profil `lean`, `2` untuk profil `micro`, rentang `1–16`).
- Partisi dihitung dari chat kanonik: `part = fnv32a(chat) % 64`, lalu dikerjakan oleh worker `lane = part % webhook_lanes`.
- Semua event untuk chat yang sama selalu jatuh ke lane yang sama, sehingga urutan `message` -> `edit` -> `revoke` -> `receipt` terjaga walaupun chat lain sedang mengalami retry.
- Setiap lane membaca maksimal `32` event atau `256 KiB` payload per siklus dari `journal.db`. Bila endpoint penerima mati berhari-hari, backlog tinggal di disk sampai retensi `WAAPI_JOURNAL_RETENTION` (default `72h`).

---

## 2. Envelope Event v2

Setiap HTTP `POST` ke `webhook_url` membawa `Content-Type: application/json` dan `User-Agent: waapi-webhook/2.0`:

```json
{
  "v": 2,
  "id": "1201",
  "event": "message",
  "instance": "sales",
  "timestamp": 1794360100,
  "data": {
    "id": "3EB0F00D1A2B3C4D5E6F",
    "chat": "6281234567890@s.whatsapp.net",
    "chat_raw": "123456789012345@lid",
    "chat_type": "dm",
    "is_group": false,
    "from": "123456789012345@lid",
    "sender_pn": "6281234567890@s.whatsapp.net",
    "sender_lid": "123456789012345@lid",
    "addressing_mode": "lid",
    "from_me": false,
    "push_name": "Budi",
    "timestamp": 1794360100,
    "type": "text",
    "text": "Halo, stok masih ada?",
    "truncated": false
  }
}
```

> [!TIP]
> **Deduplikasi di penerima (wajib):** Karena webhook menjamin *at-least-once delivery*, gunakan field `id` (atau header `X-WA-Event-Id` / `webhook-id`) sebagai kunci unik idempotency di database penerima Anda.

### Daftar Tipe Event

- **Default instance baru (`webhook_events`):** `message`, `receipt`, `connection`, `pairing`, `outbox`, `poll_vote`, `health` (serta `test` untuk `waapi webhook test`).
- **Event opsional (aktifkan via `--events` / `PATCH /settings`):** `group`, `call`, `presence`, `chat_presence`, `undecryptable`, `consent`.
- **Alamat LID vs Nomor Telepon:** Gunakan `sender_pn` saat memetakan pengirim ke nomor telepon, karena WhatsApp sering mengirim `from` dalam format `@lid`.

---

## 3. Mode Tanda Tangan HMAC

Atur format lewat `waapi edit <id> --set webhook_format=waapi|standard`:

| Aspek | Mode `waapi` (default, kompatibel v1) | Mode `standard` (Standard Webhooks) |
|---|---|---|
| Header identitas | `X-WA-Event`, `X-WA-Instance`, `X-WA-Event-Id`, `X-WA-Delivery-Attempt` | `webhook-id` (`<instance>_<seq>`) |
| Header waktu | `X-WA-Timestamp` (Unix detik) | `webhook-timestamp` (Unix detik) |
| Header signature | `X-WA-Signature` = `sha256=<hex>` | `webhook-signature` = `v1,<base64>` |
| Kunci HMAC | String `webhook_secret` utuh (`whsec_...`) sebagai byte kunci | Base64-decode bagian setelah prefix `whsec_` |
| Pesan yang ditandatangani | `<timestamp>.<raw_body>` | `<webhook-id>.<timestamp>.<raw_body>` |

Lihat secret webhook instance Anda dengan `waapi show <id>` atau putar secret baru via `waapi edit <id> --rotate-secret`.

---

## 4. Contoh Verifikasi Signature & Dedupe

Semua contoh di bawah memverifikasi: (1) toleransi waktu maksimal 300 detik untuk mencegah *replay attack*, (2) perbandingan hash *constant-time*, dan (3) dedupe memakai `id` envelope.

### Go (`net/http`)

```go
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var seenIDs sync.Map

func verifyWaapi(secret, tsStr, sigHeader string, body []byte) bool {
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil || math.Abs(float64(time.Now().Unix()-ts)) > 300 {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(tsStr))
	mac.Write([]byte("."))
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sigHeader), []byte(expected))
}

func verifyStandard(secret, id, tsStr, sigHeader string, body []byte) bool {
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil || math.Abs(float64(time.Now().Unix()-ts)) > 300 {
		return false
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + tsStr + "."))
	mac.Write(body)
	expected := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	for _, candidate := range strings.Fields(sigHeader) {
		if hmac.Equal([]byte(candidate), []byte(expected)) {
			return true
		}
	}
	return false
}

func webhookHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	secret := os.Getenv("WAAPI_WEBHOOK_SECRET")

	var valid bool
	if whID := r.Header.Get("webhook-id"); whID != "" {
		valid = verifyStandard(secret, whID, r.Header.Get("webhook-timestamp"), r.Header.Get("webhook-signature"), body)
	} else {
		valid = verifyWaapi(secret, r.Header.Get("X-WA-Timestamp"), r.Header.Get("X-WA-Signature"), body)
	}
	if !valid {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var env struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(body, &env)
	if env.ID != "" {
		if _, dup := seenIDs.LoadOrStore(env.ID, true); dup {
			w.WriteHeader(http.StatusOK) // sudah diproses sebelumnya
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}
```

### PHP

```php
<?php
$body = file_get_contents('php://input');
$secret = getenv('WAAPI_WEBHOOK_SECRET');
$headers = array_change_key_case(getallheaders(), CASE_LOWER);

function checkTimestamp(string $ts): bool {
    return ctype_digit($ts) && abs(time() - (int)$ts) <= 300;
}

$valid = false;
if (isset($headers['webhook-id'], $headers['webhook-timestamp'], $headers['webhook-signature'])) {
    if (checkTimestamp($headers['webhook-timestamp'])) {
        $key = base64_decode(preg_replace('/^whsec_/', '', $secret), true);
        if ($key !== false) {
            $signed = $headers['webhook-id'] . '.' . $headers['webhook-timestamp'] . '.' . $body;
            $expected = 'v1,' . base64_encode(hash_hmac('sha256', $signed, $key, true));
            foreach (preg_split('/\s+/', trim($headers['webhook-signature'])) as $sig) {
                if (hash_equals($expected, $sig)) {
                    $valid = true;
                    break;
                }
            }
        }
    }
} elseif (isset($headers['x-wa-timestamp'], $headers['x-wa-signature'])) {
    if (checkTimestamp($headers['x-wa-timestamp'])) {
        $expected = 'sha256=' . hash_hmac('sha256', $headers['x-wa-timestamp'] . '.' . $body, $secret);
        $valid = hash_equals($expected, $headers['x-wa-signature']);
    }
}

if (!$valid) {
    http_response_code(401);
    exit('Invalid signature');
}

$payload = json_decode($body, true);
$eventId = $payload['id'] ?? null; // simpan $eventId di DB / Redis SETNX untuk dedupe
http_response_code(200);
```

### Node.js (`express` / `crypto`)

```javascript
import crypto from "node:crypto";
import express from "express";

const app = express();
const processedIds = new Set();

function safeEqual(a, b) {
  const ba = Buffer.from(a);
  const bb = Buffer.from(b);
  return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
}

app.post("/webhook/waapi", express.raw({ type: "application/json" }), (req, res) => {
  const secret = process.env.WAAPI_WEBHOOK_SECRET || "";
  const rawBody = req.body;
  const now = Math.floor(Date.now() / 1000);

  let valid = false;
  if (req.headers["webhook-id"]) {
    const id = String(req.headers["webhook-id"]);
    const ts = String(req.headers["webhook-timestamp"] || "");
    const sig = String(req.headers["webhook-signature"] || "");
    if (Math.abs(now - Number(ts)) <= 300) {
      const key = Buffer.from(secret.replace(/^whsec_/, ""), "base64");
      const mac = crypto.createHmac("sha256", key).update(`${id}.${ts}.`).update(rawBody).digest("base64");
      valid = sig.split(/\s+/).some((item) => safeEqual(item, `v1,${mac}`));
    }
  } else {
    const ts = String(req.headers["x-wa-timestamp"] || "");
    const sig = String(req.headers["x-wa-signature"] || "");
    if (Math.abs(now - Number(ts)) <= 300) {
      const mac = crypto.createHmac("sha256", secret).update(`${ts}.`).update(rawBody).digest("hex");
      valid = safeEqual(sig, `sha256=${mac}`);
    }
  }

  if (!valid) return res.status(401).send("invalid signature");

  const envelope = JSON.parse(rawBody.toString("utf8"));
  if (envelope.id && processedIds.has(envelope.id)) {
    return res.sendStatus(200);
  }
  if (envelope.id) processedIds.add(envelope.id);
  return res.sendStatus(200);
});
```

### Python (`hmac` / FastAPI atau Flask)

```python
import base64
import hashlib
import hmac
import json
import time


def verify_waapi_webhook(headers: dict[str, str], raw_body: bytes, secret: str) -> bool:
    h = {k.lower(): v for k, v in headers.items()}
    now = int(time.time())

    if "webhook-id" in h:
        ts = h.get("webhook-timestamp", "")
        if not ts.isdigit() or abs(now - int(ts)) > 300:
            return False
        key = base64.b64decode(secret.removeprefix("whsec_"))
        signed = f"{h['webhook-id']}.{ts}.".encode() + raw_body
        expected = "v1," + base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()
        return any(hmac.compare_digest(part, expected) for part in h.get("webhook-signature", "").split())

    ts = h.get("x-wa-timestamp", "")
    sig = h.get("x-wa-signature", "")
    if not ts.isdigit() or abs(now - int(ts)) > 300:
        return False
    signed = f"{ts}.".encode() + raw_body
    expected = "sha256=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, expected)
```

---

## 5. Manajemen CLI, Retry, Pause 410 & DLQ

| Aksi | Perintah CLI | Perilaku |
|---|---|---|
| Pasang webhook baru (dari head `seq` sekarang) | `waapi webhook set sales https://api.anda.com/wa` | `--from now` adalah default; pakai `--from retained` untuk kirim backlog |
| Ganti ke mode Standard Webhooks | `waapi edit sales --set webhook_format=standard` | Header berubah menjadi `webhook-id`, `webhook-timestamp`, `webhook-signature` |
| Lanjutkan pasca `410 Gone` | `waapi webhook resume sales` | Menghapus status pause `gone` dan melanjutkan tepat dari cursor tersimpan |
| Lihat daftar antrean mati (`webhook_dead`) | `waapi webhook dlq sales ls` | Error `4xx` non-410 dipindah ke DLQ maks 3 percobaan; `5xx`/jaringan maks `webhook_max_attempts` (default `12`) |
| Kirim ulang entri DLQ | `waapi webhook dlq sales replay --all` (atau `--seq 1201`) | Menghapus baris DLQ bila HTTP `2xx` |
| Kosongkan DLQ | `waapi webhook dlq sales purge` | Menghapus seluruh backlog `webhook_dead` instance |
| Uji webhook | `waapi webhook test sales` | Mengirim event `type: "test"` langsung ke target |
