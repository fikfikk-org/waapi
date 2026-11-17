# Kebijakan Keamanan (`SECURITY.md`)

## Versi yang Didukung

| Versi   | Status Dukungan | Keterangan                                                                               |
| ------- | --------------- | ---------------------------------------------------------------------------------------- |
| `2.0.x` | ✅ Didukung     | Branch utama V2: scope granular, `KeyPolicy`, rilis bertanda tangan Ed25519, audit log   |
| `1.0.x` | ⚠️ Legacy       | Hanya dukungan migrasi menuju `2.0.0`; rilis lama v1 tidak memiliki tanda tangan Ed25519 |

---

## Arsitektur Keamanan v2

1. **Autentikasi & Otorisasi API Key (`internal/auth`)**
   - Mendukung header `X-API-Key: wak_...` maupun `Authorization: Bearer wak_...`.
   - Hanya hash SHA-256 yang disimpan di `app.db`; nilai key mentah hanya ditampilkan sekali saat dibuat atau dirotasi.
   - Setiap key mendukung `instance_id`, daftar `scopes` terpisah koma (`instances:read`, `pair`, `messages:send`, `messages:read`, `media:write`, `events:read`, `groups:read`, `groups:write`, `health:read`, `settings:admin`, `hooks:admin`, `mcp`, atau `*`), batas `rate_per_sec`, `expires_at`, rotasi dengan grace period (`waapi key rotate`), serta `KeyPolicy` (`reply_window`, `allowed_chats`, `first_contact`, `max_sends_per_hour`, `destructive`, `tools`).

2. **Verifikasi Rilis Bertanda Tangan (`waapi update`)**
   - Binary rilis diverifikasi secara _fail-closed_ memakai tanda tangan **Ed25519** (Base64 `StdEncoding`) pada berkas `checksums.txt.sig` atas `checksums.txt`.
   - `releasePublicKeys` di [internal/cli/release_keys.go](internal/cli/release_keys.go) sengaja dikosongkan (`[]string{}`) hingga maintainer menanamkan public key resmi hasil `go run ./tools/sign -gen`. Tanpa public key tertanam atau `.sig` yang cocok, `waapi update` menolak memasang binary baru (kecuali flag eksplisit `--insecure-no-verify` untuk rilis v1 lama).
   - Bila daemon hidup saat `waapi update`, CLI memicu snapshot database atomik (`POST /control/backup`) sebelum mengganti binary.

3. **Control Plane Privat & Audit Log (`control.sock`)**
   - Control plane tidak pernah dibuka di TCP publik dan hanya mendengarkan Unix Domain Socket `$WAAPI_DATA_DIR/control.sock` dengan pembuatan atomik (socket sementara -> `chmod 0660` -> `rename`).
   - Seluruh aksi administratif mutatif (instance, API key, settings, context purge, backup, hooks) dicatat ke `audit_log` dengan identitas UID peer Linux (`SO_PEERCRED`) atau ID API key.

4. **Proteksi SSRF, Hooks & Responder AI**
   - Pengambilan media eksternal (`media.url`), aksi HTTP Reactive Hooks, dan transport MCP/AI melalui [internal/netguard](internal/netguard/netguard.go) menolak IP loopback, RFC1918, link-local, CGNAT, NAT64, dan 6to4 setelah resolusi DNS (kecuali rule hooks `trusted=1` dari control plane yang diizinkan ke `http://` loopback lokal `127.0.0.1`/`::1`/`localhost`, dan `WAAPI_AI_BASE_URL` yang disetel langsung oleh operator di environment).
   - Rule hooks yang dibuat via REST publik selalu memaksa `trusted=false` termasuk saat `PUT`.
   - `WAAPI_AI_API_KEY` hanya disimpan di memori/environment dan **tidak** pernah ditulis ke SQLite maupun log. Respons AI tanpa _tool calling_.
   - Respons HTTP `5xx` pada API publik dimasking tanpa `err.Error()` internal; pelacakan dilakukan melalui `request_id` (`X-Request-Id`) yang tercatat di log daemon.

5. **Hardening systemd (`deploy/wa-api.service`)**
   - Daemon dijalankan dengan user unprivileged `waapi`, `NoNewPrivileges=true`, `ProtectSystem=strict`, `ProtectHome=true`, `ReadWritePaths=/var/lib/waapi`, `PrivateTmp=true`, `MemoryDenyWriteExecute=true`, serta `SystemCallFilter`.

---

## Melaporkan Celah Keamanan

Jika Anda menemukan celah keamanan pada `waapi` (misalnya terkait autentikasi `X-API-Key`/`Bearer`, bypass `KeyPolicy`/MCP, validasi SSRF `netguard`, verifikasi tanda tangan Ed25519, atau izin akses `control.sock`), mohon **jangan** membuka Issue publik secara langsung.

Gunakan fitur **Report a vulnerability** pada tab **Security -> Advisories** di repositori GitHub:
https://github.com/fikfikk-org/waapi/security/advisories/new

Kami akan meninjau laporan Anda dan menerbitkan perbaikan beserta penandaan rilis bertanda tangan secepat mungkin.
