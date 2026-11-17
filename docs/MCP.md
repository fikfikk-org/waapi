# Panduan Agent-Native MCP Server (`docs/MCP.md`)

`waapi` menyediakan server **Model Context Protocol (MCP)** bawaan (`github.com/modelcontextprotocol/go-sdk v1.8.0`) tanpa proses sampingan:

- Mendukung spesifikasi **MCP `2026-07-28`** (*stateless*, mendukung `server/discover`) serta fallback **`2025-11-25`**.
- **Lazy initialization:** `0` goroutine dan `0` alokasi server MCP sampai request MCP pertama tiba.
- **Keamanan berbasis server (`KeyPolicy`), bukan prompt:** seluruh aksi kirim wajib melewati `outbox.Enqueue` (`Origin{Kind: "mcp"}`) dan pemeriksaan Guardian.

---

## 1. Dua Jalur Transport

### A. Stdio Bridge (`waapi mcp`) untuk Klien Lokal (Claude Desktop / Cursor / IDE)

Menjembatani JSON-RPC `stdin`/`stdout` (batas `1 MiB` per baris, konkurensi default `8`) menuju `control.sock` (`/control/mcp`):

```bash
waapi mcp --key k_7f3a9c
```

Jika `--key` dikosongkan, bridge mewarisi policy operator lokal default (`first_contact: "ask"`, `destructive: "ask"`).

Contoh konfigurasi klien MCP:

```json
{ "mcpServers": { "waapi": { "command": "waapi", "args": ["mcp", "--key", "k_7f3a9c"] } } }
```

### B. Streamable HTTP di Port Publik (`POST /mcp`)

Endpoint `POST /mcp` dipasang langsung di router HTTP publik dengan syarat:
- API key memiliki scope `mcp` (`Authorization: Bearer wak_...` atau `X-API-Key: wak_...`).
- Jika klien peramban mengirim header `Origin`, nilainya **wajib** cocok persis (`scheme + host [+ port]`) dengan daftar `WAAPI_MCP_ALLOWED_ORIGINS` (bila env kosong, request ber-`Origin` ditolak `403 origin_forbidden`). Request non-browser tanpa header `Origin` diizinkan lewat.
- Matikan seluruh endpoint MCP dengan `WAAPI_MCP=off` (`404 mcp_disabled`).

> [!NOTE]
> Sesuai implementasi [internal/httpapi/router.go](../internal/httpapi/router.go#L364), router publik mendaftarkan `POST /mcp` secara eksplisit (permintaan `GET`/`DELETE` pada `/mcp` membalas `405 Method Not Allowed`), sementara `/control/mcp` pada UDS control plane melayani penuh metode MCP yang diteruskan oleh `waapi mcp`.

Contoh konfigurasi klien MCP jarak jauh:

```json
{ "mcpServers": { "waapi": { "type": "http", "url": "https://wa.example.com/mcp", "headers": { "Authorization": "Bearer wak_..." } } } }
```

---

## 2. Daftar Tools & Resources MCP

### 15 Tool Tersedia (`tools/list`)

Daftar tool disaring otomatis oleh middleware berdasarkan `KeyPolicy.Tools` (bila `tools` kosong pada policy, default mengaktifkan 10 tool `readOnly` + `send_message`, `react_message`, `mark_read`; tool destruktif tetap membutuhkan izin eksplisit atau `destructive: "ask"`).

| Tool | Scope | Destruktif | Fungsi |
|---|---|---|---|
| `list_instances` | `instances:read` | Tidak (`readOnly`) | Daftar status & metadata instance |
| `get_health` | `health:read` | Tidak (`readOnly`) | Laporan skor, state & kuota Guardian |
| `list_chats` | `messages:read` | Tidak (`readOnly`) | Daftar chat terbaru dari Context Store (409 bila nonaktif) |
| `get_conversation` | `messages:read` | Tidak (`readOnly`) | Riwayat pesan satu chat (menandai `untrusted: true`) |
| `search_messages` | `messages:read` | Tidak (`readOnly`) | Pencarian FTS5 BM25 pada Context Store |
| `get_events` | `events:read` | Tidak (`readOnly`) | Tarik event dari `journal.db` |
| `check_numbers` | `messages:read` | Tidak (`readOnly`) | Verifikasi nomor terdaftar di WhatsApp |
| `get_message_status` | `messages:read` | Tidak (`readOnly`) | Status pesan keluar (`queued`/`sent`/`delivered`/`read`) |
| `list_groups` | `groups:read` | Tidak (`readOnly`) | Daftar grup WhatsApp yang diikuti |
| `get_group_info` | `groups:read` | Tidak (`readOnly`) | Metadata & daftar peserta grup |
| `send_message` | `messages:send` | Tidak | Antrekan pesan teks/media/lokasi/poll ke Durable Outbox |
| `react_message` | `messages:send` | Tidak | Kirim/hapus reaksi emoji lewat outbox |
| `mark_read` | `messages:send` | Tidak | Tandai pesan masuk sebagai terbaca |
| `revoke_message` | `messages:send` | Ya (`destructive`) | Tarik pesan terkirim lewat outbox |
| `manage_group_participants` | `groups:write` | Ya (`destructive`) | Tambah/keluarkan/promote/demote peserta grup |

### Resources & Subscriptions

- Resource template: `waapi://instances/{instance}/chats/{+chat}` dan `waapi://instances/{instance}/health`.
- Langganan (`resources/subscribe`) dibatasi maksimal `32` URI secara global dan hanya menyalakan fanout `journal.Subscribe` selama jumlah URI aktif `> 0`.

---

## 3. `KeyPolicy` & Alur Persetujuan Manusia (MRTR)

Membuat API key khusus AI agent dengan scope `mcp`:

```bash
cat > policy-agent.json <<'EOF'
{
  "reply_window": "24h",
  "allowed_chats": ["6281*"],
  "first_contact": "ask",
  "max_sends_per_hour": 30,
  "destructive": "ask",
  "tools": []
}
EOF

waapi key add --name ai-agent --instance sales --scopes "mcp,instances:read,messages:read,messages:send,events:read,health:read" --policy policy-agent.json
```

Jika `--scopes` menyertakan `mcp` dan `--policy` tidak diberikan, `waapi` mengisi `DefaultMCPPolicy()` otomatis (`reply_window: "24h"`, `first_contact: "ask"`, `max_sends_per_hour: 30`, `destructive: "ask"`).

### Alur Persetujuan Manusia (*Multi Round-Trip Requests*)

1. Ketika agent memanggil `send_message` ke kontak baru (`first_contact: "ask"`) atau tool destruktif (`destructive: "ask"` tanpa persetujuan):
   - Jika klien mendukung MCP elicitation, server mengirim dialog konfirmasi kepada operator.
   - Jika klien stateless/tanpa elicitation, server mengembalikan error JSON-RPC `-32042` (`InputRequired`) berisi detail aksi serta `approval_token` **HMAC-SHA256** (berlaku `5 menit`, terikat pada key ID, tool, instance, target, dan hash argumen).
2. Setelah manusia menyetujui, agent mengulang panggilan tool yang sama sambil menyertakan argumen `approval_token`. Token diverifikasi secara kriptografis sebelum aksi diantrekan ke outbox.
3. Seluruh teks pesan eksternal di `get_conversation`, `search_messages`, dan `get_events` diberi flag `untrusted: true`, sementara batas riil (`reply_window`, `allowed_chats`, `max_sends_per_hour`) selalu ditegakkan oleh server (`policy.Enforcer`), tidak pernah diserahkan ke prompt LLM.
