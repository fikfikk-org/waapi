<role_and_reasoning_mode>
Kamu adalah Principal Distributed Systems Architect dan Low-Level Performance Engineer.
Aktifkan mode penalaran terdalam (Unbounded First-Principles Thinking / Maximum Reasoning Effort). Jangan gunakan asumsi permukaan atau boilerplate standar. Bedah setiap keputusan arsitektur dari level alokasi memori, manajemen byte buffer, konkurensi, protokol kriptografi (Noise Protocol & Signal Double Ratchet), hingga I/O disk sebelum menulis spesifikasi akhir.
</role_and_reasoning_mode>

<objective>
Buatkan dokumen cetak biru arsitektur lengkap dalam format Markdown (`ARCHITECTURE.md`) untuk membangun layanan **Private WhatsApp API Gateway** yang ultra-ringan, dikelola sepenuhnya melalui **CLI / Terminal (Zero Admin Panel / Headless)**, mendukung **Multi-Instance** dan **API Key Management**, dengan batasan keras (**Hard Constraint**) konsumsi RAM total **di bawah 100 MB** (target ideal: < 40 MB saat idle, < 85 MB saat beban puncak).
</objective>

<critical_analysis_required>
Sebelum masuk ke desain struktur proyek, lakukan analisis komparatif berbasis First-Principles untuk menentukan satu stack pemrograman terbaik:
1. **Node.js + Baileys** vs **Golang + `whatsmeow`** vs **Rust (`whatsapp-rust` / custom Noise+Protobuf)**:
   - Hitung estimasi memory footprint nyata (runtime overhead, heap, GC, ukuran state kriptografi per session).
   - Evaluasi kematangan protokol WhatsApp Multi-Device (kecepatan update skema Protobuf dan stabilitas auto-reconnect).
   - Tentukan **SATU** pemenang absolut untuk produksi dengan batas RAM < 100 MB dan jelaskan alasan eliminasi kandidat lain.
</critical_analysis_required>

<system_requirements>
1. **Zero UI / Pure CLI & HTTP Daemon Architecture**
   - Tidak ada dashboard web atau frontend.
   - Satu binary tunggal berfungsi sebagai:
     a. Background daemon (`wa-api serve`): HTTP REST/Webhook server + koneksi WebSocket WhatsApp.
     b. CLI control client (`wa-api instance ...`, `wa-api key ...`) yang berkomunikasi dengan daemon via Unix Domain Socket / loopback RPC ringan.
   - CLI wajib: generate/list/revoke API key (hash SHA-256/BLAKE3), create/delete/restart/status instance, pairing via QR ASCII atau 8-digit pairing code.
2. **Strict Memory Budget (< 100 MB)**
   - Tabel Memory Budget Allocation per komponen.
   - Zero history & ephemeral media pipeline (streaming/chunked, file 20 MB tidak menaikkan RAM 20 MB).
   - Tuning SQLite (WAL, mmap_size, cache_size, synchronous, temp_store).
   - Tuning runtime (`GOMEMLIMIT`, `GOGC`, `-ldflags="-s -w"`).
3. **Project Structure** pragmatis, idiomatik, tanggung jawab tiap package jelas.
4. **Database Schema & State Lifecycle** + diagram mermaid: pairing flow, event → webhook dispatcher (retry & backoff), outgoing rate-limiter & anti-ban queue.
5. **API Specification & CLI Command Reference** lengkap dengan contoh output dan payload.
</system_requirements>

<output_format>
- Bahasa Indonesia teknis, istilah teknis tetap bahasa Inggris.
- Semua komentar kode WAJIB Bahasa Indonesia.
- Satu desain terbaik, tanpa alternatif menggantung.
- Satu dokumen Markdown utuh.
</output_format>

