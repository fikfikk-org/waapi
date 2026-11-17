# Laporan Pengukuran & Burn-in waapi v2.0.0

Dokumen ini mencatat metrik yang **benar-benar terukur** selama pengembangan v2.0.0 serta templat pengukuran **Lampiran A** (`scripts/burnin.sh`, `scripts/memprobe.sh`, dan eksperimen A/B) untuk server ARM64 Linux target.

> [!IMPORTANT]
> Seluruh pengembangan dan pengujian Batch 10–16 dijalankan pada host **Windows (`windows/amd64`, Intel Core i7-10750H, Go 1.27.4, `CGO_ENABLED=0`)**. Pengukuran RSS `/proc/self/smaps_rollup`, cgroup v2 `MemoryPeak`, `systemd-analyze security`, dan skenario WhatsApp live **A.1–A.8** harus dieksekusi oleh operator pada VPS/SBC Linux ARM64 target sebelum merge `v2-dev` -> `main` dan penandaan tag `v2.0.0`.

---

## 1. Pengukuran Lokal Terverifikasi (`windows/amd64` build host)

Diukur pada branch `v2/b16-docs-release` memakai Go `1.27.4` (`CGO_ENABLED=0`, `-trimpath -ldflags="-s -w -buildid="`):

| Metrik Terukur Lokal | Target Blueprint (§1.3) | Hasil Nyata Terukur | Catatan |
|---|---|---|---|
| Ukuran binary `linux/amd64` (stripped) | ≤ 24 MB (`≤ 24 MiB`) | **`24,608,892` byte** (`23.47 MiB` / `24.61 MB`) | Di bawah `24 MiB` (`25,165,824` byte), sedikit melampaui `24 MB` desimal karena penambahan `github.com/modelcontextprotocol/go-sdk v1.8.0`, engine FTS5, hooks, dan paket `internal/ai` |
| Ukuran binary `linux/arm64` (stripped) | ≤ 24 MB | **`23,134,332` byte** (`22.06 MiB` / `23.13 MB`) | Memenuhi target baik dalam MB desimal maupun MiB biner |
| `BenchmarkJournal_Append-12` (`1000x`, SSD Windows) | p99 ≤ 1 ms (SSD) / ≤ 5 ms (eMMC) | **rata-rata `215,586 ns/op` (`~0.22 ms/op`)**, `3,928 B/op`, `80 allocs/op` | Benchmark cepat di disk SSD lokal; uji `10000x` p99 di eMMC/SD ARM64 dilakukan via A.2 |

---

## 2. Tabel Hasil Lampiran A (Target Server Linux ARM64)

| Metrik | Target | Hasil | Catatan |
|---|---|---|---|
| RSS p99 24 jam (MiB) | ≤ 22 | _menunggu eksekusi server target ARM64 Linux_ | Jalankan `scripts/burnin.sh 1440 bench/burnin` |
| `Pss_Anon` p99 (MiB) | ≤ 10 | _menunggu eksekusi server target ARM64 Linux_ | Dihitung otomatis dari `burnin.jsonl` oleh `scripts/burnin.sh` |
| Append journal p99 (ms) | ≤ 5 | _menunggu eksekusi server target ARM64 Linux_ | `go test -run='^$' -bench=BenchmarkJournal_Append -benchtime=10000x ./internal/journal` |
| Deteksi socket mati (s) | ≤ 75 | _menunggu eksekusi server target ARM64 Linux_ | Simulasi half-open `iptables -I OUTPUT -p tcp --dport 443 -j DROP` (A.3) |
| Kehilangan pesan A.4 / A.5 | 0 / 0 | _menunggu eksekusi server target ARM64 Linux_ | Uji `SIGKILL` berulang jalur masuk & jalur keluar (A.4, A.5) |
| `MemoryPeak` delta A.7 (MiB) | ≤ 20 | _menunggu eksekusi server target ARM64 Linux_ | Unggah media 64 MiB lewat `/v2/instances/{id}/media` dengan drop-behind aktif |
| Outage webhook 30 menit (A.6) | Delta anon < 1 MiB, 100 event terurut | _menunggu eksekusi server target ARM64 Linux_ | Matikan endpoint penerima lalu pulihkan |
| Pairing spike recovery (A.8) | Kembali `Green` ≤ 60 detik | _menunggu eksekusi server target ARM64 Linux_ | Amati level Governor pasca pairing instance ke-3 |

---

## 3. Eksperimen A/B Tuning Memori & Probe Fitur Opsional

| Eksperimen | RSS p99 (MiB) | `Pss_Anon` (MiB) | Latency p99 Enqueue (ms) | Status Default |
|---|---|---|---|---|
| Baseline (`lean`, THP host apa adanya) | _menunggu ARM64 Linux_ | _menunggu ARM64 Linux_ | _menunggu ARM64 Linux_ | Aktif (`deploy/install.sh`) |
| `GODEBUG=disablethp=1` (bila `waapi doctor --deep` melaporkan THP `always`) | _menunggu ARM64 Linux_ | _menunggu ARM64 Linux_ | _menunggu ARM64 Linux_ | **Mati** secara default |
| `WAAPI_PAGEOUT_AFTER_BOOT=cold` (`MADV_COLD`) | _menunggu ARM64 Linux_ | _menunggu ARM64 Linux_ | _menunggu ARM64 Linux_ | **Mati** secara default (`off`) |
| Profil `micro` vs `lean` | _menunggu ARM64 Linux_ | _menunggu ARM64 Linux_ | _menunggu ARM64 Linux_ | `lean` adalah default |
| `make memprobe`: context store mati vs hidup; hooks `50` rule; AI mati vs hidup; sebelum & sesudah MCP pertama | _menunggu Linux `/proc`_ | _menunggu Linux `/proc`_ | — | Wajib dijalankan di Linux via `make memprobe` sebelum tag `v2.0.0` |
