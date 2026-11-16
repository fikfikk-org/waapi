# Laporan Pengukuran & Burn-in waapi v2.0.0

Dokumen ini berisi template hasil pengujian **Lampiran A** (`scripts/burnin.sh`, `scripts/memprobe.sh`, dan eksperimen A/B ARM64) untuk diisi setelah dieksekusi di server Linux target.

> [!NOTE]
> Pengembangan Batch 10 dilakukan pada host Windows sehingga eksekusi `scripts/memprobe.sh`, `scripts/burnin.sh` (24 jam), `systemd-analyze security waapi`, dan `MADV_COLD`/`disablethp` A/B harus dijalankan pada VPS/SBC Linux ARM64 target sebelum rilis `v2.0.0`.

## Tabel Hasil Lampiran A

| Metrik | Target | Hasil | Catatan |
|---|---|---|---|
| RSS p99 24 jam (MiB) | ≤ 22 | _menunggu ARM64_ | Jalankan `scripts/burnin.sh 1440 bench/burnin` |
| Pss_Anon p99 (MiB) | ≤ 10 | _menunggu ARM64_ | Dihitung otomatis dari `burnin.jsonl` oleh `scripts/burnin.sh` |
| Append journal p99 (ms) | ≤ 5 | _menunggu ARM64_ | `go test -run='^$' -bench=BenchmarkJournal_Append -benchtime=10000x ./internal/journal` |
| Deteksi socket mati (s) | ≤ 75 | _menunggu ARM64_ | Simulasi half-open `iptables -I OUTPUT -p tcp --dport 443 -j DROP` |
| Kehilangan pesan A.4 / A.5 | 0 / 0 | _menunggu ARM64_ | Uji `SIGKILL` berulang jalur masuk & jalur keluar |
| `MemoryPeak` delta A.7 (MiB) | ≤ 20 | _menunggu ARM64_ | Unggah media 64 MiB lewat `/v2/instances/{id}/media` dengan drop-behind aktif |

## Eksperimen A/B Tuning Memori (Batch 10)

| Eksperimen | RSS p99 (MiB) | `Pss_Anon` (MiB) | Latency p99 Enqueue (ms) | Status Default |
|---|---|---|---|---|
| Baseline (`lean`, THP host apa adanya) | _menunggu ARM64_ | _menunggu ARM64_ | _menunggu ARM64_ | Aktif (`deploy/install.sh`) |
| `GODEBUG=disablethp=1` (jika THP `always`) | _menunggu ARM64_ | _menunggu ARM64_ | _menunggu ARM64_ | **Mati** secara default |
| `WAAPI_PAGEOUT_AFTER_BOOT=cold` (`MADV_COLD`) | _menunggu ARM64_ | _menunggu ARM64_ | _menunggu ARM64_ | **Mati** secara default (`off`) |
| Profil `micro` vs `lean` | _menunggu ARM64_ | _menunggu ARM64_ | _menunggu ARM64_ | `lean` adalah default |
