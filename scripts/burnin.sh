#!/usr/bin/env bash
# Skrip burn-in memori 24 jam sesuai Lampiran A.1 BLUEPRINT_V2.md.
# Penggunaan:
#   scripts/burnin.sh [durasi_menit] [output_dir]
# Contoh uji penuh 24 jam:
#   scripts/burnin.sh 1440 bench/burnin
set -euo pipefail

DURATION_MIN="${1:-1440}"
OUT_DIR="${2:-bench/burnin}"
INTERVAL_SEC="${INTERVAL_SEC:-60}"
SERVICE="${WAAPI_SERVICE:-waapi}"

if ! command -v waapi >/dev/null 2>&1; then
  echo "ERROR: binary waapi tidak ditemukan di PATH." >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq wajib terpasang untuk meringkas burnin.jsonl." >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"
JSONL_FILE="${OUT_DIR}/burnin.jsonl"
SYSTEMD_LOG="${OUT_DIR}/systemd-mem.log"

summarize() {
  if [[ ! -s "${JSONL_FILE}" ]]; then
    echo "Belum ada sampel di ${JSONL_FILE}."
    return 0
  fi
  echo "=== Ringkasan Burn-in (${JSONL_FILE}) ==="
  jq -s '
    def p99(arr):
      (arr | sort) as $s |
      if ($s | length) == 0 then 0
      else $s[((($s | length) * 99 + 99) / 100 | floor) - 1]
      end;
    {
      samples: length,
      rss_p99_mib: ((p99([.[].proc.vm_rss_bytes // 0]) / 1048576) * 100 | round / 100),
      pss_anon_p99_mib: ((p99([.[].smaps.pss_anon_bytes // 0]) / 1048576) * 100 | round / 100),
      cgroup_anon_p99_mib: ((p99([.[].cgroup.stat.anon_bytes // 0]) / 1048576) * 100 | round / 100),
      oom_kill_max: ([.[].cgroup.events.oom_kill // 0] | max),
      red_samples: ([.[] | select((.governor_level // "green") == "red")] | length),
      yellow_samples: ([.[] | select((.governor_level // "green") == "yellow")] | length)
    }
  ' "${JSONL_FILE}"
}

trap summarize EXIT

echo "Memulai burn-in A.1: durasi=${DURATION_MIN} menit, interval=${INTERVAL_SEC}s, service=${SERVICE}"
for ((i = 1; i <= DURATION_MIN; i++)); do
  ts="$(date -Iseconds)"
  if raw="$(waapi doctor --deep --json 2>/dev/null)"; then
    printf '%s\n' "${raw}" | jq -c --arg ts "${ts}" '. + {sampled_at: $ts}' >> "${JSONL_FILE}"
  else
    echo "${ts} WARN gagal mengambil waapi doctor --deep --json" >> "${SYSTEMD_LOG}"
  fi

  if command -v systemctl >/dev/null 2>&1; then
    {
      printf '%s ' "${ts}"
      systemctl show "${SERVICE}" -p MemoryCurrent -p MemoryPeak 2>/dev/null | tr '\n' ' '
      printf '\n'
    } >> "${SYSTEMD_LOG}"
  fi

  if (( i < DURATION_MIN )); then
    sleep "${INTERVAL_SEC}"
  fi
done
