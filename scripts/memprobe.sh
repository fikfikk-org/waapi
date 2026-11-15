#!/usr/bin/env bash
set -euo pipefail

# Skrip memprobe untuk mengukur anatomi memori idle binary waapi pada kernel Linux.
# Penggunaan: scripts/memprobe.sh [git-ref] [output.json]

REF="${1:-HEAD}"
OUT="${2:-bench/memprobe.json}"

if [ ! -r /proc/self/smaps_rollup ]; then
	echo "ERROR: memprobe membutuhkan Linux procfs (/proc/self/smaps_rollup tidak tersedia di OS ini)." >&2
	exit 1
fi

ROOT="$(git rev-parse --show-toplevel)"
GIT_COMMIT="$(git -C "$ROOT" rev-parse --short "$REF")"
SAFE_REF="$(printf '%s' "$REF" | tr -c 'A-Za-z0-9._-' '_')"

BIN_DIR="$ROOT/bench/.bin"
mkdir -p "$BIN_DIR" "$(dirname "$OUT")"

BIN_PATH="$BIN_DIR/waapi-$SAFE_REF-$GIT_COMMIT"
WORKTREE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/waapi-wt.XXXXXX")"
TMP_DATA="$(mktemp -d "${TMPDIR:-/tmp}/waapi-memprobe.XXXXXX")"
DAEMON_PID=""

cleanup() {
	if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
		kill -TERM "$DAEMON_PID" 2>/dev/null || true
		wait "$DAEMON_PID" 2>/dev/null || true
	fi
	git -C "$ROOT" worktree remove --force "$WORKTREE_DIR" >/dev/null 2>&1 || rm -rf "$WORKTREE_DIR"
	rm -rf "$TMP_DATA"
}
trap cleanup EXIT

# Checkout ref target ke worktree terpisah agar working tree utama tidak tersentuh
git -C "$ROOT" worktree add --detach "$WORKTREE_DIR" "$REF" >/dev/null

# Build ke direktori bench/.bin pada disk (bukan tmpfs) agar halaman binary tercatat sebagai RssFile/Pss_File
(
	cd "$WORKTREE_DIR"
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -buildid=" -o "$BIN_PATH" ./cmd/waapi
)

BINARY_BYTES="$(wc -c < "$BIN_PATH" | tr -d '[:space:]')"

# Jalankan daemon idle dengan data dir terisolasi dan port acak
WAAPI_DATA_DIR="$TMP_DATA" WAAPI_HTTP_ADDR="127.0.0.1:0" "$BIN_PATH" serve >"$TMP_DATA/daemon.log" 2>&1 &
DAEMON_PID=$!

# Tunggu control socket siap maksimal 10 detik
for _ in $(seq 1 100); do
	if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
		echo "ERROR: daemon berhenti lebih awal saat startup:" >&2
		cat "$TMP_DATA/daemon.log" >&2
		exit 1
	fi
	if [ -S "$TMP_DATA/control.sock" ]; then
		break
	fi
	sleep 0.1
done

if [ ! -S "$TMP_DATA/control.sock" ]; then
	echo "ERROR: control.sock tidak muncul dalam 10 detik" >&2
	cat "$TMP_DATA/daemon.log" >&2
	exit 1
fi

# Tunggu 15 detik agar inisialisasi latar belakang dan GC awal mengendap
sleep 15

if [ ! -r "/proc/$DAEMON_PID/status" ] || [ ! -r "/proc/$DAEMON_PID/smaps_rollup" ]; then
	echo "ERROR: /proc/$DAEMON_PID/{status,smaps_rollup} tidak dapat dibaca" >&2
	exit 1
fi

RSS_KIB="$(awk '/^VmRSS:/ { print $2; exit }' "/proc/$DAEMON_PID/status")"
PSS_ANON_KIB="$(awk '/^Pss_Anon:/ { print $2; exit }' "/proc/$DAEMON_PID/smaps_rollup")"
PSS_FILE_KIB="$(awk '/^Pss_File:/ { print $2; exit }' "/proc/$DAEMON_PID/smaps_rollup")"

RSS_KIB="${RSS_KIB:-0}"
PSS_ANON_KIB="${PSS_ANON_KIB:-0}"
PSS_FILE_KIB="${PSS_FILE_KIB:-0}"

# Ambil jumlah goroutine dari keluaran waapi doctor format v1 maupun v2
DOCTOR_OUT="$(WAAPI_DATA_DIR="$TMP_DATA" "$BIN_PATH" doctor)"
GOROUTINES="$(printf '%s\n' "$DOCTOR_OUT" | awk '/^Goroutines[[:space:]]+/ { print $2; exit }')"
GOROUTINES="${GOROUTINES:-0}"

# Simpan keluaran doctor --deep --json bila binary mendukungnya
DEEP_JSON="null"
if DEEP_RAW="$(WAAPI_DATA_DIR="$TMP_DATA" "$BIN_PATH" doctor --deep --json 2>/dev/null)"; then
	if printf '%s' "$DEEP_RAW" | grep -q '^{'; then
		DEEP_JSON="$DEEP_RAW"
	fi
fi

# Hentikan daemon dengan SIGTERM sebelum menulis hasil akhir
kill -TERM "$DAEMON_PID"
wait "$DAEMON_PID" || true
DAEMON_PID=""

cat > "$OUT" <<EOF
{
  "git": "$GIT_COMMIT",
  "ref": "$REF",
  "binary_bytes": $BINARY_BYTES,
  "rss_kib": $RSS_KIB,
  "pss_anon_kib": $PSS_ANON_KIB,
  "pss_file_kib": $PSS_FILE_KIB,
  "goroutines": $GOROUTINES,
  "doctor_deep": $DEEP_JSON
}
EOF

echo "Memprobe selesai: $OUT (git=$GIT_COMMIT, rss_kib=$RSS_KIB, pss_anon_kib=$PSS_ANON_KIB, pss_file_kib=$PSS_FILE_KIB, goroutines=$GOROUTINES)"
