#!/usr/bin/env bash
# Skrip update & rollback standalone untuk waapi (https://github.com/fikfikk-org/waapi)
# Contoh pemakaian:
#   bash deploy/update.sh --check          # cek versi terbaru tanpa mengubah sistem
#   sudo bash deploy/update.sh             # update ke versi terbaru (latest)
#   sudo bash deploy/update.sh v1.0.0      # pasang/downgrade ke versi v1.0.0
#   sudo bash deploy/update.sh --rollback  # kembalikan ke backup lokal terakhir
set -euo pipefail

REPO="fikfikk-org/waapi"
BIN_PATH="/usr/local/bin/waapi"
DATA_DIR="${WAAPI_DATA_DIR:-/var/lib/waapi}"
BACKUP_DIR="${DATA_DIR}/backups"

# Deteksi arsitektur CPU
ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64|amd64)  GOARCH="amd64" ;;
  aarch64|arm64) GOARCH="arm64" ;;
  *)
    echo "Arsitektur tidak didukung: ${ARCH}" >&2
    exit 1
    ;;
esac
ASSET="waapi-linux-${GOARCH}"

# Ambil versi terpasang saat ini jika binary ada
CURRENT_VER="belum-terpasang"
if [[ -x "${BIN_PATH}" ]]; then
  CURRENT_VER="$("${BIN_PATH}" version 2>/dev/null | awk '{print "v"$2}' | sed 's/^vv/v/' || echo "unknown")"
fi

MODE="${1:-latest}"

# Mode 1: hanya cek versi terbaru di GitHub
if [[ "${MODE}" == "--check" || "${MODE}" == "check" ]]; then
  echo "Versi terpasang : ${CURRENT_VER}"
  LATEST_TAG="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep -m1 '"tag_name"' | cut -d'"' -f4 || true)"
  if [[ -z "${LATEST_TAG}" ]]; then
    echo "Belum ada rilis di GitHub (${REPO})."
    exit 0
  fi
  echo "Versi terbaru   : ${LATEST_TAG}"
  if [[ "${CURRENT_VER}" == "${LATEST_TAG}" ]]; then
    echo "✔ Anda sudah menggunakan versi terbaru."
  else
    echo "⬆ Update tersedia! Jalankan: sudo waapi update:latest"
  fi
  exit 0
fi

# Operasi tulis membutuhkan akses root
if [[ "${EUID}" -ne 0 ]]; then
  echo "Jalankan dengan sudo: sudo bash deploy/update.sh ${MODE}" >&2
  exit 1
fi

mkdir -p "${BACKUP_DIR}"

# Mode 2: rollback ke backup lokal terakhir
if [[ "${MODE}" == "--rollback" || "${MODE}" == "rollback" ]]; then
  TARGET_VER="${2:-}"
  if [[ -n "${TARGET_VER}" ]]; then
    [[ "${TARGET_VER}" != v* ]] && TARGET_VER="v${TARGET_VER}"
    CANDIDATE="${BACKUP_DIR}/waapi-${TARGET_VER}"
  else
    CANDIDATE="$(ls -1t "${BACKUP_DIR}"/waapi-v* 2>/dev/null | head -n1 || true)"
  fi

  if [[ -z "${CANDIDATE}" || ! -f "${CANDIDATE}" ]]; then
    echo "Backup lokal tidak ditemukan di ${BACKUP_DIR}." >&2
    exit 1
  fi

  echo "Mengembalikan binary dari ${CANDIDATE}..."
  install -m 0755 "${CANDIDATE}" "${BIN_PATH}"
  systemctl restart waapi || true
  echo "✔ Rollback selesai. Versi aktif: $("${BIN_PATH}" version)"
  exit 0
fi

# Mode 3: update ke latest atau ke tag tertentu
if [[ "${MODE}" == "latest" ]]; then
  TARGET_TAG="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep -m1 '"tag_name"' | cut -d'"' -f4)"
else
  TARGET_TAG="${MODE}"
  [[ "${TARGET_TAG}" != v* ]] && TARGET_TAG="v${TARGET_TAG}"
fi

if [[ -z "${TARGET_TAG}" ]]; then
  echo "Gagal menentukan tag rilis dari GitHub." >&2
  exit 1
fi

# Backup binary lama sebelum diganti
if [[ -x "${BIN_PATH}" && "${CURRENT_VER}" == v* ]]; then
  cp -f "${BIN_PATH}" "${BACKUP_DIR}/waapi-${CURRENT_VER}"
  chmod 0755 "${BACKUP_DIR}/waapi-${CURRENT_VER}"
  echo "✔ Binary lama (${CURRENT_VER}) di-backup ke ${BACKUP_DIR}/waapi-${CURRENT_VER}"
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

BASE_URL="https://github.com/${REPO}/releases/download/${TARGET_TAG}"
echo "Mengunduh ${TARGET_TAG} (${ASSET})..."
curl -fsSL "${BASE_URL}/${ASSET}" -o "${TMP_DIR}/${ASSET}"

# Verifikasi SHA-256 jika checksums.txt tersedia
if curl -fsSL "${BASE_URL}/checksums.txt" -o "${TMP_DIR}/checksums.txt" 2>/dev/null; then
  (cd "${TMP_DIR}" && grep " ${ASSET}\$" checksums.txt | sha256sum -c -)
fi

install -m 0755 "${TMP_DIR}/${ASSET}" "${BIN_PATH}"
systemctl restart waapi || true
echo "✔ Berhasil memperbarui waapi ke ${TARGET_TAG}."
