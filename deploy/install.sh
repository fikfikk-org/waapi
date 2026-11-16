#!/usr/bin/env bash
# Skrip instalasi & upgrade waapi (systemd + binary statis) untuk https://github.com/fikfikk-org/waapi
set -euo pipefail

REPO="fikfikk-org/waapi"
BIN_PATH="/usr/local/bin/waapi"
DATA_DIR="/var/lib/waapi"
SERVICE_FILE="/etc/systemd/system/waapi.service"
PROFILE="${WAAPI_PROFILE:-lean}"

case "${PROFILE}" in
  micro)
    PROFILE_GOMEMLIMIT="20MiB"
    PROFILE_MEM_HIGH="40M"
    PROFILE_MEM_MAX="64M"
    ;;
  lean)
    PROFILE_GOMEMLIMIT="28MiB"
    PROFILE_MEM_HIGH="56M"
    PROFILE_MEM_MAX="80M"
    ;;
  classic)
    PROFILE_GOMEMLIMIT="80MiB"
    PROFILE_MEM_HIGH="90M"
    PROFILE_MEM_MAX="100M"
    ;;
  *)
    echo "Profil WAAPI_PROFILE tidak valid: ${PROFILE} (pilih: micro, lean, classic)" >&2
    exit 1
    ;;
esac

# Pastikan skrip dijalankan sebagai root
if [[ "${EUID}" -ne 0 ]]; then
  echo "Jalankan installer dengan sudo: curl -fsSL https://raw.githubusercontent.com/${REPO}/main/deploy/install.sh | sudo bash" >&2
  exit 1
fi

# Deteksi arsitektur CPU untuk memilih binary statis yang tepat
ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64|amd64)  GOARCH="amd64" ;;
  aarch64|arm64) GOARCH="arm64" ;;
  *)
    echo "Arsitektur tidak didukung: ${ARCH}" >&2
    exit 1
    ;;
esac

# Buat group dan system user waapi jika belum ada
if ! getent group waapi >/dev/null; then
  groupadd --system waapi
fi
if ! id -u waapi >/dev/null 2>&1; then
  useradd --system --gid waapi --home-dir "${DATA_DIR}" --shell /usr/sbin/nologin waapi
fi

# Tambahkan user pemanggil sudo ke group waapi agar bisa menjalankan CLI waapi tanpa sudo
if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
  usermod -aG waapi "${SUDO_USER}" || true
fi

# Siapkan direktori data dengan permission group (2770) agar control.sock dapat diakses group waapi
mkdir -p "${DATA_DIR}" "${DATA_DIR}/tmp"
chown -R waapi:waapi "${DATA_DIR}"
chmod 2770 "${DATA_DIR}"
chmod 0700 "${DATA_DIR}/tmp"

# Pasang binary: prioritaskan hasil build lokal, lalu GitHub Release terbaru, lalu build dari source
if [[ -f "./bin/waapi" ]]; then
  echo "Memasang binary lokal ./bin/waapi..."
  install -m 0755 ./bin/waapi "${BIN_PATH}"
elif [[ -f "./waapi" ]]; then
  echo "Memasang binary lokal ./waapi..."
  install -m 0755 ./waapi "${BIN_PATH}"
else
  TMP_BIN="$(mktemp)"
  RELEASE_URL="https://github.com/${REPO}/releases/latest/download/waapi-linux-${GOARCH}"
  echo "Mengunduh binary statis dari ${RELEASE_URL}..."
  if curl -fsSL "${RELEASE_URL}" -o "${TMP_BIN}"; then
    install -m 0755 "${TMP_BIN}" "${BIN_PATH}"
    rm -f "${TMP_BIN}"
  elif command -v go >/dev/null 2>&1; then
    rm -f "${TMP_BIN}"
    echo "Release belum tersedia, mengompilasi langsung dengan Go toolchain..."
    GOBIN="/usr/local/bin" CGO_ENABLED=0 go install -trimpath -ldflags="-s -w -buildid=" "github.com/${REPO}/cmd/waapi@latest"
  else
    rm -f "${TMP_BIN}"
    echo "Gagal mengunduh binary dari GitHub Release dan Go belum terpasang." >&2
    exit 1
  fi
fi

# Pasang konfigurasi environment global agar CLI waapi selalu mengarah ke /var/lib/waapi
cat >/etc/profile.d/waapi.sh <<'EOF'
# Set direktori data default untuk CLI waapi
export WAAPI_DATA_DIR="/var/lib/waapi"
EOF
chmod 0644 /etc/profile.d/waapi.sh

# Backup unit systemd lama sebelum menimpa dengan konfigurasi v2
if [[ -f "${SERVICE_FILE}" ]]; then
  cp -f "${SERVICE_FILE}" "${SERVICE_FILE}.bak"
  chmod 0644 "${SERVICE_FILE}.bak"
  echo "✔ Unit lama di-backup ke ${SERVICE_FILE}.bak"
fi

# Tulis unit systemd berbasis profil agar install.sh dapat dijalankan standalone via curl | sudo bash
cat >"${SERVICE_FILE}" <<EOF
# Unit systemd waapi v2 untuk profil ${PROFILE}
[Unit]
Description=waapi WhatsApp API Gateway v2
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
NotifyAccess=main
User=waapi
Group=waapi
UMask=0007
Environment=WAAPI_DATA_DIR=${DATA_DIR}
Environment=WAAPI_HTTP_ADDR=127.0.0.1:8080
Environment=WAAPI_PROFILE=${PROFILE}
Environment=GOMEMLIMIT=${PROFILE_GOMEMLIMIT}
Environment=GOGC=50
Environment=GOMAXPROCS=2
# Aktifkan hanya bila waapi doctor --deep melaporkan THP "always", lalu bandingkan RSS
#Environment=GODEBUG=disablethp=1
ExecStart=/usr/local/bin/waapi serve
Restart=always
RestartSec=3
TimeoutStartSec=90
WatchdogSec=30s

# Throttle & batas maksimum cgroup v2 sesuai profil ${PROFILE} tanpa swap
MemoryHigh=${PROFILE_MEM_HIGH}
MemoryMax=${PROFILE_MEM_MAX}
MemorySwapMax=0
TasksMax=64
LimitNOFILE=4096

# Hardening keamanan sistem operasi
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${DATA_DIR}
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
ProtectClock=true
ProtectHostname=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=true
RestrictRealtime=true
RestrictSUIDSGID=true
LockPersonality=true
MemoryDenyWriteExecute=true
SystemCallArchitectures=native
SystemCallFilter=@system-service madvise fadvise64 sync_file_range
CapabilityBoundingSet=
AmbientCapabilities=

[Install]
WantedBy=multi-user.target
EOF
chmod 0644 "${SERVICE_FILE}"

if command -v systemd-analyze >/dev/null 2>&1; then
  if ! systemd-analyze syscall-filter @system-service >/dev/null 2>&1; then
    echo "Catatan: systemd-analyze syscall-filter tidak tersedia; daftar syscall eksplisit tetap terpasang."
  fi
fi

systemctl daemon-reload
systemctl enable --now waapi
systemctl restart waapi

echo "✔ waapi terpasang dan berjalan. Cek dengan: waapi status"
