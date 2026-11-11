#!/usr/bin/env bash
# Skrip instalasi & upgrade waapi (systemd + binary statis) untuk https://github.com/fikfikk-org/waapi
set -euo pipefail

REPO="fikfikk-org/waapi"
BIN_PATH="/usr/local/bin/waapi"
DATA_DIR="/var/lib/waapi"
SERVICE_FILE="/etc/systemd/system/waapi.service"

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

# Tulis unit systemd langsung agar install.sh dapat dijalankan standalone via curl | sudo bash
cat >"${SERVICE_FILE}" <<'EOF'
# Unit systemd untuk daemon waapi dengan batas RAM keras 100 MB
[Unit]
Description=waapi Private WhatsApp API Gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=waapi
Group=waapi
UMask=0007
Environment=WAAPI_DATA_DIR=/var/lib/waapi
Environment=WAAPI_HTTP_ADDR=127.0.0.1:8080
Environment=GOMEMLIMIT=80MiB
Environment=GOGC=50
Environment=GOMAXPROCS=2
ExecStart=/usr/local/bin/waapi serve
Restart=always
RestartSec=3

# Throttle memori mulai 90 MB dan batasi maksimum 100 MB tanpa swap
MemoryHigh=90M
MemoryMax=100M
MemorySwapMax=0

# Hardening keamanan sistem operasi
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/waapi
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF
chmod 0644 "${SERVICE_FILE}"

systemctl daemon-reload
systemctl enable --now waapi
systemctl restart waapi

echo "✔ waapi terpasang dan berjalan. Cek dengan: waapi status"
