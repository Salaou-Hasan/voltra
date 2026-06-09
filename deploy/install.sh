#!/usr/bin/env bash
set -euo pipefail

# NeonDB idempotent installer for Linux

# Detect OS
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Error: This installer only supports Linux." >&2
  exit 1
fi

INSTALL_BIN="/usr/local/bin/neondb"
DATA_DIR="/var/lib/neondb"
SERVICE_FILE="/etc/systemd/system/neondb.service"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Installing NeonDB..."

# Download latest release binary
REPO="Salaou-Hasan/NeonDB"
LATEST_TAG="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')"
BINARY_URL="https://github.com/${REPO}/releases/download/${LATEST_TAG}/neondb-linux-x86_64"

echo "==> Downloading NeonDB ${LATEST_TAG} from ${BINARY_URL}..."
curl -fsSL "$BINARY_URL" -o "$INSTALL_BIN"
chmod +x "$INSTALL_BIN"
echo "==> Binary installed to ${INSTALL_BIN}"

# Create neondb system user if not exists
if ! id -u neondb &>/dev/null; then
  echo "==> Creating neondb system user..."
  useradd --system --no-create-home --shell /usr/sbin/nologin neondb
else
  echo "==> neondb user already exists, skipping."
fi

# Create data directory
echo "==> Creating data directory ${DATA_DIR}..."
mkdir -p "$DATA_DIR"
chown neondb:neondb "$DATA_DIR"

# Install systemd service
echo "==> Installing systemd service..."
cp "${SCRIPT_DIR}/neondb.service" "$SERVICE_FILE"

# Enable and start service
echo "==> Enabling and starting neondb service..."
systemctl daemon-reload
systemctl enable --now neondb

echo ""
echo "NeonDB is installed and running."
echo "  WebSocket port : 3000"
echo "  Metrics port   : 3001"
echo ""
echo "Check status : systemctl status neondb"
echo "View logs    : journalctl -u neondb -f"
