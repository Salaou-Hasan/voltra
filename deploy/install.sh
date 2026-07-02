#!/usr/bin/env bash
set -euo pipefail

# Voltra idempotent installer for Linux

# Detect OS
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Error: This installer only supports Linux." >&2
  exit 1
fi

INSTALL_BIN="/usr/local/bin/voltra"
DATA_DIR="/var/lib/voltra"
SERVICE_FILE="/etc/systemd/system/voltra.service"
# ${BASH_SOURCE[0]} is unset under `set -u` when this script is piped
# straight into bash (curl ... | bash) — stdin has no source path.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-.}")" && pwd)"

echo "==> Installing Voltra..."

# Download latest release binary. Releases are published to a separate
# repo (voltra-releases) by .github/workflows/release.yml, not the source repo.
REPO="Salaou-Hasan/voltra-releases"
LATEST_TAG="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')"
BINARY_URL="https://github.com/${REPO}/releases/download/${LATEST_TAG}/voltra-linux-x86_64"

echo "==> Downloading Voltra ${LATEST_TAG} from ${BINARY_URL}..."
curl -fsSL "$BINARY_URL" -o "$INSTALL_BIN"
chmod +x "$INSTALL_BIN"
echo "==> Binary installed to ${INSTALL_BIN}"

# Create voltra system user if not exists
if ! id -u voltra &>/dev/null; then
  echo "==> Creating voltra system user..."
  useradd --system --no-create-home --shell /usr/sbin/nologin voltra
else
  echo "==> voltra user already exists, skipping."
fi

# Create data directory
echo "==> Creating data directory ${DATA_DIR}..."
mkdir -p "$DATA_DIR"
chown voltra:voltra "$DATA_DIR"

# Install systemd service. When this script is piped straight into bash
# (curl ... | bash) there is no local voltra.service next to it, so fall
# back to fetching it from the source repo.
echo "==> Installing systemd service..."
if [[ -f "${SCRIPT_DIR}/voltra.service" ]]; then
  cp "${SCRIPT_DIR}/voltra.service" "$SERVICE_FILE"
else
  curl -fsSL "https://raw.githubusercontent.com/Salaou-Hasan/voltra/master/deploy/voltra.service" -o "$SERVICE_FILE"
fi

# Enable and start service
echo "==> Enabling and starting voltra service..."
systemctl daemon-reload
systemctl enable --now voltra

echo ""
echo "Voltra is installed and running."
echo "  WebSocket port : 3000"
echo "  Metrics port   : 3001"
echo ""
echo "Check status : systemctl status voltra"
echo "View logs    : journalctl -u voltra -f"
