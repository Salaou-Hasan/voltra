#!/usr/bin/env bash
set -euo pipefail

# Voltra — Proxmox VE LXC installer
#
# Run this ON THE PROXMOX HOST (not inside a container). It creates a
# lightweight unprivileged LXC container, installs Voltra + systemd
# inside it via deploy/install.sh, and (optionally) registers the
# container with Proxmox's HA manager for auto-restart/migration.
#
# Usage (on the Proxmox host):
#   bash -c "$(wget -qLO - https://raw.githubusercontent.com/Salaou-Hasan/voltra/master/deploy/proxmox-install.sh)"
#
# Override any default via env vars, e.g.:
#   CTID=210 CORES=8 MEMORY=8192 STORAGE=local-zfs bash deploy/proxmox-install.sh

if ! command -v pct &>/dev/null; then
  echo "Error: 'pct' not found — this script must run on a Proxmox VE host." >&2
  exit 1
fi

CTID="${CTID:-$(pvesh get /cluster/nextid)}"
HOSTNAME="${HOSTNAME:-voltra-${CTID}}"
STORAGE="${STORAGE:-local-lvm}"
TEMPLATE_STORAGE="${TEMPLATE_STORAGE:-local}"
CORES="${CORES:-4}"
MEMORY="${MEMORY:-4096}"
SWAP="${SWAP:-512}"
DISK_GB="${DISK_GB:-16}"
BRIDGE="${BRIDGE:-vmbr0}"
TEMPLATE="${TEMPLATE:-debian-12-standard_12.7-1_amd64.tar.zst}"
ENABLE_HA="${ENABLE_HA:-0}"

echo "==> Voltra Proxmox LXC installer"
echo "    CTID=${CTID}  hostname=${HOSTNAME}  cores=${CORES}  memory=${MEMORY}MB  disk=${DISK_GB}GB  storage=${STORAGE}"

# ── Ensure the Debian template is present ──────────────────────────────────
if ! pveam list "${TEMPLATE_STORAGE}" 2>/dev/null | grep -q "${TEMPLATE}"; then
  echo "==> Fetching template ${TEMPLATE}..."
  pveam update
  pveam download "${TEMPLATE_STORAGE}" "${TEMPLATE}"
fi

# ── Create the unprivileged container ──────────────────────────────────────
echo "==> Creating container ${CTID}..."
pct create "${CTID}" "${TEMPLATE_STORAGE}:vztmpl/${TEMPLATE}" \
  --hostname "${HOSTNAME}" \
  --cores "${CORES}" \
  --memory "${MEMORY}" \
  --swap "${SWAP}" \
  --rootfs "${STORAGE}:${DISK_GB}" \
  --net0 "name=eth0,bridge=${BRIDGE},ip=dhcp" \
  --unprivileged 1 \
  --features nesting=1 \
  --onboot 1 \
  --start 0

echo "==> Starting container ${CTID}..."
pct start "${CTID}"

# Wait for networking inside the container.
echo "==> Waiting for network..."
for _ in $(seq 1 30); do
  if pct exec "${CTID}" -- getent hosts github.com &>/dev/null; then
    break
  fi
  sleep 2
done

# ── Install Voltra inside the container ────────────────────────────────────
echo "==> Installing Voltra inside container ${CTID}..."
pct exec "${CTID}" -- bash -c \
  "apt-get update -qq && apt-get install -y -qq curl >/dev/null && \
   curl -fsSL https://raw.githubusercontent.com/Salaou-Hasan/voltra/master/deploy/install.sh | bash"

# ── Optional: hand the container to Proxmox HA ─────────────────────────────
if [[ "${ENABLE_HA}" == "1" ]]; then
  echo "==> Registering ct:${CTID} with Proxmox HA manager..."
  ha-manager add "ct:${CTID}" --state started --max_restart 3 --max_relocate 1
fi

IP="$(pct exec "${CTID}" -- hostname -I | awk '{print $1}')"
echo ""
echo "==> Voltra is running in CT ${CTID} (${HOSTNAME})"
echo "    WebSocket : ws://${IP}:3000"
echo "    Metrics   : http://${IP}:3001/healthz"
echo "    Admin     : http://${IP}:3001/admin"
echo ""
echo "    Enter container : pct enter ${CTID}"
echo "    View logs       : pct exec ${CTID} -- journalctl -u voltra -f"
[[ "${ENABLE_HA}" == "1" ]] && echo "    HA status       : ha-manager status"
