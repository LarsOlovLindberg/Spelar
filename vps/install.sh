#!/usr/bin/env bash
set -euo pipefail

REMOTE_ROOT=${REMOTE_ROOT:-/opt/spelar_eu}
VPS_USER=${VPS_USER:-spelar}
SERVICE_NAME=${SERVICE_NAME:-spelar-agent}

echo "[install] remote_root=$REMOTE_ROOT"

sudo apt-get update -y
sudo apt-get install -y python3 python3-venv python3-pip ca-certificates

# Create a dedicated service user (non-login)
if ! id -u "$VPS_USER" >/dev/null 2>&1; then
  sudo useradd --system --create-home --home-dir "/home/$VPS_USER" --shell /usr/sbin/nologin "$VPS_USER"
fi

sudo mkdir -p "$REMOTE_ROOT"
sudo chown -R "$VPS_USER":"$VPS_USER" "$REMOTE_ROOT"

cd "$REMOTE_ROOT/vps"

# Create venv if missing
if [ ! -d ".venv" ]; then
  sudo -u "$VPS_USER" python3 -m venv .venv
fi

sudo -u "$VPS_USER" "$REMOTE_ROOT/vps/.venv/bin/python" -m pip install --upgrade pip
sudo -u "$VPS_USER" "$REMOTE_ROOT/vps/.venv/bin/python" -m pip install -r requirements.txt

echo "[install] done. Next: ensure /etc/spelar-agent.env exists and start systemd unit ($SERVICE_NAME)."
