#!/usr/bin/env bash
# Install the Illumio S3 -> SIEM Collector from an offline bundle.
set -euo pipefail

INSTALL_DIR="/opt/illumio-collector"
CONFIG_DIR="/etc/illumio-collector"
STATE_DIR="/var/lib/illumio-collector/state"
LOG_DIR="/var/log/illumio-collector"
SERVICE_USER="illumio-collector"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${BUNDLE_DIR}"

echo "==> Copy bundle to ${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"
cp -r app systemd wheels VERSION "${INSTALL_DIR}/"
if [[ ! -d "${INSTALL_DIR}/python" ]]; then
  echo "==> Extract portable Python runtime"
  tar -xzf python-runtime.tar.gz -C "${INSTALL_DIR}"
fi

echo "==> Install wheels (offline)"
"${INSTALL_DIR}/python/bin/python3" -m pip install \
  --no-index \
  --find-links="${INSTALL_DIR}/wheels" \
  -r "${INSTALL_DIR}/app/requirements.txt"

echo "==> Prepare config dir"
mkdir -p "${CONFIG_DIR}"
if [[ ! -f "${CONFIG_DIR}/config.yaml" ]]; then
  cp "${INSTALL_DIR}/app/config.example.yaml" "${CONFIG_DIR}/config.yaml"
  chmod 600 "${CONFIG_DIR}/config.yaml"
fi

echo "==> Create service user and state dirs"
id -u "${SERVICE_USER}" >/dev/null 2>&1 || \
  useradd --system --shell /sbin/nologin "${SERVICE_USER}"
mkdir -p "${STATE_DIR}" "${LOG_DIR}"
chown -R "${SERVICE_USER}:${SERVICE_USER}" "${STATE_DIR}" "${LOG_DIR}" "${INSTALL_DIR}"

echo "==> Install systemd unit"
install -m 0644 "${INSTALL_DIR}/systemd/illumio-collector.service" \
  /etc/systemd/system/illumio-collector.service
systemctl daemon-reload
systemctl enable illumio-collector

cat <<'ENDMSG'

============================================================
Install complete.

 1. Edit the config:   sudo vi /etc/illumio-collector/config.yaml
 2. Start the service: sudo systemctl start illumio-collector
 3. Watch the logs:    sudo journalctl -u illumio-collector -f
============================================================
ENDMSG
