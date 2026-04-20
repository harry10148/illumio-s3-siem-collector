#!/usr/bin/env bash
# Uninstall the Illumio S3 -> SIEM Collector from Linux.
#
# By default, config (/etc/illumio-collector) and state/logs
# (/var/lib/illumio-collector, /var/log/illumio-collector) are
# preserved so that a reinstall can resume without data loss.
#
# Use --purge to also remove config and state.
#
# Usage:
#   sudo bash scripts/uninstall.sh           # keep config + state
#   sudo bash scripts/uninstall.sh --purge   # remove everything
set -euo pipefail

INSTALL_DIR="/opt/illumio-collector"
CONFIG_DIR="/etc/illumio-collector"
STATE_DIR="/var/lib/illumio-collector"
LOG_DIR="/var/log/illumio-collector"
SERVICE_USER="illumio-collector"
SERVICE_FILE="/etc/systemd/system/illumio-collector.service"

PURGE=false
for arg in "$@"; do
  [[ "${arg}" == "--purge" ]] && PURGE=true
done

if [[ "$(id -u)" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

# --- stop and disable service ---
if systemctl is-active --quiet illumio-collector 2>/dev/null; then
  echo "==> Stopping service"
  systemctl stop illumio-collector
fi

if systemctl is-enabled --quiet illumio-collector 2>/dev/null; then
  echo "==> Disabling service"
  systemctl disable illumio-collector
fi

if [[ -f "${SERVICE_FILE}" ]]; then
  echo "==> Removing systemd unit"
  rm -f "${SERVICE_FILE}"
  systemctl daemon-reload
fi

# --- remove application files ---
if [[ -d "${INSTALL_DIR}" ]]; then
  echo "==> Removing ${INSTALL_DIR}"
  rm -rf "${INSTALL_DIR}"
fi

# --- remove service user ---
if id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  echo "==> Removing service user ${SERVICE_USER}"
  userdel "${SERVICE_USER}"
fi

# --- purge config and data (only with --purge) ---
if [[ "${PURGE}" == "true" ]]; then
  echo "==> Purging config: ${CONFIG_DIR}"
  rm -rf "${CONFIG_DIR}"
  echo "==> Purging state and logs: ${STATE_DIR}  ${LOG_DIR}"
  rm -rf "${STATE_DIR}" "${LOG_DIR}"
  echo "==> Done (purged)."
else
  cat <<'ENDMSG'
==> Done.

Config and state are preserved:
  /etc/illumio-collector/config.yaml  — edit before next install
  /var/lib/illumio-collector/state/   — checkpoint files (resume position)
  /var/log/illumio-collector/         — log files

To also remove these, re-run with --purge:
  sudo bash scripts/uninstall.sh --purge
ENDMSG
fi
