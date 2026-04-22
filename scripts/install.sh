#!/usr/bin/env bash
# Install the Illumio S3 -> SIEM Collector.
#
# Supports two modes (auto-detected):
#   bundle   — run from inside an extracted offline bundle
#   gitclone — run from the repository root's scripts/ directory
#
# Options:
#   --user <name>   Run the service as this existing user.
#                   Default: creates and uses a dedicated 'illumio-collector'
#                   system account (recommended for production).
#                   Use --user root or --user $(logname) for quick testing.
#
# Usage:
#   Bundle:    sudo ./install.sh [--user <name>]
#   Git clone: sudo bash scripts/install.sh [--user <name>]
set -euo pipefail

INSTALL_DIR="/opt/illumio-collector"
CONFIG_DIR="/etc/illumio-collector"
STATE_DIR="/var/lib/illumio-collector/state"
LOG_DIR="/var/log/illumio-collector"
SERVICE_FILE="/etc/systemd/system/illumio-collector.service"

# Default service user — overridable with --user
SERVICE_USER="illumio-collector"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

# ---------- parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --user) SERVICE_USER="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------- detect mode ----------
if [[ -f "${SCRIPT_DIR}/python-runtime.tar.gz" ]]; then
  MODE="bundle"
  BUNDLE_DIR="${SCRIPT_DIR}"
else
  MODE="gitclone"
  REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

echo "==> Mode: ${MODE}  service-user: ${SERVICE_USER}"

# ---------- copy application code ----------
mkdir -p "${INSTALL_DIR}/app"

if [[ "${MODE}" == "bundle" ]]; then
  cp -r "${BUNDLE_DIR}/app/." "${INSTALL_DIR}/app/"
  cp -r "${BUNDLE_DIR}/wheels" "${INSTALL_DIR}/"
  [[ -f "${BUNDLE_DIR}/VERSION" ]] && cp "${BUNDLE_DIR}/VERSION" "${INSTALL_DIR}/"
  # Make uninstall.sh available after install
  [[ -f "${BUNDLE_DIR}/uninstall.sh" ]] && \
    install -m 0755 "${BUNDLE_DIR}/uninstall.sh" "${INSTALL_DIR}/uninstall.sh"
else
  for item in collector.py s3_log_checker.py core sources mappers sinks mappings siem_parser requirements.txt config.example.yaml; do
    cp -r "${REPO_ROOT}/${item}" "${INSTALL_DIR}/app/"
  done
  [[ -f "${REPO_ROOT}/scripts/uninstall.sh" ]] && \
    install -m 0755 "${REPO_ROOT}/scripts/uninstall.sh" "${INSTALL_DIR}/uninstall.sh"
  [[ -f "${REPO_ROOT}/scripts/preflight.sh" ]] && \
    install -m 0755 "${REPO_ROOT}/scripts/preflight.sh" "${INSTALL_DIR}/preflight.sh"
fi

# ---------- save install metadata ----------
cat > "${INSTALL_DIR}/INSTALL_META" <<EOF
service_user=${SERVICE_USER}
install_mode=${MODE}
installed=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

# ---------- Python runtime + dependencies ----------
if [[ "${MODE}" == "bundle" ]]; then
  if [[ ! -d "${INSTALL_DIR}/python" ]]; then
    echo "==> Extract portable Python runtime"
    tar -xzf "${BUNDLE_DIR}/python-runtime.tar.gz" -C "${INSTALL_DIR}"
  fi
  PYTHON="${INSTALL_DIR}/python/bin/python3"
  echo "==> Install wheels (offline)"
  "${PYTHON}" -m pip install \
    --no-index \
    --find-links="${INSTALL_DIR}/wheels" \
    -r "${INSTALL_DIR}/app/requirements.txt"
else
  command -v python3 >/dev/null 2>&1 || { echo "python3 not found — install it first." >&2; exit 1; }
  if [[ ! -d "${INSTALL_DIR}/venv" ]]; then
    echo "==> Create Python venv"
    python3 -m venv "${INSTALL_DIR}/venv"
  fi
  PYTHON="${INSTALL_DIR}/venv/bin/python3"
  echo "==> Install dependencies"
  "${PYTHON}" -m pip install --upgrade pip -q
  "${PYTHON}" -m pip install -r "${INSTALL_DIR}/app/requirements.txt" -q
fi

# ---------- config ----------
mkdir -p "${CONFIG_DIR}"
if [[ ! -f "${CONFIG_DIR}/config.yaml" ]]; then
  # Replace relative log/state paths with absolute system paths so the
  # service can write under ProtectSystem=strict.
  sed \
    -e "s|dir: \./logs\b|dir: ${LOG_DIR}|g" \
    -e "s|dir: logs\b|dir: ${LOG_DIR}|g" \
    -e "s|dir: \./state\b|dir: ${STATE_DIR}|g" \
    -e "s|dir: state\b|dir: ${STATE_DIR}|g" \
    "${INSTALL_DIR}/app/config.example.yaml" > "${CONFIG_DIR}/config.yaml"
  chmod 640 "${CONFIG_DIR}/config.yaml"   # root can write, service user can read
fi

# ---------- service user and state dirs ----------
# Create dedicated system user only if not using an existing account
if [[ "${SERVICE_USER}" == "illumio-collector" ]]; then
  id -u "${SERVICE_USER}" >/dev/null 2>&1 || \
    useradd --system --shell /sbin/nologin --no-create-home "${SERVICE_USER}"
else
  # Verify the specified user exists
  id -u "${SERVICE_USER}" >/dev/null 2>&1 || {
    echo "Error: user '${SERVICE_USER}' does not exist." >&2
    exit 1
  }
fi

mkdir -p "${STATE_DIR}" "${LOG_DIR}"
chown -R "${SERVICE_USER}:" "${STATE_DIR}" "${LOG_DIR}" "${INSTALL_DIR}"
# Config dir: root owns (only root can edit), service user group-reads
chmod 750 "${CONFIG_DIR}"
chown root:"${SERVICE_USER}" "${CONFIG_DIR}"
chmod 640 "${CONFIG_DIR}/config.yaml"
chown root:"${SERVICE_USER}" "${CONFIG_DIR}/config.yaml"

# ---------- systemd unit ----------
echo "==> Install systemd unit"
# Determine Group — use same as User (works for both system and regular users)
SVC_GROUP="$(id -gn "${SERVICE_USER}")"

cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Illumio S3 to SIEM Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SVC_GROUP}
WorkingDirectory=${INSTALL_DIR}/app
Environment=PYTHONUNBUFFERED=1
ExecStart=${PYTHON} ${INSTALL_DIR}/app/collector.py --config ${CONFIG_DIR}/config.yaml
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=${STATE_DIR%/state} ${LOG_DIR}
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF
chmod 0644 "${SERVICE_FILE}"
systemctl daemon-reload
systemctl enable illumio-collector

cat <<ENDMSG

============================================================
Install complete.  (service user: ${SERVICE_USER})

 Uninstall:   sudo ${INSTALL_DIR}/uninstall.sh
              sudo ${INSTALL_DIR}/uninstall.sh --purge

 1. Edit the config:   sudo vi ${CONFIG_DIR}/config.yaml
 2. Start the service: sudo systemctl start illumio-collector
 3. Watch the logs:    sudo journalctl -u illumio-collector -f
============================================================
ENDMSG
