#!/usr/bin/env bash
# Preflight check — verify config and S3 connectivity before installing.
#
# Supports two modes (auto-detected):
#   bundle   — run from inside an extracted offline bundle
#   gitclone — run from inside the repository (scripts/ directory)
#
# Nothing is written to /opt, /etc, or systemd — safe to run as any user.
#
# Usage:
#   bash preflight.sh [--config /path/to/config.yaml] [--test-s3]
#   (bundle mode: --config is optional, defaults to app/config.example.yaml)
#
# Options:
#   --config <path>   Path to config.yaml (optional; auto-detected if omitted)
#   --test-s3         Also verify S3 connectivity using credentials in config
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH=""
TEST_S3=false

# ---------- parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)  CONFIG_PATH="$2"; shift 2 ;;
    --test-s3) TEST_S3=true; shift ;;
    -h|--help)
      sed -n '2,14p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ---------- detect mode ----------
if [[ -f "${SCRIPT_DIR}/python-runtime.tar.gz" ]]; then
  MODE="bundle"
  BUNDLE_DIR="${SCRIPT_DIR}"
  APP_DIR="${BUNDLE_DIR}/app"
else
  MODE="gitclone"
  REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
  APP_DIR="${REPO_ROOT}"
fi

echo "==> Mode: ${MODE}"

# ---------- resolve config ----------
if [[ -z "${CONFIG_PATH}" ]]; then
  if [[ "${MODE}" == "bundle" ]]; then
    CONFIG_PATH="${APP_DIR}/config.example.yaml"
  else
    if   [[ -f "${REPO_ROOT}/config.yaml"         ]]; then CONFIG_PATH="${REPO_ROOT}/config.yaml"
    elif [[ -f "${REPO_ROOT}/config.example.yaml" ]]; then CONFIG_PATH="${REPO_ROOT}/config.example.yaml"
    fi
  fi
  [[ -n "${CONFIG_PATH}" ]] && echo "==> --config not specified; using ${CONFIG_PATH}"
fi
if [[ -z "${CONFIG_PATH}" || ! -f "${CONFIG_PATH}" ]]; then
  echo "Error: config not found: '${CONFIG_PATH}'. Use --config <path>." >&2
  exit 1
fi

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "${TMPDIR}"; }
trap cleanup EXIT

# ---------- Python setup ----------
if [[ "${MODE}" == "bundle" ]]; then
  echo "==> Extracting Python runtime (temp)"
  tar -xzf "${BUNDLE_DIR}/python-runtime.tar.gz" -C "${TMPDIR}" 2>/dev/null
  PYTHON="${TMPDIR}/python/bin/python3"
  echo "==> Installing wheels (offline)"
  "${PYTHON}" -m pip install \
    --no-index \
    --find-links="${BUNDLE_DIR}/wheels" \
    -r "${APP_DIR}/requirements.txt" \
    -q
else
  command -v python3 >/dev/null 2>&1 || { echo "Error: python3 not found." >&2; exit 1; }
  echo "==> Creating temp venv"
  python3 -m venv "${TMPDIR}/venv"
  PYTHON="${TMPDIR}/venv/bin/python3"
  echo "==> Installing dependencies"
  "${PYTHON}" -m pip install --upgrade pip -q
  "${PYTHON}" -m pip install -r "${APP_DIR}/requirements.txt" -q
fi

# ---------- config validation ----------
echo ""
echo "---------- config validation ----------"
"${PYTHON}" "${APP_DIR}/collector.py" --config "${CONFIG_PATH}" --dry-run
echo ""

# ---------- S3 connectivity ----------
if [[ "${TEST_S3}" == "true" ]]; then
  echo "---------- S3 connectivity test ----------"
  EXTRACT_SCRIPT=$(cat <<'PYEOF'
import sys, yaml
with open(sys.argv[1]) as f:
    cfg = yaml.safe_load(f)
aws = cfg.get("aws", {})
src = cfg.get("source", {})
print(aws.get("access_key",""))
print(aws.get("secret_key",""))
print(src.get("bucket",""))
print(src.get("fqdn",""))
print(str(src.get("org_id","")))
print(aws.get("region","") or "")
PYEOF
)
  read -r AK SK BUCKET FQDN ORG_ID REGION \
    < <("${PYTHON}" -c "${EXTRACT_SCRIPT}" "${CONFIG_PATH}")

  CHECKER_ARGS=(
    "--bucket"     "${BUCKET}"
    "--fqdn"       "${FQDN}"
    "--org-id"     "${ORG_ID}"
    "--access-key" "${AK}"
    "--secret-key" "${SK}"
  )
  [[ -n "${REGION}" ]] && CHECKER_ARGS+=("--region" "${REGION}")

  "${PYTHON}" "${APP_DIR}/s3_log_checker.py" "${CHECKER_ARGS[@]}"
  echo ""
fi

# ---------- result ----------
if [[ "${MODE}" == "bundle" ]]; then
  NEXT_STEP="sudo ./install.sh"
else
  NEXT_STEP="sudo bash scripts/install.sh"
fi

echo "=========================================="
echo "PASS — config and dependencies look good."
echo "You can now run:  ${NEXT_STEP}"
echo "=========================================="
