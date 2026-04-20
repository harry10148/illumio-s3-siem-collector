#!/usr/bin/env bash
# Preflight check — verify the bundle and config before installing.
#
# Extracts Python to a temp directory, installs wheels, then runs
# config validation and an optional S3 connectivity test.
# Nothing is written to /opt, /etc, or systemd — safe to run as any user.
#
# Usage (from inside extracted bundle directory):
#   bash preflight.sh --config /path/to/config.yaml
#   bash preflight.sh --config config.yaml --test-s3
#
# Options:
#   --config <path>   Path to config.yaml (required)
#   --test-s3         Also verify S3 connectivity using credentials in config
set -euo pipefail

BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH=""
TEST_S3=false

# ---------- parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)   CONFIG_PATH="$2"; shift 2 ;;
    --test-s3)  TEST_S3=true; shift ;;
    -h|--help)
      sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${CONFIG_PATH}" ]]; then
  echo "Error: --config <path> is required." >&2
  echo "Usage: bash preflight.sh --config /path/to/config.yaml [--test-s3]" >&2
  exit 1
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Error: config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi

if [[ ! -f "${BUNDLE_DIR}/python-runtime.tar.gz" ]]; then
  echo "Error: python-runtime.tar.gz not found." >&2
  echo "Run this script from inside the extracted bundle directory." >&2
  exit 1
fi

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf "${TMPDIR}"; }
trap cleanup EXIT

echo "==> Extracting Python runtime (temp)"
tar -xzf "${BUNDLE_DIR}/python-runtime.tar.gz" -C "${TMPDIR}" 2>/dev/null
PYTHON="${TMPDIR}/python/bin/python3"

echo "==> Installing wheels (offline)"
"${PYTHON}" -m pip install \
  --no-index \
  --find-links="${BUNDLE_DIR}/wheels" \
  -r "${BUNDLE_DIR}/app/requirements.txt" \
  -q

echo ""
echo "---------- config validation ----------"
"${PYTHON}" "${BUNDLE_DIR}/app/collector.py" \
  --config "${CONFIG_PATH}" --dry-run
echo ""

if [[ "${TEST_S3}" == "true" ]]; then
  echo "---------- S3 connectivity test ----------"
  # Extract S3 credentials from config using Python (avoids bash YAML parsing)
  EXTRACT_SCRIPT=$(cat <<'PYEOF'
import sys, yaml
with open(sys.argv[1]) as f:
    cfg = yaml.safe_load(f)
aws = cfg.get("aws", {})
src = cfg.get("source", {})
vals = [
    aws.get("access_key",""),
    aws.get("secret_key",""),
    src.get("bucket",""),
    src.get("fqdn",""),
    str(src.get("org_id","")),
    aws.get("region","") or "",
]
print("\n".join(vals))
PYEOF
)
  read -r AK SK BUCKET FQDN ORG_ID REGION \
    < <("${PYTHON}" -c "${EXTRACT_SCRIPT}" "${CONFIG_PATH}")

  CHECKER_ARGS=(
    "--bucket"   "${BUCKET}"
    "--fqdn"     "${FQDN}"
    "--org-id"   "${ORG_ID}"
    "--access-key" "${AK}"
    "--secret-key"  "${SK}"
  )
  [[ -n "${REGION}" ]] && CHECKER_ARGS+=("--region" "${REGION}")

  "${PYTHON}" "${BUNDLE_DIR}/app/s3_log_checker.py" "${CHECKER_ARGS[@]}"
  echo ""
fi

echo "=========================================="
echo "PASS — bundle and config look good."
echo "You can now run:  sudo ./install.sh"
echo "=========================================="
