#!/usr/bin/env bash
# Build offline install bundle for Linux x86_64.
#
# Usage (on a host WITH internet access):
#   git clone <repo>  &&  cd illumio_s3_collector
#   bash scripts/build_offline_bundle.sh
#   -> dist/illumio-collector-linux-x86_64-v1.0.tar.gz
#
# The bundle is self-contained: it includes Python 3.11 runtime + all wheels.
# The target (offline) host needs only x86_64 CPU + glibc >= 2.17.
# No Python, pip, or internet access required on the target.
#
# To update: git pull && bash scripts/build_offline_bundle.sh
set -euo pipefail

VERSION="${VERSION:-1.0}"
PBS_TAG="${PBS_TAG:-20240415}"
PY_VER="${PY_VER:-3.11.9}"
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/dist}"
PY_RUNTIME_SHA256="${PY_RUNTIME_SHA256:-78b1c16a9fd032997ba92a60f46a64f795cd18ff335659dfdf6096df277b24d5}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$(mktemp -d)"
BUNDLE="${BUILD_DIR}/bundle"

mkdir -p "${BUNDLE}/app" "${BUNDLE}/wheels" "${OUT_DIR}"

echo "==> Downloading python-build-standalone cpython-${PY_VER}+${PBS_TAG}"
curl -fL -o "${BUNDLE}/python-runtime.tar.gz" \
  "https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_TAG}/cpython-${PY_VER}+${PBS_TAG}-x86_64-unknown-linux-gnu-install_only.tar.gz"
echo "==> Verifying python runtime SHA256"
echo "${PY_RUNTIME_SHA256}  ${BUNDLE}/python-runtime.tar.gz" | sha256sum -c -

echo "==> Downloading wheels for manylinux2014_x86_64 / py3.11"
# Use whatever python3 is available — the --python-version flag controls
# which wheels are downloaded, not which Python runs this script.
python3 -m pip download \
  --only-binary=:all: \
  --platform manylinux2014_x86_64 \
  --python-version 3.11 --implementation cp --abi cp311 \
  -d "${BUNDLE}/wheels" \
  -r "${REPO_ROOT}/requirements.txt"

echo "==> Copying application code"
cp -r \
  "${REPO_ROOT}/collector.py" \
  "${REPO_ROOT}/s3_log_checker.py" \
  "${REPO_ROOT}/core" "${REPO_ROOT}/sources" "${REPO_ROOT}/mappers" \
  "${REPO_ROOT}/sinks" "${REPO_ROOT}/mappings" \
  "${REPO_ROOT}/siem_parser" "${REPO_ROOT}/tests" "${REPO_ROOT}/docs" \
  "${REPO_ROOT}/requirements.txt" \
  "${REPO_ROOT}/config.example.yaml" \
  "${REPO_ROOT}/README.md" \
  "${BUNDLE}/app/"

cp "${REPO_ROOT}/scripts/install.sh"    "${BUNDLE}/install.sh"
cp "${REPO_ROOT}/scripts/uninstall.sh"  "${BUNDLE}/uninstall.sh"
cp "${REPO_ROOT}/scripts/preflight.sh"  "${BUNDLE}/preflight.sh"
chmod +x "${BUNDLE}/install.sh" "${BUNDLE}/uninstall.sh" "${BUNDLE}/preflight.sh"

cat > "${BUNDLE}/VERSION" <<EOF
illumio-s3-siem-collector v${VERSION}
built: $(date -u +%Y-%m-%dT%H:%M:%SZ)
host:  $(uname -a)
python: cpython-${PY_VER}+${PBS_TAG} (x86_64 linux gnu)
EOF

TARBALL="${OUT_DIR}/illumio-collector-linux-x86_64-v${VERSION}.tar.gz"
tar -C "${BUILD_DIR}" -czf "${TARBALL}" bundle
(cd "${OUT_DIR}" && sha256sum "$(basename "${TARBALL}")") > "${OUT_DIR}/SHA256SUMS-linux.txt"

echo "==> Done: ${TARBALL}"
rm -rf "${BUILD_DIR}"
