#!/bin/sh
# install.sh — Download and install spqr-router.
#
# Usage:
#   curl -fsSL https://pg-sharding.tech/install.sh | sh
#   curl -fsSL https://pg-sharding.tech/install.sh | VERSION=3.2.0 sh
#   curl -fsSL https://pg-sharding.tech/install.sh | INSTALL_DIR=./mydir sh
set -eu

GITHUB_REPO="pg-sharding/spqr"
INSTALL_DIR="${INSTALL_DIR:-./spqr}"
VERSION="${VERSION:-}"
BASE_URL="https://github.com/${GITHUB_REPO}/releases/download"

# --- Helpers ---

log() { printf '[spqr] %s\n' "$*"; }
err() { printf '[spqr] ERROR: %s\n' "$*" >&2; }
die() { err "$@"; exit 1; }

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

# --- Detection ---

detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)       die "unsupported operating system: $(uname -s). SPQR supports Linux and macOS." ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)   echo "amd64" ;;
        aarch64|arm64)  echo "arm64" ;;
        *)              die "unsupported architecture: $(uname -m). SPQR supports amd64 and arm64." ;;
    esac
}

detect_version() {
    if [ -n "${VERSION}" ]; then
        # Strip leading 'v' if provided.
        echo "${VERSION#v}"
        return
    fi

    log "detecting latest version..." >&2
    LATEST=$(curl -fsSL "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" \
        | grep '"tag_name"' \
        | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')

    if [ -z "${LATEST}" ]; then
        die "could not detect latest version. Set VERSION=x.y.z and retry."
    fi
    echo "${LATEST}"
}

# --- Checksum ---

verify_checksum() {
    archive_file="$1"
    checksums_file="$2"
    archive_name="$(basename "${archive_file}")"

    expected=$(grep "${archive_name}" "${checksums_file}" | awk '{print $1}')
    if [ -z "${expected}" ]; then
        die "no checksum found for ${archive_name} in SHA256SUMS."
    fi

    if command -v sha256sum >/dev/null 2>&1; then
        actual=$(sha256sum "${archive_file}" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        actual=$(shasum -a 256 "${archive_file}" | awk '{print $1}')
    else
        die "neither sha256sum nor shasum found — cannot verify checksum."
    fi

    if [ "${expected}" != "${actual}" ]; then
        die "checksum mismatch for ${archive_name}
  expected: ${expected}
  actual:   ${actual}
This could indicate a corrupted download or a tampered file."
    fi
    log "checksum verified."
}

# --- Main ---

main() {
    need_cmd curl
    need_cmd tar
    need_cmd uname

    OS=$(detect_os)
    ARCH=$(detect_arch)
    VER=$(detect_version)

    log "installing spqr-router ${VER} (${OS}/${ARCH})..."

    ARCHIVE_NAME="spqr-router-${VER}-${OS}-${ARCH}.tar.gz"
    DOWNLOAD_URL="${BASE_URL}/${VER}/${ARCHIVE_NAME}"
    CHECKSUMS_URL="${BASE_URL}/${VER}/SHA256SUMS"

    if [ -d "${INSTALL_DIR}" ]; then
        die "destination '${INSTALL_DIR}' already exists. Remove it or set INSTALL_DIR to a different path."
    fi

    TMPDIR=$(mktemp -d)
    trap 'rm -rf "${TMPDIR}"' EXIT

    log "downloading ${ARCHIVE_NAME}..."
    curl -fsSL -o "${TMPDIR}/${ARCHIVE_NAME}" "${DOWNLOAD_URL}" \
        || die "download failed. Check that version '${VER}' exists at:
  https://github.com/${GITHUB_REPO}/releases/tag/${VER}"

    log "downloading checksums..."
    curl -fsSL -o "${TMPDIR}/SHA256SUMS" "${CHECKSUMS_URL}" \
        || die "failed to download SHA256SUMS."

    verify_checksum "${TMPDIR}/${ARCHIVE_NAME}" "${TMPDIR}/SHA256SUMS"

    log "extracting to ${INSTALL_DIR}/..."
    mkdir -p "${INSTALL_DIR}"
    tar xzf "${TMPDIR}/${ARCHIVE_NAME}" -C "${INSTALL_DIR}"

    log ""
    log "installation complete!"
    log ""
    log "  binary:   ${INSTALL_DIR}/spqr-router"
    log "  config:   ${INSTALL_DIR}/examples/router.yaml"
    log "  init sql: ${INSTALL_DIR}/examples/init.sql"
    log ""
    log "quick start:"
    log "  cd ${INSTALL_DIR} && ./spqr-router run --config examples/2shardproxy.yaml"
    log ""
    log "docs: https://docs.pg-sharding.tech/welcome/get_started"
}

main
