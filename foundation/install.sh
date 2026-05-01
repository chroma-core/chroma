#!/usr/bin/env bash
set -eo pipefail

# ----------------------------------------------
# Foundation CLI Installer Script
# Usage:
#   curl -sSL https://raw.githubusercontent.com/chroma-core/chroma/main/rust/foundation/install.sh | bash
# ----------------------------------------------

GITHUB_REPO="chroma-core/chroma"
RELEASE_PREFIX="foundation-cli"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Print error and exit
error() {
    echo -e "${RED}Error: $1${NC}" >&2
    exit 1
}

# Detect the operating system and architecture
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    # Normalize architecture names
    case "${ARCH}" in
        x86_64)  ARCH="amd64" ;;
        aarch64) ARCH="arm64" ;;
        arm64)   ARCH="arm64" ;;
        *) error "Unsupported architecture: ${ARCH}" ;;
    esac

    # M0: macOS only
    if [ "${OS}" != "darwin" ]; then
        error "Unsupported OS: ${OS}. Pre-built binaries for Linux/Windows are coming in M0+.\nDownload manually from: https://github.com/${GITHUB_REPO}/releases"
    fi
}

# Check required dependencies
check_dependencies() {
    # curl is required and is always present on macOS
    if ! command -v curl >/dev/null 2>&1; then
        error "curl is required but not installed."
    fi

    # jq is required for JSON parsing
    if ! command -v jq >/dev/null 2>&1; then
        echo "jq is required but not installed."
        if command -v brew >/dev/null 2>&1; then
            echo "Installing jq via Homebrew..."
            brew install jq || error "Failed to install jq via Homebrew. Please install it manually and re-run.\n  macOS: brew install jq"
            echo "✅ jq installed successfully."
        else
            error "Please install jq and try again.\n  macOS: brew install jq"
        fi
    fi
}

# Get the latest release tag, preferring stable over pre-release
get_latest_release() {
    RELEASES_URL="https://api.github.com/repos/${GITHUB_REPO}/releases"
    RELEASES=$(curl -sf "${RELEASES_URL}") || error "Failed to fetch releases from GitHub. Check your network connection."

    # Try stable release first (prerelease == false)
    LATEST=$(echo "${RELEASES}" | jq -r \
        '[.[] | select(.prerelease == false) | select(.tag_name | startswith("'"${RELEASE_PREFIX}"'-"))] | first | .tag_name')

    if [ -n "${LATEST}" ] && [ "${LATEST}" != "null" ]; then
        echo "${LATEST}"
        return
    fi

    # Fall back to pre-release
    LATEST=$(echo "${RELEASES}" | jq -r \
        '[.[] | select(.prerelease == true) | select(.tag_name | startswith("'"${RELEASE_PREFIX}"'-"))] | first | .tag_name')

    if [ -n "${LATEST}" ] && [ "${LATEST}" != "null" ]; then
        echo -e "${YELLOW}Warning: No stable release found. Latest pre-release: ${LATEST}${NC}" >&2
        # Allow non-interactive override via env var (e.g. CI or headless remote installs)
        if [ "${FOUNDATION_INSTALL_PRERELEASE:-}" = "1" ]; then
            echo "FOUNDATION_INSTALL_PRERELEASE=1 set — skipping confirmation." >&2
            echo "${LATEST}"
            return
        fi
        # Fall back gracefully if /dev/tty is unavailable (non-interactive environment)
        if ! read -r -p "This is a pre-release. Continue? [y/N] " CONFIRM </dev/tty 2>/dev/null; then
            error "No stable release available and running non-interactively.\nSet FOUNDATION_INSTALL_PRERELEASE=1 to install the pre-release without prompting."
        fi
        if [[ "${CONFIRM}" =~ ^[Yy]$ ]]; then
            echo "${LATEST}"
            return
        else
            error "Installation cancelled."
        fi
    fi

    error "No foundation-cli releases found.\nCheck: https://github.com/${GITHUB_REPO}/releases"
}

# Download and install the binary
install_binary() {
    TAG="${1}"
    # Strip the prefix to get the version number: foundation-cli-v0.1.0 -> 0.1.0
    VERSION_NUM="${TAG#${RELEASE_PREFIX}-v}"
    ASSET_NAME="${RELEASE_PREFIX}-v${VERSION_NUM}_${OS}_${ARCH}.tar.gz"
    DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/download/${TAG}/${ASSET_NAME}"

    echo "Downloading ${ASSET_NAME}..."

    TMP_DIR=$(mktemp -d)
    trap "rm -rf '${TMP_DIR}'" EXIT

    if ! curl -sL --fail "${DOWNLOAD_URL}" | tar xz -C "${TMP_DIR}"; then
        error "Failed to download or extract ${DOWNLOAD_URL}\nCheck that the release exists: https://github.com/${GITHUB_REPO}/releases/tag/${TAG}"
    fi

    BINARY_PATH="${TMP_DIR}/foundation"
    if [ ! -f "${BINARY_PATH}" ]; then
        error "Binary 'foundation' not found in archive."
    fi

    chmod +x "${BINARY_PATH}"

    # Install to /usr/local/bin if writable, otherwise ~/.local/bin
    if [ -w "/usr/local/bin" ]; then
        INSTALL_DIR="/usr/local/bin"
    else
        INSTALL_DIR="${HOME}/.local/bin"
        mkdir -p "${INSTALL_DIR}"
    fi

    mv "${BINARY_PATH}" "${INSTALL_DIR}/foundation"

    echo -e "${GREEN}✅ foundation v${VERSION_NUM} installed to ${INSTALL_DIR}/foundation${NC}"

    # Warn if install dir isn't in PATH
    if [[ ":${PATH}:" != *":${INSTALL_DIR}:"* ]]; then
        echo ""
        echo -e "${YELLOW}NOTE: ${INSTALL_DIR} is not in your PATH.${NC}"
        echo "Add this line to your ~/.zshrc or ~/.bashrc:"
        echo "  export PATH=\"\$PATH:${INSTALL_DIR}\""
    fi
}

main() {
    echo "Installing foundation CLI..."
    detect_platform
    check_dependencies
    VERSION=$(get_latest_release)
    install_binary "${VERSION}"
}

main
