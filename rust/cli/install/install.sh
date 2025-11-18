#!/usr/bin/env bash
set -e

# ----------------------------------------------
# Chroma CLI Installer Script
# Usage:
#   curl -sSL https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh | bash
# ----------------------------------------------
REPO="chroma-core/chroma"
RELEASE="cli-1.2.2"

OS=$(uname -s)
ARCH=$(uname -m)
ASSET=""

case "$OS" in
  Linux*)
    ASSET="chroma-linux"
    ;;
  Darwin*)
    if [ "$ARCH" = "arm64" ]; then
      ASSET="chroma-macos-arm64"
    else
      ASSET="chroma-macos-intel"
    fi
    ;;
  MINGW*|MSYS*|CYGWIN*)
    ASSET="chroma-windows.exe"
    ;;
  *)
    echo "Unsupported OS: $OS"
    exit 1
    ;;
esac

DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${RELEASE}/${ASSET}"
echo "Downloading ${ASSET} from ${DOWNLOAD_URL}..."
curl -L "$DOWNLOAD_URL" -o chroma

chmod +x chroma

if [ -w /usr/local/bin ]; then
  sudo mv chroma /usr/local/bin/chroma
else
  mkdir -p "$HOME/.local/bin"
  mv chroma "$HOME/.local/bin/chroma"

  case ":$PATH:" in
    *:"$HOME/.local/bin":*)
      ;;
    *)
      echo "====================="
      echo "Warning ⚠️: $HOME/.local/bin is not in your PATH."
      echo "To add it, you can run:"
      echo '  export PATH="$HOME/.local/bin:$PATH"'
      echo "====================="
      ;;
  esac
fi
