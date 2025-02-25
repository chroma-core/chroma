#!/usr/bin/env python3
import os
import sys
import subprocess
import platform
import urllib.request

def find_chroma_binary():
    """
    Look for the chroma binary in the default install locations.
    These are the only locations considered valid by the installer.
    """
    default_paths = [
        "/usr/local/bin/chroma",
        os.path.expanduser("~/.local/bin/chroma")
    ]
    for path in default_paths:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return None

def install_chroma_unix():
    """Download and install chroma using the official installer script on Unix-like systems."""
    install_script_url = "https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh"
    print("Chroma CLI not found. Installing using the official installer script…")
    try:
        # Run the installer script by piping it to bash.
        subprocess.run(f"curl -sSL {install_script_url} | bash", shell=True, check=True)
    except subprocess.CalledProcessError:
        sys.exit("Failed to install chroma CLI tool on Unix.")

def install_chroma_windows():
    """Download the Windows binary of chroma and install it locally."""
    release = "cli-0.1.0"
    repo = "chroma-core/chroma"
    asset = "chroma-windows.exe"
    download_url = f"https://github.com/{repo}/releases/download/{release}/{asset}"
    print(f"Chroma CLI not found. Downloading Windows binary from {download_url}…")

    # For example, install to %LOCALAPPDATA%\chroma\
    local_dir = os.path.join(os.getenv("LOCALAPPDATA", os.getcwd()), "chroma")
    os.makedirs(local_dir, exist_ok=True)
    binary_path = os.path.join(local_dir, "chroma.exe")

    try:
        urllib.request.urlretrieve(download_url, binary_path)
    except Exception as e:
        sys.exit(f"Failed to download chroma CLI: {e}")

    os.chmod(binary_path, 0o755)
    print(f"Installed chroma CLI to {binary_path}.")
    return binary_path

def ensure_chroma_installed():
    """
    Check if the chroma binary exists in the default installation locations.
    If not, run the installer script.
    """
    chroma_path = find_chroma_binary()
    if chroma_path:
        return chroma_path

    if platform.system() == "Windows":
        chroma_path = install_chroma_windows()
    else:
        install_chroma_unix()
        chroma_path = find_chroma_binary()
        if not chroma_path:
            sys.exit("Installation failed: 'chroma' not found in default locations after installation.")
    return chroma_path

def main():
    # Get any arguments passed to the Python proxy.
    args = sys.argv[1:]
    # Ensure the real CLI is installed, installing if necessary.
    chroma_path = ensure_chroma_installed()
    # Forward the command-line arguments to the real chroma CLI.
    try:
        result = subprocess.run([chroma_path] + args)
        sys.exit(result.returncode)
    except Exception as e:
        sys.exit(f"Error executing chroma CLI: {e}")

if __name__ == "__main__":
    main()
