#!/usr/bin/env python3
import os
import sys
import subprocess
import platform

def find_chroma_cli_binary():
    """
    Look for the chroma binary in the default install locations.
    """
    default_paths = []
    if platform.system() == "Windows":
        default_paths.extend([
            os.path.join(os.environ.get("ProgramFiles", "C:\\Program Files"), "Chroma", "chroma.exe"),
            os.path.join(os.environ.get("USERPROFILE", ""), "bin", "chroma.exe")
        ])
    else:
        default_paths = [
            "/usr/local/bin/chroma",
            os.path.expanduser("~/.local/bin/chroma")
        ]

    for path in default_paths:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return None

def install_chroma_cli_unix():
    """Download and install chroma using the official installer script on Unix-like systems."""
    install_script_url = "https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh"
    print("Chroma CLI not found. Installing using the official installer script…")
    try:
        # Run the installer script by piping it to bash.
        subprocess.run(f"curl -sSL {install_script_url} | bash", shell=True, check=True)
    except subprocess.CalledProcessError:
        sys.exit("Failed to install chroma CLI tool on Unix.")

def install_chroma_cli_windows():
    """Download and install chroma using the official PowerShell installer script on Windows."""
    install_script_url = "https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.ps1"
    print("Chroma CLI not found. Installing using the official PowerShell installer script…")
    try:
        subprocess.run([
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-Command", f"iex (New-Object Net.WebClient).DownloadString('{install_script_url}')"
        ], check=True)
    except subprocess.CalledProcessError:
        sys.exit("Failed to install chroma CLI tool on Windows.")

def ensure_chroma_cli_installed():
    """
    Check if the chroma binary exists in the expected installation locations.
    If not, run the appropriate installer script.
    """
    chroma_path = find_chroma_cli_binary()
    if chroma_path:
        return chroma_path

    if platform.system() == "Windows":
        install_chroma_cli_windows()
    else:
        install_chroma_cli_unix()

    chroma_path = find_chroma_cli_binary()
    if not chroma_path:
        sys.exit("Installation failed: 'chroma' not found after installation.")
    return chroma_path

def main():
    args = sys.argv[1:]
    chroma_path = ensure_chroma_cli_installed()
    try:
        result = subprocess.run([chroma_path] + args)
        sys.exit(result.returncode)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        sys.exit(f"Error executing chroma CLI: {e}")

if __name__ == "__main__":
    main()
