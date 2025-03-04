import ctypes
import os
import platform
import subprocess


def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


def windows_setup():
    if is_admin():
        expected_install_dir = os.path.join(os.environ.get("ProgramFiles", "C:\\Program Files"), "Chroma")
    else:
        expected_install_dir = os.path.join(os.environ["USERPROFILE"], "bin")
    os.environ["PATH"] = expected_install_dir + ";" + os.environ["PATH"]


def test_cli_wrapper():
    if platform.system() == "Windows":
        windows_setup()

    result = subprocess.run(
        ["python", "-m", "chromadb.cli.cli", "--help"],
        capture_output=True,
        text=True,
        check=True
    )
    help_output = result.stdout
    print("Help output:", help_output)

    assert "run" in help_output, "'run' not found in help output"
