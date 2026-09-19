import subprocess
import sys


def test_import_without_home_directory() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from unittest.mock import patch; "
            "patcher = patch('pathlib.Path.home', "
            "side_effect=RuntimeError('no home')); "
            "patcher.start(); import chromadb",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
