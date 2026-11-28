import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import chromadb


def _run_in_subprocess(
    code: str, home: Optional[str]
) -> subprocess.CompletedProcess[str]:
    """Run code in a fresh interpreter, with $HOME unset or set to home."""
    repo_root = Path(chromadb.__file__).resolve().parent.parent
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(repo_root), env.get("PYTHONPATH", "")])
    if home is None:
        env.pop("HOME", None)
    else:
        env["HOME"] = home
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )


# No $HOME, and no passwd entry for the uid: what systemd DynamicUser
# and distroless containers produce. Stubbing expanduser reproduces the
# missing passwd entry without needing such an environment.
_NO_HOME_PREAMBLE = (
    "import os; os.environ.pop('HOME', None); os.path.expanduser = lambda p: p; "
)


def test_import_without_home_directory() -> None:
    """import chromadb must not resolve the home directory.

    Home-based paths are only needed when the ONNX embedding function
    or the telemetry client actually use them, so importing must succeed
    even where Path.home() raises.
    """
    result = _run_in_subprocess(_NO_HOME_PREAMBLE + "import chromadb", home=None)
    assert result.returncode == 0, result.stderr


def test_home_based_paths_resolve_on_first_use(tmp_path: Path) -> None:
    """The lazily resolved paths are unchanged once $HOME is available."""
    code = (
        "from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2;"
        "from chromadb.telemetry.product import ProductTelemetryClient;"
        "print(ONNXMiniLM_L6_V2.DOWNLOAD_PATH);"
        "print(ProductTelemetryClient.USER_ID_PATH)"
    )
    result = _run_in_subprocess(code, home=str(tmp_path))
    assert result.returncode == 0, result.stderr
    lines = result.stdout.strip().splitlines()
    assert lines[0] == str(
        tmp_path / ".cache" / "chroma" / "onnx_models" / "all-MiniLM-L6-v2"
    )
    assert lines[1] == str(tmp_path / ".cache" / "chroma" / "telemetry_user_id")


def test_home_based_path_access_still_fails_without_home() -> None:
    """Deferring the lookup moves the failure to first use, not import."""
    code = (
        _NO_HOME_PREAMBLE
        + "from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2;"
        "ONNXMiniLM_L6_V2.DOWNLOAD_PATH"
    )
    result = _run_in_subprocess(code, home=None)
    assert result.returncode != 0
    assert "Could not determine home directory" in result.stderr
