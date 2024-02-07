import os

import pytest
from typer.testing import CliRunner

from chromadb.api.client import SharedSystemClient
from chromadb.cli.cli import app
from chromadb.cli.utils import set_log_file_path

runner = CliRunner()


@pytest.fixture(autouse=True)
def reset_client_settings() -> None:
    SharedSystemClient._identifer_to_system = {}


def test_app() -> None:
    result = runner.invoke(
        app,
        [
            "run",
            "--path",
            "chroma_test_data",
            "--port",
            "8001",
            "--test",
        ],
    )
    assert "chroma_test_data" in result.stdout
    assert "8001" in result.stdout


def test_system_info() -> None:
    result = runner.invoke(
        app,
        [
            "env",
        ],
    )
    assert "chroma_version" in result.stdout
    assert "python_version" in result.stdout
    assert "datetime" in result.stdout


def test_system_info_with_remote() -> None:
    if "CHROMA_INTEGRATION_TEST" not in os.environ:
        pytest.skip("Remote server not running")
    result = runner.invoke(
        app,
        [
            "env",
            "--remote",
            f"http://localhost:{os.environ.get('CHROMA_SERVER_HTTP_PORT', 8000)}",
        ],
        env={
            "CHROMA_SERVER_HOST": f"http://localhost:{os.environ.get('CHROMA_SERVER_HTTP_PORT', 8000)}"
        },
    )
    assert "chroma_version" in result.stdout
    assert "python_version" in result.stdout
    assert "datetime" in result.stdout
def test_utils_set_log_file_path() -> None:
    log_config = set_log_file_path("chromadb/log_config.yml", "test.log")
    assert log_config["handlers"]["file"]["filename"] == "test.log"
