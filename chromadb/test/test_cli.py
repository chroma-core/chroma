import os
import shutil
import tempfile
from typing import List, Dict, Generator

import pytest
import requests
from hypothesis import given, strategies as st, settings
from typer.testing import CliRunner

from chromadb.api.client import SharedSystemClient
from chromadb.cli.cli import app

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
            "info",
        ],
    )
    assert "chroma_version" in result.stdout
    assert "python_version" in result.stdout
    assert "datetime" in result.stdout


def test_system_info_with_remote() -> None:
    try:
        if (
            requests.get(
                f"http://localhost:{os.environ.get('CHROMA_SERVER_HTTP_PORT', 8000)}/api/v1/heartbeat"
            ).status_code
            != 200
        ):
            pytest.skip("Remote server not running")
    except requests.exceptions.ConnectionError:
        pytest.skip("Remote server not running")
    result = runner.invoke(
        app,
        [
            "env",
            "info",
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


# Example list of dictionaries
dicts_list = [
    {"--python-version": ["python_version"]},
    {"--os-info": ["os", "os_version", "os_release"]},
    {"--memory-info": ["memory_info"]},
    {"--cpu-info": ["cpu_info"]},
    {"--disk-info": ["disk_info"]},
]


@pytest.fixture(scope="module")
def tempdir() -> Generator[str, None, None]:
    tempdir = tempfile.mkdtemp()
    yield tempdir
    shutil.rmtree(tempdir, ignore_errors=True)


@settings(max_examples=50)
@given(
    flags_dict=st.iterables(
        elements=st.sampled_from(dicts_list), min_size=1, max_size=len(dicts_list)
    )
)
def test_system_info_with_flags(
    tempdir: str, flags_dict: List[Dict[str, List[str]]]
) -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping integration tests")
    flags = []
    check_response_flags = []
    for di in flags_dict:
        flags.append(list(di.keys())[0])
        check_response_flags.extend(list(di.values())[0])

    result = runner.invoke(
        app,
        ["env", "info", "--path", f"{tempdir}", *flags],
    )
    for flag in check_response_flags:
        assert flag in result.stdout
