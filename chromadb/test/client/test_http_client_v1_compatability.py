import multiprocessing
import os
import subprocess
import sys
import tempfile
from types import ModuleType
from unittest.mock import patch

from multiprocessing.connection import Connection

from chromadb.config import System
from chromadb.test.conftest import _fastapi_fixture
from chromadb.api import ServerAPI

base_install_dir = tempfile.gettempdir() + "/persistence_test_chromadb_versions"

VERSIONED_MODULES = ["pydantic", "numpy"]


def get_path_to_version_install(version: str) -> str:
    return base_install_dir + "/" + version


def switch_to_version(version: str) -> ModuleType:
    module_name = "chromadb"
    old_modules = {
        n: m
        for n, m in sys.modules.items()
        if n == module_name
        or (n.startswith(module_name + "."))
        or n in VERSIONED_MODULES
        or (any(n.startswith(m + ".") for m in VERSIONED_MODULES))
    }
    for n in old_modules:
        del sys.modules[n]
    # Load the target version and override the path to the installed version
    # https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
    sys.path.insert(0, get_path_to_version_install(version))
    import chromadb

    assert chromadb.__version__ == version
    return chromadb


def get_path_to_version_library(version: str) -> str:
    return get_path_to_version_install(version) + "/chromadb/__init__.py"


def install_version(version: str) -> None:
    # Check if already installed
    version_library = get_path_to_version_library(version)
    if os.path.exists(version_library):
        return
    path = get_path_to_version_install(version)
    install(f"chromadb=={version}", path)


def install(pkg: str, path: str) -> int:
    # -q -q to suppress pip output to ERROR level
    # https://pip.pypa.io/en/stable/cli/pip/#quiet
    print("Purging pip cache")
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "cache",
            "purge",
        ]
    )
    print(f"Installing chromadb version {pkg} to {path}")
    return subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "-q",
            "-q",
            "install",
            pkg,
            "--no-binary=chroma-hnswlib",
            "--target={}".format(path),
        ]
    )


def try_old_client(old_version: str, port: int, conn: Connection) -> None:
    try:
        old_module = switch_to_version(old_version)
        settings = old_module.Settings()
        settings.chroma_server_http_port = port
        with patch("chromadb.api.client.Client._validate_tenant_database"):
            api = old_module.HttpClient(settings=settings, port=port)

        # Try a few operations and ensure they work
        col = api.get_or_create_collection(name="test")
        col.add(
            ids=["1", "2", "3"],
            documents=["test document 1", "test document 2", "test document 3"],
        )
        col.get(ids=["1", "2", "3"])
    except Exception as e:
        conn.send(e)
        raise e


def test_http_client_bw_compatibility() -> None:
    # Start the v2 server
    api_fixture = _fastapi_fixture()
    sys: System = next(api_fixture)
    sys.reset_state()
    api = sys.instance(ServerAPI)
    api.heartbeat()
    port = sys.settings.chroma_server_http_port

    old_version = "0.5.11"  # Module with known v1 client
    install_version(old_version)

    ctx = multiprocessing.get_context("spawn")
    conn1, conn2 = multiprocessing.Pipe()
    p = ctx.Process(
        target=try_old_client,
        args=(old_version, port, conn2),
    )
    p.start()
    p.join()

    if conn1.poll():
        e = conn1.recv()
        raise e

    p.close()
