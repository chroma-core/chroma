import sys
import os
import shutil
import subprocess
import tempfile
from typing import Generator, Tuple
from hypothesis import given
import pytest
import json
from urllib import request
import importlib
from chromadb.config import Settings
from chromadb.test.configurations import (
    persist_old_version_configurations,
)
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from importlib.util import spec_from_file_location, module_from_spec
from packaging import version as packaging_version
import re

version_re = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


def versions():
    """Returns the pinned minimum version and the latest version of chromadb."""
    url = "https://pypi.org/pypi/chromadb/json"
    data = json.load(request.urlopen(request.Request(url)))
    versions = list(data["releases"].keys())
    # Older versions on pypi contain "devXYZ" suffixes
    versions = [v for v in versions if version_re.match(v)]
    versions.sort(key=packaging_version.Version)
    return ["0.3.21", versions[-1]]


test_old_versions = versions()
base_install_dir = tempfile.gettempdir() + "/persistence_test_chromadb_versions"


def get_path_to_version_install(version):
    return base_install_dir + "/" + version


def get_path_to_version_library(version):
    return get_path_to_version_install(version) + "/chromadb/__init__.py"


def install_version(version):
    # Check if already installed
    version_library = get_path_to_version_library(version)
    if os.path.exists(version_library):
        return
    path = get_path_to_version_install(version)
    install(f"chromadb=={version}", path)


def install(pkg, path):
    # -q -q to suppress pip output to ERROR level
    # https://pip.pypa.io/en/stable/cli/pip/#quiet
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
            "--target={}".format(path),
        ]
    )


def switch_to_version(version):
    module_name = "chromadb"
    # Remove old version from sys.modules, except test modules
    old_modules = {
        n: m
        for n, m in sys.modules.items()
        if n == module_name
        or (n.startswith(module_name + ".") and not n.startswith(module_name + ".test"))
    }
    for n in old_modules:
        del sys.modules[n]

    # Load the target version
    new_module = None
    if version == "current":
        # Will import from current working directory
        new_module = importlib.import_module(module_name)
    else:
        path = get_path_to_version_library(version)
        spec = spec_from_file_location(module_name, path)
        assert spec is not None and spec.loader is not None
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        new_module = module
        assert new_module.__version__ == version

    sys.modules[module_name] = new_module
    return new_module


@pytest.fixture(
    scope="module", params=persist_old_version_configurations(test_old_versions)
)
def version_settings(request) -> Generator[Tuple[str, Settings], None, None]:
    configuration = request.param
    version = configuration[0]
    install_version(version)
    yield configuration
    # Cleanup the installed version
    path = get_path_to_version_install(version)
    shutil.rmtree(path)
    # TODO: Once we share the api fixtures between tests, we can move this cleanup to
    # the shared fixture
    # Cleanup the persisted data
    data_path = configuration[1].persist_directory
    if os.path.exists(data_path):
        shutil.rmtree(data_path)


@given(
    collection_strategy=strategies.collections(),
    embeddings_strategy=strategies.embedding_set(),
)
def test_cycle_versions(
    version_settings: Settings,
    collection_strategy: strategies.Collection,
    embeddings_strategy: strategies.EmbeddingSet,
):
    # Test backwards compatibility
    # For the current version, ensure that we can load a collection from
    # the previous versions
    version, settings = version_settings

    # Add data with an old version + check the invariants are preserved in that version
    if version == "0.3.21":
        # Old versions do not support upper case collection names
        collection_strategy["name"] = collection_strategy["name"].lower()
    old_module = switch_to_version(version)
    api = old_module.Client(settings)
    api.reset()
    coll = api.create_collection(
        **collection_strategy, embedding_function=lambda x: None
    )
    coll.add(**embeddings_strategy)

    invariants.count(
        api,
        coll.name,
        len(embeddings_strategy["ids"]),
    )
    invariants.metadatas_match(coll, embeddings_strategy)
    invariants.documents_match(coll, embeddings_strategy)
    invariants.ids_match(coll, embeddings_strategy)
    invariants.ann_accuracy(coll, embeddings_strategy)

    api.persist()
    del api

    # Switch to the current version (local working directory) and check the invariants
    # are preserved for the collection
    current_module = switch_to_version("current")
    api = current_module.Client(settings)
    coll = api.get_collection(
        name=collection_strategy["name"], embedding_function=lambda x: None
    )
    invariants.count(
        api,
        coll.name,
        len(embeddings_strategy["ids"]),
    )
    invariants.metadatas_match(coll, embeddings_strategy)
    invariants.documents_match(coll, embeddings_strategy)
    invariants.ids_match(coll, embeddings_strategy)
    invariants.ann_accuracy(coll, embeddings_strategy)
    del api
