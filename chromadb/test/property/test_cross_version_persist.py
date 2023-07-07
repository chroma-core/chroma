from multiprocessing.connection import Connection
import sys
import os
import shutil
import subprocess
import tempfile
from types import ModuleType
from typing import Callable, Generator, List, Tuple
from hypothesis import given, settings
import hypothesis.strategies as st
import pytest
import json
from urllib import request
from chromadb.api import API
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from packaging import version as packaging_version
import re
import multiprocessing
from chromadb import Client
from chromadb.config import Settings

MINIMUM_VERSION = "0.3.20"
COLLECTION_NAME_LOWERCASE_VERSION = "0.3.21"
version_re = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


def _patch_uppercase_coll_name(
    collection: strategies.Collection, embeddings: strategies.RecordSet
) -> None:
    """Old versions didn't handle uppercase characters in collection names"""
    collection.name = collection.name.lower()


def _patch_empty_dict_metadata(
    collection: strategies.Collection, embeddings: strategies.RecordSet
) -> None:
    """Old versions do the wrong thing when metadata is a single empty dict"""
    if embeddings["metadatas"] == {}:
        embeddings["metadatas"] = None


version_patches: List[
    Tuple[str, Callable[[strategies.Collection, strategies.RecordSet], None]]
] = [
    ("0.3.21", _patch_uppercase_coll_name),
    ("0.3.21", _patch_empty_dict_metadata),
]


def patch_for_version(
    version: str, collection: strategies.Collection, embeddings: strategies.RecordSet
) -> None:
    """Override aspects of the collection and embeddings, before testing, to account for
    breaking changes in old versions."""

    for patch_version, patch in version_patches:
        if packaging_version.Version(version) <= packaging_version.Version(
            patch_version
        ):
            patch(collection, embeddings)


def versions() -> List[str]:
    """Returns the pinned minimum version and the latest version of chromadb."""
    url = "https://pypi.org/pypi/chromadb/json"
    data = json.load(request.urlopen(request.Request(url)))
    versions = list(data["releases"].keys())
    # Older versions on pypi contain "devXYZ" suffixes
    versions = [v for v in versions if version_re.match(v)]
    versions.sort(key=packaging_version.Version)
    return [MINIMUM_VERSION, versions[-1]]


def configurations(versions: List[str]) -> List[Tuple[str, Settings]]:
    return [
        (
            version,
            Settings(
                chroma_api_impl="local",
                chroma_db_impl="duckdb+parquet",
                persist_directory=tempfile.gettempdir() + "/tests/" + version + "/",
            ),
        )
        for version in versions
    ]


test_old_versions = versions()
base_install_dir = tempfile.gettempdir() + "/persistence_test_chromadb_versions"


# This fixture is not shared with the rest of the tests because it is unique in how it
# installs the versions of chromadb
@pytest.fixture(scope="module", params=configurations(test_old_versions))  # type: ignore
def version_settings(request) -> Generator[Tuple[str, Settings], None, None]:
    configuration = request.param
    version = configuration[0]
    install_version(version)
    yield configuration
    # Cleanup the installed version
    path = get_path_to_version_install(version)
    shutil.rmtree(path)
    # Cleanup the persisted data
    data_path = configuration[1].persist_directory
    if os.path.exists(data_path):
        shutil.rmtree(data_path)


def get_path_to_version_install(version: str) -> str:
    return base_install_dir + "/" + version


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


def switch_to_version(version: str) -> ModuleType:
    module_name = "chromadb"
    # Remove old version from sys.modules, except test modules
    old_modules = {
        n: m
        for n, m in sys.modules.items()
        if n == module_name or (n.startswith(module_name + "."))
    }
    for n in old_modules:
        del sys.modules[n]

    # Load the target version and override the path to the installed version
    # https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
    sys.path.insert(0, get_path_to_version_install(version))
    import chromadb

    assert chromadb.__version__ == version
    return chromadb


class not_implemented_ef(EmbeddingFunction):
    def __call__(self, texts: Documents) -> Embeddings:
        assert False, "Embedding function should not be called"


def persist_generated_data_with_old_version(
    version: str,
    settings: Settings,
    collection_strategy: strategies.Collection,
    embeddings_strategy: strategies.RecordSet,
    conn: Connection,
) -> None:
    try:
        old_module = switch_to_version(version)
        api: API = old_module.Client(settings)
        api.reset()
        coll = api.create_collection(
            name=collection_strategy.name,
            metadata=collection_strategy.metadata,
            # In order to test old versions, we can't rely on the not_implemented function
            embedding_function=not_implemented_ef(),
        )
        coll.add(**embeddings_strategy)
        # We can't use the invariants module here because it uses the current version
        # Just use some basic checks for sanity and manual testing where you break the new
        # version

        check_embeddings = invariants.wrap_all(embeddings_strategy)
        # Check count
        assert coll.count() == len(check_embeddings["embeddings"] or [])
        # Check ids
        result = coll.get()
        actual_ids = result["ids"]
        embedding_id_to_index = {id: i for i, id in enumerate(check_embeddings["ids"])}
        actual_ids = sorted(actual_ids, key=lambda id: embedding_id_to_index[id])
        assert actual_ids == check_embeddings["ids"]
        api.persist()
    except Exception as e:
        conn.send(e)
        raise e


# Since we can't pickle the embedding function, we always generate record sets with embeddings
collection_st: st.SearchStrategy[strategies.Collection] = st.shared(
    strategies.collections(with_hnsw_params=True, has_embeddings=True), key="coll"
)


@given(
    collection_strategy=collection_st,
    embeddings_strategy=strategies.recordsets(collection_st),
)
@pytest.mark.skipif(
    sys.version_info.major < 3
    or (sys.version_info.major == 3 and sys.version_info.minor <= 7),
    reason="The mininum supported versions of chroma do not work with python <= 3.7",
)
@pytest.mark.xfail(
    reason="As we migrate to sqlite, we will not support old versions of chromadb and instead require manual migration. The minimum version will be increased to 0.4.0 and this test will be expected to pass."
)
@settings(deadline=None)
def test_cycle_versions(
    version_settings: Tuple[str, Settings],
    collection_strategy: strategies.Collection,
    embeddings_strategy: strategies.RecordSet,
) -> None:
    # # Test backwards compatibility
    # # For the current version, ensure that we can load a collection from
    # # the previous versions
    version, settings = version_settings

    patch_for_version(version, collection_strategy, embeddings_strategy)

    # Can't pickle a function, and we won't need them
    collection_strategy.embedding_function = None
    collection_strategy.known_metadata_keys = {}

    # Run the task in a separate process to avoid polluting the current process
    # with the old version. Using spawn instead of fork to avoid sharing the
    # current process memory which would cause the old version to be loaded
    ctx = multiprocessing.get_context("spawn")
    conn1, conn2 = multiprocessing.Pipe()
    p = ctx.Process(
        target=persist_generated_data_with_old_version,
        args=(version, settings, collection_strategy, embeddings_strategy, conn2),
    )
    p.start()
    p.join()

    if conn1.poll():
        e = conn1.recv()
        raise e

    # Switch to the current version (local working directory) and check the invariants
    # are preserved for the collection
    api = Client(settings)
    coll = api.get_collection(
        name=collection_strategy.name,
        embedding_function=not_implemented_ef(),
    )
    invariants.count(coll, embeddings_strategy)
    invariants.metadatas_match(coll, embeddings_strategy)
    invariants.documents_match(coll, embeddings_strategy)
    invariants.ids_match(coll, embeddings_strategy)
    invariants.ann_accuracy(coll, embeddings_strategy)
