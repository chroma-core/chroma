import sys
import subprocess
import tempfile
from hypothesis import given
import pytest
import importlib
from chromadb.config import Settings
from chromadb.test.configurations import (
    persist_old_version_configurations,
)
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from importlib.util import spec_from_file_location, module_from_spec

# TODO: fetch this from pypi
test_old_versions = [
    # "0.3.18",  # 0.3.19 was a bad release
    # "0.3.20",
    "0.3.18",
]
base_install_dir = tempfile.gettempdir() + "/persistence_test_chromadb_versions"


def get_path_to_version_install(version):
    return base_install_dir + "/" + version


def get_path_to_version_library(version):
    return get_path_to_version_install(version) + "/chromadb/__init__.py"


@pytest.fixture(scope="module", params=test_old_versions, autouse=True)
def install_old_versions(request):
    version = request.param
    path = get_path_to_version_install(version)
    print(path)
    install(f"chromadb=={version}", path)


def install(pkg, path):
    return subprocess.check_call(
        [sys.executable, "-m", "pip", "install", pkg, "--target={}".format(path)]
    )


def switch_to_version(version):
    module_name = "chromadb"
    if version == "current":
        # Will import from current working directory
        current_module = importlib.import_module(module_name)
        return current_module
    else:
        module_name = f"{module_name}_{version}"

    if module_name in sys.modules:
        del sys.modules[module_name]
    path = get_path_to_version_library(version)
    spec = spec_from_file_location(module_name, path)

    assert spec is not None and spec.loader is not None

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[module_name] = module
    assert module.__version__ == version
    return module


@pytest.fixture(
    scope="module", params=persist_old_version_configurations(test_old_versions)
)
def settings(request):
    configuration = request.param
    return configuration


@given(
    collection_strategy=strategies.collections(),
    embeddings_strategy=strategies.embedding_set(),
)
def test_cycle_versions(
    settings: Settings,
    collection_strategy: strategies.Collection,
    embeddings_strategy: strategies.EmbeddingSet,
):
    # Test backwards compatibility
    # For the current version, ensure that we can load a collection from
    # the previous versions
    print("SWITCH")
    old_module = switch_to_version("0.3.18")
    print(old_module.__version__)
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

    current_module = switch_to_version("current")
    print(current_module.__version__)
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
    print("TESTS PASS")
