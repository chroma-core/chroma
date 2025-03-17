import pytest
from typing import Generator, Dict, Any, cast
import numpy as np
from chromadb.config import Settings, System
from chromadb.api.types import (
    EmbeddingFunction,
    Embeddings,
    Space,
    Embeddable,
)
import os
import shutil
from chromadb.api.client import Client as ClientCreator
from chromadb.api.collection_configuration import (
    CreateCollectionConfiguration,
    load_collection_configuration_from_json,
    CreateHNSWConfiguration,
)

configurations = (
    [
        Settings(
            chroma_api_impl="chromadb.api.rust.RustBindingsAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            allow_reset=True,
            is_persistent=True,
        )
    ]
    if "CHROMA_RUST_BINDINGS_TEST_ONLY" in os.environ
    else [
        Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            allow_reset=True,
            is_persistent=True,
        ),
    ]
)


@pytest.fixture(scope="module", params=configurations)
def settings(request: pytest.FixtureRequest) -> Generator[Settings, None, None]:
    configuration = request.param
    save_path = configuration.persist_directory
    if not os.path.exists(save_path):
        os.makedirs(save_path, exist_ok=True)
    yield configuration
    if os.path.exists(save_path):
        shutil.rmtree(save_path, ignore_errors=True)


class LegacyEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __init__(self) -> None:
        pass

    def __call__(self, input: Embeddable) -> Embeddings:
        return cast(Embeddings, np.array([[1.0, 2.0]], dtype=np.float32))

    @staticmethod
    def name() -> str:
        return "legacy_ef"


class CustomEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __init__(self, dim: int = 3):
        self._dim = dim

    def __call__(self, input: Embeddable) -> Embeddings:
        return cast(Embeddings, np.array([[1.0] * self._dim], dtype=np.float32))

    @staticmethod
    def name() -> str:
        return "custom_ef"

    def get_config(self) -> Dict[str, Any]:
        return {"dim": self._dim}

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "CustomEmbeddingFunction":
        return CustomEmbeddingFunction(dim=config["dim"])

    def default_space(self) -> Space:
        return Space.COSINE


def test_legacy_embedding_function(settings: Settings) -> None:
    """Test creating and getting collections with legacy embedding functions"""
    system = System(settings)
    system.start()
    client = ClientCreator.from_system(system)
    client.reset()

    # Create with legacy embedding function
    coll = client.create_collection(
        name="test_legacy",
        embedding_function=LegacyEmbeddingFunction(),
    )

    # Verify the configuration marks it as legacy
    config = load_collection_configuration_from_json(coll._model.configuration_json)
    if config and isinstance(config, dict):
        ef_config = config.get("embedding_function", {})  # type: ignore
        if isinstance(ef_config, dict):
            assert ef_config.get("type") == "legacy"

    # Get with same legacy function
    coll2 = client.get_collection(
        name="test_legacy",
        embedding_function=LegacyEmbeddingFunction(),
    )

    # Add and query should work
    coll2.add(ids=["1"], documents=["test"])
    results = coll2.query(query_texts=["test"], n_results=1)
    assert len(results["ids"]) == 1

    system.stop()


def test_legacy_metadata(settings: Settings) -> None:
    """Test creating collections with legacy metadata format"""
    system = System(settings)
    system.start()
    client = ClientCreator.from_system(system)
    client.reset()

    # Create with legacy metadata
    legacy_metadata = {
        "hnsw:space": "cosine",
        "hnsw:construction_ef": 100,
        "hnsw:M": 10,  # This is the legacy name for max_neighbors
    }
    coll = client.create_collection(
        name="test_legacy_metadata",
        metadata=legacy_metadata,
    )

    # Verify the configuration contains the legacy settings
    config = load_collection_configuration_from_json(coll._model.configuration_json)
    if config and isinstance(config, dict):
        hnsw_config = cast(CreateHNSWConfiguration, config.get("hnsw", {}))
        assert str(hnsw_config.get("space")) == str(Space.COSINE)
        assert hnsw_config.get("ef_construction") == 100
        assert hnsw_config.get("max_neighbors") == 10

    system.stop()


def test_new_configuration(settings: Settings) -> None:
    """Test creating collections with new configuration format"""
    system = System(settings)
    system.start()
    client = ClientCreator.from_system(system)
    client.reset()

    # Create with new configuration
    hnsw_config: CreateHNSWConfiguration = {
        "space": Space.COSINE,  # Use enum value
        "ef_construction": 100,
        "max_neighbors": 10,  # Changed from M to max_neighbors
        "ef_search": 20,
        "num_threads": 2,
    }
    config: CreateCollectionConfiguration = {
        "hnsw": hnsw_config,
        "embedding_function": CustomEmbeddingFunction(dim=5),
    }

    coll = client.create_collection(
        name="test_new_config",
        configuration=config,
    )

    # Verify configuration is preserved
    loaded_config = load_collection_configuration_from_json(
        coll._model.configuration_json
    )
    if loaded_config and isinstance(loaded_config, dict):
        hnsw_config = cast(CreateHNSWConfiguration, loaded_config.get("hnsw", {}))
        ef_config = loaded_config.get("embedding_function", {})  # type: ignore
        if isinstance(ef_config, dict):
            assert hnsw_config.get("space") == Space.COSINE
            assert hnsw_config.get("ef_construction") == 100
            assert hnsw_config.get("max_neighbors") == 10
            assert ef_config.get("type") == "known"
            assert ef_config.get("name") == "custom_ef"

    system.stop()


def test_invalid_configurations(settings: Settings) -> None:
    """Test validation of invalid configurations"""
    system = System(settings)
    system.start()
    client = ClientCreator.from_system(system)
    client.reset()

    # Test invalid HNSW parameters
    with pytest.raises(ValueError):
        invalid_hnsw: CreateHNSWConfiguration = {
            "ef_construction": -1,  # Invalid negative value
            "space": Space.COSINE,  # Required field
        }
        client.create_collection(
            name="test_invalid",
            configuration={"hnsw": invalid_hnsw},
        )

    # Test invalid space for embedding function
    class InvalidSpaceEF(CustomEmbeddingFunction):
        def supported_spaces(self) -> list[Space]:
            return [Space.L2]

    with pytest.raises(ValueError):
        invalid_space_hnsw: CreateHNSWConfiguration = {
            "space": Space.COSINE,  # Use enum value
        }
        client.create_collection(
            name="test_invalid_space",
            configuration={
                "hnsw": invalid_space_hnsw,
                "embedding_function": InvalidSpaceEF(),
            },
        )

    system.stop()


# TODO: @jai uncomment once update collection is implemented in rust
# def test_configuration_updates(settings: Settings) -> None:
#     """Test updating collection configurations"""
#     system = System(settings)
#     system.start()
#     client = ClientCreator.from_system(system)
#     client.reset()

#     # Create initial collection
#     initial_hnsw: CreateHNSWConfiguration = {
#         "ef_search": 10,
#         "num_threads": 2,
#         "space": Space.COSINE,  # Required field
#     }
#     coll = client.create_collection(
#         name="test_updates",
#         configuration={"hnsw": initial_hnsw},
#     )

#     # Update configuration
#     update_hnsw: CreateHNSWConfiguration = {
#         "ef_search": 20,
#         "num_threads": 4,
#         "space": Space.COSINE,  # Required field
#     }
#     update_config: UpdateCollectionConfiguration = {
#         "hnsw": update_hnsw,
#     }
#     coll.modify(configuration=update_config)

#     # Verify updates
#     loaded_config = load_collection_configuration_from_json(coll._model.configuration_json)
#     if loaded_config and isinstance(loaded_config, dict):
#         hnsw_config = loaded_config.get("hnsw", {})
#         if isinstance(hnsw_config, dict):
#             assert hnsw_config.get("ef_search") == 20
#             assert hnsw_config.get("num_threads") == 4

#     system.stop()


def test_configuration_persistence(settings: Settings) -> None:
    """Test configuration persistence across client restarts"""
    system = System(settings)
    system.start()
    client = ClientCreator.from_system(system)
    client.reset()

    # Create collection with specific configuration
    hnsw_config: CreateHNSWConfiguration = {
        "space": Space.COSINE,  # Use enum value
        "ef_construction": 100,
        "max_neighbors": 10,  # Changed from M to max_neighbors
    }
    config: CreateCollectionConfiguration = {
        "hnsw": hnsw_config,
        "embedding_function": CustomEmbeddingFunction(dim=5),
    }

    client.create_collection(
        name="test_persist_config",
        configuration=config,
    )

    # Stop and restart system
    system.stop()
    del client
    del system

    # Create new system and verify configuration
    system2 = System(settings)
    system2.start()
    client2 = ClientCreator.from_system(system2)

    coll = client2.get_collection(
        name="test_persist_config",
    )

    loaded_config = load_collection_configuration_from_json(
        coll._model.configuration_json
    )
    if loaded_config and isinstance(loaded_config, dict):
        hnsw_config = cast(CreateHNSWConfiguration, loaded_config.get("hnsw", {}))
        ef_config = loaded_config.get("embedding_function", {})  # type: ignore
        if isinstance(ef_config, dict):
            assert hnsw_config.get("space") == Space.COSINE
            assert hnsw_config.get("ef_construction") == 100
            assert hnsw_config.get("max_neighbors") == 10
            assert ef_config.get("name") == "custom_ef"
            assert ef_config.get("config", {}).get("dim") == 5

    system2.stop()
