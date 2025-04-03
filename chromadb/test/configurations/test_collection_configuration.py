import pytest
from typing import Dict, Any, cast
import numpy as np
from chromadb.config import System
from chromadb.api.types import (
    EmbeddingFunction,
    Embeddings,
    Space,
    Embeddable,
)
from chromadb.api.client import Client as ClientCreator
from chromadb.api import ClientAPI
from chromadb.api.collection_configuration import (
    CreateCollectionConfiguration,
    UpdateCollectionConfiguration,
    load_collection_configuration_from_json,
    CreateHNSWConfiguration,
    UpdateHNSWConfiguration,
)
from chromadb.utils.embedding_functions import register_embedding_function


class LegacyEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __init__(self) -> None:
        pass

    def __call__(self, input: Embeddable) -> Embeddings:
        return cast(Embeddings, np.array([[1.0, 2.0]], dtype=np.float32))


class LegacyEmbeddingFunctionWithName(EmbeddingFunction[Embeddable]):
    def __init__(self) -> None:
        pass

    def __call__(self, input: Embeddable) -> Embeddings:
        return cast(Embeddings, np.array([[1.0, 2.0]], dtype=np.float32))

    @staticmethod
    def name() -> str:
        return "legacy_ef"


@register_embedding_function
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
        return "cosine"


def test_legacy_embedding_function(client: ClientAPI) -> None:
    """Test creating and getting collections with legacy embedding functions"""
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
    else:
        assert False, f"config: {config}"

    # Get with same legacy function
    coll2 = client.get_collection(
        name="test_legacy",
        embedding_function=LegacyEmbeddingFunction(),
    )

    # Add and query should work
    coll2.add(ids=["1"], documents=["test"])
    results = coll2.query(query_texts=["test"], n_results=1)
    assert len(results["ids"]) == 1


def test_legacy_embedding_function_with_name(client: ClientAPI) -> None:
    """Test creating and getting collections with legacy embedding functions"""
    client.reset()

    # Create with legacy embedding function
    coll = client.create_collection(
        name="test_legacy",
        embedding_function=LegacyEmbeddingFunctionWithName(),
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
        embedding_function=LegacyEmbeddingFunctionWithName(),
    )

    # Add and query should work
    coll2.add(ids=["1"], documents=["test"])
    results = coll2.query(query_texts=["test"], n_results=1)
    assert len(results["ids"]) == 1


def test_legacy_metadata(client: ClientAPI) -> None:
    """Test creating collections with legacy metadata format"""
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
        assert str(hnsw_config.get("space")) == str("cosine")
        assert hnsw_config.get("ef_construction") == 100
        assert hnsw_config.get("max_neighbors") == 10


def test_new_configuration(client: ClientAPI) -> None:
    """Test creating collections with new configuration format"""
    client.reset()

    # Create with new configuration
    hnsw_config: CreateHNSWConfiguration = {
        "space": "cosine",  # Use enum value
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
            assert hnsw_config.get("space") == "cosine"
            assert hnsw_config.get("ef_construction") == 100
            assert hnsw_config.get("max_neighbors") == 10
            assert ef_config.get("type") == "known"
            assert ef_config.get("name") == "custom_ef"


def test_invalid_configurations(client: ClientAPI) -> None:
    """Test validation of invalid configurations"""
    client.reset()

    # Test invalid HNSW parameters
    with pytest.raises(ValueError):
        invalid_hnsw: CreateHNSWConfiguration = {
            "ef_construction": -1,
            "space": "cosine",
        }
        client.create_collection(
            name="test_invalid",
            configuration={"hnsw": invalid_hnsw},
        )

    # Test invalid space for embedding function
    class InvalidSpaceEF(CustomEmbeddingFunction):
        def supported_spaces(self) -> list[Space]:
            return ["l2"]

    with pytest.raises(ValueError):
        invalid_space_hnsw: CreateHNSWConfiguration = {
            "space": "cosine",  # Use enum value
        }
        client.create_collection(
            name="test_invalid_space",
            configuration={
                "hnsw": invalid_space_hnsw,
                "embedding_function": InvalidSpaceEF(),
            },
        )


def test_configuration_updates(client: ClientAPI) -> None:
    """Test updating collection configurations"""
    client.reset()

    # Create initial collection
    initial_hnsw: CreateHNSWConfiguration = {
        "ef_search": 10,
        "num_threads": 1,
        "space": "cosine",
    }
    coll = client.create_collection(
        name="test_updates",
        configuration={"hnsw": initial_hnsw},
    )

    # Update configuration
    update_hnsw: UpdateHNSWConfiguration = {
        "ef_search": 20,
        "num_threads": 2,
    }
    update_config: UpdateCollectionConfiguration = {
        "hnsw": update_hnsw,
    }
    coll.modify(configuration=update_config)

    # Verify updates
    loaded_config = coll.configuration_json
    if loaded_config and isinstance(loaded_config, dict):
        hnsw_config = loaded_config.get("hnsw", {})
        if isinstance(hnsw_config, dict):
            assert hnsw_config.get("ef_search") == 20
            assert hnsw_config.get("num_threads") == 2


def test_configuration_persistence(sqlite_persistent: System) -> None:
    """Test configuration persistence across client restarts"""
    client = ClientCreator.from_system(sqlite_persistent)
    client.reset()

    # Create collection with specific configuration
    hnsw_config: CreateHNSWConfiguration = {
        "space": "cosine",
        "ef_construction": 100,
        "max_neighbors": 10,
    }
    config: CreateCollectionConfiguration = {
        "hnsw": hnsw_config,
        "embedding_function": CustomEmbeddingFunction(dim=5),
    }

    client.create_collection(
        name="test_persist_config",
        configuration=config,
    )

    system2 = System(client.get_settings())

    # Stop and restart system
    del client

    # Create new system and verify configuration
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
            assert hnsw_config.get("space") == "cosine"
            assert hnsw_config.get("ef_construction") == 100
            assert hnsw_config.get("max_neighbors") == 10
            assert ef_config.get("name") == "custom_ef"
            assert ef_config.get("config", {}).get("dim") == 5

    system2.stop()


def test_configuration_result_format(client: ClientAPI) -> None:
    """Test updating collection configurations"""
    client.reset()

    # Create initial collection
    initial_hnsw: CreateHNSWConfiguration = {
        "ef_search": 10,
        "num_threads": 2,
        "space": "cosine",  # Required field
    }
    coll = client.create_collection(
        name="test_updates",
        configuration={"hnsw": initial_hnsw},
    )

    print(coll._model.configuration_json)
    assert coll._model.configuration_json is not None
    hnsw_config = coll._model.configuration_json.get("hnsw")
    assert hnsw_config is not None
    assert hnsw_config.get("ef_search") == 10
    assert hnsw_config.get("num_threads") == 2
    assert hnsw_config.get("space") == "cosine"
