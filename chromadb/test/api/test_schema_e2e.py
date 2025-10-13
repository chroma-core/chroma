from chromadb.api.collection_configuration import (
    load_collection_configuration_from_json,
)
from chromadb.api.types import (
    Schema,
    SparseVectorIndexConfig,
    SparseEmbeddingFunction,
    SparseVector,
    StringInvertedIndexConfig,
    IntInvertedIndexConfig,
    FloatInvertedIndexConfig,
    BoolInvertedIndexConfig,
    VectorIndexConfig,
    HnswIndexConfig,
    SpannIndexConfig,
    FtsIndexConfig,
    EmbeddingFunction,
    Embeddings,
)
from chromadb.test.conftest import (
    ClientFactories,
    is_spann_disabled_mode,
    skip_reason_spann_disabled,
)
from chromadb.utils.embedding_functions import register_embedding_function
from typing import List, Dict, Any, cast
from uuid import uuid4
import pytest


@register_embedding_function
class SimpleEmbeddingFunction(EmbeddingFunction[List[str]]):
    """Simple embedding function with stable configuration for persistence tests."""

    def __init__(self, dim: int = 4):
        self._dim = dim

    def __call__(self, input: List[str]) -> Embeddings:
        vector = [float(i) for i in range(self._dim)]
        return cast(Embeddings, [vector for _ in input])

    @staticmethod
    def name() -> str:
        return "simple_ef"

    def get_config(self) -> Dict[str, Any]:
        return {"dim": self._dim}

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "SimpleEmbeddingFunction":
        return SimpleEmbeddingFunction(dim=config["dim"])

    def default_space(self) -> str:  # type: ignore[override]
        return "cosine"


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_schema_spann_vector_config_persistence(
    client_factories: "ClientFactories",
) -> None:
    """Ensure schema-provided SPANN settings persist across client restarts."""

    client = client_factories.create_client_from_system()
    client.reset()

    collection_name = f"schema_spann_{uuid4().hex}"

    schema = Schema()
    embedding_function = SimpleEmbeddingFunction(dim=6)
    schema.create_index(
        config=VectorIndexConfig(
            space="cosine",
            embedding_function=embedding_function,
            spann=SpannIndexConfig(
                search_nprobe=16,
                write_nprobe=32,
                ef_construction=120,
                max_neighbors=24,
            ),
        )
    )

    collection = client.get_or_create_collection(
        name=collection_name,
        schema=schema,
    )

    persisted_schema = collection.schema
    assert persisted_schema is not None

    vector_index = persisted_schema.key_overrides["$embedding"].float_list.vector_index
    assert vector_index.enabled is True
    assert vector_index.config.spann is not None
    spann_config = vector_index.config.spann
    assert spann_config.search_nprobe == 16
    assert spann_config.write_nprobe == 32
    assert spann_config.ef_construction == 120
    assert spann_config.max_neighbors == 24

    ef = vector_index.config.embedding_function
    assert ef is not None
    assert ef.name() == "simple_ef"
    assert ef.get_config() == {"dim": 6}

    persisted_json = persisted_schema.serialize_to_json()
    spann_json = persisted_json["key_overrides"]["$embedding"]["#float_list"][
        "$vector_index"
    ]["config"]["spann"]
    assert spann_json["search_nprobe"] == 16
    assert spann_json["write_nprobe"] == 32

    client_reloaded = client_factories.create_client_from_system()
    reloaded_collection = client_reloaded.get_collection(
        name=collection_name,
        embedding_function=SimpleEmbeddingFunction(dim=6),
    )

    reloaded_schema = reloaded_collection.schema
    assert reloaded_schema is not None
    reloaded_vector_index = reloaded_schema.key_overrides[
        "$embedding"
    ].float_list.vector_index
    assert reloaded_vector_index.config.spann is not None
    assert reloaded_vector_index.config.spann.search_nprobe == 16
    assert reloaded_vector_index.config.spann.write_nprobe == 32
