from chromadb.api import ClientAPI
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
    SpannIndexConfig,
    EmbeddingFunction,
    Embeddings,
)
from chromadb.test.conftest import (
    ClientFactories,
    is_spann_disabled_mode,
    skip_if_not_cluster,
    skip_reason_spann_disabled,
)
from chromadb.test.utils.wait_for_version_increase import (
    get_collection_version,
    wait_for_version_increase,
)
from chromadb.utils.embedding_functions import (
    register_embedding_function,
    register_sparse_embedding_function,
)
from chromadb.api.models.Collection import Collection
from chromadb.errors import InvalidArgumentError
from chromadb.execution.expression import Knn, Search
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, cast
from uuid import uuid4
import numpy as np
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

    print(persisted_schema.serialize_to_json())

    embedding_override = persisted_schema.keys["#embedding"].float_list
    assert embedding_override is not None
    vector_index = embedding_override.vector_index
    assert vector_index is not None
    assert vector_index.enabled is True
    assert vector_index.config is not None
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
    spann_json = persisted_json["keys"]["#embedding"]["float_list"]["vector_index"][
        "config"
    ]["spann"]
    assert spann_json["search_nprobe"] == 16
    assert spann_json["write_nprobe"] == 32

    client_reloaded = client_factories.create_client_from_system()
    reloaded_collection = client_reloaded.get_collection(
        name=collection_name,
        embedding_function=SimpleEmbeddingFunction(dim=6),  # type: ignore[arg-type]
    )

    reloaded_schema = reloaded_collection.schema
    assert reloaded_schema is not None
    reloaded_embedding_override = reloaded_schema.keys["#embedding"].float_list
    assert reloaded_embedding_override is not None
    reloaded_vector_index = reloaded_embedding_override.vector_index
    assert reloaded_vector_index is not None
    assert reloaded_vector_index.config is not None
    assert reloaded_vector_index.config.spann is not None
    assert reloaded_vector_index.config.spann.search_nprobe == 16
    assert reloaded_vector_index.config.spann.write_nprobe == 32


@register_sparse_embedding_function
class DeterministicSparseEmbeddingFunction(SparseEmbeddingFunction[List[str]]):
    """Sparse embedding function that emits predictable token/value pairs."""

    def __init__(self, label: str = "det_sparse"):
        self._label = label

    def __call__(self, input: List[str]) -> List[SparseVector]:
        return [
            SparseVector(indices=[idx], values=[float(len(text) + idx)])
            for idx, text in enumerate(input)
        ]

    @staticmethod
    def name() -> str:
        return "det_sparse"

    def get_config(self) -> Dict[str, Any]:
        return {"label": self._label}

    @staticmethod
    def build_from_config(
        config: Dict[str, Any]
    ) -> "DeterministicSparseEmbeddingFunction":
        return DeterministicSparseEmbeddingFunction(config.get("label", "det_sparse"))


def _create_isolated_collection(
    client_factories: "ClientFactories",
    schema: Optional[Schema] = None,
    embedding_function: Optional[EmbeddingFunction[Any]] = None,
) -> Tuple[Collection, ClientAPI]:
    """Provision a new temporary collection and return it with the backing client."""
    client = client_factories.create_client_from_system()
    client.reset()

    collection_name = f"schema_e2e_{uuid4().hex}"
    if schema is not None:
        collection = client.get_or_create_collection(
            name=collection_name,
            schema=schema,
        )
    else:
        if embedding_function is not None:
            collection = client.get_or_create_collection(
                name=collection_name,
                embedding_function=embedding_function,
            )
        else:
            collection = client.get_or_create_collection(
                name=collection_name,
            )

    return collection, client


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_schema_defaults_enable_indexed_operations(
    client_factories: "ClientFactories",
) -> None:
    """Validate default schema indexes support filtering, updates, and embeddings."""
    collection, client = _create_isolated_collection(client_factories)

    schema = collection.schema
    assert schema is not None
    assert schema.defaults is not None
    assert schema.defaults.string is not None
    string_index = schema.defaults.string.string_inverted_index
    assert string_index is not None
    assert string_index.enabled is True
    assert schema.defaults.int_value is not None
    int_index = schema.defaults.int_value.int_inverted_index
    assert int_index is not None
    assert int_index.enabled is True
    assert schema.defaults.float_value is not None
    float_index = schema.defaults.float_value.float_inverted_index
    assert float_index is not None
    assert float_index.enabled is True
    assert schema.defaults.boolean is not None
    bool_index = schema.defaults.boolean.bool_inverted_index
    assert bool_index is not None
    assert bool_index.enabled is True

    document_override = schema.keys["#document"].string
    assert document_override is not None
    fts_index = document_override.fts_index
    assert fts_index is not None
    assert fts_index.enabled is True

    embedding_override = schema.keys["#embedding"].float_list
    assert embedding_override is not None
    vector_index = embedding_override.vector_index
    assert vector_index is not None
    assert vector_index.enabled is True

    ids = ["doc-1", "doc-2", "doc-3"]
    documents = ["alpha", "beta", "gamma"]
    metadatas: List[Mapping[str, Any]] = [
        {"category": "news", "rating": 5, "price": 9.5, "is_active": True},
        {"category": "science", "rating": 7, "price": 2.5, "is_active": False},
        {"category": "news", "rating": 3, "price": 5.0, "is_active": True},
    ]

    collection.add(ids=ids, documents=documents, metadatas=metadatas)

    filtered = collection.get(where={"category": "science"})
    assert set(filtered["ids"]) == {"doc-2"}

    numeric_filter = collection.get(where={"rating": 3})
    assert set(numeric_filter["ids"]) == {"doc-3"}

    bool_filter = collection.get(where={"is_active": False})
    assert set(bool_filter["ids"]) == {"doc-2"}

    collection.update(ids=["doc-1"], metadatas=[{"rating": 6, "category": "updates"}])
    rating_after_update = collection.get(where={"rating": 6})
    assert set(rating_after_update["ids"]) == {"doc-1"}

    collection.upsert(
        ids=["doc-2"],
        documents=["beta-updated"],
        metadatas=[{"price": 2.5, "category": "science"}],
    )

    embeddings_payload = collection.get(ids=["doc-1"], include=["embeddings"])
    assert embeddings_payload["embeddings"] is not None
    assert len(embeddings_payload["embeddings"]) == 1

    # Ensure underlying schema persisted across fetches
    reloaded = client.get_collection(collection.name)
    assert reloaded.schema is not None
    assert reloaded.schema.serialize_to_json() == schema.serialize_to_json()


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_get_or_create_and_get_collection_preserve_schema(
    client_factories: "ClientFactories",
) -> None:
    """Ensure repeated collection lookups reuse the persisted schema definition."""
    base_schema = Schema()
    base_schema.create_index(
        key="custom_tag",
        config=StringInvertedIndexConfig(),
    )
    base_schema.create_index(
        key="importance",
        config=IntInvertedIndexConfig(),
    )

    collection, client = _create_isolated_collection(
        client_factories,
        schema=base_schema,
    )

    assert collection.schema is not None
    initial_schema_json = collection.schema.serialize_to_json()
    assert "custom_tag" in initial_schema_json["keys"]
    assert "importance" in initial_schema_json["keys"]

    second_reference = client.get_or_create_collection(name=collection.name)
    assert second_reference.schema is not None
    assert second_reference.schema.serialize_to_json() == initial_schema_json

    fetched = client.get_collection(name=collection.name)
    assert fetched.schema is not None
    assert fetched.schema.serialize_to_json() == initial_schema_json

    second_reference.add(
        ids=["schema-preserve"],
        documents=["doc"],
        metadatas=[{"custom_tag": "alpha", "importance": 10}],
    )

    stored = fetched.get(where={"custom_tag": "alpha"})
    assert set(stored["ids"]) == {"schema-preserve"}


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_delete_collection_resets_schema_configuration(
    client_factories: "ClientFactories",
) -> None:
    """Deleting and recreating a collection should drop prior schema overrides."""
    schema = Schema()
    schema.create_index(
        key="transient_key",
        config=StringInvertedIndexConfig(),
    )

    collection, client = _create_isolated_collection(
        client_factories,
        schema=schema,
    )

    assert collection.schema is not None
    assert "transient_key" in collection.schema.keys

    client.delete_collection(name=collection.name)

    recreated = client.create_collection(name=collection.name)
    assert recreated.schema is not None
    recreated_json = recreated.schema.serialize_to_json()
    baseline_json = Schema().serialize_to_json()
    assert "transient_key" not in recreated_json["keys"]
    assert set(recreated_json["keys"].keys()) == set(baseline_json["keys"].keys())


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_vector_source_key_and_index_constraints(
    client_factories: "ClientFactories",
) -> None:
    """Sparse vector configs honor source key embedding and single-index enforcement."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="source-test")

    schema = Schema()
    schema.create_index(
        key="sparse_metadata",
        config=SparseVectorIndexConfig(
            source_key="raw_text",
            embedding_function=sparse_ef,
        ),
    )
    schema.create_index(key="tag_a", config=StringInvertedIndexConfig())
    schema.create_index(key="tag_b", config=StringInvertedIndexConfig())

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    assert collection.schema is not None
    assert "sparse_metadata" in collection.schema.keys
    assert "tag_a" in collection.schema.keys
    assert "tag_b" in collection.schema.keys

    collection.add(
        ids=["sparse-1"],
        documents=["source document"],
        metadatas=[{"raw_text": "oranges", "tag_a": "citrus", "tag_b": "fruit"}],
    )

    stored = collection.get(ids=["sparse-1"], include=["metadatas"])
    assert stored["metadatas"] is not None
    metadata = stored["metadatas"][0]
    assert metadata is not None
    assert metadata["tag_a"] == "citrus"
    assert metadata["tag_b"] == "fruit"
    assert metadata["raw_text"] == "oranges"
    assert "sparse_metadata" in metadata
    sparse_payload = cast(SparseVector, metadata["sparse_metadata"])
    assert sparse_payload == sparse_ef(["oranges"])[0]

    search_result = collection.search(
        Search().rank(Knn(key="sparse_metadata", query=cast(Any, sparse_payload)))
    )
    assert len(search_result["ids"]) == 1
    assert "sparse-1" in search_result["ids"][0]

    with pytest.raises(ValueError):
        collection.schema.create_index(
            key="another_sparse",
            config=SparseVectorIndexConfig(source_key="raw_text"),
        )

    string_filter = collection.get(where={"tag_b": "fruit"})
    assert set(string_filter["ids"]) == {"sparse-1"}


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_schema_persistence_with_custom_overrides(
    client_factories: "ClientFactories",
) -> None:
    """Custom schema overrides persist across new client instances."""
    schema = Schema()
    schema.create_index(key="title", config=StringInvertedIndexConfig())
    schema.create_index(key="published_year", config=IntInvertedIndexConfig())
    schema.create_index(key="score", config=FloatInvertedIndexConfig())
    schema.create_index(key="is_featured", config=BoolInvertedIndexConfig())

    collection, client = _create_isolated_collection(
        client_factories,
        schema=schema,
    )

    collection.add(
        ids=["persist-1"],
        documents=["persistent doc"],
        metadatas=[
            {
                "title": "Schema Persistence",
                "published_year": 2024,
                "score": 4.5,
                "is_featured": True,
            }
        ],
    )

    assert collection.schema is not None
    expected_schema_json = collection.schema.serialize_to_json()

    reloaded_client = client_factories.create_client_from_system()
    reloaded_collection = reloaded_client.get_collection(name=collection.name)
    assert reloaded_collection.schema is not None
    assert reloaded_collection.schema.serialize_to_json() == expected_schema_json

    fetched = reloaded_collection.get(where={"title": "Schema Persistence"})
    assert set(fetched["ids"]) == {"persist-1"}


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_collection_embed_uses_schema_or_collection_embedding_function(
    client_factories: "ClientFactories",
) -> None:
    """_embed should respect schema-provided and direct embedding functions."""

    schema_emb_fn = SimpleEmbeddingFunction(dim=5)
    schema = Schema().create_index(
        config=VectorIndexConfig(embedding_function=schema_emb_fn)
    )
    schema_collection, _ = _create_isolated_collection(
        client_factories,
        schema=schema,
    )

    schema_embeddings = schema_collection._embed(["schema document"])
    assert schema_embeddings is not None
    assert np.allclose(schema_embeddings[0], [0.0, 1.0, 2.0, 3.0, 4.0])

    direct_emb_fn = SimpleEmbeddingFunction(dim=3)
    direct_collection, _ = _create_isolated_collection(
        client_factories,
        embedding_function=direct_emb_fn,
    )

    direct_embeddings = direct_collection._embed(["direct document"])
    assert direct_embeddings is not None
    assert np.allclose(direct_embeddings[0], [0.0, 1.0, 2.0])


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_schema_delete_index_and_restore(
    client_factories: "ClientFactories",
) -> None:
    """Toggling inverted index enablement reflects in query behavior."""
    disabled_defaults = Schema().delete_index(config=StringInvertedIndexConfig())
    collection, client = _create_isolated_collection(
        client_factories,
        schema=disabled_defaults,
    )

    collection.add(
        ids=["no-index"],
        documents=["doc"],
        metadatas=[{"global_field": "value"}],
    )

    with pytest.raises(Exception):
        collection.get(where={"global_field": "value"})

    client.delete_collection(name=collection.name)

    disabled_key_schema = (
        Schema()
        .create_index(config=StringInvertedIndexConfig())
        .delete_index(key="category", config=StringInvertedIndexConfig())
    )
    recreated = client.get_or_create_collection(
        name=collection.name, schema=disabled_key_schema
    )
    recreated.add(
        ids=["key-disabled"],
        documents=["doc"],
        metadatas=[{"category": "news"}],
    )

    with pytest.raises(Exception):
        recreated.get(where={"category": "news"})

    client.delete_collection(name=collection.name)

    restored_schema = Schema().create_index(
        key="category", config=StringInvertedIndexConfig()
    )
    restored = client.get_or_create_collection(
        name=collection.name, schema=restored_schema
    )
    restored.add(
        ids=["key-enabled"],
        documents=["doc"],
        metadatas=[{"category": "news"}],
    )

    search = restored.get(where={"category": "news"})
    assert set(search["ids"]) == {"key-enabled"}


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_disabled_metadata_index_filters_raise_invalid_argument(
    client_factories: "ClientFactories",
) -> None:
    """Disabled metadata inverted index should block filter-based operations."""
    schema = Schema().delete_index(
        key="restricted_tag", config=StringInvertedIndexConfig()
    )
    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    collection.add(
        ids=["restricted-doc"],
        embeddings=cast(Embeddings, [[0.1, 0.2, 0.3, 0.4]]),
        metadatas=[{"restricted_tag": "blocked"}],
        documents=["doc"],
    )

    assert collection.schema is not None
    schema_entry = collection.schema.keys["restricted_tag"].string
    assert schema_entry is not None
    index_config = schema_entry.string_inverted_index
    assert index_config is not None
    assert index_config.enabled is False

    filter_payload: Dict[str, Any] = {"restricted_tag": "blocked"}
    search_request = Search(where=filter_payload)

    def _expect_disabled_error(operation: Callable[[], Any]) -> None:
        with pytest.raises(InvalidArgumentError) as exc_info:
            operation()
        assert "Cannot filter using metadata key 'restricted_tag'" in str(
            exc_info.value
        )

    operations: List[Callable[[], Any]] = [
        lambda: collection.get(where=filter_payload),
        lambda: collection.query(
            query_embeddings=cast(Embeddings, [[0.1, 0.2, 0.3, 0.4]]),
            n_results=1,
            where=filter_payload,
        ),
        lambda: collection.search(search_request),
        lambda: collection.delete(where=filter_payload),
    ]

    for operation in operations:
        _expect_disabled_error(operation)


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_schema_discovers_new_keys_after_compaction(
    client_factories: "ClientFactories",
) -> None:
    """Compaction promotes unseen metadata keys into discoverable schema entries."""
    collection, client = _create_isolated_collection(client_factories)

    initial_version = get_collection_version(client, collection.name)

    batch_size = 251
    ids = [f"discover-add-{i}" for i in range(batch_size)]
    documents = [f"doc {i}" for i in range(batch_size)]
    metadatas: List[Mapping[str, Any]] = [
        {"discover_add": f"topic_{i}"} for i in range(batch_size)
    ]

    collection.add(ids=ids, documents=documents, metadatas=metadatas)

    wait_for_version_increase(client, collection.name, initial_version)

    reloaded = client.get_collection(collection.name)
    assert reloaded.schema is not None
    assert "discover_add" in reloaded.schema.keys
    discover_add_config = reloaded.schema.keys["discover_add"].string
    assert discover_add_config is not None
    string_inverted_index = discover_add_config.string_inverted_index
    assert string_inverted_index is not None
    assert string_inverted_index.enabled is True

    next_version = get_collection_version(client, collection.name)

    upsert_count = 260
    upsert_ids = [f"discover-upsert-{i}" for i in range(upsert_count)]
    upsert_docs = [f"upsert doc {i}" for i in range(upsert_count)]
    upsert_metadatas: List[Mapping[str, Any]] = [
        {"discover_upsert": f"topic_{i}"} for i in range(upsert_count)
    ]

    collection.upsert(
        ids=upsert_ids,
        documents=upsert_docs,
        metadatas=upsert_metadatas,
    )

    wait_for_version_increase(client, collection.name, next_version)

    post_upsert = client.get_collection(collection.name)
    assert post_upsert.schema is not None
    assert "discover_upsert" in post_upsert.schema.keys
    discover_upsert_config = post_upsert.schema.keys["discover_upsert"].string
    assert discover_upsert_config is not None
    upsert_inverted_index = discover_upsert_config.string_inverted_index
    assert upsert_inverted_index is not None
    assert upsert_inverted_index.enabled is True

    result = collection.get(where={"discover_add": "topic_42"})
    assert set(result["ids"]) == {"discover-add-42"}

    result_upsert = collection.get(where={"discover_upsert": "topic_42"})
    assert set(result_upsert["ids"]) == {"discover-upsert-42"}

    reload_client = client_factories.create_client_from_system()
    persisted = reload_client.get_collection(collection.name)
    assert persisted.schema is not None
    assert "discover_add" in persisted.schema.keys
    assert "discover_upsert" in persisted.schema.keys


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_schema_rejects_conflicting_discoverable_key_types(
    client_factories: "ClientFactories",
) -> None:
    """Conflicting value types should not corrupt discoverable schema entries."""
    collection, client = _create_isolated_collection(client_factories)

    initial_version = get_collection_version(client, collection.name)

    ids = [f"conflict-{i}" for i in range(251)]
    metadatas: List[Mapping[str, Any]] = [
        {"conflict_key": f"value_{i}"} for i in range(251)
    ]
    documents = [f"doc {i}" for i in range(251)]
    collection.add(ids=ids, documents=documents, metadatas=metadatas)

    wait_for_version_increase(client, collection.name, initial_version)

    collection.upsert(
        ids=["conflict-bad"],
        documents=["bad doc"],
        metadatas=[{"conflict_key": 100}],
    )

    collection.update(
        ids=["conflict-0"],
        metadatas=[{"conflict_key": 200}],
    )

    schema = client.get_collection(collection.name).schema
    assert schema is not None
    assert "conflict_key" in schema.keys
    conflict_entry = schema.keys["conflict_key"]
    if (
        conflict_entry.string is not None
        and conflict_entry.string.string_inverted_index is not None
    ):
        assert conflict_entry.string.string_inverted_index.enabled is True

    fetch = collection.get(where={"conflict_key": "value_10"})
    assert set(fetch["ids"]) == {"conflict-10"}

    conflict_bad_meta = collection.get(ids=["conflict-bad"], include=["metadatas"])
    assert conflict_bad_meta["metadatas"] is not None
    bad_metadata = conflict_bad_meta["metadatas"][0]
    assert bad_metadata is not None
    assert isinstance(bad_metadata["conflict_key"], (int, float))


@skip_if_not_cluster()
@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_collection_fork_inherits_and_isolates_schema(
    client_factories: "ClientFactories",
) -> None:
    """Assert forked collections inherit schema and evolve independently of the parent."""
    schema = Schema()
    schema.create_index(key="shared_key", config=StringInvertedIndexConfig())

    collection, client = _create_isolated_collection(
        client_factories,
        schema=schema,
    )
    collection.add(
        ids=["parent-1"],
        documents=["parent doc"],
        metadatas=[{"shared_key": "parent"}],
    )

    assert collection.schema is not None
    parent_schema_json = collection.schema.serialize_to_json()

    fork_name = f"{collection.name}_fork"
    forked = collection.fork(fork_name)

    assert forked.schema is not None
    assert forked.schema.serialize_to_json() == parent_schema_json

    fork_version = get_collection_version(client, forked.name)

    fork_ids = [f"fork-{i}" for i in range(251)]
    fork_docs = [f"fork doc {i}" for i in range(251)]
    fork_metadatas: List[Mapping[str, Any]] = [
        {"shared_key": "parent", "child_only": f"value_{i}"} for i in range(251)
    ]
    forked.upsert(ids=fork_ids, documents=fork_docs, metadatas=fork_metadatas)

    wait_for_version_increase(client, forked.name, fork_version)

    updated_child = client.get_collection(forked.name)
    assert updated_child.schema is not None
    assert "child_only" in updated_child.schema.keys
    child_only_config = updated_child.schema.keys["child_only"].string
    assert child_only_config is not None
    child_inverted_index = child_only_config.string_inverted_index
    assert child_inverted_index is not None
    assert child_inverted_index.enabled is True

    reloaded_parent = client.get_collection(collection.name)
    assert reloaded_parent.schema is not None
    assert "child_only" not in reloaded_parent.schema.keys

    parent_results = reloaded_parent.get(where={"shared_key": "parent"})
    assert set(parent_results["ids"]) == {"parent-1"}

    child_results = forked.get(where={"child_only": "value_10"})
    assert set(child_results["ids"]) == {"fork-10"}


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_schema_embedding_configuration_enforced(
    client_factories: "ClientFactories",
) -> None:
    """Schema-provided embedding functions drive both dense and sparse embeddings."""
    vector_schema = Schema().create_index(
        config=VectorIndexConfig(embedding_function=SimpleEmbeddingFunction(dim=5))
    )
    vector_collection, _ = _create_isolated_collection(
        client_factories,
        schema=vector_schema,
        embedding_function=SimpleEmbeddingFunction(dim=5),
    )

    vector_collection.add(
        ids=["embed-1"],
        documents=["embedding document"],
    )

    embedded = vector_collection.get(ids=["embed-1"], include=["embeddings"])
    assert embedded["embeddings"] is not None
    assert np.allclose(embedded["embeddings"][0], [0.0, 1.0, 2.0, 3.0, 4.0])

    sparse_ef = DeterministicSparseEmbeddingFunction()
    sparse_schema = Schema().create_index(
        key="sparse_auto",
        config=SparseVectorIndexConfig(
            source_key="text_to_embed",
            embedding_function=sparse_ef,
        ),
    )
    sparse_collection, _ = _create_isolated_collection(
        client_factories,
        schema=sparse_schema,
    )

    sparse_collection.add(
        ids=["sparse-text"],
        documents=["doc"],
        metadatas=[{"text_to_embed": "schema embedding"}],
    )
    sparse_query = sparse_ef(["schema embedding"])[0]
    sparse_result = sparse_collection.get(ids=["sparse-text"], include=["metadatas"])
    assert sparse_result["metadatas"] is not None
    sparse_meta = sparse_result["metadatas"][0]
    assert sparse_meta is not None
    assert "sparse_auto" in sparse_meta
    sparse_payload = cast(SparseVector, sparse_meta["sparse_auto"])
    assert sparse_payload == sparse_query

    sparse_search = sparse_collection.search(
        Search().rank(Knn(key="sparse_auto", query=cast(Any, sparse_payload)))
    )
    assert len(sparse_search["ids"]) == 1
    assert "sparse-text" in sparse_search["ids"][0]

    sparse_collection.add(
        ids=["sparse-numeric"],
        documents=["doc"],
        metadatas=[{"text_to_embed": 5}],
    )

    numeric_meta = sparse_collection.get(ids=["sparse-numeric"], include=["metadatas"])
    assert numeric_meta["metadatas"] is not None
    numeric_metadata = numeric_meta["metadatas"][0]
    assert numeric_metadata is not None
    assert "sparse_auto" not in numeric_metadata


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_schema_precedence_for_overrides_discoverables_and_defaults(
    client_factories: "ClientFactories",
) -> None:
    """Explicit overrides take precedence over disabled defaults and discoverables."""
    schema = (
        Schema()
        .delete_index(config=StringInvertedIndexConfig())
        .create_index(key="explicit_key", config=StringInvertedIndexConfig())
    )

    collection, client = _create_isolated_collection(
        client_factories,
        schema=schema,
    )

    ids = [f"precedence-{i}" for i in range(260)]
    documents = [f"doc {i}" for i in range(260)]
    metadatas: List[Mapping[str, Any]] = [
        {"explicit_key": "explicit", "discover_key": f"discover_{i}"}
        for i in range(260)
    ]

    initial_version = get_collection_version(client, collection.name)
    collection.add(ids=ids, documents=documents, metadatas=metadatas)
    wait_for_version_increase(client, collection.name, initial_version)

    schema_state = client.get_collection(collection.name).schema
    assert schema_state is not None
    assert "explicit_key" in schema_state.keys
    explicit_key_string = schema_state.keys["explicit_key"].string
    assert explicit_key_string is not None
    explicit_inverted_index = explicit_key_string.string_inverted_index
    assert explicit_inverted_index is not None
    assert explicit_inverted_index.enabled is True

    assert "discover_key" in schema_state.keys
    discover_key_string = schema_state.keys["discover_key"].string
    assert discover_key_string is not None
    discover_inverted_index = discover_key_string.string_inverted_index
    assert discover_inverted_index is not None
    assert discover_inverted_index.enabled is False

    explicit_result = collection.get(where={"explicit_key": "explicit"})
    assert set(explicit_result["ids"]) == set(ids)

    with pytest.raises(Exception):
        collection.get(where={"discover_key": "discover_5"})


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_document_source_no_metadata(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse embedding auto-generation using #document as source with no metadata."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="doc_no_meta")

    schema = Schema().create_index(
        key="auto_sparse",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    # Add documents without metadata
    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["hello world", "test document", "short"],
    )

    # Verify sparse embeddings were auto-generated in metadata
    result = collection.get(ids=["doc1", "doc2", "doc3"], include=["metadatas"])
    assert result["metadatas"] is not None

    # Expected embeddings from batched call (indices will be [0, 1, 2])
    expected_batch = sparse_ef(["hello world", "test document", "short"])

    for i, doc_id in enumerate(["doc1", "doc2", "doc3"]):
        metadata = result["metadatas"][i]
        assert metadata is not None
        assert "auto_sparse" in metadata

        # Verify the sparse embedding matches expected output from batch
        actual = cast(SparseVector, metadata["auto_sparse"])
        assert actual.indices == expected_batch[i].indices
        assert actual.values == expected_batch[i].values


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_document_source_and_metadata(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse embedding with #document source when metadata is also provided."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="doc_with_meta")

    schema = Schema().create_index(
        key="doc_sparse",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    # Add documents with metadata
    collection.add(
        ids=["m1", "m2"],
        documents=["alpha", "beta"],
        metadatas=[
            {"category": "test", "value": 42},
            {"category": "prod", "value": 99},
        ],
    )

    result = collection.get(ids=["m1", "m2"], include=["metadatas"])
    assert result["metadatas"] is not None

    # Verify original metadata is preserved
    assert result["metadatas"][0]["category"] == "test"
    assert result["metadatas"][0]["value"] == 42
    assert result["metadatas"][1]["category"] == "prod"
    assert result["metadatas"][1]["value"] == 99

    # Verify sparse embeddings were added
    assert "doc_sparse" in result["metadatas"][0]
    assert "doc_sparse" in result["metadatas"][1]

    # Expected from batch call
    expected_batch = sparse_ef(["alpha", "beta"])
    actual_m1 = cast(SparseVector, result["metadatas"][0]["doc_sparse"])
    assert actual_m1.indices == expected_batch[0].indices


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_metadata_source_key(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse embedding using a metadata field as source."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="meta_source")

    schema = Schema().create_index(
        key="content_sparse",
        config=SparseVectorIndexConfig(
            source_key="content",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    # Add with source field in metadata
    collection.add(
        ids=["s1", "s2", "s3"],
        documents=["doc1", "doc2", "doc3"],
        metadatas=[
            {"content": "sparse content one"},
            {"content": "sparse content two"},
            {"content": "sparse content three"},
        ],
    )

    result = collection.get(ids=["s1", "s2", "s3"], include=["metadatas"])
    assert result["metadatas"] is not None

    # Expected from batch call
    expected_batch = sparse_ef(
        ["sparse content one", "sparse content two", "sparse content three"]
    )

    for i in range(3):
        metadata = result["metadatas"][i]
        assert metadata is not None
        assert "content_sparse" in metadata

        # Original content field should still exist
        assert "content" in metadata

        # Verify sparse embedding was generated from content field
        actual = cast(SparseVector, metadata["content_sparse"])
        assert actual.indices == expected_batch[i].indices
        assert actual.values == expected_batch[i].values


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_mixed_metadata_none_and_filled(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse embedding with mixed metadata (None, empty, filled)."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="mixed_meta")

    schema = Schema().create_index(
        key="mixed_sparse",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    # Add with None metadata items mixed in
    collection.add(
        ids=["n1", "n2", "n3", "n4"],
        documents=["doc one", "doc two", "doc three", "doc four"],
        metadatas=[
            None,  # type: ignore
            None,  # type: ignore
            {"existing": "data"},  # Filled metadata
            None,  # type: ignore
        ],
    )

    result = collection.get(ids=["n1", "n2", "n3", "n4"], include=["metadatas"])
    assert result["metadatas"] is not None

    # Expected from batch call
    expected_batch = sparse_ef(["doc one", "doc two", "doc three", "doc four"])

    # All should have sparse embeddings added
    for i, metadata in enumerate(result["metadatas"]):
        assert metadata is not None
        assert "mixed_sparse" in metadata

        # Verify correct embedding for each document
        actual = cast(SparseVector, metadata["mixed_sparse"])
        assert actual.indices == expected_batch[i].indices

    # Third one should still have existing data
    assert result["metadatas"][2]["existing"] == "data"


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_skips_existing_values(
    client_factories: "ClientFactories",
) -> None:
    """Test that sparse auto-embedding doesn't overwrite existing values."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="preserve")

    schema = Schema().create_index(
        key="preserve_sparse",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    # Pre-create a sparse vector
    existing_sparse = SparseVector(indices=[999], values=[123.456])

    collection.add(
        ids=["preserve1", "preserve2"],
        documents=["auto document", "manual document"],
        metadatas=[
            None,  # type: ignore
            {"preserve_sparse": existing_sparse},  # Should be preserved
        ],
    )

    result = collection.get(ids=["preserve1", "preserve2"], include=["metadatas"])
    assert result["metadatas"] is not None

    # First should have auto-generated embedding (only one doc was auto-embedded)
    auto_meta = result["metadatas"][0]
    assert auto_meta is not None
    assert "preserve_sparse" in auto_meta
    expected_auto = sparse_ef(["auto document"])[0]  # Single item batch
    actual_auto = cast(SparseVector, auto_meta["preserve_sparse"])
    assert actual_auto.indices == expected_auto.indices

    # Second should preserve the manually provided one
    manual_meta = result["metadatas"][1]
    assert manual_meta is not None
    actual_manual = cast(SparseVector, manual_meta["preserve_sparse"])
    assert actual_manual.indices == [999]
    assert actual_manual.values == [123.456]


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_missing_source_field(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse embedding when source metadata field is missing or wrong type."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="missing_field")

    schema = Schema().create_index(
        key="field_sparse",
        config=SparseVectorIndexConfig(
            source_key="text_field",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    collection.add(
        ids=["f1", "f2", "f3", "f4"],
        documents=["doc1", "doc2", "doc3", "doc4"],
        metadatas=[
            {"text_field": "valid text"},  # Valid string
            {"text_field": 123},  # Wrong type (int)
            {"other_field": "value"},  # Missing source field
            None,  # type: ignore
        ],
    )

    result = collection.get(ids=["f1", "f2", "f3", "f4"], include=["metadatas"])
    assert result["metadatas"] is not None

    # Only first one should have sparse embedding (single item batch)
    assert "field_sparse" in result["metadatas"][0]
    expected = sparse_ef(["valid text"])[0]  # Single item batch
    actual = cast(SparseVector, result["metadatas"][0]["field_sparse"])
    assert actual.indices == expected.indices

    # Others should NOT have sparse embedding
    assert "field_sparse" not in result["metadatas"][1]
    assert "field_sparse" not in result["metadatas"][2]
    assert result["metadatas"][3] is None  # No metadata provided, stays None


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_string_inverted_index(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse auto-embedding works alongside string inverted indexes."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="with_string_index")

    schema = Schema()
    schema.create_index(
        key="category",
        config=StringInvertedIndexConfig(),
    )
    schema.create_index(
        key="sparse_field",
        config=SparseVectorIndexConfig(
            source_key="custom_text",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    collection.add(
        ids=["multi1", "multi2"],
        documents=["main document", "another document"],
        metadatas=[
            {"custom_text": "field content", "category": "cat1"},
            {"custom_text": "different content", "category": "cat2"},
        ],
    )

    result = collection.get(ids=["multi1", "multi2"], include=["metadatas"])
    assert result["metadatas"] is not None

    # Expected from batch call
    expected_batch = sparse_ef(["field content", "different content"])

    for i, metadata in enumerate(result["metadatas"]):
        assert metadata is not None

        # Sparse embedding should be present
        assert "sparse_field" in metadata

        # Verify sparse embedding uses custom_text field
        actual_field = cast(SparseVector, metadata["sparse_field"])
        assert actual_field.indices == expected_batch[i].indices

        # Category should be searchable
        assert "category" in metadata

    # Test filtering with string inverted index
    filtered = collection.get(where={"category": "cat1"})
    assert set(filtered["ids"]) == {"multi1"}


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_dense_and_sparse_auto_embeddings_together(
    client_factories: "ClientFactories",
) -> None:
    """Test that dense and sparse auto-embeddings work together."""
    dense_ef = SimpleEmbeddingFunction(dim=4)
    sparse_ef = DeterministicSparseEmbeddingFunction(label="with_dense")

    schema = Schema()
    schema.create_index(config=VectorIndexConfig(embedding_function=dense_ef))
    schema.create_index(
        key="sparse_key",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(
        client_factories,
        schema=schema,
        embedding_function=dense_ef,
    )

    collection.add(
        ids=["both1", "both2"],
        documents=["combined document", "another doc"],
    )

    result = collection.get(
        ids=["both1", "both2"],
        include=["embeddings", "metadatas"],
    )

    # Verify dense embeddings
    assert result["embeddings"] is not None
    assert len(result["embeddings"]) == 2
    assert len(result["embeddings"][0]) == 4

    # Verify sparse embeddings in metadata
    assert result["metadatas"] is not None
    for metadata in result["metadatas"]:
        assert metadata is not None
        assert "sparse_key" in metadata


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_update_and_upsert(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse auto-embedding with update and upsert operations."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="update_upsert")

    schema = Schema().create_index(
        key="update_sparse",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    # Initial add
    collection.add(
        ids=["up1"],
        documents=["original doc"],
    )

    # Update with new document
    collection.update(
        ids=["up1"],
        documents=["updated doc"],
    )

    result_update = collection.get(ids=["up1"], include=["metadatas", "documents"])
    assert result_update["metadatas"] is not None
    assert result_update["documents"] is not None
    assert result_update["documents"][0] == "updated doc"
    assert "update_sparse" in result_update["metadatas"][0]

    # Verify sparse embedding matches updated document (single item batch)
    expected = sparse_ef(["updated doc"])[0]
    actual = cast(SparseVector, result_update["metadatas"][0]["update_sparse"])
    assert actual.indices == expected.indices

    # Upsert new document
    collection.upsert(
        ids=["up2"],
        documents=["upserted doc"],
    )

    result_upsert = collection.get(ids=["up2"], include=["metadatas"])
    assert result_upsert["metadatas"] is not None
    assert "update_sparse" in result_upsert["metadatas"][0]

    # Single item batch
    expected_upsert = sparse_ef(["upserted doc"])[0]
    actual_upsert = cast(SparseVector, result_upsert["metadatas"][0]["update_sparse"])
    assert actual_upsert.indices == expected_upsert.indices


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_persistence_across_client_reload(
    client_factories: "ClientFactories",
) -> None:
    """Test that sparse auto-embedding config persists across client reloads."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="persist_test")

    schema = Schema().create_index(
        key="persist_sparse",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, client = _create_isolated_collection(client_factories, schema=schema)
    collection_name = collection.name

    collection.add(
        ids=["persist1"],
        documents=["persistent document"],
    )

    # Reload client
    reloaded_client = client_factories.create_client_from_system()
    reloaded_collection = reloaded_client.get_collection(
        name=collection_name,
    )

    # Verify schema persisted
    assert reloaded_collection.schema is not None
    assert "persist_sparse" in reloaded_collection.schema.keys

    # Add new document with reloaded collection
    reloaded_collection.add(
        ids=["persist2"],
        documents=["new document after reload"],
    )

    # Verify both documents have sparse embeddings
    result = reloaded_collection.get(
        ids=["persist1", "persist2"],
        include=["metadatas"],
    )
    assert result["metadatas"] is not None
    assert "persist_sparse" in result["metadatas"][0]
    assert "persist_sparse" in result["metadatas"][1]


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_batch_operations(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse auto-embedding with large batch of documents."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="batch_test")

    schema = Schema().create_index(
        key="batch_sparse",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    # Add large batch
    batch_size = 100
    ids = [f"batch-{i}" for i in range(batch_size)]
    documents = [f"document number {i}" for i in range(batch_size)]

    collection.add(ids=ids, documents=documents)

    # Verify all have sparse embeddings
    result = collection.get(ids=ids[:10], include=["metadatas"])
    assert result["metadatas"] is not None

    # Expected from batch call (batch of 100, we check first 10)
    expected_batch = sparse_ef(documents)

    for i, metadata in enumerate(result["metadatas"]):
        assert metadata is not None
        assert "batch_sparse" in metadata

        actual = cast(SparseVector, metadata["batch_sparse"])
        assert actual.indices == expected_batch[i].indices


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_sparse_auto_embedding_with_empty_documents(
    client_factories: "ClientFactories",
) -> None:
    """Test sparse auto-embedding handles empty/None documents gracefully."""
    sparse_ef = DeterministicSparseEmbeddingFunction(label="empty_test")

    schema = Schema().create_index(
        key="empty_sparse",
        config=SparseVectorIndexConfig(
            source_key="#document",
            embedding_function=sparse_ef,
        ),
    )

    collection, _ = _create_isolated_collection(client_factories, schema=schema)

    # Add with empty string document
    collection.add(
        ids=["empty1"],
        documents=[""],
    )

    result = collection.get(ids=["empty1"], include=["metadatas"])
    assert result["metadatas"] is not None

    # Should still generate sparse embedding (empty vector)
    metadata = result["metadatas"][0]
    assert metadata is not None
    assert "empty_sparse" in metadata
