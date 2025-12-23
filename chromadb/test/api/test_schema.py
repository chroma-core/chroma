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
    Cmek,
    CmekProvider,
)
from chromadb.execution.expression.operator import Key
from typing import List, Dict, Any
from pydantic import ValidationError
import pytest


class MockSparseEmbeddingFunction(SparseEmbeddingFunction[List[str]]):
    """Mock sparse embedding function for testing."""

    def __init__(self, name: str = "mock_sparse"):
        self._name = name

    def __call__(self, input: List[str]) -> List[SparseVector]:
        return [SparseVector(indices=[0, 1], values=[1.0, 1.0]) for _ in input]

    @staticmethod
    def name() -> str:
        return "mock_sparse"

    def get_config(self) -> Dict[str, Any]:
        return {"name": self._name}

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "MockSparseEmbeddingFunction":
        return MockSparseEmbeddingFunction(config.get("name", "mock_sparse"))


class MockEmbeddingFunction(EmbeddingFunction[List[str]]):
    """Mock embedding function for testing."""

    def __init__(self, model_name: str = "mock_model"):
        self._model_name = model_name

    def __call__(self, input: List[str]) -> Embeddings:
        import numpy as np

        # Return mock embeddings (3-dimensional)
        return [np.array([1.0, 2.0, 3.0], dtype=np.float32) for _ in input]

    @staticmethod
    def name() -> str:
        return "mock_embedding"

    def get_config(self) -> Dict[str, Any]:
        return {"model_name": self._model_name}

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "MockEmbeddingFunction":
        return MockEmbeddingFunction(config.get("model_name", "mock_model"))

    def default_space(self) -> str:  # type: ignore
        return "cosine"

    def supported_spaces(self) -> List[str]:  # type: ignore
        return ["cosine", "l2", "ip"]


class TestNewSchema:
    """Test cases for the new Schema class."""

    def test_default_schema_initialization(self) -> None:
        """Test that Schema() initializes with correct defaults."""
        schema = Schema()

        # Verify defaults are populated
        assert schema.defaults is not None

        # Verify string value type defaults
        assert schema.defaults.string is not None
        assert schema.defaults.string.fts_index is not None
        assert schema.defaults.string.fts_index.enabled is False  # Disabled by default
        assert schema.defaults.string.string_inverted_index is not None
        assert (
            schema.defaults.string.string_inverted_index.enabled is True
        )  # Enabled by default

        # Verify float_list value type defaults
        assert schema.defaults.float_list is not None
        assert schema.defaults.float_list.vector_index is not None
        assert (
            schema.defaults.float_list.vector_index.enabled is False
        )  # Disabled by default

        # Verify sparse_vector value type defaults
        assert schema.defaults.sparse_vector is not None
        assert schema.defaults.sparse_vector.sparse_vector_index is not None
        assert (
            schema.defaults.sparse_vector.sparse_vector_index.enabled is False
        )  # Disabled by default

        # Verify int_value type defaults
        assert schema.defaults.int_value is not None
        assert schema.defaults.int_value.int_inverted_index is not None
        assert (
            schema.defaults.int_value.int_inverted_index.enabled is True
        )  # Enabled by default

        # Verify float_value type defaults
        assert schema.defaults.float_value is not None
        assert schema.defaults.float_value.float_inverted_index is not None
        assert (
            schema.defaults.float_value.float_inverted_index.enabled is True
        )  # Enabled by default

        # Verify boolean type defaults
        assert schema.defaults.boolean is not None
        assert schema.defaults.boolean.bool_inverted_index is not None
        assert (
            schema.defaults.boolean.bool_inverted_index.enabled is True
        )  # Enabled by default

        # Verify keys are populated
        assert schema.keys is not None
        assert len(schema.keys) == 2  # Should have #document and #embedding

        # Verify #document key override (FTS enabled, string inverted disabled)
        assert "#document" in schema.keys
        assert schema.keys["#document"].string is not None
        assert schema.keys["#document"].string.fts_index is not None
        assert schema.keys["#document"].string.fts_index.enabled is True
        assert schema.keys["#document"].string.string_inverted_index is not None
        assert schema.keys["#document"].string.string_inverted_index.enabled is False

        # Verify #embedding key override (vector index enabled)
        assert "#embedding" in schema.keys
        assert schema.keys["#embedding"].float_list is not None
        assert schema.keys["#embedding"].float_list.vector_index is not None
        assert schema.keys["#embedding"].float_list.vector_index.enabled is True
        assert (
            schema.keys["#embedding"].float_list.vector_index.config.source_key
            == "#document"
        )

    def test_create_sparse_vector_index_on_key(self) -> None:
        """Test creating a sparse vector index on a specific key with default config."""
        schema = Schema()

        # Create sparse vector index on a custom key with default config
        config = SparseVectorIndexConfig()
        result = schema.create_index(config=config, key="custom_sparse_key")

        # Should return self for chaining
        assert result is schema

        # Verify the key override was created
        assert "custom_sparse_key" in schema.keys

        # Verify sparse_vector type was set for this key
        assert schema.keys["custom_sparse_key"].sparse_vector is not None
        assert (
            schema.keys["custom_sparse_key"].sparse_vector.sparse_vector_index
            is not None
        )

        # Verify it's enabled and has the correct config
        assert (
            schema.keys["custom_sparse_key"].sparse_vector.sparse_vector_index.enabled
            is True
        )
        assert (
            schema.keys["custom_sparse_key"].sparse_vector.sparse_vector_index.config
            == config
        )

        # Verify other value types for this key are None (not initialized)
        assert schema.keys["custom_sparse_key"].string is None
        assert schema.keys["custom_sparse_key"].float_list is None
        assert schema.keys["custom_sparse_key"].int_value is None
        assert schema.keys["custom_sparse_key"].float_value is None
        assert schema.keys["custom_sparse_key"].boolean is None

        # Verify defaults were not affected
        assert schema.defaults.sparse_vector is not None
        assert schema.defaults.sparse_vector.sparse_vector_index is not None
        assert (
            schema.defaults.sparse_vector.sparse_vector_index.enabled is False
        )  # Still disabled by default

    def test_create_sparse_vector_index_with_custom_config(self) -> None:
        """Test creating a sparse vector index with custom config including embedding function."""
        schema = Schema()

        # Create custom sparse vector config with embedding function and source key
        embedding_func = MockSparseEmbeddingFunction(name="custom_sparse_ef")
        config = SparseVectorIndexConfig(
            embedding_function=embedding_func, source_key="custom_document_field"
        )

        # Create sparse vector index on a custom key
        result = schema.create_index(config=config, key="sparse_embeddings")

        # Should return self for chaining
        assert result is schema

        # Verify the key override was created
        assert "sparse_embeddings" in schema.keys
        assert schema.keys["sparse_embeddings"].sparse_vector is not None
        assert (
            schema.keys["sparse_embeddings"].sparse_vector.sparse_vector_index
            is not None
        )

        # Verify it's enabled
        sparse_index = schema.keys[
            "sparse_embeddings"
        ].sparse_vector.sparse_vector_index
        assert sparse_index.enabled is True

        # Verify the config has our custom settings
        assert sparse_index.config.embedding_function == embedding_func
        assert sparse_index.config.source_key == "custom_document_field"

        # Verify the embedding function is the same instance
        assert sparse_index.config.embedding_function.name() == "mock_sparse"
        assert sparse_index.config.embedding_function.get_config() == {
            "name": "custom_sparse_ef"
        }

        # Verify global defaults were not overridden
        assert schema.defaults.sparse_vector is not None
        assert schema.defaults.sparse_vector.sparse_vector_index is not None
        assert (
            schema.defaults.sparse_vector.sparse_vector_index.enabled is False
        )  # Still disabled by default
        assert (
            schema.defaults.sparse_vector.sparse_vector_index.config.embedding_function
            is None
        )  # No custom embedding function

    def test_delete_index_on_key(self) -> None:
        """Test disabling string inverted index on a specific key."""
        schema = Schema()

        # Create a config and disable it on a specific key
        config = StringInvertedIndexConfig()
        result = schema.delete_index(config=config, key="custom_text_key")

        # Should return self for chaining
        assert result is schema

        # Verify the key override was created
        assert "custom_text_key" in schema.keys

        # Verify string inverted index is disabled for this key
        assert schema.keys["custom_text_key"].string is not None
        assert schema.keys["custom_text_key"].string.string_inverted_index is not None
        assert (
            schema.keys["custom_text_key"].string.string_inverted_index.enabled is False
        )

        # Verify other keys are not affected - check #document key
        assert "#document" in schema.keys
        assert schema.keys["#document"].string is not None
        assert schema.keys["#document"].string.string_inverted_index is not None
        assert (
            schema.keys["#document"].string.string_inverted_index.enabled is False
        )  # Was disabled by default in #document

        # Verify other keys are not affected - check #embedding key (shouldn't have string config)
        assert "#embedding" in schema.keys
        assert (
            schema.keys["#embedding"].string is None
        )  # #embedding doesn't have string configs

        # Verify global defaults are not affected
        assert schema.defaults.string is not None
        assert schema.defaults.string.string_inverted_index is not None
        assert (
            schema.defaults.string.string_inverted_index.enabled is True
        )  # Global default is still enabled

    def test_chained_create_and_delete_operations(self) -> None:
        """Test chaining create_index() and delete_index() operations together."""
        schema = Schema()

        # Chain multiple operations:
        # 1. Create sparse vector index on "embeddings_key"
        # 2. Disable string inverted index on "text_key_1"
        # 3. Disable string inverted index on "text_key_2"
        sparse_config = SparseVectorIndexConfig(
            source_key="raw_text", embedding_function=MockSparseEmbeddingFunction()
        )
        string_config = StringInvertedIndexConfig()

        result = (
            schema.create_index(config=sparse_config, key="embeddings_key")
            .delete_index(config=string_config, key="text_key_1")
            .delete_index(config=string_config, key="text_key_2")
        )

        # Should return self for chaining
        assert result is schema

        # Verify all three key overrides were created
        assert "embeddings_key" in schema.keys
        assert "text_key_1" in schema.keys
        assert "text_key_2" in schema.keys

        # Verify sparse vector index on "embeddings_key" is enabled
        assert schema.keys["embeddings_key"].sparse_vector is not None
        assert (
            schema.keys["embeddings_key"].sparse_vector.sparse_vector_index is not None
        )
        assert (
            schema.keys["embeddings_key"].sparse_vector.sparse_vector_index.enabled
            is True
        )
        assert (
            schema.keys[
                "embeddings_key"
            ].sparse_vector.sparse_vector_index.config.source_key
            == "raw_text"
        )

        # Verify only sparse_vector is set for embeddings_key (other types are None)
        assert schema.keys["embeddings_key"].string is None
        assert schema.keys["embeddings_key"].float_list is None
        assert schema.keys["embeddings_key"].int_value is None
        assert schema.keys["embeddings_key"].float_value is None
        assert schema.keys["embeddings_key"].boolean is None

        # Verify string inverted index on "text_key_1" is disabled
        assert schema.keys["text_key_1"].string is not None
        assert schema.keys["text_key_1"].string.string_inverted_index is not None
        assert schema.keys["text_key_1"].string.string_inverted_index.enabled is False

        # Verify only string is set for text_key_1 (other types are None)
        assert schema.keys["text_key_1"].sparse_vector is None
        assert schema.keys["text_key_1"].float_list is None
        assert schema.keys["text_key_1"].int_value is None
        assert schema.keys["text_key_1"].float_value is None
        assert schema.keys["text_key_1"].boolean is None

        # Verify string inverted index on "text_key_2" is disabled
        assert schema.keys["text_key_2"].string is not None
        assert schema.keys["text_key_2"].string.string_inverted_index is not None
        assert schema.keys["text_key_2"].string.string_inverted_index.enabled is False

        # Verify only string is set for text_key_2 (other types are None)
        assert schema.keys["text_key_2"].sparse_vector is None
        assert schema.keys["text_key_2"].float_list is None
        assert schema.keys["text_key_2"].int_value is None
        assert schema.keys["text_key_2"].float_value is None
        assert schema.keys["text_key_2"].boolean is None

        # Verify global defaults are not affected
        assert schema.defaults.sparse_vector is not None
        assert schema.defaults.sparse_vector.sparse_vector_index is not None
        assert (
            schema.defaults.sparse_vector.sparse_vector_index.enabled is False
        )  # Still disabled globally

        assert schema.defaults.string is not None
        assert schema.defaults.string.string_inverted_index is not None
        assert (
            schema.defaults.string.string_inverted_index.enabled is True
        )  # Still enabled globally

        # Verify pre-existing key overrides (#document, #embedding) are not affected
        assert "#document" in schema.keys
        assert "#embedding" in schema.keys
        assert schema.keys["#document"].string is not None
        assert schema.keys["#document"].string.fts_index is not None
        assert (
            schema.keys["#document"].string.fts_index.enabled is True
        )  # Still enabled
        assert schema.keys["#embedding"].float_list is not None
        assert schema.keys["#embedding"].float_list.vector_index is not None
        assert (
            schema.keys["#embedding"].float_list.vector_index.enabled is True
        )  # Still enabled

    def test_vector_index_config_and_restrictions(self) -> None:
        """Test vector index configuration and key restrictions."""
        schema = Schema()
        vector_config = VectorIndexConfig(space="cosine", source_key="custom_source")

        # Test 1: CAN set vector config globally - applies to defaults and #embedding
        result = schema.create_index(config=vector_config)
        assert result is schema  # Should return self for chaining

        # Verify the vector config was applied to defaults (enabled state preserved as False)
        assert schema.defaults.float_list is not None
        assert schema.defaults.float_list.vector_index is not None
        assert (
            schema.defaults.float_list.vector_index.enabled is False
        )  # Still disabled in defaults
        assert schema.defaults.float_list.vector_index.config.space == "cosine"
        assert (
            schema.defaults.float_list.vector_index.config.source_key == "custom_source"
        )

        # Verify the vector config was also applied to #embedding (enabled state preserved as True)
        # Note: source_key should NOT be overridden on #embedding - it should stay as "#document"
        assert schema.keys["#embedding"].float_list is not None
        assert schema.keys["#embedding"].float_list.vector_index is not None
        assert (
            schema.keys["#embedding"].float_list.vector_index.enabled is True
        )  # Still enabled on #embedding
        assert (
            schema.keys["#embedding"].float_list.vector_index.config.space == "cosine"
        )
        assert (
            schema.keys["#embedding"].float_list.vector_index.config.source_key
            == "#document"
        )  # Preserved, NOT overridden

        # Test 2: Cannot create vector index on custom key
        vector_config2 = VectorIndexConfig(space="l2")
        with pytest.raises(
            ValueError, match="Vector index cannot be enabled on specific keys"
        ):
            schema.create_index(config=vector_config2, key="my_vectors")

        # Test 3: Cannot create vector index on #document key (special key blocked globally)
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#document'"
        ):
            schema.create_index(config=vector_config2, key="#document")

        # Test 4: Cannot create vector index on #embedding key (special key blocked globally)
        vector_config3 = VectorIndexConfig(space="ip")
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#embedding'"
        ):
            schema.create_index(config=vector_config3, key="#embedding")

    def test_vector_index_with_embedding_function_and_hnsw(self) -> None:
        """Test setting embedding function and HNSW config for vector index."""
        schema = Schema()

        # Create a custom embedding function and HNSW config
        mock_ef = MockEmbeddingFunction(model_name="custom_model_v2")
        hnsw_config = HnswIndexConfig(
            ef_construction=200, max_neighbors=32, ef_search=100
        )

        # Set vector config with embedding function, space, and HNSW config
        vector_config = VectorIndexConfig(
            embedding_function=mock_ef,
            space="l2",  # Override default space from EF
            hnsw=hnsw_config,
            source_key="custom_document_field",
        )

        result = schema.create_index(config=vector_config)
        assert result is schema

        # Verify defaults: should have EF, space, HNSW, and source_key
        assert schema.defaults.float_list is not None
        defaults_vector = schema.defaults.float_list.vector_index
        assert defaults_vector is not None
        assert defaults_vector.enabled is False
        assert defaults_vector.config.embedding_function is mock_ef
        assert defaults_vector.config.embedding_function.name() == "mock_embedding"
        assert defaults_vector.config.embedding_function.get_config() == {
            "model_name": "custom_model_v2"
        }
        assert defaults_vector.config.space == "l2"
        assert defaults_vector.config.hnsw is not None
        assert defaults_vector.config.hnsw.ef_construction == 200
        assert defaults_vector.config.hnsw.max_neighbors == 32
        assert defaults_vector.config.hnsw.ef_search == 100
        assert defaults_vector.config.source_key == "custom_document_field"

        # Verify #embedding: should have EF, space, HNSW, but source_key is preserved as "#document"
        assert schema.keys["#embedding"].float_list is not None
        embedding_vector = schema.keys["#embedding"].float_list.vector_index
        assert embedding_vector is not None
        assert embedding_vector.enabled is True
        assert embedding_vector.config.embedding_function is mock_ef
        assert embedding_vector.config.space == "l2"
        assert embedding_vector.config.hnsw is not None
        assert embedding_vector.config.hnsw.ef_construction == 200
        assert (
            embedding_vector.config.source_key == "#document"
        )  # Preserved, NOT overridden by user config

    def test_fts_index_config_and_restrictions(self) -> None:
        """Test FTS index configuration and key restrictions."""
        schema = Schema()
        fts_config = FtsIndexConfig()

        # Test 1: CAN set FTS config globally - applies to defaults and #document
        result = schema.create_index(config=fts_config)
        assert result is schema  # Should return self for chaining

        # Verify the FTS config was applied to defaults (enabled state preserved as False)
        assert schema.defaults.string is not None
        assert schema.defaults.string.fts_index is not None
        assert (
            schema.defaults.string.fts_index.enabled is False
        )  # Still disabled in defaults
        assert schema.defaults.string.fts_index.config == fts_config

        # Verify the FTS config was also applied to #document (enabled state preserved as True)
        assert schema.keys["#document"].string is not None
        assert schema.keys["#document"].string.fts_index is not None
        assert (
            schema.keys["#document"].string.fts_index.enabled is True
        )  # Still enabled on #document
        assert schema.keys["#document"].string.fts_index.config == fts_config

        # Test 2: Cannot create FTS index on custom key
        fts_config2 = FtsIndexConfig()
        with pytest.raises(
            ValueError, match="FTS index cannot be enabled on specific keys"
        ):
            schema.create_index(config=fts_config2, key="custom_text_field")

        # Test 3: Cannot create FTS index on #embedding key (special key blocked globally)
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#embedding'"
        ):
            schema.create_index(config=fts_config2, key="#embedding")

        # Test 4: Cannot create FTS index on #document key (special key blocked globally)
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#document'"
        ):
            schema.create_index(config=fts_config2, key="#document")

    def test_special_keys_blocked_for_all_index_types(self) -> None:
        """Test that #embedding and #document keys are blocked for all index types."""
        schema = Schema()

        # Test with StringInvertedIndexConfig on #document
        string_config = StringInvertedIndexConfig()
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#document'"
        ):
            schema.create_index(config=string_config, key="#document")

        # Test with StringInvertedIndexConfig on #embedding
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#embedding'"
        ):
            schema.create_index(config=string_config, key="#embedding")

        # Test with SparseVectorIndexConfig on #document
        sparse_config = SparseVectorIndexConfig()
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#document'"
        ):
            schema.create_index(config=sparse_config, key="#document")

        # Test with SparseVectorIndexConfig on #embedding
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#embedding'"
        ):
            schema.create_index(config=sparse_config, key="#embedding")

    def test_cannot_enable_all_indexes_for_key(self) -> None:
        """Test that enabling all indexes for a key is not allowed."""
        schema = Schema()

        # Try to enable all indexes for a custom key (config=None, key="my_key")
        with pytest.raises(
            ValueError, match="Cannot enable all index types for key 'my_key'"
        ):
            schema.create_index(key="my_key")

        # Try to disable all indexes for a custom key (config=None, key="my_key")
        with pytest.raises(
            ValueError, match="Cannot disable all index types for key 'my_key'"
        ):
            schema.delete_index(key="my_key")

    def test_cannot_delete_vector_or_fts_index(self) -> None:
        """Test that deleting vector and FTS indexes is not allowed."""
        schema = Schema()

        # Try to delete vector index globally
        vector_config = VectorIndexConfig()
        with pytest.raises(
            ValueError, match="Deleting vector index is not currently supported"
        ):
            schema.delete_index(config=vector_config)

        # Try to delete vector index on a custom key
        with pytest.raises(
            ValueError, match="Deleting vector index is not currently supported"
        ):
            schema.delete_index(config=vector_config, key="my_vectors")

        # Try to delete FTS index globally
        fts_config = FtsIndexConfig()
        with pytest.raises(
            ValueError, match="Deleting FTS index is not currently supported"
        ):
            schema.delete_index(config=fts_config)

        # Try to delete FTS index on a custom key
        with pytest.raises(
            ValueError, match="Deleting FTS index is not currently supported"
        ):
            schema.delete_index(config=fts_config, key="my_text")

    def test_disable_string_inverted_index_globally(self) -> None:
        """Test disabling string inverted index globally."""
        schema = Schema()

        # Verify string inverted index is enabled by default in global defaults
        assert schema.defaults.string is not None
        assert schema.defaults.string.string_inverted_index is not None
        assert schema.defaults.string.string_inverted_index.enabled is True

        # Disable string inverted index globally
        string_config = StringInvertedIndexConfig()
        result = schema.delete_index(config=string_config)
        assert result is schema  # Should return self for chaining

        # Verify it's now disabled in defaults
        assert schema.defaults.string.string_inverted_index is not None
        assert schema.defaults.string.string_inverted_index.enabled is False
        assert schema.defaults.string.string_inverted_index.config == string_config

        # Verify key overrides are not affected (e.g., #document still has its config)
        assert schema.keys["#document"].string is not None
        assert schema.keys["#document"].string.string_inverted_index is not None
        assert (
            schema.keys["#document"].string.string_inverted_index.enabled is False
        )  # #document has it disabled

    def test_disable_string_inverted_index_on_key(self) -> None:
        """Test disabling string inverted index on a specific key."""
        schema = Schema()

        # Disable string inverted index on a custom key
        string_config = StringInvertedIndexConfig()
        result = schema.delete_index(config=string_config, key="my_text_field")
        assert result is schema

        # Verify it's disabled on the custom key
        assert "my_text_field" in schema.keys
        assert schema.keys["my_text_field"].string is not None
        assert schema.keys["my_text_field"].string.string_inverted_index is not None
        assert (
            schema.keys["my_text_field"].string.string_inverted_index.enabled is False
        )
        assert (
            schema.keys["my_text_field"].string.string_inverted_index.config
            == string_config
        )

        # Verify other value types on this key are None (sparse override)
        assert schema.keys["my_text_field"].float_list is None
        assert schema.keys["my_text_field"].sparse_vector is None
        assert schema.keys["my_text_field"].int_value is None

        # Verify global defaults are not affected
        assert schema.defaults.string is not None
        assert schema.defaults.string.string_inverted_index is not None
        assert schema.defaults.string.string_inverted_index.enabled is True

        # Verify other key overrides are not affected
        assert schema.keys["#document"].string is not None
        assert schema.keys["#document"].string.string_inverted_index is not None
        assert schema.keys["#document"].string.string_inverted_index.enabled is False
        assert schema.keys["#embedding"].float_list is not None
        assert schema.keys["#embedding"].float_list.vector_index is not None
        assert schema.keys["#embedding"].float_list.vector_index.enabled is True

    def test_disable_int_inverted_index(self) -> None:
        """Test disabling int inverted index globally and on a specific key."""
        schema = Schema()

        # Verify int inverted index is enabled by default
        assert schema.defaults.int_value is not None
        assert schema.defaults.int_value.int_inverted_index is not None
        assert schema.defaults.int_value.int_inverted_index.enabled is True

        # Test 1: Disable int inverted index globally
        int_config = IntInvertedIndexConfig()
        result = schema.delete_index(config=int_config)
        assert result is schema

        # Verify it's now disabled in defaults
        assert schema.defaults.int_value.int_inverted_index.enabled is False
        assert schema.defaults.int_value.int_inverted_index.config == int_config

        # Test 2: Disable int inverted index on a specific key
        int_config2 = IntInvertedIndexConfig()
        result = schema.delete_index(config=int_config2, key="age_field")
        assert result is schema

        # Verify it's disabled on the custom key
        assert "age_field" in schema.keys
        assert schema.keys["age_field"].int_value is not None
        assert schema.keys["age_field"].int_value.int_inverted_index is not None
        assert schema.keys["age_field"].int_value.int_inverted_index.enabled is False
        assert (
            schema.keys["age_field"].int_value.int_inverted_index.config == int_config2
        )

        # Verify sparse override (only int_value is set)
        assert schema.keys["age_field"].string is None
        assert schema.keys["age_field"].float_list is None
        assert schema.keys["age_field"].sparse_vector is None
        assert schema.keys["age_field"].float_value is None
        assert schema.keys["age_field"].boolean is None

        # Verify other keys are not affected
        assert schema.keys["#document"].string is not None
        assert schema.keys["#embedding"].float_list is not None

    def test_serialize_deserialize_default_schema(self) -> None:
        """Test serialization and deserialization of a default Schema."""
        # Create a default schema
        original = Schema()

        # Serialize to JSON
        json_data = original.serialize_to_json()

        # Verify the top-level structure
        assert "defaults" in json_data
        assert "keys" in json_data
        assert isinstance(json_data["defaults"], dict)
        assert isinstance(json_data["keys"], dict)

        # Verify defaults structure in detail
        defaults = json_data["defaults"]

        # Check string
        assert "string" in defaults
        assert "fts_index" in defaults["string"]
        assert defaults["string"]["fts_index"]["enabled"] is False
        assert defaults["string"]["fts_index"]["config"] == {}
        assert "string_inverted_index" in defaults["string"]
        assert defaults["string"]["string_inverted_index"]["enabled"] is True
        assert defaults["string"]["string_inverted_index"]["config"] == {}

        # Check float_list
        assert "float_list" in defaults
        assert "vector_index" in defaults["float_list"]
        assert defaults["float_list"]["vector_index"]["enabled"] is False
        vector_config = defaults["float_list"]["vector_index"]["config"]
        assert "space" in vector_config
        assert vector_config["space"] == "l2"  # Default space
        assert "embedding_function" in vector_config
        assert vector_config["embedding_function"]["type"] == "known"
        assert vector_config["embedding_function"]["name"] == "default"
        assert vector_config["embedding_function"]["config"] == {}

        # Check sparse_vector
        assert "sparse_vector" in defaults
        assert "sparse_vector_index" in defaults["sparse_vector"]
        assert defaults["sparse_vector"]["sparse_vector_index"]["enabled"] is False
        sparse_vector_config = defaults["sparse_vector"]["sparse_vector_index"][
            "config"
        ]
        # SparseVectorIndexConfig has embedding_function field with unknown default
        assert "embedding_function" in sparse_vector_config
        assert sparse_vector_config["embedding_function"] == {"type": "unknown"}

        # Check int
        assert "int" in defaults
        assert "int_inverted_index" in defaults["int"]
        assert defaults["int"]["int_inverted_index"]["enabled"] is True
        assert defaults["int"]["int_inverted_index"]["config"] == {}

        # Check float
        assert "float" in defaults
        assert "float_inverted_index" in defaults["float"]
        assert defaults["float"]["float_inverted_index"]["enabled"] is True
        assert defaults["float"]["float_inverted_index"]["config"] == {}

        # Check bool
        assert "bool" in defaults
        assert "bool_inverted_index" in defaults["bool"]
        assert defaults["bool"]["bool_inverted_index"]["enabled"] is True
        assert defaults["bool"]["bool_inverted_index"]["config"] == {}

        # Verify key overrides structure in detail
        keys = json_data["keys"]

        # Check #document
        assert "#document" in keys
        assert "string" in keys["#document"]
        assert "fts_index" in keys["#document"]["string"]
        assert keys["#document"]["string"]["fts_index"]["enabled"] is True
        assert keys["#document"]["string"]["fts_index"]["config"] == {}
        assert "string_inverted_index" in keys["#document"]["string"]
        assert keys["#document"]["string"]["string_inverted_index"]["enabled"] is False
        assert keys["#document"]["string"]["string_inverted_index"]["config"] == {}

        # Check #embedding
        assert "#embedding" in keys
        assert "float_list" in keys["#embedding"]
        assert "vector_index" in keys["#embedding"]["float_list"]
        assert keys["#embedding"]["float_list"]["vector_index"]["enabled"] is True
        embedding_vector_config = keys["#embedding"]["float_list"]["vector_index"][
            "config"
        ]
        assert "space" in embedding_vector_config
        assert embedding_vector_config["space"] == "l2"  # Default space
        assert "source_key" in embedding_vector_config
        assert embedding_vector_config["source_key"] == "#document"
        assert "embedding_function" in embedding_vector_config
        assert embedding_vector_config["embedding_function"]["type"] == "known"
        assert embedding_vector_config["embedding_function"]["name"] == "default"
        assert embedding_vector_config["embedding_function"]["config"] == {}

        # Deserialize back to Schema
        deserialized = Schema.deserialize_from_json(json_data)

        # Verify deserialized schema matches original - exhaustive validation
        # Check defaults.string
        assert deserialized.defaults.string is not None
        assert deserialized.defaults.string.fts_index is not None
        assert deserialized.defaults.string.fts_index.enabled is False
        assert (
            deserialized.defaults.string.fts_index.enabled
            == original.defaults.string.fts_index.enabled
        )  # type: ignore[union-attr]
        assert deserialized.defaults.string.string_inverted_index is not None
        assert deserialized.defaults.string.string_inverted_index.enabled is True
        assert (
            deserialized.defaults.string.string_inverted_index.enabled
            == original.defaults.string.string_inverted_index.enabled
        )  # type: ignore[union-attr]

        # Check defaults.float_list (vector index)
        assert deserialized.defaults.float_list is not None
        assert deserialized.defaults.float_list.vector_index is not None
        assert deserialized.defaults.float_list.vector_index.enabled is False
        assert (
            deserialized.defaults.float_list.vector_index.enabled
            == original.defaults.float_list.vector_index.enabled
        )  # type: ignore[union-attr]
        # Space is resolved during serialization, so deserialized has explicit value
        assert deserialized.defaults.float_list.vector_index.config.space == "l2"
        # Check embedding function is preserved
        assert (
            deserialized.defaults.float_list.vector_index.config.embedding_function
            is not None
        )
        assert (
            deserialized.defaults.float_list.vector_index.config.embedding_function.name()
            == "default"
        )
        assert (
            original.defaults.float_list.vector_index.config.embedding_function.name()
            == "default"
        )  # type: ignore[union-attr]

        # Check defaults.sparse_vector
        assert deserialized.defaults.sparse_vector is not None
        assert deserialized.defaults.sparse_vector.sparse_vector_index is not None
        assert deserialized.defaults.sparse_vector.sparse_vector_index.enabled is False
        assert (
            deserialized.defaults.sparse_vector.sparse_vector_index.enabled
            == original.defaults.sparse_vector.sparse_vector_index.enabled
        )  # type: ignore[union-attr]

        # Check defaults.int_value
        assert deserialized.defaults.int_value is not None
        assert deserialized.defaults.int_value.int_inverted_index is not None
        assert deserialized.defaults.int_value.int_inverted_index.enabled is True
        assert (
            deserialized.defaults.int_value.int_inverted_index.enabled
            == original.defaults.int_value.int_inverted_index.enabled
        )  # type: ignore[union-attr]

        # Check defaults.float_value
        assert deserialized.defaults.float_value is not None
        assert deserialized.defaults.float_value.float_inverted_index is not None
        assert deserialized.defaults.float_value.float_inverted_index.enabled is True
        assert (
            deserialized.defaults.float_value.float_inverted_index.enabled
            == original.defaults.float_value.float_inverted_index.enabled
        )  # type: ignore[union-attr]

        # Check defaults.boolean
        assert deserialized.defaults.boolean is not None
        assert deserialized.defaults.boolean.bool_inverted_index is not None
        assert deserialized.defaults.boolean.bool_inverted_index.enabled is True
        assert (
            deserialized.defaults.boolean.bool_inverted_index.enabled
            == original.defaults.boolean.bool_inverted_index.enabled
        )  # type: ignore[union-attr]

        # Check keys.#document
        assert "#document" in deserialized.keys
        assert deserialized.keys["#document"].string is not None
        assert deserialized.keys["#document"].string.fts_index is not None
        assert deserialized.keys["#document"].string.fts_index.enabled is True
        assert (
            deserialized.keys["#document"].string.fts_index.enabled
            == original.keys["#document"].string.fts_index.enabled
        )  # type: ignore[union-attr]
        assert deserialized.keys["#document"].string.string_inverted_index is not None
        assert (
            deserialized.keys["#document"].string.string_inverted_index.enabled is False
        )
        assert (
            deserialized.keys["#document"].string.string_inverted_index.enabled
            == original.keys["#document"].string.string_inverted_index.enabled
        )  # type: ignore[union-attr]

        # Check keys.#embedding
        assert "#embedding" in deserialized.keys
        assert deserialized.keys["#embedding"].float_list is not None
        assert deserialized.keys["#embedding"].float_list.vector_index is not None
        assert deserialized.keys["#embedding"].float_list.vector_index.enabled is True
        assert (
            deserialized.keys["#embedding"].float_list.vector_index.enabled
            == original.keys["#embedding"].float_list.vector_index.enabled
        )  # type: ignore[union-attr]
        # Verify source_key is preserved
        assert (
            deserialized.keys["#embedding"].float_list.vector_index.config.source_key
            == "#document"
        )
        assert (
            original.keys["#embedding"].float_list.vector_index.config.source_key
            == "#document"
        )  # type: ignore[union-attr]
        # Verify space is preserved (resolved during serialization)
        assert (
            deserialized.keys["#embedding"].float_list.vector_index.config.space == "l2"
        )
        # Verify embedding function is preserved
        assert (
            deserialized.keys[
                "#embedding"
            ].float_list.vector_index.config.embedding_function
            is not None
        )
        assert (
            deserialized.keys[
                "#embedding"
            ].float_list.vector_index.config.embedding_function.name()
            == "default"
        )
        assert (
            original.keys[
                "#embedding"
            ].float_list.vector_index.config.embedding_function.name()
            == "default"
        )  # type: ignore[union-attr]

    def test_serialize_deserialize_with_vector_config_no_ef(self) -> None:
        """Test serialization/deserialization of Schema with vector config where embedding_function=None."""
        # Create a default schema and modify vector config with ef=None
        original = Schema()
        vector_config = VectorIndexConfig(
            space="cosine",
            embedding_function=None,  # Explicitly set to None
        )
        original.create_index(config=vector_config)

        # Serialize to JSON
        json_data = original.serialize_to_json()

        # Verify defaults structure - vector index should reflect the changes
        defaults = json_data["defaults"]
        assert "float_list" in defaults
        assert "vector_index" in defaults["float_list"]
        vector_json = defaults["float_list"]["vector_index"]
        assert vector_json["enabled"] is False  # Still disabled in defaults
        assert vector_json["config"]["space"] == "cosine"  # User-specified space
        # When ef=None, it should serialize as legacy
        assert vector_json["config"]["embedding_function"]["type"] == "legacy"

        # Verify #embedding also has the updated config
        keys = json_data["keys"]
        assert "#embedding" in keys
        embedding_vector_json = keys["#embedding"]["float_list"]["vector_index"]
        assert embedding_vector_json["enabled"] is True  # Still enabled on #embedding
        assert (
            embedding_vector_json["config"]["space"] == "cosine"
        )  # User-specified space
        assert embedding_vector_json["config"]["source_key"] == "#document"  # Preserved
        # When ef=None, it should serialize as legacy
        assert embedding_vector_json["config"]["embedding_function"]["type"] == "legacy"

        # Deserialize back to Schema
        deserialized = Schema.deserialize_from_json(json_data)

        # Verify deserialized schema has the correct values
        # Check defaults.float_list (vector index)
        assert deserialized.defaults.float_list is not None
        assert deserialized.defaults.float_list.vector_index is not None
        assert deserialized.defaults.float_list.vector_index.enabled is False
        assert (
            deserialized.defaults.float_list.vector_index.config.space == "cosine"
        )  # User space preserved
        # ef=None should deserialize as None (legacy)
        assert (
            deserialized.defaults.float_list.vector_index.config.embedding_function
            is None
        )

        # Check #embedding vector index
        assert "#embedding" in deserialized.keys
        assert deserialized.keys["#embedding"].float_list is not None
        assert deserialized.keys["#embedding"].float_list.vector_index is not None
        assert deserialized.keys["#embedding"].float_list.vector_index.enabled is True
        assert (
            deserialized.keys["#embedding"].float_list.vector_index.config.space
            == "cosine"
        )  # User space preserved
        assert (
            deserialized.keys["#embedding"].float_list.vector_index.config.source_key
            == "#document"
        )  # Preserved
        # ef=None should deserialize as None (legacy)
        assert (
            deserialized.keys[
                "#embedding"
            ].float_list.vector_index.config.embedding_function
            is None
        )

    def test_serialize_deserialize_with_custom_ef(self) -> None:
        """Test serialization/deserialization of Schema with custom embedding function."""
        # Register the mock embedding function so it can be deserialized
        from chromadb.utils.embedding_functions import known_embedding_functions

        known_embedding_functions["mock_embedding"] = MockEmbeddingFunction

        try:
            # Create a default schema and modify vector config with custom EF
            original = Schema()
            custom_ef = MockEmbeddingFunction(model_name="custom_model_v3")
            hnsw_config = HnswIndexConfig(
                ef_construction=256, max_neighbors=48, ef_search=128
            )
            vector_config = VectorIndexConfig(
                embedding_function=custom_ef,
                space="ip",  # Inner product
                hnsw=hnsw_config,
            )
            original.create_index(config=vector_config)

            # Serialize to JSON
            json_data = original.serialize_to_json()

            # Verify defaults structure - vector index should reflect the changes
            defaults = json_data["defaults"]
            assert "float_list" in defaults
            assert "vector_index" in defaults["float_list"]
            vector_json = defaults["float_list"]["vector_index"]
            assert vector_json["enabled"] is False  # Still disabled in defaults
            assert vector_json["config"]["space"] == "ip"  # User-specified space
            # Custom EF should serialize as known type
            assert vector_json["config"]["embedding_function"]["type"] == "known"
            assert (
                vector_json["config"]["embedding_function"]["name"] == "mock_embedding"
            )
            assert (
                vector_json["config"]["embedding_function"]["config"]["model_name"]
                == "custom_model_v3"
            )
            # HNSW config should be present
            assert "hnsw" in vector_json["config"]
            assert vector_json["config"]["hnsw"]["ef_construction"] == 256
            assert vector_json["config"]["hnsw"]["max_neighbors"] == 48
            assert vector_json["config"]["hnsw"]["ef_search"] == 128

            # Verify #embedding also has the updated config
            keys = json_data["keys"]
            assert "#embedding" in keys
            embedding_vector_json = keys["#embedding"]["float_list"]["vector_index"]
            assert (
                embedding_vector_json["enabled"] is True
            )  # Still enabled on #embedding
            assert (
                embedding_vector_json["config"]["space"] == "ip"
            )  # User-specified space
            assert (
                embedding_vector_json["config"]["source_key"] == "#document"
            )  # Preserved
            # Custom EF should serialize as known type
            assert (
                embedding_vector_json["config"]["embedding_function"]["type"] == "known"
            )
            assert (
                embedding_vector_json["config"]["embedding_function"]["name"]
                == "mock_embedding"
            )
            assert (
                embedding_vector_json["config"]["embedding_function"]["config"][
                    "model_name"
                ]
                == "custom_model_v3"
            )
            # HNSW config should be present
            assert "hnsw" in embedding_vector_json["config"]
            assert embedding_vector_json["config"]["hnsw"]["ef_construction"] == 256
            assert embedding_vector_json["config"]["hnsw"]["max_neighbors"] == 48
            assert embedding_vector_json["config"]["hnsw"]["ef_search"] == 128

            # Deserialize back to Schema
            deserialized = Schema.deserialize_from_json(json_data)

            # Verify deserialized schema has the correct values
            # Check defaults.float_list (vector index)
            assert deserialized.defaults.float_list is not None
            assert deserialized.defaults.float_list.vector_index is not None
            assert deserialized.defaults.float_list.vector_index.enabled is False
            assert (
                deserialized.defaults.float_list.vector_index.config.space == "ip"
            )  # User space preserved
            # Custom EF should be reconstructed
            assert (
                deserialized.defaults.float_list.vector_index.config.embedding_function
                is not None
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.embedding_function.name()
                == "mock_embedding"
            )
            # Verify the EF config is correct
            ef_config = deserialized.defaults.float_list.vector_index.config.embedding_function.get_config()
            assert ef_config["model_name"] == "custom_model_v3"
            # HNSW config should be preserved
            assert deserialized.defaults.float_list.vector_index.config.hnsw is not None
            assert (
                deserialized.defaults.float_list.vector_index.config.hnsw.ef_construction
                == 256
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.hnsw.max_neighbors
                == 48
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.hnsw.ef_search
                == 128
            )

            # Check #embedding vector index
            assert "#embedding" in deserialized.keys
            assert deserialized.keys["#embedding"].float_list is not None
            assert deserialized.keys["#embedding"].float_list.vector_index is not None
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.enabled is True
            )
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.space
                == "ip"
            )  # User space preserved
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.source_key
                == "#document"
            )  # Preserved
            # Custom EF should be reconstructed
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.embedding_function
                is not None
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.embedding_function.name()
                == "mock_embedding"
            )
            # Verify the EF config is correct
            ef_config_embedding = deserialized.keys[
                "#embedding"
            ].float_list.vector_index.config.embedding_function.get_config()
            assert ef_config_embedding["model_name"] == "custom_model_v3"
            # HNSW config should be preserved
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.hnsw
                is not None
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.hnsw.ef_construction
                == 256
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.hnsw.max_neighbors
                == 48
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.hnsw.ef_search
                == 128
            )
        finally:
            # Clean up: remove the mock function from known_embedding_functions
            if "mock_embedding" in known_embedding_functions:
                del known_embedding_functions["mock_embedding"]

    def test_serialize_deserialize_with_spann_config(self) -> None:
        """Test serialization/deserialization of Schema with SPANN index config."""
        # Register the mock embedding function so it can be deserialized
        from chromadb.utils.embedding_functions import known_embedding_functions

        known_embedding_functions["mock_embedding"] = MockEmbeddingFunction

        try:
            # Create a default schema and modify vector config with SPANN
            original = Schema()
            custom_ef = MockEmbeddingFunction(model_name="spann_model")
            spann_config = SpannIndexConfig(
                search_nprobe=100, write_nprobe=50, ef_construction=200, ef_search=150
            )
            vector_config = VectorIndexConfig(
                embedding_function=custom_ef, space="cosine", spann=spann_config
            )
            original.create_index(config=vector_config)

            # Serialize to JSON
            json_data = original.serialize_to_json()

            # Verify defaults structure - vector index should reflect the changes
            defaults = json_data["defaults"]
            assert "float_list" in defaults
            assert "vector_index" in defaults["float_list"]
            vector_json = defaults["float_list"]["vector_index"]
            assert vector_json["enabled"] is False  # Still disabled in defaults
            assert vector_json["config"]["space"] == "cosine"  # User-specified space
            # Custom EF should serialize as known type
            assert vector_json["config"]["embedding_function"]["type"] == "known"
            assert (
                vector_json["config"]["embedding_function"]["name"] == "mock_embedding"
            )
            assert (
                vector_json["config"]["embedding_function"]["config"]["model_name"]
                == "spann_model"
            )
            # SPANN config should be present
            assert "spann" in vector_json["config"]
            assert vector_json["config"]["spann"]["search_nprobe"] == 100
            assert vector_json["config"]["spann"]["write_nprobe"] == 50
            assert vector_json["config"]["spann"]["ef_construction"] == 200
            assert vector_json["config"]["spann"]["ef_search"] == 150
            # HNSW should not be present
            assert vector_json["config"].get("hnsw") is None

            # Verify #embedding also has the updated config
            keys = json_data["keys"]
            assert "#embedding" in keys
            embedding_vector_json = keys["#embedding"]["float_list"]["vector_index"]
            assert (
                embedding_vector_json["enabled"] is True
            )  # Still enabled on #embedding
            assert (
                embedding_vector_json["config"]["space"] == "cosine"
            )  # User-specified space
            assert (
                embedding_vector_json["config"]["source_key"] == "#document"
            )  # Preserved
            # Custom EF should serialize as known type
            assert (
                embedding_vector_json["config"]["embedding_function"]["type"] == "known"
            )
            assert (
                embedding_vector_json["config"]["embedding_function"]["name"]
                == "mock_embedding"
            )
            assert (
                embedding_vector_json["config"]["embedding_function"]["config"][
                    "model_name"
                ]
                == "spann_model"
            )
            # SPANN config should be present
            assert "spann" in embedding_vector_json["config"]
            assert embedding_vector_json["config"]["spann"]["search_nprobe"] == 100
            assert embedding_vector_json["config"]["spann"]["write_nprobe"] == 50
            assert embedding_vector_json["config"]["spann"]["ef_construction"] == 200
            assert embedding_vector_json["config"]["spann"]["ef_search"] == 150
            # HNSW should not be present
            assert embedding_vector_json["config"].get("hnsw") is None

            # Deserialize back to Schema
            deserialized = Schema.deserialize_from_json(json_data)

            # Verify deserialized schema has the correct values
            # Check defaults.float_list (vector index)
            assert deserialized.defaults.float_list is not None
            assert deserialized.defaults.float_list.vector_index is not None
            assert deserialized.defaults.float_list.vector_index.enabled is False
            assert (
                deserialized.defaults.float_list.vector_index.config.space == "cosine"
            )  # User space preserved
            # Custom EF should be reconstructed
            assert (
                deserialized.defaults.float_list.vector_index.config.embedding_function
                is not None
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.embedding_function.name()
                == "mock_embedding"
            )
            # Verify the EF config is correct
            ef_config = deserialized.defaults.float_list.vector_index.config.embedding_function.get_config()
            assert ef_config["model_name"] == "spann_model"
            # SPANN config should be preserved
            assert (
                deserialized.defaults.float_list.vector_index.config.spann is not None
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.spann.search_nprobe
                == 100
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.spann.write_nprobe
                == 50
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.spann.ef_construction
                == 200
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.spann.ef_search
                == 150
            )
            # HNSW should be None
            assert deserialized.defaults.float_list.vector_index.config.hnsw is None

            # Check #embedding vector index
            assert "#embedding" in deserialized.keys
            assert deserialized.keys["#embedding"].float_list is not None
            assert deserialized.keys["#embedding"].float_list.vector_index is not None
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.enabled is True
            )
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.space
                == "cosine"
            )  # User space preserved
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.source_key
                == "#document"
            )  # Preserved
            # Custom EF should be reconstructed
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.embedding_function
                is not None
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.embedding_function.name()
                == "mock_embedding"
            )
            # Verify the EF config is correct
            ef_config_embedding = deserialized.keys[
                "#embedding"
            ].float_list.vector_index.config.embedding_function.get_config()
            assert ef_config_embedding["model_name"] == "spann_model"
            # SPANN config should be preserved
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.spann
                is not None
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.spann.search_nprobe
                == 100
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.spann.write_nprobe
                == 50
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.spann.ef_construction
                == 200
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.spann.ef_search
                == 150
            )
            # HNSW should be None
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.hnsw
                is None
            )
        finally:
            # Clean up: remove the mock function from known_embedding_functions
            if "mock_embedding" in known_embedding_functions:
                del known_embedding_functions["mock_embedding"]

    def test_serialize_deserialize_complex_mixed_modifications(self) -> None:
        """Test serialization/deserialization with multiple mixed schema modifications."""
        # Register the mock embedding functions so they can be deserialized
        from chromadb.utils.embedding_functions import known_embedding_functions

        known_embedding_functions["mock_embedding"] = MockEmbeddingFunction
        known_embedding_functions["mock_sparse"] = MockSparseEmbeddingFunction  # type: ignore[assignment]

        try:
            # Create a default schema and apply multiple modifications
            original = Schema()

            # 1. Set custom vector config globally (space + HNSW)
            custom_ef = MockEmbeddingFunction(model_name="mixed_test_model")
            hnsw_config = HnswIndexConfig(ef_construction=300, max_neighbors=64)
            vector_config = VectorIndexConfig(
                embedding_function=custom_ef, space="ip", hnsw=hnsw_config
            )
            original.create_index(config=vector_config)

            # 2. Enable sparse vector index on "embeddings_field" key
            sparse_ef = MockSparseEmbeddingFunction(name="sparse_model")
            sparse_config = SparseVectorIndexConfig(
                embedding_function=sparse_ef, source_key="text_field"
            )
            original.create_index(config=sparse_config, key="embeddings_field")

            # 3. Disable string_inverted_index on "tags" key
            string_config = StringInvertedIndexConfig()
            original.delete_index(config=string_config, key="tags")

            # 4. Disable int_inverted_index on "count" key
            int_config = IntInvertedIndexConfig()
            original.delete_index(config=int_config, key="count")

            # 5. Disable float_inverted_index on "price" key
            float_config = FloatInvertedIndexConfig()
            original.delete_index(config=float_config, key="price")

            # Serialize to JSON
            json_data = original.serialize_to_json()

            # Verify JSON structure has all modifications
            defaults = json_data["defaults"]
            keys = json_data["keys"]

            # Check defaults reflect global vector config changes
            assert defaults["float_list"]["vector_index"]["config"]["space"] == "ip"
            assert (
                defaults["float_list"]["vector_index"]["config"]["hnsw"][
                    "ef_construction"
                ]
                == 300
            )
            assert (
                defaults["float_list"]["vector_index"]["config"]["hnsw"][
                    "max_neighbors"
                ]
                == 64
            )

            # Check key overrides exist for all modified keys
            assert "embeddings_field" in keys
            assert "tags" in keys
            assert "count" in keys
            assert "price" in keys
            assert "#document" in keys  # Default key
            assert "#embedding" in keys  # Default key with vector config

            # Exhaustive validation of embeddings_field
            embeddings_field_json = keys["embeddings_field"]
            assert "sparse_vector" in embeddings_field_json
            assert (
                embeddings_field_json["sparse_vector"]["sparse_vector_index"]["enabled"]
                is True
            )
            assert (
                embeddings_field_json["sparse_vector"]["sparse_vector_index"]["config"][
                    "source_key"
                ]
                == "text_field"
            )
            assert (
                embeddings_field_json["sparse_vector"]["sparse_vector_index"]["config"][
                    "embedding_function"
                ]["type"]
                == "known"
            )
            assert (
                embeddings_field_json["sparse_vector"]["sparse_vector_index"]["config"][
                    "embedding_function"
                ]["name"]
                == "mock_sparse"
            )
            assert (
                embeddings_field_json["sparse_vector"]["sparse_vector_index"]["config"][
                    "embedding_function"
                ]["config"]["name"]
                == "sparse_model"
            )
            # Verify sparse override: only sparse_vector should be present
            assert "string" not in embeddings_field_json
            assert "float_list" not in embeddings_field_json
            assert "int" not in embeddings_field_json
            assert "float" not in embeddings_field_json
            assert "bool" not in embeddings_field_json

            # Exhaustive validation of tags
            tags_json = keys["tags"]
            assert "string" in tags_json
            assert tags_json["string"]["string_inverted_index"]["enabled"] is False
            assert tags_json["string"]["string_inverted_index"]["config"] == {}
            # FTS should not be present (not modified)
            assert "fts_index" not in tags_json["string"]
            # Verify sparse override: only string should be present
            assert "sparse_vector" not in tags_json
            assert "float_list" not in tags_json
            assert "int" not in tags_json
            assert "float" not in tags_json
            assert "bool" not in tags_json

            # Exhaustive validation of count
            count_json = keys["count"]
            assert "int" in count_json
            assert count_json["int"]["int_inverted_index"]["enabled"] is False
            assert count_json["int"]["int_inverted_index"]["config"] == {}
            # Verify sparse override: only int should be present
            assert "string" not in count_json
            assert "sparse_vector" not in count_json
            assert "float_list" not in count_json
            assert "float" not in count_json
            assert "bool" not in count_json

            # Exhaustive validation of price
            price_json = keys["price"]
            assert "float" in price_json
            assert price_json["float"]["float_inverted_index"]["enabled"] is False
            assert price_json["float"]["float_inverted_index"]["config"] == {}
            # Verify sparse override: only float should be present
            assert "string" not in price_json
            assert "sparse_vector" not in price_json
            assert "float_list" not in price_json
            assert "int" not in price_json
            assert "bool" not in price_json

            # Exhaustive validation of #embedding
            embedding_json = keys["#embedding"]
            assert "float_list" in embedding_json
            assert embedding_json["float_list"]["vector_index"]["enabled"] is True
            assert (
                embedding_json["float_list"]["vector_index"]["config"]["space"] == "ip"
            )
            assert (
                embedding_json["float_list"]["vector_index"]["config"]["source_key"]
                == "#document"
            )
            assert (
                embedding_json["float_list"]["vector_index"]["config"][
                    "embedding_function"
                ]["type"]
                == "known"
            )
            assert (
                embedding_json["float_list"]["vector_index"]["config"][
                    "embedding_function"
                ]["name"]
                == "mock_embedding"
            )
            assert (
                embedding_json["float_list"]["vector_index"]["config"][
                    "embedding_function"
                ]["config"]["model_name"]
                == "mixed_test_model"
            )
            assert (
                embedding_json["float_list"]["vector_index"]["config"]["hnsw"][
                    "ef_construction"
                ]
                == 300
            )
            assert (
                embedding_json["float_list"]["vector_index"]["config"]["hnsw"][
                    "max_neighbors"
                ]
                == 64
            )
            assert (
                embedding_json["float_list"]["vector_index"]["config"].get("spann")
                is None
            )
            # Verify sparse override: only float_list should be present
            assert "string" not in embedding_json
            assert "sparse_vector" not in embedding_json
            assert "int" not in embedding_json
            assert "float" not in embedding_json
            assert "bool" not in embedding_json

            # Exhaustive validation of #document (unchanged, but with FTS enabled)
            document_json = keys["#document"]
            assert "string" in document_json
            assert document_json["string"]["fts_index"]["enabled"] is True
            assert document_json["string"]["fts_index"]["config"] == {}
            assert document_json["string"]["string_inverted_index"]["enabled"] is False
            assert document_json["string"]["string_inverted_index"]["config"] == {}
            # Verify sparse override: only string should be present
            assert "sparse_vector" not in document_json
            assert "float_list" not in document_json
            assert "int" not in document_json
            assert "float" not in document_json
            assert "bool" not in document_json

            # Deserialize back to Schema
            deserialized = Schema.deserialize_from_json(json_data)

            # Verify all modifications are preserved after deserialization
            # 1. Check global vector config
            assert deserialized.defaults.float_list is not None
            assert deserialized.defaults.float_list.vector_index is not None
            assert deserialized.defaults.float_list.vector_index.config.space == "ip"
            assert deserialized.defaults.float_list.vector_index.config.hnsw is not None
            assert (
                deserialized.defaults.float_list.vector_index.config.hnsw.ef_construction
                == 300
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.hnsw.max_neighbors
                == 64
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.embedding_function
                is not None
            )
            assert (
                deserialized.defaults.float_list.vector_index.config.embedding_function.name()
                == "mock_embedding"
            )

            # 2. Check embeddings_field sparse vector
            assert "embeddings_field" in deserialized.keys
            assert deserialized.keys["embeddings_field"].sparse_vector is not None
            assert (
                deserialized.keys["embeddings_field"].sparse_vector.sparse_vector_index
                is not None
            )
            assert (
                deserialized.keys[
                    "embeddings_field"
                ].sparse_vector.sparse_vector_index.enabled
                is True
            )
            assert (
                deserialized.keys[
                    "embeddings_field"
                ].sparse_vector.sparse_vector_index.config.source_key
                == "text_field"
            )
            # Sparse override: other value types should be None
            assert deserialized.keys["embeddings_field"].string is None
            assert deserialized.keys["embeddings_field"].float_list is None
            assert deserialized.keys["embeddings_field"].int_value is None

            # 3. Check tags has string_inverted_index disabled
            assert "tags" in deserialized.keys
            assert deserialized.keys["tags"].string is not None
            assert deserialized.keys["tags"].string.string_inverted_index is not None
            assert (
                deserialized.keys["tags"].string.string_inverted_index.enabled is False
            )
            # Sparse override: other value types should be None
            assert deserialized.keys["tags"].sparse_vector is None
            assert deserialized.keys["tags"].float_list is None

            # 4. Check count has int_inverted_index disabled
            assert "count" in deserialized.keys
            assert deserialized.keys["count"].int_value is not None
            assert deserialized.keys["count"].int_value.int_inverted_index is not None
            assert (
                deserialized.keys["count"].int_value.int_inverted_index.enabled is False
            )
            # Sparse override: other value types should be None
            assert deserialized.keys["count"].string is None
            assert deserialized.keys["count"].float_list is None

            # 5. Check price has float_inverted_index disabled
            assert "price" in deserialized.keys
            assert deserialized.keys["price"].float_value is not None
            assert (
                deserialized.keys["price"].float_value.float_inverted_index is not None
            )
            assert (
                deserialized.keys["price"].float_value.float_inverted_index.enabled
                is False
            )
            # Sparse override: other value types should be None
            assert deserialized.keys["price"].string is None
            assert deserialized.keys["price"].sparse_vector is None

            # 6. Check #embedding has updated vector config
            assert "#embedding" in deserialized.keys
            assert deserialized.keys["#embedding"].float_list is not None
            assert deserialized.keys["#embedding"].float_list.vector_index is not None
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.space
                == "ip"
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.source_key
                == "#document"
            )
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.hnsw
                is not None
            )
            assert (
                deserialized.keys[
                    "#embedding"
                ].float_list.vector_index.config.hnsw.ef_construction
                == 300
            )

            # 7. Verify defaults for unchanged indexes remain correct
            assert deserialized.defaults.string is not None
            assert deserialized.defaults.string.string_inverted_index is not None
            assert (
                deserialized.defaults.string.string_inverted_index.enabled is True
            )  # Still enabled globally
            assert deserialized.defaults.int_value is not None
            assert deserialized.defaults.int_value.int_inverted_index is not None
            assert (
                deserialized.defaults.int_value.int_inverted_index.enabled is True
            )  # Still enabled globally
            assert deserialized.defaults.sparse_vector is not None
            assert deserialized.defaults.sparse_vector.sparse_vector_index is not None
            assert (
                deserialized.defaults.sparse_vector.sparse_vector_index.enabled is False
            )  # Still disabled globally
        finally:
            # Clean up: remove the mock functions from known_embedding_functions
            if "mock_embedding" in known_embedding_functions:
                del known_embedding_functions["mock_embedding"]
            if "mock_sparse" in known_embedding_functions:
                del known_embedding_functions["mock_sparse"]

    def test_multiple_index_types_on_same_key(self) -> None:
        """Test that multiple index types can coexist on the same key."""
        schema = Schema()

        # Enable sparse vector on "multi_field"
        sparse_config = SparseVectorIndexConfig(
            source_key="source", embedding_function=MockSparseEmbeddingFunction()
        )
        schema.create_index(config=sparse_config, key="multi_field")

        # Also enable string_inverted_index on the same key
        string_config = StringInvertedIndexConfig()
        schema.create_index(config=string_config, key="multi_field")

        # Verify both indexes exist on the same key
        assert "multi_field" in schema.keys
        multi_field = schema.keys["multi_field"]
        assert multi_field.sparse_vector is not None
        assert multi_field.sparse_vector.sparse_vector_index is not None
        assert multi_field.sparse_vector.sparse_vector_index.enabled is True

        assert multi_field.string is not None
        assert multi_field.string.string_inverted_index is not None
        assert multi_field.string.string_inverted_index.enabled is True

        # Verify other value types are still None (sparse override)
        assert schema.keys["multi_field"].float_list is None
        assert schema.keys["multi_field"].int_value is None
        assert schema.keys["multi_field"].float_value is None
        assert schema.keys["multi_field"].boolean is None

        # Serialize and verify both are present in JSON
        json_data = schema.serialize_to_json()
        multi_field_json = json_data["keys"]["multi_field"]
        assert "sparse_vector" in multi_field_json
        assert "string" in multi_field_json
        assert (
            multi_field_json["sparse_vector"]["sparse_vector_index"]["enabled"] is True
        )
        assert multi_field_json["string"]["string_inverted_index"]["enabled"] is True

        # Deserialize and verify both survive roundtrip
        deserialized = Schema.deserialize_from_json(json_data)
        assert "multi_field" in deserialized.keys
        des_multi_field = deserialized.keys["multi_field"]
        assert des_multi_field.sparse_vector is not None
        assert des_multi_field.sparse_vector.sparse_vector_index is not None
        assert des_multi_field.sparse_vector.sparse_vector_index.enabled is True
        assert des_multi_field.string is not None
        assert des_multi_field.string.string_inverted_index is not None
        assert des_multi_field.string.string_inverted_index.enabled is True

    def test_override_then_revert_to_default(self) -> None:
        """Test that disabling an index reverts to default behavior (key may still exist with disabled state)."""
        schema = Schema()

        # Enable string_inverted_index on "temp_field"
        string_config = StringInvertedIndexConfig()
        schema.create_index(config=string_config, key="temp_field")

        # Verify it's enabled
        assert "temp_field" in schema.keys
        temp_field_initial = schema.keys["temp_field"]
        assert temp_field_initial.string is not None
        assert temp_field_initial.string.string_inverted_index is not None
        assert temp_field_initial.string.string_inverted_index.enabled is True

        # Now disable it
        schema.delete_index(config=string_config, key="temp_field")

        # Verify it's now disabled (key still exists but with disabled state)
        assert "temp_field" in schema.keys
        temp_field = schema.keys["temp_field"]
        assert temp_field.string is not None
        assert temp_field.string.string_inverted_index is not None
        assert temp_field.string.string_inverted_index.enabled is False

        # Serialize and verify disabled state is preserved
        json_data = schema.serialize_to_json()
        assert "temp_field" in json_data["keys"]
        temp_field_json = json_data["keys"]["temp_field"]
        assert "string" in temp_field_json
        assert temp_field_json["string"]["string_inverted_index"]["enabled"] is False

        # Deserialize and verify disabled state survives roundtrip
        deserialized = Schema.deserialize_from_json(json_data)
        assert "temp_field" in deserialized.keys
        des_temp_field = deserialized.keys["temp_field"]
        assert des_temp_field.string is not None
        assert des_temp_field.string.string_inverted_index is not None
        assert des_temp_field.string.string_inverted_index.enabled is False

    def test_error_handling_invalid_operations(self) -> None:
        """Test that invalid operations raise appropriate errors."""
        schema = Schema()

        # Test 1: Cannot create index on #embedding key
        vector_config = VectorIndexConfig()
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#embedding'"
        ):
            schema.create_index(config=vector_config, key="#embedding")

        # Test 2: Cannot create index on #document key
        fts_config = FtsIndexConfig()
        with pytest.raises(
            ValueError, match="Cannot create index on special key '#document'"
        ):
            schema.create_index(config=fts_config, key="#document")

        # Test 3: Cannot enable all indexes globally
        with pytest.raises(ValueError, match="Cannot enable all index types globally"):
            schema.create_index()

        # Test 4: Cannot enable all indexes for a specific key
        with pytest.raises(
            ValueError, match="Cannot enable all index types for key 'mykey'"
        ):
            schema.create_index(key="mykey")

        # Test 5: Cannot disable all indexes for a specific key
        with pytest.raises(
            ValueError, match="Cannot disable all index types for key 'mykey'"
        ):
            schema.delete_index(key="mykey")

        # Test 6: Cannot delete vector index
        with pytest.raises(
            ValueError, match="Deleting vector index is not currently supported"
        ):
            schema.delete_index(config=vector_config)

        # Test 7: Cannot delete FTS index
        with pytest.raises(
            ValueError, match="Deleting FTS index is not currently supported"
        ):
            schema.delete_index(config=fts_config)

        # Test 8: Cannot create vector index on custom key
        with pytest.raises(
            ValueError, match="Vector index cannot be enabled on specific keys"
        ):
            schema.create_index(config=vector_config, key="custom_field")

        # Test 9: Cannot create FTS index on custom key
        with pytest.raises(
            ValueError, match="FTS index cannot be enabled on specific keys"
        ):
            schema.create_index(config=fts_config, key="custom_field")

    def test_empty_schema_serialization(self) -> None:
        """Test serialization/deserialization of an unmodified schema."""
        # Create a schema without any modifications
        original = Schema()

        # Serialize
        json_data = original.serialize_to_json()

        # Verify only default keys exist in keys
        assert len(json_data["keys"]) == 2
        assert "#document" in json_data["keys"]
        assert "#embedding" in json_data["keys"]

        # Deserialize
        deserialized = Schema.deserialize_from_json(json_data)

        # Verify defaults match
        defaults = deserialized.defaults
        assert defaults.string is not None
        assert defaults.string.string_inverted_index is not None
        assert defaults.string.string_inverted_index.enabled is True
        assert defaults.string.fts_index is not None
        assert defaults.string.fts_index.enabled is False
        assert defaults.float_list is not None
        assert defaults.float_list.vector_index is not None
        assert defaults.float_list.vector_index.enabled is False
        assert defaults.sparse_vector is not None
        assert defaults.sparse_vector.sparse_vector_index is not None
        assert defaults.sparse_vector.sparse_vector_index.enabled is False
        assert defaults.int_value is not None
        assert defaults.int_value.int_inverted_index is not None
        assert defaults.int_value.int_inverted_index.enabled is True
        assert defaults.float_value is not None
        assert defaults.float_value.float_inverted_index is not None
        assert defaults.float_value.float_inverted_index.enabled is True
        assert defaults.boolean is not None
        assert defaults.boolean.bool_inverted_index is not None
        assert defaults.boolean.bool_inverted_index.enabled is True

        # Verify only default keys exist in keys
        assert len(deserialized.keys) == 2
        assert "#document" in deserialized.keys
        assert "#embedding" in deserialized.keys

    def test_multiple_serialize_deserialize_roundtrips(self) -> None:
        """Test that multiple serialization/deserialization cycles preserve schema integrity."""
        # Register the mock embedding function
        from chromadb.utils.embedding_functions import known_embedding_functions

        known_embedding_functions["mock_embedding"] = MockEmbeddingFunction

        try:
            # Create a complex schema
            original = Schema()
            custom_ef = MockEmbeddingFunction(model_name="roundtrip_model")
            hnsw_config = HnswIndexConfig(ef_construction=150, max_neighbors=40)
            vector_config = VectorIndexConfig(
                embedding_function=custom_ef, space="cosine", hnsw=hnsw_config
            )
            original.create_index(config=vector_config)
            original.create_index(
                config=SparseVectorIndexConfig(
                    source_key="text", embedding_function=MockSparseEmbeddingFunction()
                ),
                key="embeddings",
            )
            original.delete_index(config=StringInvertedIndexConfig(), key="tags")

            # First roundtrip
            json1 = original.serialize_to_json()
            schema1 = Schema.deserialize_from_json(json1)

            # Second roundtrip
            json2 = schema1.serialize_to_json()
            schema2 = Schema.deserialize_from_json(json2)

            # Third roundtrip
            json3 = schema2.serialize_to_json()
            schema3 = Schema.deserialize_from_json(json3)

            # Verify all schemas are identical
            # Check vector config persists
            for schema in [schema1, schema2, schema3]:
                assert schema.defaults.float_list is not None
                assert schema.defaults.float_list.vector_index is not None
                assert schema.defaults.float_list.vector_index.config.space == "cosine"
                assert schema.defaults.float_list.vector_index.config.hnsw is not None
                assert (
                    schema.defaults.float_list.vector_index.config.hnsw.ef_construction
                    == 150
                )
                assert (
                    schema.defaults.float_list.vector_index.config.hnsw.max_neighbors
                    == 40
                )
                assert (
                    schema.defaults.float_list.vector_index.config.embedding_function
                    is not None
                )
                assert (
                    schema.defaults.float_list.vector_index.config.embedding_function.name()
                    == "mock_embedding"
                )

                # Check sparse vector on embeddings key
                assert "embeddings" in schema.keys
                embeddings_override = schema.keys["embeddings"]
                assert embeddings_override.sparse_vector is not None
                assert embeddings_override.sparse_vector.sparse_vector_index is not None
                assert (
                    embeddings_override.sparse_vector.sparse_vector_index.enabled
                    is True
                )
                assert (
                    embeddings_override.sparse_vector.sparse_vector_index.config.source_key
                    == "text"
                )

                # Check disabled string index on tags key
                assert "tags" in schema.keys
                tags_override = schema.keys["tags"]
                assert tags_override.string is not None
                assert tags_override.string.string_inverted_index is not None
                assert tags_override.string.string_inverted_index.enabled is False

            # Verify semantic equivalence: all three schemas should have same number of overrides
            assert len(schema1.keys) == len(schema2.keys) == len(schema3.keys)
            assert (
                set(schema1.keys.keys())
                == set(schema2.keys.keys())
                == set(schema3.keys.keys())
            )

        finally:
            # Clean up
            if "mock_embedding" in known_embedding_functions:
                del known_embedding_functions["mock_embedding"]

    def test_many_keys_stress(self) -> None:
        """Test schema with many key overrides (stress test)."""
        schema = Schema()

        # Create 50 key overrides with different configurations
        for i in range(50):
            key_name = f"field_{i}"
            if i == 0:
                # Enable sparse vector on ONE key only
                schema.create_index(
                    config=SparseVectorIndexConfig(
                        source_key=f"source_{i}",
                        embedding_function=MockSparseEmbeddingFunction(),
                    ),
                    key=key_name,
                )
            elif i % 2 == 1:
                # Disable string inverted index
                schema.delete_index(config=StringInvertedIndexConfig(), key=key_name)
            else:
                # Disable int inverted index
                schema.delete_index(config=IntInvertedIndexConfig(), key=key_name)

        # Verify all 50 keys + 2 defaults exist
        assert len(schema.keys) == 52  # 50 custom + #document + #embedding

        # Verify a sample of keys
        assert "field_0" in schema.keys
        field_0 = schema.keys["field_0"]
        assert field_0.sparse_vector is not None
        assert field_0.sparse_vector.sparse_vector_index is not None
        assert field_0.sparse_vector.sparse_vector_index.enabled is True

        assert "field_1" in schema.keys
        field_1 = schema.keys["field_1"]
        assert field_1.string is not None
        assert field_1.string.string_inverted_index is not None
        assert field_1.string.string_inverted_index.enabled is False

        assert "field_2" in schema.keys
        field_2 = schema.keys["field_2"]
        assert field_2.int_value is not None
        assert field_2.int_value.int_inverted_index is not None
        assert field_2.int_value.int_inverted_index.enabled is False

        # Serialize
        json_data = schema.serialize_to_json()
        assert len(json_data["keys"]) == 52

        # Deserialize
        deserialized = Schema.deserialize_from_json(json_data)
        assert len(deserialized.keys) == 52

        # Spot check deserialized values
        assert "field_0" in deserialized.keys  # i == 0 -> sparse vector
        des_field_0 = deserialized.keys["field_0"]
        assert des_field_0.sparse_vector is not None
        assert des_field_0.sparse_vector.sparse_vector_index is not None
        assert des_field_0.sparse_vector.sparse_vector_index.enabled is True
        assert (
            des_field_0.sparse_vector.sparse_vector_index.config.source_key
            == "source_0"
        )

        assert "field_49" in deserialized.keys  # 49 % 2 == 1 -> string disabled
        des_field_49 = deserialized.keys["field_49"]
        assert des_field_49.string is not None
        assert des_field_49.string.string_inverted_index is not None
        assert des_field_49.string.string_inverted_index.enabled is False

        assert "field_48" in deserialized.keys  # 48 % 2 == 0 -> int disabled
        des_field_48 = deserialized.keys["field_48"]
        assert des_field_48.int_value is not None
        assert des_field_48.int_value.int_inverted_index is not None
        assert des_field_48.int_value.int_inverted_index.enabled is False

    def test_chained_operations(self) -> None:
        """Test chaining multiple create_index and delete_index operations."""
        schema = Schema()

        # Chain multiple operations
        result = (
            schema.create_index(
                config=SparseVectorIndexConfig(
                    source_key="text", embedding_function=MockSparseEmbeddingFunction()
                ),
                key="field1",
            )
            .delete_index(config=StringInvertedIndexConfig(), key="field2")
            .delete_index(config=StringInvertedIndexConfig(), key="field3")
            .delete_index(config=IntInvertedIndexConfig(), key="field4")
        )

        # Verify chaining returns the same schema object
        assert result is schema

        # Verify all operations were applied
        assert "field1" in schema.keys
        field1 = schema.keys["field1"]
        assert field1.sparse_vector is not None
        assert field1.sparse_vector.sparse_vector_index is not None
        assert field1.sparse_vector.sparse_vector_index.enabled is True

        assert "field2" in schema.keys
        field2 = schema.keys["field2"]
        assert field2.string is not None
        assert field2.string.string_inverted_index is not None
        assert field2.string.string_inverted_index.enabled is False

        assert "field3" in schema.keys
        field3 = schema.keys["field3"]
        assert field3.string is not None
        assert field3.string.string_inverted_index is not None
        assert field3.string.string_inverted_index.enabled is False

        assert "field4" in schema.keys
        field4 = schema.keys["field4"]
        assert field4.int_value is not None
        assert field4.int_value.int_inverted_index is not None
        assert field4.int_value.int_inverted_index.enabled is False

    def test_float_and_bool_inverted_indexes(self) -> None:
        """Test enabling/disabling float and bool inverted indexes."""
        schema = Schema()

        # Verify defaults
        assert schema.defaults.float_value is not None
        assert schema.defaults.float_value.float_inverted_index is not None
        assert schema.defaults.float_value.float_inverted_index.enabled is True
        assert schema.defaults.boolean is not None
        assert schema.defaults.boolean.bool_inverted_index is not None
        assert schema.defaults.boolean.bool_inverted_index.enabled is True

        # Disable float inverted index globally
        float_config = FloatInvertedIndexConfig()
        schema.delete_index(config=float_config)
        assert schema.defaults.float_value.float_inverted_index is not None
        assert schema.defaults.float_value.float_inverted_index.enabled is False

        # Disable bool inverted index globally
        bool_config = BoolInvertedIndexConfig()
        schema.delete_index(config=bool_config)
        assert schema.defaults.boolean.bool_inverted_index is not None
        assert schema.defaults.boolean.bool_inverted_index.enabled is False

        # Enable float inverted index on a specific key
        schema.create_index(config=FloatInvertedIndexConfig(), key="price")
        assert "price" in schema.keys
        assert schema.keys["price"].float_value.float_inverted_index.enabled is True

        # Disable bool inverted index on a specific key
        schema.delete_index(config=BoolInvertedIndexConfig(), key="is_active")
        assert "is_active" in schema.keys
        assert schema.keys["is_active"].boolean.bool_inverted_index.enabled is False

        # Serialize and verify
        json_data = schema.serialize_to_json()
        assert (
            json_data["defaults"]["float"]["float_inverted_index"]["enabled"] is False
        )
        assert json_data["defaults"]["bool"]["bool_inverted_index"]["enabled"] is False
        assert (
            json_data["keys"]["price"]["float"]["float_inverted_index"]["enabled"]
            is True
        )
        assert (
            json_data["keys"]["is_active"]["bool"]["bool_inverted_index"]["enabled"]
            is False
        )

        # Deserialize and verify
        deserialized = Schema.deserialize_from_json(json_data)
        assert deserialized.defaults.float_value.float_inverted_index.enabled is False
        assert deserialized.defaults.boolean.bool_inverted_index.enabled is False
        assert (
            deserialized.keys["price"].float_value.float_inverted_index.enabled is True
        )
        assert (
            deserialized.keys["is_active"].boolean.bool_inverted_index.enabled is False
        )

    def test_space_inference_from_embedding_function(self) -> None:
        """Test that space is correctly inferred from embedding function when not explicitly set."""
        # Register the mock embedding function
        from chromadb.utils.embedding_functions import known_embedding_functions

        known_embedding_functions["mock_embedding"] = MockEmbeddingFunction

        try:
            schema = Schema()

            # Create vector config with EF but WITHOUT explicit space
            # MockEmbeddingFunction has default_space() = "cosine"
            custom_ef = MockEmbeddingFunction(model_name="space_inference_test")
            vector_config = VectorIndexConfig(
                embedding_function=custom_ef
                # Note: space is NOT specified, should be inferred from EF
            )
            schema.create_index(config=vector_config)

            # Serialize to JSON
            json_data = schema.serialize_to_json()

            # Verify that space was inferred and set to "cosine" in serialized JSON
            defaults_vector = json_data["defaults"]["float_list"]["vector_index"]
            assert defaults_vector["config"]["space"] == "cosine"  # Inferred from EF

            # Verify #embedding key also has inferred space
            embedding_vector = json_data["keys"]["#embedding"]["float_list"][
                "vector_index"
            ]
            assert embedding_vector["config"]["space"] == "cosine"  # Inferred from EF

            # Deserialize and verify space is preserved
            deserialized = Schema.deserialize_from_json(json_data)
            assert deserialized.defaults.float_list is not None
            assert deserialized.defaults.float_list.vector_index is not None
            assert (
                deserialized.defaults.float_list.vector_index.config.space == "cosine"
            )

            assert deserialized.keys["#embedding"].float_list is not None
            assert deserialized.keys["#embedding"].float_list.vector_index is not None
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.space
                == "cosine"
            )

        finally:
            # Clean up
            if "mock_embedding" in known_embedding_functions:
                del known_embedding_functions["mock_embedding"]

    def test_explicit_space_overrides_embedding_function_default(self) -> None:
        """Test that explicit space parameter overrides the embedding function's default space."""
        # Register the mock embedding function
        from chromadb.utils.embedding_functions import known_embedding_functions

        known_embedding_functions["mock_embedding"] = MockEmbeddingFunction

        try:
            schema = Schema()

            # Create vector config with EF and EXPLICIT space that differs from EF default
            # MockEmbeddingFunction has default_space() = "cosine"
            # But we explicitly set space = "l2"
            custom_ef = MockEmbeddingFunction(model_name="override_test")
            vector_config = VectorIndexConfig(
                embedding_function=custom_ef,
                space="l2",  # Explicitly override the EF's default
            )
            schema.create_index(config=vector_config)

            # Serialize to JSON
            json_data = schema.serialize_to_json()

            # Verify that explicit space overrode the EF default
            defaults_vector = json_data["defaults"]["float_list"]["vector_index"]
            assert (
                defaults_vector["config"]["space"] == "l2"
            )  # User-specified, not "cosine"

            embedding_vector = json_data["keys"]["#embedding"]["float_list"][
                "vector_index"
            ]
            assert (
                embedding_vector["config"]["space"] == "l2"
            )  # User-specified, not "cosine"

            # Deserialize and verify explicit space is preserved
            deserialized = Schema.deserialize_from_json(json_data)
            assert deserialized.defaults.float_list is not None
            assert deserialized.defaults.float_list.vector_index is not None
            assert deserialized.defaults.float_list.vector_index.config.space == "l2"

            assert deserialized.keys["#embedding"].float_list is not None
            assert deserialized.keys["#embedding"].float_list.vector_index is not None
            assert (
                deserialized.keys["#embedding"].float_list.vector_index.config.space
                == "l2"
            )

        finally:
            # Clean up
            if "mock_embedding" in known_embedding_functions:
                del known_embedding_functions["mock_embedding"]

    def test_space_inference_with_no_embedding_function(self) -> None:
        """Test space handling when no embedding function is provided (legacy mode)."""
        schema = Schema()

        # Create vector config with explicit space but NO embedding function (legacy)
        vector_config = VectorIndexConfig(
            embedding_function=None,
            space="ip",  # Must be explicit since no EF to infer from
        )
        schema.create_index(config=vector_config)

        # Serialize to JSON
        json_data = schema.serialize_to_json()

        # Verify space is correctly set
        defaults_vector = json_data["defaults"]["float_list"]["vector_index"]
        assert defaults_vector["config"]["space"] == "ip"
        assert defaults_vector["config"]["embedding_function"]["type"] == "legacy"

        embedding_vector = json_data["keys"]["#embedding"]["float_list"]["vector_index"]
        assert embedding_vector["config"]["space"] == "ip"
        assert embedding_vector["config"]["embedding_function"]["type"] == "legacy"

        # Deserialize and verify
        deserialized = Schema.deserialize_from_json(json_data)
        assert deserialized.defaults.float_list is not None
        assert deserialized.defaults.float_list.vector_index is not None
        assert deserialized.defaults.float_list.vector_index.config.space == "ip"
        assert (
            deserialized.defaults.float_list.vector_index.config.embedding_function
            is None
        )

    def test_space_inference_multiple_roundtrips(self) -> None:
        """Test that inferred space remains stable across multiple serialization roundtrips."""
        # Register the mock embedding function
        from chromadb.utils.embedding_functions import known_embedding_functions

        known_embedding_functions["mock_embedding"] = MockEmbeddingFunction

        try:
            # Create schema with inferred space (no explicit space)
            original = Schema()
            custom_ef = MockEmbeddingFunction(model_name="roundtrip_space_test")
            vector_config = VectorIndexConfig(
                embedding_function=custom_ef
            )  # No explicit space
            original.create_index(config=vector_config)

            # First roundtrip
            json1 = original.serialize_to_json()
            assert (
                json1["defaults"]["float_list"]["vector_index"]["config"]["space"]
                == "cosine"
            )
            schema1 = Schema.deserialize_from_json(json1)

            # Second roundtrip
            json2 = schema1.serialize_to_json()
            assert (
                json2["defaults"]["float_list"]["vector_index"]["config"]["space"]
                == "cosine"
            )
            schema2 = Schema.deserialize_from_json(json2)

            # Third roundtrip
            json3 = schema2.serialize_to_json()
            assert (
                json3["defaults"]["float_list"]["vector_index"]["config"]["space"]
                == "cosine"
            )

            # Verify all schemas have the inferred space
            for schema in [schema1, schema2]:
                assert schema.defaults.float_list is not None
                assert schema.defaults.float_list.vector_index is not None
                assert schema.defaults.float_list.vector_index.config.space == "cosine"

        finally:
            # Clean up
            if "mock_embedding" in known_embedding_functions:
                del known_embedding_functions["mock_embedding"]

    def test_keys_have_independent_configs(self) -> None:
        """Test that each key override has its own independent config (no inheritance from defaults)."""
        schema = Schema()

        # Enable sparse vector on a key - it gets exactly what we specify
        sparse_config = SparseVectorIndexConfig(
            source_key="default_source",
            embedding_function=MockSparseEmbeddingFunction(),
        )
        schema.create_index(config=sparse_config, key="field1")

        # Verify field1 has the sparse vector with the specified source_key
        assert "field1" in schema.keys
        field1 = schema.keys["field1"]
        assert field1.sparse_vector is not None
        assert field1.sparse_vector.sparse_vector_index is not None
        assert field1.sparse_vector.sparse_vector_index.enabled is True
        assert (
            field1.sparse_vector.sparse_vector_index.config.source_key
            == "default_source"
        )

        # Now create another key with a DIFFERENT config (use string_inverted_index instead)
        string_config = StringInvertedIndexConfig()
        schema.create_index(config=string_config, key="field2")

        # Verify field2 has its own config
        assert "field2" in schema.keys
        field2 = schema.keys["field2"]
        assert field2.string is not None
        assert field2.string.string_inverted_index is not None
        assert field2.string.string_inverted_index.enabled is True

        # Verify field1 is unchanged
        assert (
            field1.sparse_vector.sparse_vector_index.config.source_key
            == "default_source"
        )

    def test_global_default_changes_dont_affect_existing_overrides(self) -> None:
        """Test that changes to global defaults don't affect already-created key overrides."""
        # Register the mock embedding function
        from chromadb.utils.embedding_functions import known_embedding_functions

        known_embedding_functions["mock_embedding"] = MockEmbeddingFunction

        try:
            schema = Schema()

            # Create initial vector config with HNSW
            ef1 = MockEmbeddingFunction(model_name="initial_model")
            hnsw1 = HnswIndexConfig(ef_construction=100, max_neighbors=16)
            vector_config1 = VectorIndexConfig(
                embedding_function=ef1, space="cosine", hnsw=hnsw1
            )
            schema.create_index(config=vector_config1)

            # Capture the initial state of #embedding
            initial_embedding_hnsw = schema.keys[
                "#embedding"
            ].float_list.vector_index.config.hnsw  # type: ignore[union-attr]
            assert initial_embedding_hnsw is not None
            assert initial_embedding_hnsw.ef_construction == 100
            assert initial_embedding_hnsw.max_neighbors == 16

            # Now change the global vector config to different values
            ef2 = MockEmbeddingFunction(model_name="updated_model")
            hnsw2 = HnswIndexConfig(ef_construction=200, max_neighbors=32)
            vector_config2 = VectorIndexConfig(
                embedding_function=ef2, space="l2", hnsw=hnsw2
            )
            schema.create_index(config=vector_config2)

            # Verify global defaults changed
            assert schema.defaults.float_list is not None
            assert schema.defaults.float_list.vector_index is not None
            assert schema.defaults.float_list.vector_index.config.space == "l2"
            assert schema.defaults.float_list.vector_index.config.hnsw is not None
            assert (
                schema.defaults.float_list.vector_index.config.hnsw.ef_construction
                == 200
            )
            assert (
                schema.defaults.float_list.vector_index.config.hnsw.max_neighbors == 32
            )

            # Verify #embedding was also updated (since it's the target of vector config)
            assert schema.keys["#embedding"].float_list is not None
            assert schema.keys["#embedding"].float_list.vector_index is not None
            updated_embedding_hnsw = schema.keys[
                "#embedding"
            ].float_list.vector_index.config.hnsw
            assert updated_embedding_hnsw is not None
            assert updated_embedding_hnsw.ef_construction == 200
            assert updated_embedding_hnsw.max_neighbors == 32
            assert (
                schema.keys["#embedding"].float_list.vector_index.config.space == "l2"
            )

        finally:
            # Clean up
            if "mock_embedding" in known_embedding_functions:
                del known_embedding_functions["mock_embedding"]

    def test_key_specific_overrides_are_independent(self) -> None:
        """Test that modifying one key's overrides doesn't affect other keys."""
        schema = Schema()

        # Create sparse vector on one key and string indexes on others
        schema.create_index(
            config=SparseVectorIndexConfig(
                source_key="source_a", embedding_function=MockSparseEmbeddingFunction()
            ),
            key="key_a",
        )
        schema.create_index(config=StringInvertedIndexConfig(), key="key_b")
        schema.create_index(config=StringInvertedIndexConfig(), key="key_c")

        # Verify each key has its own config
        assert (
            schema.keys["key_a"].sparse_vector.sparse_vector_index.config.source_key
            == "source_a"
        )  # type: ignore[union-attr]
        assert schema.keys["key_b"].string.string_inverted_index.enabled is True  # type: ignore[union-attr]
        assert schema.keys["key_c"].string.string_inverted_index.enabled is True  # type: ignore[union-attr]

        # Now disable string inverted index on key_b
        schema.delete_index(config=StringInvertedIndexConfig(), key="key_b")

        # Verify key_b is disabled
        assert schema.keys["key_b"].string.string_inverted_index.enabled is False  # type: ignore[union-attr]

        # Verify key_a and key_c are unaffected
        key_a = schema.keys["key_a"]
        assert key_a.sparse_vector is not None
        assert key_a.sparse_vector.sparse_vector_index is not None
        assert key_a.sparse_vector.sparse_vector_index.enabled is True
        assert key_a.sparse_vector.sparse_vector_index.config.source_key == "source_a"

        key_c = schema.keys["key_c"]
        assert key_c.string is not None
        assert key_c.string.string_inverted_index is not None
        assert key_c.string.string_inverted_index.enabled is True

        # Serialize and deserialize to ensure independence is preserved
        json_data = schema.serialize_to_json()
        deserialized = Schema.deserialize_from_json(json_data)

        # Verify after roundtrip
        assert (
            deserialized.keys[
                "key_a"
            ].sparse_vector.sparse_vector_index.config.source_key
            == "source_a"
        )
        assert deserialized.keys["key_b"].string.string_inverted_index.enabled is False
        assert deserialized.keys["key_c"].string.string_inverted_index.enabled is True

    def test_global_default_disable_then_key_enable(self) -> None:
        """Test disabling an index globally, then enabling it on specific keys."""
        schema = Schema()

        # Verify string_inverted_index is enabled by default
        assert schema.defaults.string is not None
        assert schema.defaults.string.string_inverted_index is not None
        assert schema.defaults.string.string_inverted_index.enabled is True

        # Disable string_inverted_index globally
        schema.delete_index(config=StringInvertedIndexConfig())
        assert schema.defaults.string.string_inverted_index.enabled is False

        # Now enable it on specific keys
        schema.create_index(config=StringInvertedIndexConfig(), key="important_field")
        schema.create_index(config=StringInvertedIndexConfig(), key="searchable_field")

        # Verify global default is still disabled
        assert schema.defaults.string.string_inverted_index.enabled is False

        # Verify specific keys have it enabled
        important = schema.keys["important_field"]
        assert important.string is not None
        assert important.string.string_inverted_index is not None
        assert important.string.string_inverted_index.enabled is True

        searchable = schema.keys["searchable_field"]
        assert searchable.string is not None
        assert searchable.string.string_inverted_index is not None
        assert searchable.string.string_inverted_index.enabled is True

        # Verify other keys would inherit the disabled global default
        # (by checking serialization - keys without overrides shouldn't appear)
        json_data = schema.serialize_to_json()

        # Only our explicitly modified keys + defaults (#document, #embedding) should be in overrides
        assert "important_field" in json_data["keys"]
        assert "searchable_field" in json_data["keys"]
        assert "#document" in json_data["keys"]
        assert "#embedding" in json_data["keys"]

        # A hypothetical "other_field" would NOT be in overrides (uses global default)
        assert "other_field" not in json_data["keys"]

    def test_partial_override_fills_from_defaults(self) -> None:
        """Test that when you override one aspect of a value type, other indexes still follow defaults."""
        schema = Schema()

        # Enable sparse vector on a key
        schema.create_index(
            config=SparseVectorIndexConfig(
                source_key="my_source", embedding_function=MockSparseEmbeddingFunction()
            ),
            key="multi_index_field",
        )

        # This key now has sparse_vector overridden, but string, int, etc. should still follow global defaults
        field = schema.keys["multi_index_field"]

        # Sparse vector is explicitly set
        assert field.sparse_vector is not None
        assert field.sparse_vector.sparse_vector_index is not None
        assert field.sparse_vector.sparse_vector_index.enabled is True

        # Other value types are None (will fall back to global defaults)
        assert field.string is None
        assert field.int_value is None
        assert field.float_value is None
        assert field.boolean is None
        assert field.float_list is None

        # Serialize to verify sparse override behavior
        json_data = schema.serialize_to_json()
        field_json = json_data["keys"]["multi_index_field"]

        # Only sparse_vector should be in the JSON for this key
        assert "sparse_vector" in field_json
        assert "string" not in field_json  # Falls back to global
        assert "int" not in field_json
        assert "float" not in field_json
        assert "bool" not in field_json
        assert "float_list" not in field_json

        # Deserialize and verify
        deserialized = Schema.deserialize_from_json(json_data)
        des_field = deserialized.keys["multi_index_field"]

        # Sparse vector is set
        assert des_field.sparse_vector is not None
        assert des_field.sparse_vector.sparse_vector_index is not None
        assert des_field.sparse_vector.sparse_vector_index.enabled is True

        # Others are None (sparse override)
        assert des_field.string is None
        assert des_field.int_value is None

    def test_cmek_basic_creation(self) -> None:
        """Test basic CMEK creation and validation."""
        # Test GCP CMEK creation
        cmek = Cmek.gcp(
            "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key"
        )
        assert cmek.provider == CmekProvider.GCP
        assert (
            cmek.resource
            == "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key"
        )

        # Test valid pattern
        assert cmek.validate_pattern() is True

        # Test invalid pattern
        invalid_cmek = Cmek.gcp("invalid-format")
        assert invalid_cmek.validate_pattern() is False

    def test_cmek_serialization(self) -> None:
        """Test CMEK serialization and deserialization."""
        cmek = Cmek.gcp("projects/p/locations/l/keyRings/r/cryptoKeys/k")

        # Serialize - should use snake_case format matching Rust serde
        cmek_dict = cmek.to_dict()
        assert cmek_dict == {"gcp": "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
        assert "gcp" in cmek_dict
        assert cmek_dict["gcp"] == "projects/p/locations/l/keyRings/r/cryptoKeys/k"

        # Deserialize
        restored = Cmek.from_dict(cmek_dict)
        assert restored.provider == CmekProvider.GCP
        assert restored.resource == cmek.resource

    def test_cmek_in_schema(self) -> None:
        """Test CMEK integration with Schema using set_cmek() method."""
        schema = Schema()

        # Initially no CMEK
        assert schema.cmek is None

        # Add CMEK using set_cmek()
        cmek = Cmek.gcp("projects/test/locations/us/keyRings/ring/cryptoKeys/key")
        result = schema.set_cmek(cmek)

        # Verify method returns self for chaining
        assert result is schema

        # Verify CMEK is set
        assert schema.cmek is not None
        assert schema.cmek.provider == CmekProvider.GCP
        assert (
            schema.cmek.resource
            == "projects/test/locations/us/keyRings/ring/cryptoKeys/key"
        )

        # Test removing CMEK by passing None
        schema.set_cmek(None)
        assert schema.cmek is None

        # Test method chaining
        cmek2 = Cmek.gcp("projects/p/locations/l/keyRings/r/cryptoKeys/k")
        schema2 = Schema().set_cmek(cmek2)
        assert schema2.cmek is not None
        assert schema2.cmek.resource == "projects/p/locations/l/keyRings/r/cryptoKeys/k"

    def test_cmek_schema_serialization(self) -> None:
        """Test Schema serialization with CMEK."""
        cmek = Cmek.gcp("projects/p/locations/l/keyRings/r/cryptoKeys/k")
        schema = Schema().set_cmek(cmek)

        # Serialize
        json_data = schema.serialize_to_json()

        # Verify CMEK is in JSON with snake_case format
        assert "cmek" in json_data
        assert json_data["cmek"] == {
            "gcp": "projects/p/locations/l/keyRings/r/cryptoKeys/k"
        }
        assert "gcp" in json_data["cmek"]
        assert (
            json_data["cmek"]["gcp"] == "projects/p/locations/l/keyRings/r/cryptoKeys/k"
        )

        # Deserialize
        deserialized = Schema.deserialize_from_json(json_data)
        assert deserialized.cmek is not None
        assert deserialized.cmek.provider == CmekProvider.GCP
        assert deserialized.cmek.resource == cmek.resource

    def test_cmek_schema_without_cmek_serialization(self) -> None:
        """Test Schema serialization without CMEK (backward compatibility)."""
        schema = Schema()
        # Don't set CMEK

        # Serialize
        json_data = schema.serialize_to_json()

        # CMEK should not be in JSON
        assert "cmek" not in json_data

        # Deserialize
        deserialized = Schema.deserialize_from_json(json_data)
        assert deserialized.cmek is None

    def test_cmek_invalid_deserialization(self) -> None:
        """Test that invalid CMEK data raises a warning and sets cmek to None."""
        with pytest.raises(ValueError, match="Unsupported or missing CMEK provider in data"):
            Schema.deserialize_from_json(
                {"defaults": {}, "keys": {}, "cmek": {}}
            )

        with pytest.raises(ValueError, match="Unsupported or missing CMEK provider in data"):
            Schema.deserialize_from_json(
                {
                    "defaults": {},
                    "keys": {},
                    "cmek": {"invalid_provider": "some-resource"},
                }
            )

def test_sparse_vector_cannot_be_created_globally() -> None:
    """Test that sparse vector index cannot be created globally (without a key)."""
    schema = Schema()
    sparse_config = SparseVectorIndexConfig()

    # Try to enable sparse vector globally - should fail
    with pytest.raises(
        ValueError, match="Sparse vector index must be created on a specific key"
    ):
        schema.create_index(config=sparse_config)


def test_sparse_vector_cannot_be_deleted() -> None:
    """Test that sparse vector index cannot be deleted (temporarily disallowed)."""
    schema = Schema()
    sparse_config = SparseVectorIndexConfig()

    # Create sparse vector on a key first
    schema.create_index(config=sparse_config, key="my_key")
    assert schema.keys["my_key"].sparse_vector is not None
    assert schema.keys["my_key"].sparse_vector.sparse_vector_index is not None
    assert schema.keys["my_key"].sparse_vector.sparse_vector_index.enabled is True

    # Try to delete it - should fail
    with pytest.raises(
        ValueError, match="Deleting sparse vector index is not currently supported"
    ):
        schema.delete_index(config=sparse_config, key="my_key")


def test_create_index_accepts_key_type() -> None:
    """Test that create_index accepts both str and Key types for the key parameter."""
    schema = Schema()

    # Test with string key
    string_config = StringInvertedIndexConfig()
    schema.create_index(config=string_config, key="test_field_str")

    # Verify the index was created with string key
    assert "test_field_str" in schema.keys
    assert schema.keys["test_field_str"].string is not None
    assert schema.keys["test_field_str"].string.string_inverted_index is not None
    assert schema.keys["test_field_str"].string.string_inverted_index.enabled is True

    # Test with Key type
    int_config = IntInvertedIndexConfig()
    schema.create_index(config=int_config, key=Key("test_field_key"))

    # Verify the index was created with Key type (should be stored as string internally)
    assert "test_field_key" in schema.keys
    assert schema.keys["test_field_key"].int_value is not None
    assert schema.keys["test_field_key"].int_value.int_inverted_index is not None
    assert schema.keys["test_field_key"].int_value.int_inverted_index.enabled is True

    # Test that both approaches produce equivalent results
    schema2 = Schema()
    schema2.create_index(config=string_config, key="same_field")

    schema3 = Schema()
    schema3.create_index(config=string_config, key=Key("same_field"))

    # Both should have the same configuration
    assert schema2.keys["same_field"].string is not None
    assert schema2.keys["same_field"].string.string_inverted_index is not None
    assert schema3.keys["same_field"].string is not None
    assert schema3.keys["same_field"].string.string_inverted_index is not None
    assert (
        schema2.keys["same_field"].string.string_inverted_index.enabled
        == schema3.keys["same_field"].string.string_inverted_index.enabled
    )


def test_delete_index_accepts_key_type() -> None:
    """Test that delete_index accepts both str and Key types for the key parameter."""
    schema = Schema()

    # First, create some indexes to delete
    string_config = StringInvertedIndexConfig()
    int_config = IntInvertedIndexConfig()

    # Test delete with string key
    schema.delete_index(config=string_config, key="test_field_str")

    # Verify the index was disabled with string key
    assert "test_field_str" in schema.keys
    assert schema.keys["test_field_str"].string is not None
    assert schema.keys["test_field_str"].string.string_inverted_index is not None
    assert schema.keys["test_field_str"].string.string_inverted_index.enabled is False

    # Test delete with Key type
    schema.delete_index(config=int_config, key=Key("test_field_key"))

    # Verify the index was disabled with Key type (should be stored as string internally)
    assert "test_field_key" in schema.keys
    assert schema.keys["test_field_key"].int_value is not None
    assert schema.keys["test_field_key"].int_value.int_inverted_index is not None
    assert schema.keys["test_field_key"].int_value.int_inverted_index.enabled is False

    # Test that both approaches produce equivalent results
    schema2 = Schema()
    schema2.delete_index(config=string_config, key="same_field")

    schema3 = Schema()
    schema3.delete_index(config=string_config, key=Key("same_field"))

    # Both should have the same configuration
    assert schema2.keys["same_field"].string is not None
    assert schema2.keys["same_field"].string.string_inverted_index is not None
    assert schema3.keys["same_field"].string is not None
    assert schema3.keys["same_field"].string.string_inverted_index is not None
    assert (
        schema2.keys["same_field"].string.string_inverted_index.enabled
        == schema3.keys["same_field"].string.string_inverted_index.enabled
    )


def test_create_index_rejects_special_keys() -> None:
    """Test that create_index rejects special keys like Key.DOCUMENT and Key.EMBEDDING."""
    schema = Schema()
    string_config = StringInvertedIndexConfig()

    # Test that Key.DOCUMENT is rejected (first check catches it)
    with pytest.raises(
        ValueError, match="Cannot create index on special key '#document'"
    ):
        schema.create_index(config=string_config, key=Key.DOCUMENT)

    # Test that Key.EMBEDDING is rejected (first check catches it)
    with pytest.raises(
        ValueError, match="Cannot create index on special key '#embedding'"
    ):
        schema.create_index(config=string_config, key=Key.EMBEDDING)

    # Test that string "#document" is also rejected (for consistency)
    with pytest.raises(
        ValueError, match="Cannot create index on special key '#document'"
    ):
        schema.create_index(config=string_config, key="#document")

    # Test that any other key starting with # is rejected (second check)
    with pytest.raises(ValueError, match="key cannot begin with '#'"):
        schema.create_index(config=string_config, key="#custom_key")

    # Test with Key object for custom special key
    with pytest.raises(ValueError, match="key cannot begin with '#'"):
        schema.create_index(config=string_config, key=Key("#custom"))


def test_delete_index_rejects_special_keys() -> None:
    """Test that delete_index rejects special keys like Key.DOCUMENT and Key.EMBEDDING."""
    schema = Schema()
    string_config = StringInvertedIndexConfig()

    # Test that Key.DOCUMENT is rejected (first check catches it)
    with pytest.raises(
        ValueError, match="Cannot delete index on special key '#document'"
    ):
        schema.delete_index(config=string_config, key=Key.DOCUMENT)

    # Test that Key.EMBEDDING is rejected (first check catches it)
    with pytest.raises(
        ValueError, match="Cannot delete index on special key '#embedding'"
    ):
        schema.delete_index(config=string_config, key=Key.EMBEDDING)

    # Test that string "#embedding" is also rejected (for consistency)
    with pytest.raises(
        ValueError, match="Cannot delete index on special key '#embedding'"
    ):
        schema.delete_index(config=string_config, key="#embedding")

    # Test that any other key starting with # is rejected (second check)
    with pytest.raises(ValueError, match="key cannot begin with '#'"):
        schema.delete_index(config=string_config, key="#custom_key")

    # Test with Key object for custom special key
    with pytest.raises(ValueError, match="key cannot begin with '#'"):
        schema.delete_index(config=string_config, key=Key("#custom"))


def test_vector_index_config_source_key_accepts_key_type() -> None:
    """Test that VectorIndexConfig.source_key accepts both str and Key types."""
    # Test with string
    config1 = VectorIndexConfig(source_key="my_field")
    assert config1.source_key == "my_field"
    assert isinstance(config1.source_key, str)

    # Test with Key object
    config2 = VectorIndexConfig(source_key=Key("my_field"))  # type: ignore[arg-type]
    assert config2.source_key == "my_field"
    assert isinstance(config2.source_key, str)

    # Test with Key.DOCUMENT
    config3 = VectorIndexConfig(source_key=Key.DOCUMENT)  # type: ignore[arg-type]
    assert config3.source_key == "#document"
    assert isinstance(config3.source_key, str)

    # Test that both approaches produce the same result
    config4 = VectorIndexConfig(source_key="test")
    config5 = VectorIndexConfig(source_key=Key("test"))  # type: ignore[arg-type]
    assert config4.source_key == config5.source_key

    # Test with None
    config6 = VectorIndexConfig(source_key=None)
    assert config6.source_key is None

    # Test serialization works correctly
    config7 = VectorIndexConfig(source_key=Key("serialize_test"))  # type: ignore[arg-type]
    config_dict = config7.model_dump()
    assert config_dict["source_key"] == "serialize_test"
    assert isinstance(config_dict["source_key"], str)


def test_sparse_vector_index_config_source_key_accepts_key_type() -> None:
    """Test that SparseVectorIndexConfig.source_key accepts both str and Key types."""
    # Test with string
    config1 = SparseVectorIndexConfig(source_key="my_field")
    assert config1.source_key == "my_field"
    assert isinstance(config1.source_key, str)

    # Test with Key object
    config2 = SparseVectorIndexConfig(source_key=Key("my_field"))  # type: ignore[arg-type]
    assert config2.source_key == "my_field"
    assert isinstance(config2.source_key, str)

    # Test with Key.DOCUMENT
    config3 = SparseVectorIndexConfig(source_key=Key.DOCUMENT)  # type: ignore[arg-type]
    assert config3.source_key == "#document"
    assert isinstance(config3.source_key, str)

    # Test that both approaches produce the same result
    config4 = SparseVectorIndexConfig(source_key="test")
    config5 = SparseVectorIndexConfig(source_key=Key("test"))  # type: ignore[arg-type]
    assert config4.source_key == config5.source_key

    # Test with None
    config6 = SparseVectorIndexConfig(source_key=None)
    assert config6.source_key is None

    # Test serialization works correctly
    config7 = SparseVectorIndexConfig(source_key=Key("serialize_test"))  # type: ignore[arg-type]
    config_dict = config7.model_dump()
    assert config_dict["source_key"] == "serialize_test"
    assert isinstance(config_dict["source_key"], str)


def test_config_source_key_rejects_invalid_types() -> None:
    """Test that config validators reject invalid types for source_key."""
    # Test VectorIndexConfig rejects invalid types
    with pytest.raises(ValueError, match="source_key must be str or Key"):
        VectorIndexConfig(source_key=123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="source_key must be str or Key"):
        VectorIndexConfig(source_key=["not", "valid"])  # type: ignore[arg-type]

    # Test SparseVectorIndexConfig rejects invalid types
    with pytest.raises(ValueError, match="source_key must be str or Key"):
        SparseVectorIndexConfig(source_key=123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="source_key must be str or Key"):
        SparseVectorIndexConfig(source_key={"not": "valid"})  # type: ignore[arg-type]


def test_config_source_key_validates_special_keys() -> None:
    """Test that source_key only allows #document, rejects other special keys."""
    # Test VectorIndexConfig
    # #document is allowed (string)
    config1 = VectorIndexConfig(source_key="#document")
    assert config1.source_key == "#document"

    # #document is allowed (Key)
    config2 = VectorIndexConfig(source_key=Key.DOCUMENT)  # type: ignore[arg-type]
    assert config2.source_key == "#document"

    # #embedding is rejected (string)
    with pytest.raises(ValueError, match="source_key cannot begin with '#'"):
        VectorIndexConfig(source_key="#embedding")

    # #embedding is rejected (Key)
    with pytest.raises(ValueError, match="source_key cannot begin with '#'"):
        VectorIndexConfig(source_key=Key.EMBEDDING)  # type: ignore[arg-type]

    # #metadata is rejected
    with pytest.raises(ValueError, match="source_key cannot begin with '#'"):
        VectorIndexConfig(source_key="#metadata")

    # #score is rejected
    with pytest.raises(ValueError, match="source_key cannot begin with '#'"):
        VectorIndexConfig(source_key="#score")

    # Any other key starting with # is rejected
    with pytest.raises(ValueError, match="source_key cannot begin with '#'"):
        VectorIndexConfig(source_key="#custom")

    # Regular keys (no #) are allowed
    config3 = VectorIndexConfig(source_key="my_field")
    assert config3.source_key == "my_field"

    # Test SparseVectorIndexConfig
    # #document is allowed (string)
    config4 = SparseVectorIndexConfig(source_key="#document")
    assert config4.source_key == "#document"

    # #document is allowed (Key)
    config5 = SparseVectorIndexConfig(source_key=Key.DOCUMENT)  # type: ignore[arg-type]
    assert config5.source_key == "#document"

    # #embedding is rejected (string)
    with pytest.raises(ValueError, match="source_key cannot begin with '#'"):
        SparseVectorIndexConfig(source_key="#embedding")

    # #embedding is rejected (Key)
    with pytest.raises(ValueError, match="source_key cannot begin with '#'"):
        SparseVectorIndexConfig(source_key=Key.EMBEDDING)  # type: ignore[arg-type]

    # #metadata is rejected
    with pytest.raises(ValueError, match="source_key cannot begin with '#'"):
        SparseVectorIndexConfig(source_key="#metadata")

    # Regular keys (no #) are allowed
    config6 = SparseVectorIndexConfig(source_key="my_field")
    assert config6.source_key == "my_field"


def test_sparse_vector_config_requires_ef_with_source_key() -> None:
    """Test that SparseVectorIndexConfig raises ValueError when source_key is provided without embedding_function."""
    schema = Schema()

    # Attempt to create sparse vector index with source_key but no embedding_function
    with pytest.raises(ValueError) as exc_info:
        schema.create_index(
            key="invalid_sparse",
            config=SparseVectorIndexConfig(
                source_key="text_field",
                # No embedding_function provided - should raise ValueError
            ),
        )

    # Verify the error message mentions both source_key and embedding_function
    error_msg = str(exc_info.value)
    assert "source_key" in error_msg.lower()
    assert "embedding_function" in error_msg.lower()


def test_config_classes_reject_invalid_fields() -> None:
    """Test that all config classes reject invalid/unknown fields."""
    # Test SparseVectorIndexConfig rejects invalid field 'key' instead of 'source_key'
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        SparseVectorIndexConfig(key=Key.DOCUMENT)  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "key" in error_msg.lower()
    assert "extra" in error_msg.lower() or "permitted" in error_msg.lower()

    # Test VectorIndexConfig rejects invalid fields
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        VectorIndexConfig(invalid_field="test")  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "invalid_field" in error_msg.lower()
    assert "extra" in error_msg.lower() or "permitted" in error_msg.lower()

    # Test FtsIndexConfig rejects invalid fields
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        FtsIndexConfig(invalid_field="test")  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "invalid_field" in error_msg.lower()
    assert "extra" in error_msg.lower() or "permitted" in error_msg.lower()

    # Test StringInvertedIndexConfig rejects invalid fields
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        StringInvertedIndexConfig(invalid_field="test")  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "invalid_field" in error_msg.lower()
    assert "extra" in error_msg.lower() or "permitted" in error_msg.lower()

    # Test IntInvertedIndexConfig rejects invalid fields
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        IntInvertedIndexConfig(invalid_field=123)  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "invalid_field" in error_msg.lower()
    assert "extra" in error_msg.lower() or "permitted" in error_msg.lower()

    # Test FloatInvertedIndexConfig rejects invalid fields
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        FloatInvertedIndexConfig(invalid_field=1.23)  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "invalid_field" in error_msg.lower()
    assert "extra" in error_msg.lower() or "permitted" in error_msg.lower()

    # Test BoolInvertedIndexConfig rejects invalid fields
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        BoolInvertedIndexConfig(invalid_field=True)  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "invalid_field" in error_msg.lower()
    assert "extra" in error_msg.lower() or "permitted" in error_msg.lower()

    # Test HnswIndexConfig rejects invalid fields
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        HnswIndexConfig(invalid_field=123)  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "invalid_field" in error_msg.lower()

    # Test HnswIndexConfig accepts all valid fields (all are defined in the model)
    # This should not raise an error
    config = HnswIndexConfig(
        ef_construction=100,
        max_neighbors=16,
        ef_search=100,
        num_threads=4,
        batch_size=100,
        sync_threshold=1000,
        resize_factor=1.2,
    )
    assert config.ef_construction == 100
    assert config.max_neighbors == 16

    # Test SpannIndexConfig rejects invalid fields
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        SpannIndexConfig(invalid_field=123)  # type: ignore[call-arg]

    error_msg = str(exc_info.value)
    assert "invalid_field" in error_msg.lower()

    # Test SpannIndexConfig accepts internal fields (allowed by validator but not stored)
    # These should not raise an error but won't be stored as attributes
    spann_config = SpannIndexConfig(
        search_nprobe=64,
        search_rng_factor=1.0,  # type: ignore[call-arg]  # internal field - allowed but not stored
        search_rng_epsilon=10.0,  # type: ignore[call-arg]  # internal field - allowed but not stored
        nreplica_count=8,  # type: ignore[call-arg]  # internal field - allowed but not stored
        write_nprobe=32,
        write_rng_factor=1.0,  # type: ignore[call-arg]  # internal field - allowed but not stored
        write_rng_epsilon=5.0,  # type: ignore[call-arg]  # internal field - allowed but not stored
        split_threshold=50,
        num_samples_kmeans=1000,  # type: ignore[call-arg]  # internal field - allowed but not stored
        initial_lambda=100.0,  # type: ignore[call-arg]  # internal field - allowed but not stored
        reassign_neighbor_count=64,
        merge_threshold=25,
        num_centers_to_merge_to=8,  # type: ignore[call-arg]  # internal field - allowed but not stored
        ef_construction=200,
        ef_search=200,
        max_neighbors=64,
    )
    # Verify defined fields are stored
    assert spann_config.search_nprobe == 64
    assert spann_config.write_nprobe == 32
    assert spann_config.ef_construction == 200
    # Verify internal fields are not stored (they're ignored due to "extra": "ignore")
    assert not hasattr(spann_config, "search_rng_factor")
    assert not hasattr(spann_config, "nreplica_count")
    assert not hasattr(spann_config, "num_samples_kmeans")
