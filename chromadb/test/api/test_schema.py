import pytest
from typing import cast, Any
from chromadb.api.types import (
    Schema, FtsIndexConfig, VectorIndexConfig, SparseVectorIndexConfig,
    StringInvertedIndexConfig, HnswIndexConfig, Documents
)
from chromadb.utils.embedding_functions import EmbeddingFunction


class TestSchema:
    """Test cases for the Schema builder class."""

    def test_create_index_specific_config_global(self) -> None:
        """Test creating a specific index type globally."""
        schema = Schema()
        fts_config = FtsIndexConfig()

        result = schema.create_index(config=fts_config)

        # Should return self for chaining
        assert result is schema

        # Should have FTS config in global configs
        assert "FtsIndexConfig" in schema._global_configs
        assert schema._global_configs["FtsIndexConfig"].config == fts_config
        assert schema._global_configs["FtsIndexConfig"].enabled is True

        # Should not have any key-specific configs
        assert len(schema._index_configs) == 0

    def test_create_index_specific_config_key(self) -> None:
        """Test creating a specific index type for a specific key."""
        schema = Schema()
        fts_config = FtsIndexConfig()

        result = schema.create_index(config=fts_config, key="key1")

        # Should return self for chaining
        assert result is schema

        # Should have FTS config for key1
        assert "key1" in schema._index_configs
        assert "FtsIndexConfig" in schema._index_configs["key1"]
        assert schema._index_configs["key1"]["FtsIndexConfig"].config == fts_config
        assert schema._index_configs["key1"]["FtsIndexConfig"].enabled is True

        # Should not have any global configs
        assert len(schema._global_configs) == 0

    def test_create_index_all_types_key(self) -> None:
        """Test creating all index types for a specific key."""
        schema = Schema()

        result = schema.create_index(key="key1")

        # Should return self for chaining
        assert result is schema

        # Should have all index types for key1
        expected_types = [
            "FtsIndexConfig", "VectorIndexConfig", "SparseVectorIndexConfig",
            "StringInvertedIndexConfig", "IntInvertedIndexConfig",
            "FloatInvertedIndexConfig", "BoolInvertedIndexConfig"
        ]

        assert "key1" in schema._index_configs
        for index_type in expected_types:
            assert index_type in schema._index_configs["key1"]
            assert schema._index_configs["key1"][index_type].enabled is True
            assert schema._index_configs["key1"][index_type].config is not None

        # Should not have any global configs
        assert len(schema._global_configs) == 0

    def test_create_index_disallow_both_none(self) -> None:
        """Test that create_index with both config=None and key=None raises ValueError."""
        schema = Schema()

        with pytest.raises(ValueError, match="Cannot enable all index types globally"):
            schema.create_index()

    def test_delete_index_specific_config_global(self) -> None:
        """Test deleting a specific index type globally."""
        schema = Schema()
        fts_config = FtsIndexConfig()

        # First create it
        schema.create_index(config=fts_config)
        assert "FtsIndexConfig" in schema._global_configs
        assert schema._global_configs["FtsIndexConfig"].enabled is True

        # Then delete it
        result = schema.delete_index(config=fts_config)

        # Should return self for chaining
        assert result is schema

        # Should still be in global configs but disabled
        assert "FtsIndexConfig" in schema._global_configs
        assert schema._global_configs["FtsIndexConfig"].enabled is False

    def test_delete_index_specific_config_key(self) -> None:
        """Test deleting a specific index type for a specific key."""
        schema = Schema()
        fts_config = FtsIndexConfig()

        # First create it
        schema.create_index(config=fts_config, key="key1")
        assert schema._index_configs["key1"]["FtsIndexConfig"].enabled is True

        # Then delete it
        result = schema.delete_index(config=fts_config, key="key1")

        # Should return self for chaining
        assert result is schema

        # Should still be in key configs but disabled
        assert "FtsIndexConfig" in schema._index_configs["key1"]
        assert schema._index_configs["key1"]["FtsIndexConfig"].enabled is False

    def test_delete_index_all_types_key(self) -> None:
        """Test deleting all index types for a specific key."""
        schema = Schema()

        # First create all types for key1
        schema.create_index(key="key1")
        expected_types = [
            "FtsIndexConfig", "VectorIndexConfig", "SparseVectorIndexConfig",
            "StringInvertedIndexConfig", "IntInvertedIndexConfig",
            "FloatInvertedIndexConfig", "BoolInvertedIndexConfig"
        ]

        # Verify all are enabled
        for index_type in expected_types:
            assert schema._index_configs["key1"][index_type].enabled is True

        # Then delete all for key1
        result = schema.delete_index(key="key1")

        # Should return self for chaining
        assert result is schema

        # All should still be present but disabled
        for index_type in expected_types:
            assert index_type in schema._index_configs["key1"]
            assert schema._index_configs["key1"][index_type].enabled is False

    def test_delete_index_disallow_both_none(self) -> None:
        """Test that delete_index with both config=None and key=None raises ValueError."""
        schema = Schema()

        with pytest.raises(ValueError, match="Cannot disable all indexes"):
            schema.delete_index()

    def test_method_chaining(self) -> None:
        """Test that methods can be chained together."""
        schema = Schema()
        fts_config = FtsIndexConfig()
        string_config = StringInvertedIndexConfig()

        result = (schema
                  .create_index(config=fts_config, key="key1")
                  .create_index(config=string_config, key="key2")
                  .delete_index(config=fts_config, key="key1")
                  .create_index(key="key3"))

        # Should return the original schema instance
        assert result is schema

        # Verify the final state
        assert schema._index_configs["key1"]["FtsIndexConfig"].enabled is False
        assert schema._index_configs["key2"]["StringInvertedIndexConfig"].enabled is True
        assert len(schema._index_configs["key3"]) > 0  # All types should be enabled for key3

    def test_multiple_keys_same_index_type(self) -> None:
        """Test that the same index type can be configured differently for different keys."""
        schema = Schema()
        fts_config1 = FtsIndexConfig()
        fts_config2 = FtsIndexConfig()

        schema.create_index(config=fts_config1, key="key1")
        schema.create_index(config=fts_config2, key="key2")

        # Both keys should have FTS config
        assert "FtsIndexConfig" in schema._index_configs["key1"]
        assert "FtsIndexConfig" in schema._index_configs["key2"]

        # They should be independent
        assert schema._index_configs["key1"]["FtsIndexConfig"].config == fts_config1
        assert schema._index_configs["key2"]["FtsIndexConfig"].config == fts_config2

    def test_global_and_key_specific_configs(self) -> None:
        """Test mixing global and key-specific configurations."""
        schema = Schema()
        fts_config = FtsIndexConfig()
        string_config = StringInvertedIndexConfig()

        # Create global FTS config
        schema.create_index(config=fts_config)

        # Create key-specific string config
        schema.create_index(config=string_config, key="key1")

        # Verify global config
        assert "FtsIndexConfig" in schema._global_configs
        assert schema._global_configs["FtsIndexConfig"].enabled is True

        # Verify key-specific config
        assert "StringInvertedIndexConfig" in schema._index_configs["key1"]
        assert schema._index_configs["key1"]["StringInvertedIndexConfig"].enabled is True

    def test_vector_index_with_hnsw_config(self) -> None:
        """Test creating a vector index with HNSW configuration."""
        schema = Schema()
        hnsw_config = HnswIndexConfig(ef_construction=200)

        # Create a proper mock embedding function that matches the protocol
        class MockEmbeddingFunction:
            def __call__(self, input: Any) -> list[list[float]]:
                return [[1.0, 2.0, 3.0]] if isinstance(input, list) else [[1.0, 2.0, 3.0]]

        vector_config = VectorIndexConfig(
            space="l2",
            embedding_function=cast(EmbeddingFunction[Documents], MockEmbeddingFunction()),
            source_key="embeddings",
            hnsw=hnsw_config
        )

        result = schema.create_index(config=vector_config, key="key1")

        assert result is schema
        assert "VectorIndexConfig" in schema._index_configs["key1"]
        assert schema._index_configs["key1"]["VectorIndexConfig"].enabled is True
        assert schema._index_configs["key1"]["VectorIndexConfig"].config == vector_config

    def test_sparse_vector_index_config(self) -> None:
        """Test creating a sparse vector index configuration."""
        schema = Schema()

        # Create a proper mock embedding function that matches the protocol
        class MockEmbeddingFunction:
            def __call__(self, input: Any) -> list[list[float]]:
                return [[1.0, 2.0, 3.0]] if isinstance(input, list) else [[1.0, 2.0, 3.0]]

        sparse_config = SparseVectorIndexConfig(
            embedding_function=cast(EmbeddingFunction[Documents], MockEmbeddingFunction()),
            source_key="sparse_embeddings"
        )

        result = schema.create_index(config=sparse_config, key="key1")

        assert result is schema
        assert "SparseVectorIndexConfig" in schema._index_configs["key1"]
        assert schema._index_configs["key1"]["SparseVectorIndexConfig"].enabled is True

    def test_empty_schema_initialization(self) -> None:
        """Test that a new schema starts empty."""
        schema = Schema()

        assert len(schema._global_configs) == 0
        assert len(schema._index_configs) == 0

    def test_delete_nonexistent_key(self) -> None:
        """Test deleting from a key that doesn't exist."""
        schema = Schema()
        fts_config = FtsIndexConfig()

        # Try to delete from non-existent key
        result = schema.delete_index(config=fts_config, key="nonexistent")

        assert result is schema
        # Should create the key with disabled config
        assert "nonexistent" in schema._index_configs
        assert "FtsIndexConfig" in schema._index_configs["nonexistent"]
        assert schema._index_configs["nonexistent"]["FtsIndexConfig"].enabled is False

    def test_delete_nonexistent_global_config(self) -> None:
        """Test deleting a global config that doesn't exist."""
        schema = Schema()
        fts_config = FtsIndexConfig()

        # Try to delete non-existent global config
        result = schema.delete_index(config=fts_config)

        assert result is schema
        # Should create the global config as disabled
        assert "FtsIndexConfig" in schema._global_configs
        assert schema._global_configs["FtsIndexConfig"].enabled is False

    def test_complex_workflow(self) -> None:
        """Test a complex workflow with multiple operations."""
        schema = Schema()
        fts_config = FtsIndexConfig()
        string_config = StringInvertedIndexConfig()

        # Complex workflow
        result = (schema
                  .create_index(config=fts_config)  # Global FTS
                  .create_index(key="key1")  # All types for key1
                  .create_index(config=string_config, key="key2")  # String for key2
                  .delete_index(config=fts_config, key="key1")  # Disable FTS for key1
                  .delete_index(key="key2")  # Disable all for key2
                  .create_index(config=fts_config, key="key3"))  # FTS for key3

        assert result is schema

        # Verify final state
        # Global: FTS enabled
        assert schema._global_configs["FtsIndexConfig"].enabled is True

        # key1: All types enabled except FTS disabled
        assert schema._index_configs["key1"]["FtsIndexConfig"].enabled is False
        assert schema._index_configs["key1"]["StringInvertedIndexConfig"].enabled is True

        # key2: All types disabled
        assert schema._index_configs["key2"]["StringInvertedIndexConfig"].enabled is False

        # key3: FTS enabled
        assert schema._index_configs["key3"]["FtsIndexConfig"].enabled is True
