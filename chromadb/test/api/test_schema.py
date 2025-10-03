import pytest
from typing import cast, Any, Dict
from chromadb.api.types import (
    Schema, FtsIndexConfig, VectorIndexConfig, SparseVectorIndexConfig,
    StringInvertedIndexConfig, HnswIndexConfig, Documents, InternalSchema,
    InternalFtsIndex, InternalVectorIndex, InternalStringInvertedIndex
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

    def test_to_internal_schema_simple(self) -> None:
        """Test conversion to InternalSchema with simple configs."""
        schema = Schema()
        fts_config = FtsIndexConfig()
        schema.create_index(config=fts_config, key="title")

        internal = InternalSchema(schema)

        # Check structure
        assert "defaults" in internal.model_dump()
        assert "key_overrides" in internal.model_dump()

        # With initialization enabled, defaults are populated with 6 value types
        assert len(internal.defaults) == 6

        # Check that all expected value types have defaults
        expected_value_types = ["#string", "#float", "#float_list", "#sparse_vector", "#bool", "#int"]
        for value_type in expected_value_types:
            assert value_type in internal.defaults

        # Check key override - uses InternalFtsIndex
        assert "title" in internal.key_overrides
        assert "#string" in internal.key_overrides["title"]
        fts_override = internal.key_overrides["title"]["#string"]["$fts_index"]
        assert isinstance(fts_override, InternalFtsIndex)
        assert fts_override.enabled is True
        assert isinstance(fts_override.config, FtsIndexConfig)

    def test_to_internal_schema_complex(self) -> None:
        """Test conversion to InternalSchema with complex configs."""
        schema = Schema()

        # Create a vector config with HNSW parameters
        class MockEmbeddingFunction:
            def __call__(self, input: Any) -> list[list[float]]:
                return [[1.0, 2.0, 3.0]]

        vector_config = VectorIndexConfig(
            space="cosine",
            embedding_function=cast(EmbeddingFunction[Documents], MockEmbeddingFunction()),
            source_key="document",
            hnsw=HnswIndexConfig(ef_construction=200, max_neighbors=16, ef_search=100)
        )
        schema.create_index(config=vector_config, key="embedding")

        internal = InternalSchema(schema)

        # Check that the vector config uses InternalVectorIndex
        embedding_override = internal.key_overrides["embedding"]["#float_list"]["$vector_index"]
        assert isinstance(embedding_override, InternalVectorIndex)
        assert embedding_override.enabled is True
        assert isinstance(embedding_override.config, VectorIndexConfig)
        assert embedding_override.config.space == "cosine"
        assert embedding_override.config.source_key == "document"
        assert embedding_override.config.embedding_function is not None
        assert embedding_override.config.hnsw is not None
        assert embedding_override.config.hnsw.ef_construction == 200
        assert embedding_override.config.hnsw.max_neighbors == 16
        assert embedding_override.config.hnsw.ef_search == 100

    def test_to_internal_schema_global_configs(self) -> None:
        """Test conversion with global configurations."""
        schema = Schema()

        # Create global string inverted index
        string_config = StringInvertedIndexConfig()
        schema.create_index(config=string_config)

        internal = InternalSchema(schema)

        # Check that global config affects defaults - uses InternalStringInvertedIndex
        string_override = internal.defaults["#string"]["$string_inverted_index"]
        assert isinstance(string_override, InternalStringInvertedIndex)
        assert string_override.enabled is True
        assert isinstance(string_override.config, StringInvertedIndexConfig)
        # With initialization enabled, FTS index is present in defaults (set to False by default)
        assert "$fts_index" in internal.defaults["#string"]
        fts_default = internal.defaults["#string"]["$fts_index"]
        assert isinstance(fts_default, InternalFtsIndex)
        assert fts_default.enabled is False

    def test_to_internal_schema_mixed_configs(self) -> None:
        """Test conversion with both global and key-specific configs."""
        schema = Schema()

        # Global config
        string_config = StringInvertedIndexConfig()
        schema.create_index(config=string_config)

        # Key-specific override
        fts_config = FtsIndexConfig()
        schema.create_index(config=fts_config, key="document")

        internal = InternalSchema(schema)

        # Check global defaults - uses InternalStringInvertedIndex
        string_global = internal.defaults["#string"]["$string_inverted_index"]
        assert isinstance(string_global, InternalStringInvertedIndex)
        assert string_global.enabled is True
        assert isinstance(string_global.config, StringInvertedIndexConfig)
        # With initialization enabled, FTS index is present in defaults (set to False by default)
        assert "$fts_index" in internal.defaults["#string"]
        fts_default = internal.defaults["#string"]["$fts_index"]
        assert isinstance(fts_default, InternalFtsIndex)
        assert fts_default.enabled is False

        # Check key override - uses InternalFtsIndex
        assert "document" in internal.key_overrides
        fts_override = internal.key_overrides["document"]["#string"]["$fts_index"]
        assert isinstance(fts_override, InternalFtsIndex)
        assert fts_override.enabled is True
        assert isinstance(fts_override.config, FtsIndexConfig)

    def test_to_internal_schema_disabled_config(self) -> None:
        """Test that disabled configs are preserved with enabled=False."""
        schema = Schema()

        # Create a vector config with parameters
        class MockEmbeddingFunction:
            def __call__(self, input: Any) -> list[list[float]]:
                return [[1.0, 2.0, 3.0]]

        vector_config = VectorIndexConfig(
            space="cosine",
            embedding_function=cast(EmbeddingFunction[Documents], MockEmbeddingFunction()),
            source_key="document",
            hnsw=HnswIndexConfig(ef_construction=200, max_neighbors=16)
        )

        # Add it enabled first, then disable it
        schema.create_index(config=vector_config, key="embedding")
        schema.delete_index(config=vector_config, key="embedding")

        internal = InternalSchema(schema)

        # Check that the config is preserved but with enabled=False
        embedding_override = internal.key_overrides["embedding"]["#float_list"]["$vector_index"]
        assert isinstance(embedding_override, InternalVectorIndex)
        assert embedding_override.enabled is False  # ← Disabled but config preserved!
        assert isinstance(embedding_override.config, VectorIndexConfig)
        assert embedding_override.config.space == "cosine"  # ← Config params preserved
        assert embedding_override.config.hnsw is not None
        assert embedding_override.config.hnsw.ef_construction == 200  # ← Nested config preserved

    def test_default_internal_schema_population(self) -> None:
        """Test that InternalSchema starts with default population."""
        # Empty schema should have defaults populated
        schema = Schema()
        internal = InternalSchema(schema)

        # With initialization enabled, defaults and key_overrides are populated
        assert len(internal.defaults) == 6  # 6 value types with defaults
        assert len(internal.key_overrides) == 2  # $document and $embedding keys

        # Check that all expected value types have defaults
        expected_value_types = ["#string", "#float", "#float_list", "#sparse_vector", "#bool", "#int"]
        for value_type in expected_value_types:
            assert value_type in internal.defaults

    def test_default_key_overrides_population(self) -> None:
        """Test that InternalSchema starts with default key overrides."""
        # Empty schema should have key overrides populated
        schema = Schema()
        internal = InternalSchema(schema)

        # With initialization enabled, key_overrides are populated with $document and $embedding
        assert len(internal.key_overrides) == 2
        assert "$document" in internal.key_overrides
        assert "$embedding" in internal.key_overrides

    def test_user_config_overrides_defaults(self) -> None:
        """Test that user configurations are properly stored in defaults."""
        schema = Schema()

        # User enables FTS globally
        fts_config = FtsIndexConfig()
        schema.create_index(config=fts_config)

        # User disables string inverted index globally
        string_config = StringInvertedIndexConfig()
        schema.delete_index(config=string_config)

        internal = InternalSchema(schema)

        # Check that user global configs are stored
        fts_override = internal.defaults["#string"]["$fts_index"]
        assert isinstance(fts_override, InternalFtsIndex)
        assert fts_override.enabled is True  # User enabled it

        string_inverted_override = internal.defaults["#string"]["$string_inverted_index"]
        assert isinstance(string_inverted_override, InternalStringInvertedIndex)
        assert string_inverted_override.enabled is False  # User disabled it

    def test_user_config_overrides_key_defaults(self) -> None:
        """Test that user key-specific configs are properly stored."""
        schema = Schema()

        # User disables FTS for $document
        fts_config = FtsIndexConfig()
        schema.delete_index(config=fts_config, key="$document")

        # User adds vector index for custom key
        vector_config = VectorIndexConfig()
        schema.create_index(config=vector_config, key="custom_key")

        internal = InternalSchema(schema)

        # Check that user override for $document worked
        doc_fts_override = internal.key_overrides["$document"]["#string"]["$fts_index"]
        assert isinstance(doc_fts_override, InternalFtsIndex)
        assert doc_fts_override.enabled is False  # User disabled it

        # Check that user's custom key was added
        assert "custom_key" in internal.key_overrides
        custom_vector_override = internal.key_overrides["custom_key"]["#float_list"]["$vector_index"]
        assert isinstance(custom_vector_override, InternalVectorIndex)
        assert custom_vector_override.enabled is True  # User enabled it

    def test_complete_default_structure_matches_spec(self) -> None:
        """Test that InternalSchema starts empty with no default population."""
        schema = Schema()
        internal = InternalSchema(schema)

        # With initialization enabled, both should be populated
        assert len(internal.defaults) == 6  # 6 value types with defaults
        assert len(internal.key_overrides) == 2  # $document and $embedding keys

    def test_all_value_types_have_base_defaults(self) -> None:
        """Test that InternalSchema starts empty with no default population."""
        schema = Schema()
        internal = InternalSchema(schema)

        # With initialization enabled, defaults are populated
        assert len(internal.defaults) == 6  # 6 value types with defaults

    def test_embedding_vector_index_has_source_key(self) -> None:
        """Test that InternalSchema starts empty with no default population."""
        schema = Schema()
        internal = InternalSchema(schema)

        # With initialization enabled, key_overrides are populated
        assert len(internal.key_overrides) == 2  # $document and $embedding keys

    # Edge Case Tests
    def test_edge_case_special_character_keys(self) -> None:
        """Test that keys with special characters work correctly."""
        schema = Schema()

        # Test various special characters
        special_keys = [
            "key-with-dashes",
            "key_with_underscores",
            "key.with.dots",
            "key with spaces",
            "key@with#symbols$",
            "key/with/slashes",
            "key:with:colons",
            "key[with]brackets",
            "key{with}braces"
        ]

        fts_config = FtsIndexConfig()
        for key in special_keys:
            schema.create_index(config=fts_config, key=key)

        internal = InternalSchema(schema)

        # Verify all keys are present
        for key in special_keys:
            assert key in internal.key_overrides
            fts_override = internal.key_overrides[key]["#string"]["$fts_index"]
            assert isinstance(fts_override, InternalFtsIndex)
            assert fts_override.enabled is True

    def test_edge_case_very_long_keys(self) -> None:
        """Test that very long key names work correctly."""
        schema = Schema()

        # Test progressively longer keys
        long_keys = [
            "a" * 100,   # 100 chars
            "b" * 1000,  # 1K chars
            "c" * 10000,   # 10K chars
        ]

        fts_config = FtsIndexConfig()
        for key in long_keys:
            schema.create_index(config=fts_config, key=key)

        internal = InternalSchema(schema)

        # Verify all long keys work
        for key in long_keys:
            assert key in internal.key_overrides
            fts_override = internal.key_overrides[key]["#string"]["$fts_index"]
            assert isinstance(fts_override, InternalFtsIndex)
            assert fts_override.enabled is True

    def test_edge_case_overriding_hardcoded_keys(self) -> None:
        """Test user-specified $document and $embedding keys."""
        schema = Schema()

        # User disables FTS for $document
        fts_config = FtsIndexConfig()
        schema.delete_index(config=fts_config, key="$document")

        # User adds vector index for $embedding with custom config
        vector_config = VectorIndexConfig(space="cosine", source_key="custom_source")
        schema.create_index(config=vector_config, key="$embedding")

        internal = InternalSchema(schema)

        # Verify $document override worked
        doc_fts = internal.key_overrides["$document"]["#string"]["$fts_index"]
        assert isinstance(doc_fts, InternalFtsIndex)
        assert doc_fts.enabled is False  # User disabled it

        # Verify $embedding override worked
        embedding_vector = internal.key_overrides["$embedding"]["#float_list"]["$vector_index"]
        assert isinstance(embedding_vector, InternalVectorIndex)
        assert embedding_vector.enabled is True
        assert embedding_vector.config.space == "cosine"  # User's setting
        assert embedding_vector.config.source_key == "custom_source"  # User's setting

    def test_edge_case_same_key_multiple_config_types(self) -> None:
        """Test adding multiple different config types to the same key."""
        schema = Schema()

        key = "multi_config_key"

        # Add multiple config types to same key
        fts_config = FtsIndexConfig()
        vector_config = VectorIndexConfig(space="l2")
        string_config = StringInvertedIndexConfig()

        schema.create_index(config=fts_config, key=key)
        schema.create_index(config=vector_config, key=key)
        schema.create_index(config=string_config, key=key)

        internal = InternalSchema(schema)

        # Verify all config types are present for the key
        assert key in internal.key_overrides
        key_configs = internal.key_overrides[key]

        # Should have both string and float_list value types
        assert "#string" in key_configs
        assert "#float_list" in key_configs

        # Check string configs
        fts_override = key_configs["#string"]["$fts_index"]
        assert isinstance(fts_override, InternalFtsIndex)
        assert fts_override.enabled is True

        string_override = key_configs["#string"]["$string_inverted_index"]
        assert isinstance(string_override, InternalStringInvertedIndex)
        assert string_override.enabled is True

        # Check vector config
        vector_override = key_configs["#float_list"]["$vector_index"]
        assert isinstance(vector_override, InternalVectorIndex)
        assert vector_override.enabled is True
        assert vector_override.config.space == "l2"

    def test_edge_case_config_object_reuse(self) -> None:
        """Test that reusing the same config object works correctly."""
        schema = Schema()

        # Reuse the same config object for multiple keys
        shared_fts_config = FtsIndexConfig()

        keys = ["key1", "key2", "key3"]
        for key in keys:
            schema.create_index(config=shared_fts_config, key=key)

        internal = InternalSchema(schema)

        # Verify all keys have the config
        for key in keys:
            assert key in internal.key_overrides
            fts_override = internal.key_overrides[key]["#string"]["$fts_index"]
            assert isinstance(fts_override, InternalFtsIndex)
            assert fts_override.enabled is True
            # Each should have its own config instance (not shared reference)
            assert isinstance(fts_override.config, FtsIndexConfig)

    def test_edge_case_empty_vs_none_embedding_function(self) -> None:
        """Test VectorIndexConfig with None vs default embedding function."""
        schema = Schema()

        # Test with None embedding function (legacy behavior)
        vector_config_none = VectorIndexConfig(embedding_function=None)
        schema.create_index(config=vector_config_none, key="key_none")

        # Test with no embedding function specified (should default to DefaultEmbeddingFunction)
        vector_config_default = VectorIndexConfig()
        schema.create_index(config=vector_config_default, key="key_default")

        internal = InternalSchema(schema)

        # None should remain None (legacy behavior)
        none_override = internal.key_overrides["key_none"]["#float_list"]["$vector_index"]
        assert isinstance(none_override, InternalVectorIndex)
        assert none_override.config.embedding_function is None

        # Default should be DefaultEmbeddingFunction (modern behavior)
        default_override = internal.key_overrides["key_default"]["#float_list"]["$vector_index"]
        assert isinstance(default_override, InternalVectorIndex)
        from chromadb.api.types import DefaultEmbeddingFunction
        assert isinstance(default_override.config.embedding_function, DefaultEmbeddingFunction)

    def test_edge_case_case_sensitivity(self) -> None:
        """Test that key names are case sensitive."""
        schema = Schema()

        fts_config = FtsIndexConfig()

        # Add configs with different cases
        schema.create_index(config=fts_config, key="MyKey")
        schema.create_index(config=fts_config, key="mykey")
        schema.create_index(config=fts_config, key="MYKEY")

        internal = InternalSchema(schema)

        # All should be treated as different keys
        assert "MyKey" in internal.key_overrides
        assert "mykey" in internal.key_overrides
        assert "MYKEY" in internal.key_overrides
        assert len([k for k in internal.key_overrides.keys() if k.lower() == "mykey"]) == 3

    def test_edge_case_large_number_of_keys(self) -> None:
        """Test performance with a large number of keys."""
        schema = Schema()

        # Add 1000 keys
        num_keys = 1000
        fts_config = FtsIndexConfig()

        for i in range(num_keys):
            schema.create_index(config=fts_config, key=f"key_{i:04d}")

        internal = InternalSchema(schema)

        # Should handle large number of keys (plus 2 default keys: $document and $embedding)
        assert len(internal.key_overrides) == num_keys + 2

        # Spot check a few keys
        assert "key_0000" in internal.key_overrides
        assert "key_0500" in internal.key_overrides
        assert "key_0999" in internal.key_overrides

        # Verify structure is correct
        test_key = internal.key_overrides["key_0500"]
        fts_override = test_key["#string"]["$fts_index"]
        assert isinstance(fts_override, InternalFtsIndex)
        assert fts_override.enabled is True

    def test_edge_case_mixed_enable_disable_sequence(self) -> None:
        """Test complex enable/disable sequences on the same key."""
        schema = Schema()

        fts_config = FtsIndexConfig()
        key = "toggle_key"

        # Complex sequence: enable -> disable -> enable -> disable
        schema.create_index(config=fts_config, key=key)  # Enable
        schema.delete_index(config=fts_config, key=key)  # Disable
        schema.create_index(config=fts_config, key=key)  # Enable again
        schema.delete_index(config=fts_config, key=key)  # Disable again

        internal = InternalSchema(schema)

        # Final state should be disabled
        fts_override = internal.key_overrides[key]["#string"]["$fts_index"]
        assert isinstance(fts_override, InternalFtsIndex)
        assert fts_override.enabled is False  # Final state is disabled

    def test_edge_case_all_value_types_single_key(self) -> None:
        """Test adding configs for all possible value types to a single key."""
        schema = Schema()

        key = "all_types_key"

        # Add configs for different value types (those that make sense)
        fts_config = FtsIndexConfig()  # #string
        string_config = StringInvertedIndexConfig()  # #string
        vector_config = VectorIndexConfig()  # #float_list
        sparse_config = SparseVectorIndexConfig()  # #sparse_vector

        schema.create_index(config=fts_config, key=key)
        schema.create_index(config=string_config, key=key)
        schema.create_index(config=vector_config, key=key)
        schema.create_index(config=sparse_config, key=key)

        internal = InternalSchema(schema)

        # Verify all relevant value types are present
        key_configs = internal.key_overrides[key]
        assert "#string" in key_configs
        assert "#float_list" in key_configs
        assert "#sparse_vector" in key_configs

        # Verify specific configs
        assert "$fts_index" in key_configs["#string"]
        assert "$string_inverted_index" in key_configs["#string"]
        assert "$vector_index" in key_configs["#float_list"]
        assert "$sparse_vector_index" in key_configs["#sparse_vector"]

    def test_serialize_to_json_basic(self) -> None:
        """Test basic JSON serialization of InternalSchema."""
        schema = Schema()
        internal = InternalSchema(schema)

        json_data = internal.serialize_to_json()

        # Verify structure
        assert "defaults" in json_data
        assert "key_overrides" in json_data
        assert isinstance(json_data["defaults"], dict)
        assert isinstance(json_data["key_overrides"], dict)

        # With initialization enabled, InternalSchema starts with defaults
        assert len(json_data["defaults"]) == 6  # 6 value types with defaults
        assert len(json_data["key_overrides"]) == 2  # $document and $embedding keys

    def test_serialize_to_json_with_configs(self) -> None:
        """Test JSON serialization of InternalSchema with complex configurations."""
        schema = Schema()
        schema.create_index(VectorIndexConfig(source_key="custom_source"), key="test_key")

        internal = InternalSchema(schema)
        json_data = internal.serialize_to_json()

        # Verify key override serialization
        assert "test_key" in json_data["key_overrides"]
        assert "#float_list" in json_data["key_overrides"]["test_key"]

        vector_config = json_data["key_overrides"]["test_key"]["#float_list"]["$vector_index"]

        # Verify Internal*Index object serialization (nested structure)
        assert isinstance(vector_config, dict)
        assert "enabled" in vector_config
        assert vector_config["enabled"] is True
        assert "config" in vector_config
        assert vector_config["config"]["source_key"] == "custom_source"

    def test_serialize_to_json_roundtrip_compatibility(self) -> None:
        """Test that serialized JSON can be converted back to JSON string."""
        import json

        schema = Schema()
        schema.create_index(VectorIndexConfig(source_key="test"))
        schema.create_index(FtsIndexConfig(), key="doc_key")

        internal = InternalSchema(schema)
        json_data = internal.serialize_to_json()

        # Verify it can be serialized to JSON string without errors
        json_string = json.dumps(json_data)
        assert isinstance(json_string, str)
        assert len(json_string) > 0

        # Verify it can be parsed back
        parsed_data = json.loads(json_string)
        assert parsed_data == json_data

    def test_deserialize_from_json_basic(self) -> None:
        """Test basic JSON deserialization of InternalSchema."""
        # Create original schema
        schema = Schema()
        original = InternalSchema(schema)

        # Serialize and deserialize
        json_data = original.serialize_to_json()
        deserialized = InternalSchema.deserialize_from_json(json_data)

        # Verify structure matches
        assert len(deserialized.defaults) == len(original.defaults)
        assert len(deserialized.key_overrides) == len(original.key_overrides)

        # With initialization enabled, both should be populated
        assert len(deserialized.defaults) == 6  # 6 value types with defaults
        assert len(deserialized.key_overrides) == 2  # $document and $embedding keys

    def test_deserialize_from_json_with_configs(self) -> None:
        """Test JSON deserialization with complex configurations."""
        # Create schema with complex config
        schema = Schema()
        schema.create_index(VectorIndexConfig(source_key="custom_source"), key="test_key")
        original = InternalSchema(schema)

        # Serialize and deserialize
        json_data = original.serialize_to_json()
        deserialized = InternalSchema.deserialize_from_json(json_data)

        # Verify key override was preserved
        assert "test_key" in deserialized.key_overrides
        assert "#float_list" in deserialized.key_overrides["test_key"]

        vector_index = deserialized.key_overrides["test_key"]["#float_list"]["$vector_index"]
        assert isinstance(vector_index, InternalVectorIndex)
        assert vector_index.enabled is True
        assert vector_index.config.source_key == "custom_source"

    def test_serialize_deserialize_roundtrip(self) -> None:
        """Test complete serialize/deserialize roundtrip preserves all data."""
        # Create complex schema
        schema = Schema()
        schema.create_index(VectorIndexConfig(source_key="doc_source"))  # Global config
        schema.create_index(FtsIndexConfig(), key="document")  # Key-specific config
        schema.create_index(VectorIndexConfig(source_key="embed_source"), key="embedding")

        original = InternalSchema(schema)

        # Serialize and deserialize
        json_data = original.serialize_to_json()
        deserialized = InternalSchema.deserialize_from_json(json_data)

        # Verify defaults match
        for value_type in original.defaults:
            assert value_type in deserialized.defaults
            for index_name, index_value in original.defaults[value_type].items():
                deserialized_value = deserialized.defaults[value_type][index_name]
                if isinstance(index_value, bool):
                    assert deserialized_value == index_value
                else:
                    assert isinstance(deserialized_value, type(index_value))
                    if hasattr(index_value, 'enabled') and hasattr(deserialized_value, 'enabled'):
                        assert deserialized_value.enabled == index_value.enabled

        # Verify key overrides match
        for key in original.key_overrides:
            assert key in deserialized.key_overrides
            for value_type in original.key_overrides[key]:
                assert value_type in deserialized.key_overrides[key]
                for index_name, index_value in original.key_overrides[key][value_type].items():
                    deserialized_value = deserialized.key_overrides[key][value_type][index_name]
                    if isinstance(index_value, bool):
                        assert deserialized_value == index_value
                    else:
                        assert isinstance(deserialized_value, type(index_value))
                        if hasattr(index_value, 'enabled') and hasattr(deserialized_value, 'enabled'):
                            assert deserialized_value.enabled == index_value.enabled

    def test_deserialize_from_json_preserves_hardcoded_defaults(self) -> None:
        """Test that deserialization preserves hardcoded key overrides."""
        # Create empty schema
        schema = Schema()
        original = InternalSchema(schema)

        # Serialize and deserialize
        json_data = original.serialize_to_json()
        deserialized = InternalSchema.deserialize_from_json(json_data)

        # With initialization enabled, both should be populated
        assert len(deserialized.defaults) == 6  # 6 value types with defaults
        assert len(deserialized.key_overrides) == 2  # $document and $embedding keys

    def test_deserialize_handles_malformed_json(self) -> None:
        """Test that deserialization handles malformed JSON with appropriate errors."""
        # Test with missing fields
        incomplete_json: Dict[str, Any] = {"defaults": {}}  # Missing key_overrides
        deserialized = InternalSchema.deserialize_from_json(incomplete_json)
        assert isinstance(deserialized.defaults, dict)
        assert isinstance(deserialized.key_overrides, dict)

        # Test with unknown index types (should raise error)
        json_with_unknown = {
            "defaults": {
                "#string": {
                    "$unknown_index": {"some": "data"}
                }
            },
            "key_overrides": {}
        }
        with pytest.raises(ValueError, match="Unknown index type '\\$unknown_index' during deserialization"):
            InternalSchema.deserialize_from_json(json_with_unknown)

    def test_embedding_function_serialization(self) -> None:
        """Test that embedding functions are properly serialized in configs."""
        schema = Schema()

        # Create VectorIndexConfig with None embedding function
        vector_config = VectorIndexConfig(embedding_function=None, source_key="test_source")
        schema.create_index(config=vector_config, key="test_key")

        internal = InternalSchema(schema)
        json_data = internal.serialize_to_json()

        # Check that embedding function is serialized as {"type": "legacy"} (nested structure)
        vector_config_data = json_data["key_overrides"]["test_key"]["#float_list"]["$vector_index"]
        assert "config" in vector_config_data
        assert "embedding_function" in vector_config_data["config"]
        assert vector_config_data["config"]["embedding_function"] == {"type": "legacy"}
        assert vector_config_data["config"]["source_key"] == "test_source"

        # Test roundtrip deserialization
        deserialized = InternalSchema.deserialize_from_json(json_data)
        assert "test_key" in deserialized.key_overrides
        assert "#float_list" in deserialized.key_overrides["test_key"]
        assert "$vector_index" in deserialized.key_overrides["test_key"]["#float_list"]

        # Verify the deserialized config has None embedding function
        vector_index = deserialized.key_overrides["test_key"]["#float_list"]["$vector_index"]
        assert isinstance(vector_index, InternalVectorIndex)
        assert vector_index.config.embedding_function is None
        assert vector_index.config.source_key == "test_source"

    def test_space_serialization_deserialization(self) -> None:
        """Test that space is properly serialized and deserialized."""
        schema = Schema()

        # Create VectorIndexConfig with specific space
        vector_config = VectorIndexConfig(space="cosine", source_key="test")
        schema.create_index(config=vector_config, key="test_key")

        # Serialize to JSON
        internal = InternalSchema(schema)
        json_data = internal.serialize_to_json()

        # Verify space is in serialized data (nested structure)
        vector_index_data = json_data["key_overrides"]["test_key"]["#float_list"]["$vector_index"]
        assert "config" in vector_index_data
        assert "space" in vector_index_data["config"]
        assert vector_index_data["config"]["space"] == "cosine"

        # Deserialize back
        deserialized = InternalSchema.deserialize_from_json(json_data)

        # Verify space is preserved
        deserialized_vector = deserialized.key_overrides["test_key"]["#float_list"]["$vector_index"]
        assert isinstance(deserialized_vector, InternalVectorIndex)
        assert deserialized_vector.config.space == "cosine"

    def test_embedding_function_serialization_deserialization(self) -> None:
        """Test that embedding functions are properly serialized and deserialized."""
        schema = Schema()

        # Use a known embedding function that's registered
        from chromadb.api.types import DefaultEmbeddingFunction

        vector_config = VectorIndexConfig(
            space="l2",
            embedding_function=DefaultEmbeddingFunction(),
            source_key="test"
        )
        schema.create_index(config=vector_config, key="test_key")

        # Serialize to JSON
        internal = InternalSchema(schema)
        json_data = internal.serialize_to_json()

        # Verify embedding function is serialized (nested structure)
        vector_index_data = json_data["key_overrides"]["test_key"]["#float_list"]["$vector_index"]
        assert "config" in vector_index_data
        assert "embedding_function" in vector_index_data["config"]
        ef_config = vector_index_data["config"]["embedding_function"]
        assert ef_config["type"] == "known"
        assert ef_config["name"] == "default"
        assert ef_config["config"] == {}

        # Deserialize back
        deserialized = InternalSchema.deserialize_from_json(json_data)

        # Verify embedding function is reconstructed
        deserialized_vector = deserialized.key_overrides["test_key"]["#float_list"]["$vector_index"]
        assert isinstance(deserialized_vector, InternalVectorIndex)
        assert deserialized_vector.config.embedding_function is not None
        assert hasattr(deserialized_vector.config.embedding_function, '__call__')
        assert deserialized_vector.config.embedding_function.name() == "default"

    def test_legacy_embedding_function_serialization_deserialization(self) -> None:
        """Test that legacy embedding functions are properly handled."""
        schema = Schema()

        # Create VectorIndexConfig with None embedding function (legacy)
        vector_config = VectorIndexConfig(space="l2", embedding_function=None, source_key="test")
        schema.create_index(config=vector_config, key="test_key")

        # Serialize to JSON
        internal = InternalSchema(schema)
        json_data = internal.serialize_to_json()

        # Verify legacy embedding function is serialized (nested structure)
        vector_index_data = json_data["key_overrides"]["test_key"]["#float_list"]["$vector_index"]
        assert "config" in vector_index_data
        assert "embedding_function" in vector_index_data["config"]
        ef_config = vector_index_data["config"]["embedding_function"]
        assert ef_config["type"] == "legacy"

        # Deserialize back
        deserialized = InternalSchema.deserialize_from_json(json_data)

        # Verify legacy embedding function is handled
        deserialized_vector = deserialized.key_overrides["test_key"]["#float_list"]["$vector_index"]
        assert isinstance(deserialized_vector, InternalVectorIndex)
        assert deserialized_vector.config.embedding_function is None

    def test_space_resolution_from_embedding_function(self) -> None:
        """Test that space is resolved from embedding function when not provided."""
        schema = Schema()

        # Create VectorIndexConfig without space (should be resolved from embedding function)
        class MockEmbeddingFunction:

            def __call__(self, input: Any) -> list[list[float]]:
                return [[1.0, 2.0, 3.0]]

            def name(self) -> str:
                return "MockEmbeddingFunction"

            def get_config(self) -> Dict[str, Any]:
                return {"param": "value"}

            def is_legacy(self) -> bool:
                return False

            def default_space(self) -> str:
                return "l2"

            def supported_spaces(self) -> list[str]:
                return ["l2", "cosine", "ip"]

        vector_config = VectorIndexConfig(
            embedding_function=cast(EmbeddingFunction[Documents], MockEmbeddingFunction()),
            source_key="test"
        )
        schema.create_index(config=vector_config, key="test_key")

        # Serialize to JSON
        internal = InternalSchema(schema)
        json_data = internal.serialize_to_json()

        # Verify space is resolved and serialized (nested structure)
        vector_index_data = json_data["key_overrides"]["test_key"]["#float_list"]["$vector_index"]
        assert "config" in vector_index_data
        assert "space" in vector_index_data["config"]
        # MockEmbeddingFunction should have default space "l2"
        assert vector_index_data["config"]["space"] == "l2"

        # Deserialize back
        deserialized = InternalSchema.deserialize_from_json(json_data)

        # Verify space is preserved
        deserialized_vector = deserialized.key_overrides["test_key"]["#float_list"]["$vector_index"]
        assert isinstance(deserialized_vector, InternalVectorIndex)
        assert deserialized_vector.config.space == "l2"

    def test_space_validation_with_embedding_function(self) -> None:
        """Test that space validation works with embedding function."""
        schema = Schema()

        # Create VectorIndexConfig with a space that's not supported by the embedding function
        class MockEmbeddingFunction:

            def __call__(self, input: Any) -> list[list[float]]:
                return [[1.0, 2.0, 3.0]]

            def name(self) -> str:
                return "MockEmbeddingFunction"

            def get_config(self) -> Dict[str, Any]:
                return {"param": "value"}

            def is_legacy(self) -> bool:
                return False

            def default_space(self) -> str:
                return "l2"

            def supported_spaces(self) -> list[str]:
                return ["l2", "cosine"]  # Only supports l2 and cosine, not ip

        vector_config = VectorIndexConfig(
            space="ip",  # This should trigger a warning since MockEmbeddingFunction doesn't support "ip"
            embedding_function=cast(EmbeddingFunction[Documents], MockEmbeddingFunction()),
            source_key="test"
        )
        schema.create_index(config=vector_config, key="test_key")

        # Serialize to JSON (should trigger warning)
        internal = InternalSchema(schema)
        with pytest.warns(UserWarning, match="space ip is not supported"):
            json_data = internal.serialize_to_json()

        # Verify the space is still serialized (with warning) (nested structure)
        vector_index_data = json_data["key_overrides"]["test_key"]["#float_list"]["$vector_index"]
        assert vector_index_data["config"]["space"] == "ip"
