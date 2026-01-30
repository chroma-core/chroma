import math
from typing import Any, Dict, Optional, Set, Tuple, cast

from hypothesis import given

from chromadb.api import ClientAPI
from chromadb.api.collection_configuration import CreateCollectionConfiguration
from chromadb.api.types import (
    CollectionMetadata,
    EMBEDDING_KEY,
    Schema,
)
from chromadb.test.property import strategies
from chromadb.test.property.invariants import check_metadata
from chromadb.test.conftest import (
    reset,
    is_spann_disabled_mode,
)


HNSW_METADATA_TO_CONFIG: Dict[str, str] = {
    "hnsw:space": "space",
    "hnsw:construction_ef": "ef_construction",
    "hnsw:search_ef": "ef_search",
    "hnsw:M": "max_neighbors",
    "hnsw:sync_threshold": "sync_threshold",
    "hnsw:resize_factor": "resize_factor",
}

HNSW_FIELDS = [
    "space",
    "ef_construction",
    "ef_search",
    "max_neighbors",
    "sync_threshold",
    "resize_factor",
]

HNSW_DEFAULTS: Dict[str, Any] = {
    "space": "l2",
    "ef_construction": 100,
    "ef_search": 100,
    "max_neighbors": 16,
    "sync_threshold": 1000,
    "resize_factor": 1.2,
}

SPANN_FIELDS = [
    "space",
    "search_nprobe",
    "write_nprobe",
    "ef_construction",
    "ef_search",
    "max_neighbors",
    "reassign_neighbor_count",
    "split_threshold",
    "merge_threshold",
]

SPANN_DEFAULTS: Dict[str, Any] = {
    "space": "l2",
    "search_nprobe": 64,
    "write_nprobe": 32,
    "ef_construction": 200,
    "ef_search": 200,
    "max_neighbors": 64,
    "reassign_neighbor_count": 64,
    "split_threshold": 50,
    "merge_threshold": 25,
}


def _extract_vector_configs_from_schema(
    schema: Schema,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    defaults_float = schema.defaults.float_list
    assert defaults_float is not None
    defaults_vi = defaults_float.vector_index
    assert defaults_vi is not None

    embedding_float = schema.keys[EMBEDDING_KEY].float_list
    assert embedding_float is not None
    embedding_vi = embedding_float.vector_index
    assert embedding_vi is not None

    return (
        strategies.vector_index_to_dict(defaults_vi.config),
        strategies.vector_index_to_dict(embedding_vi.config),
    )


def _compute_expected_config_spann(
    metadata: Optional[CollectionMetadata],
    configuration: Optional[CreateCollectionConfiguration],
    schema_vector_index_config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    # start off creating default spann config, we slowly modify it to much whatever prop test provides
    expected = SPANN_DEFAULTS.copy()
    space_set = False
    # theres some edge cases where space is set in hnsw config and in metadata
    # in this case, we check if the space set by config is not the default, and if so, we don't try to get use the one from metadata
    # essentially if either metadata or hnsw config provides a non-default space, we use that one, with config hnsw taking priority over metadata
    should_try_metadata = True

    if configuration:
        spann_cfg = configuration.get("spann")
        if spann_cfg:
            spann_cfg_dict = cast(Dict[str, Any], spann_cfg)
            # update expected with whatever prop test provides
            expected.update(strategies.non_none_items(spann_cfg_dict))
            # if space is set in spann, this now takes priority over all else
            if spann_cfg_dict.get("space") is not None:
                expected["space"] = spann_cfg_dict["space"]
                space_set = True
                should_try_metadata = False
        hnsw_cfg = configuration.get("hnsw")
        if hnsw_cfg:
            hnsw_cfg_dict = cast(Dict[str, Any], hnsw_cfg)
            hnsw_non_none = strategies.non_none_items(hnsw_cfg_dict)
            for key, value in hnsw_non_none.items():
                if value is not None and value != HNSW_DEFAULTS[key]:
                    # if any hnsw config is not the default, we do not use metadata at all, this is used
                    # heres a sample case where this is needed: hnsw doesnt set space (so l2 by default), but sets ef_construction, metadata sets space to ip
                    # in this case, they were aware of hnsw config, and chose not to set space in it. therefore the config takes priority over metadata
                    should_try_metadata = False
            # when SPANN is active and HNSW config is provided, use space from hnsw config
            if hnsw_cfg_dict.get("space") is not None and not space_set:
                # if the space set by config is not the default, don't try to get use the one from metadata
                if hnsw_cfg_dict.get("space") != HNSW_DEFAULTS["space"]:
                    should_try_metadata = False
                expected["space"] = hnsw_cfg_dict["space"]
                space_set = True

    if schema_vector_index_config:
        if schema_vector_index_config.get("space") is not None:
            expected["space"] = schema_vector_index_config["space"]
            space_set = True
        if schema_vector_index_config.get("spann"):
            spann_schema = strategies.non_none_items(
                schema_vector_index_config["spann"]
            )
            expected.update(spann_schema)

    if (
        metadata
        and metadata.get("hnsw:space") is not None
        and metadata.get("hnsw:space") != SPANN_DEFAULTS["space"]
        and should_try_metadata
    ):
        expected["space"] = metadata["hnsw:space"]
        space_set = True

    if (
        schema_vector_index_config
        and schema_vector_index_config.get("embedding_function_default_space")
        is not None
        and schema_vector_index_config.get("embedding_function_default_space")
        != SPANN_DEFAULTS["space"]
        and not space_set
    ):
        expected["space"] = schema_vector_index_config[
            "embedding_function_default_space"
        ]
        space_set = True

    if (
        not space_set
        and configuration
        and configuration.get("embedding_function") is not None
    ):
        ef = configuration["embedding_function"]
        if hasattr(ef, "default_space"):
            expected["space"] = cast(Any, ef).default_space()

    return expected


def _compute_expected_config_hnsw(
    metadata: Optional[CollectionMetadata],
    configuration: Optional[CreateCollectionConfiguration],
    schema_vector_index_config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    expected = HNSW_DEFAULTS.copy()
    space_set = False
    configured_hnsw_keys: Set[str] = set()
    should_try_metadata = True

    if configuration:
        hnsw_cfg_raw = configuration.get("hnsw")
        if hnsw_cfg_raw is not None:
            hnsw_dict: Dict[str, Any] = cast(Dict[str, Any], hnsw_cfg_raw)
            hnsw_non_none = strategies.non_none_items(hnsw_dict)
            expected.update(hnsw_non_none)
            for key, value in hnsw_non_none.items():
                # if any hnsw config is not the default, we do not use metadata at all
                if value is not None and value != HNSW_DEFAULTS[key]:
                    should_try_metadata = False
            configured_hnsw_keys.update(hnsw_non_none.keys())
            if hnsw_non_none.get("space") is not None and not space_set:
                if hnsw_non_none.get("space") != HNSW_DEFAULTS["space"]:
                    should_try_metadata = False
                space_set = True
        spann_cfg_raw = configuration.get("spann")
        if spann_cfg_raw is not None:
            spann_dict: Dict[str, Any] = cast(Dict[str, Any], spann_cfg_raw)
            if spann_dict.get("space") is not None and not space_set:
                expected["space"] = spann_dict["space"]
                space_set = True
                should_try_metadata = False

    if should_try_metadata and metadata:
        for key, cfg_key in HNSW_METADATA_TO_CONFIG.items():
            if metadata.get(key) is None:
                continue
            if cfg_key == "space":
                expected[cfg_key] = metadata[key]
                space_set = True
                configured_hnsw_keys.add(cfg_key)
                continue
            if cfg_key not in configured_hnsw_keys:
                expected[cfg_key] = metadata[key]
                configured_hnsw_keys.add(cfg_key)

    if schema_vector_index_config:
        if schema_vector_index_config.get("space") is not None:
            expected["space"] = schema_vector_index_config["space"]
            space_set = True
        if schema_vector_index_config.get("hnsw"):
            expected.update(
                strategies.non_none_items(schema_vector_index_config["hnsw"])
            )
        elif schema_vector_index_config.get("spann"):
            # Schema provided SPANN configuration while HNSW is active; ignore.
            pass

    if (
        schema_vector_index_config
        and schema_vector_index_config.get("embedding_function_default_space")
        is not None
        and not space_set
    ):
        expected["space"] = schema_vector_index_config[
            "embedding_function_default_space"
        ]
        space_set = True

    if (
        not space_set
        and configuration
        and configuration.get("embedding_function") is not None
    ):
        ef = configuration["embedding_function"]
        if hasattr(ef, "default_space"):
            expected["space"] = cast(Any, ef).default_space()

    return expected


def _compute_expected_config(
    spann_active: bool,
    metadata: Optional[CollectionMetadata],
    configuration: Optional[CreateCollectionConfiguration],
    schema_vector_index_config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    some assumptions/assertions:
    1. we are not testing failure paths. any config built is/should be valid. invalid cases can be tested separately or in e2e tests
    ex: if configuration is set, schema is not set. if schema is set, configuration is not set. both hnsw and spann cannot be set at the same time in config or schema
    """
    if spann_active:
        return _compute_expected_config_spann(
            metadata, configuration, schema_vector_index_config
        )
    else:
        return _compute_expected_config_hnsw(
            metadata, configuration, schema_vector_index_config
        )


def _assert_config_values(
    actual: Dict[str, Any],
    expected: Dict[str, Any],
    spann_active: bool,
) -> None:
    fields = SPANN_FIELDS if spann_active else HNSW_FIELDS
    for field in fields:
        actual_value = actual.get(field)
        expected_value = expected[field]
        # Use approximate equality for floating-point values
        if isinstance(actual_value, float) and isinstance(expected_value, float):
            assert math.isclose(
                actual_value, expected_value, rel_tol=1e-9, abs_tol=1e-9
            ), f"{field} mismatch: expected {expected_value}, got {actual_value}"
        else:
            assert (
                actual_value == expected_value
            ), f"{field} mismatch: expected {expected_value}, got {actual_value}"


def _assert_schema_values(
    vector_info: Dict[str, Any],
    expected: Dict[str, Any],
    spann_active: bool,
) -> None:
    assert vector_info["space"] == expected["space"]
    if spann_active:
        spann_cfg = cast(Optional[Dict[str, Any]], vector_info["spann"])
        assert spann_cfg is not None
        for field in SPANN_FIELDS:
            if field == "space":
                continue
            actual_value = spann_cfg.get(field)
            expected_value = expected[field]
            # Use approximate equality for floating-point values
            if isinstance(actual_value, float) and isinstance(expected_value, float):
                assert math.isclose(
                    actual_value, expected_value, rel_tol=1e-9, abs_tol=1e-9
                ), f"{field} mismatch: expected {expected_value}, got {actual_value}"
            else:
                assert (
                    actual_value == expected_value
                ), f"{field} mismatch: expected {expected_value}, got {actual_value}"
    else:
        hnsw_cfg = cast(Optional[Dict[str, Any]], vector_info["hnsw"])
        assert hnsw_cfg is not None
        for field in HNSW_FIELDS:
            if field == "space":
                continue
            actual_value = hnsw_cfg.get(field)
            expected_value = expected[field]
            # Use approximate equality for floating-point values
            if isinstance(actual_value, float) and isinstance(expected_value, float):
                assert math.isclose(
                    actual_value, expected_value, rel_tol=1e-9, abs_tol=1e-9
                ), f"{field} mismatch: expected {expected_value}, got {actual_value}"
            else:
                assert (
                    actual_value == expected_value
                ), f"{field} mismatch: expected {expected_value}, got {actual_value}"


def _get_default_schema_indexes() -> Dict[str, Dict[str, Any]]:
    """
    Get expected index states for default schema (when schema=None).
    Based on Schema._initialize_defaults() and _initialize_keys().
    """
    return {
        "defaults": {
            "string_inverted": {"enabled": True},
            "int_inverted": {"enabled": True},
            "float_inverted": {"enabled": True},
            "bool_inverted": {"enabled": True},
            "sparse_vector": {"enabled": False},
            "fts_index": {"enabled": False},
            "vector_index": {"enabled": False},
        },
        "#document": {
            "string_inverted": {"enabled": False},
            "fts_index": {"enabled": True},
        },
        "#embedding": {
            "vector_index": {"enabled": True},
        },
    }


def _extract_expected_schema_indexes(
    schema: Schema,
) -> Dict[str, Dict[str, Any]]:
    """
    Extract expected index states from input schema.
    Returns a dict mapping key -> index_type -> enabled/config info.
    """
    expected: Dict[str, Dict[str, Any]] = {}

    # Check defaults
    if schema.defaults.string and schema.defaults.string.string_inverted_index:
        if "defaults" not in expected:
            expected["defaults"] = {}
        expected["defaults"]["string_inverted"] = {
            "enabled": schema.defaults.string.string_inverted_index.enabled,
        }

    if schema.defaults.int_value and schema.defaults.int_value.int_inverted_index:
        if "defaults" not in expected:
            expected["defaults"] = {}
        expected["defaults"]["int_inverted"] = {
            "enabled": schema.defaults.int_value.int_inverted_index.enabled,
        }

    if schema.defaults.float_value and schema.defaults.float_value.float_inverted_index:
        if "defaults" not in expected:
            expected["defaults"] = {}
        expected["defaults"]["float_inverted"] = {
            "enabled": schema.defaults.float_value.float_inverted_index.enabled,
        }

    if schema.defaults.boolean and schema.defaults.boolean.bool_inverted_index:
        if "defaults" not in expected:
            expected["defaults"] = {}
        expected["defaults"]["bool_inverted"] = {
            "enabled": schema.defaults.boolean.bool_inverted_index.enabled,
        }

    if (
        schema.defaults.sparse_vector
        and schema.defaults.sparse_vector.sparse_vector_index
    ):
        if "defaults" not in expected:
            expected["defaults"] = {}
        expected["defaults"]["sparse_vector"] = {
            "enabled": schema.defaults.sparse_vector.sparse_vector_index.enabled,
            "config": schema.defaults.sparse_vector.sparse_vector_index.config,
        }

    # Check per-key indexes
    for key, value_types in schema.keys.items():
        if key in (EMBEDDING_KEY, "#document"):
            # Skip special keys - they're handled by vector index test
            continue

        key_expected: Dict[str, Any] = {}

        if value_types.string and value_types.string.string_inverted_index:
            key_expected["string_inverted"] = {
                "enabled": value_types.string.string_inverted_index.enabled,
            }

        if value_types.int_value and value_types.int_value.int_inverted_index:
            key_expected["int_inverted"] = {
                "enabled": value_types.int_value.int_inverted_index.enabled,
            }

        if value_types.float_value and value_types.float_value.float_inverted_index:
            key_expected["float_inverted"] = {
                "enabled": value_types.float_value.float_inverted_index.enabled,
            }

        if value_types.boolean and value_types.boolean.bool_inverted_index:
            key_expected["bool_inverted"] = {
                "enabled": value_types.boolean.bool_inverted_index.enabled,
            }

        if value_types.sparse_vector and value_types.sparse_vector.sparse_vector_index:
            key_expected["sparse_vector"] = {
                "enabled": value_types.sparse_vector.sparse_vector_index.enabled,
                "config": value_types.sparse_vector.sparse_vector_index.config,
            }

        if key_expected:
            expected[key] = key_expected

    return expected


def _assert_schema_indexes(
    actual_schema: Schema,
    expected_indexes: Dict[str, Dict[str, Any]],
) -> None:
    """Assert that the actual schema matches expected index states."""

    # Check defaults
    if "defaults" in expected_indexes:
        defaults_expected = expected_indexes["defaults"]
        defaults_actual = actual_schema.defaults

        if "string_inverted" in defaults_expected:
            expected_enabled = defaults_expected["string_inverted"]["enabled"]
            actual_string = defaults_actual.string
            if actual_string and actual_string.string_inverted_index:
                assert (
                    actual_string.string_inverted_index.enabled == expected_enabled
                ), f"defaults string_inverted enabled mismatch: expected {expected_enabled}, got {actual_string.string_inverted_index.enabled}"
            else:
                # If not explicitly set, defaults should be enabled
                assert expected_enabled, "defaults string_inverted should be enabled"

        if "int_inverted" in defaults_expected:
            expected_enabled = defaults_expected["int_inverted"]["enabled"]
            actual_int = defaults_actual.int_value
            if actual_int and actual_int.int_inverted_index:
                assert (
                    actual_int.int_inverted_index.enabled == expected_enabled
                ), f"defaults int_inverted enabled mismatch: expected {expected_enabled}, got {actual_int.int_inverted_index.enabled}"
            else:
                assert expected_enabled, "defaults int_inverted should be enabled"

        if "float_inverted" in defaults_expected:
            expected_enabled = defaults_expected["float_inverted"]["enabled"]
            actual_float = defaults_actual.float_value
            if actual_float and actual_float.float_inverted_index:
                assert (
                    actual_float.float_inverted_index.enabled == expected_enabled
                ), f"defaults float_inverted enabled mismatch: expected {expected_enabled}, got {actual_float.float_inverted_index.enabled}"
            else:
                assert expected_enabled, "defaults float_inverted should be enabled"

        if "bool_inverted" in defaults_expected:
            expected_enabled = defaults_expected["bool_inverted"]["enabled"]
            actual_bool = defaults_actual.boolean
            if actual_bool and actual_bool.bool_inverted_index:
                assert (
                    actual_bool.bool_inverted_index.enabled == expected_enabled
                ), f"defaults bool_inverted enabled mismatch: expected {expected_enabled}, got {actual_bool.bool_inverted_index.enabled}"
            else:
                assert expected_enabled, "defaults bool_inverted should be enabled"

        if "sparse_vector" in defaults_expected:
            expected_enabled = defaults_expected["sparse_vector"]["enabled"]
            actual_sparse = defaults_actual.sparse_vector
            assert actual_sparse is not None, "defaults sparse_vector should exist"
            assert (
                actual_sparse.sparse_vector_index is not None
            ), "defaults sparse_vector_index should exist"
            assert (
                actual_sparse.sparse_vector_index.enabled == expected_enabled
            ), f"defaults sparse_vector enabled mismatch: expected {expected_enabled}, got {actual_sparse.sparse_vector_index.enabled}"
            # Validate config fields if config is provided in expected
            if "config" in defaults_expected["sparse_vector"]:
                expected_config = defaults_expected["sparse_vector"]["config"]
                actual_config = actual_sparse.sparse_vector_index.config
                if expected_config.bm25 is not None:
                    assert (
                        actual_config.bm25 == expected_config.bm25
                    ), f"defaults sparse_vector bm25 mismatch: expected {expected_config.bm25}, got {actual_config.bm25}"
                if expected_config.source_key is not None:
                    assert (
                        actual_config.source_key == expected_config.source_key
                    ), f"defaults sparse_vector source_key mismatch: expected {expected_config.source_key}, got {actual_config.source_key}"

        if "fts_index" in defaults_expected:
            expected_enabled = defaults_expected["fts_index"]["enabled"]
            actual_string = defaults_actual.string
            assert actual_string is not None, "defaults string should exist"
            assert (
                actual_string.fts_index is not None
            ), "defaults fts_index should exist"
            assert (
                actual_string.fts_index.enabled == expected_enabled
            ), f"defaults fts_index enabled mismatch: expected {expected_enabled}, got {actual_string.fts_index.enabled}"

        if "vector_index" in defaults_expected:
            expected_enabled = defaults_expected["vector_index"]["enabled"]
            actual_float_list = defaults_actual.float_list
            assert actual_float_list is not None, "defaults float_list should exist"
            assert (
                actual_float_list.vector_index is not None
            ), "defaults vector_index should exist"
            assert (
                actual_float_list.vector_index.enabled == expected_enabled
            ), f"defaults vector_index enabled mismatch: expected {expected_enabled}, got {actual_float_list.vector_index.enabled}"

    # Check per-key indexes
    for key, key_expected in expected_indexes.items():
        if key == "defaults":
            continue

        assert key in actual_schema.keys, f"Expected key '{key}' not found in schema"
        actual_value_types = actual_schema.keys[key]

        if "string_inverted" in key_expected:
            expected_enabled = key_expected["string_inverted"]["enabled"]
            actual_string = actual_value_types.string
            assert actual_string is not None, f"Key '{key}' string should exist"
            assert (
                actual_string.string_inverted_index is not None
            ), f"Key '{key}' string_inverted_index should exist"
            assert (
                actual_string.string_inverted_index.enabled == expected_enabled
            ), f"Key '{key}' string_inverted enabled mismatch: expected {expected_enabled}, got {actual_string.string_inverted_index.enabled}"

        if "int_inverted" in key_expected:
            expected_enabled = key_expected["int_inverted"]["enabled"]
            actual_int = actual_value_types.int_value
            assert actual_int is not None, f"Key '{key}' int_value should exist"
            assert (
                actual_int.int_inverted_index is not None
            ), f"Key '{key}' int_inverted_index should exist"
            assert (
                actual_int.int_inverted_index.enabled == expected_enabled
            ), f"Key '{key}' int_inverted enabled mismatch: expected {expected_enabled}, got {actual_int.int_inverted_index.enabled}"

        if "float_inverted" in key_expected:
            expected_enabled = key_expected["float_inverted"]["enabled"]
            actual_float = actual_value_types.float_value
            assert actual_float is not None, f"Key '{key}' float_value should exist"
            assert (
                actual_float.float_inverted_index is not None
            ), f"Key '{key}' float_inverted_index should exist"
            assert (
                actual_float.float_inverted_index.enabled == expected_enabled
            ), f"Key '{key}' float_inverted enabled mismatch: expected {expected_enabled}, got {actual_float.float_inverted_index.enabled}"

        if "bool_inverted" in key_expected:
            expected_enabled = key_expected["bool_inverted"]["enabled"]
            actual_bool = actual_value_types.boolean
            assert actual_bool is not None, f"Key '{key}' boolean should exist"
            assert (
                actual_bool.bool_inverted_index is not None
            ), f"Key '{key}' bool_inverted_index should exist"
            assert (
                actual_bool.bool_inverted_index.enabled == expected_enabled
            ), f"Key '{key}' bool_inverted enabled mismatch: expected {expected_enabled}, got {actual_bool.bool_inverted_index.enabled}"

        if "sparse_vector" in key_expected:
            expected_enabled = key_expected["sparse_vector"]["enabled"]
            expected_config = key_expected["sparse_vector"]["config"]
            actual_sparse = actual_value_types.sparse_vector
            assert actual_sparse is not None, f"Key '{key}' sparse_vector should exist"
            assert (
                actual_sparse.sparse_vector_index is not None
            ), f"Key '{key}' sparse_vector_index should exist"
            assert (
                actual_sparse.sparse_vector_index.enabled == expected_enabled
            ), f"Key '{key}' sparse_vector enabled mismatch: expected {expected_enabled}, got {actual_sparse.sparse_vector_index.enabled}"
            # Validate config fields match
            actual_config = actual_sparse.sparse_vector_index.config
            if expected_config.bm25 is not None:
                assert (
                    actual_config.bm25 == expected_config.bm25
                ), f"Key '{key}' sparse_vector bm25 mismatch: expected {expected_config.bm25}, got {actual_config.bm25}"
            if expected_config.source_key is not None:
                assert (
                    actual_config.source_key == expected_config.source_key
                ), f"Key '{key}' sparse_vector source_key mismatch: expected {expected_config.source_key}, got {actual_config.source_key}"

        if "fts_index" in key_expected:
            expected_enabled = key_expected["fts_index"]["enabled"]
            actual_string = actual_value_types.string
            assert actual_string is not None, f"Key '{key}' string should exist"
            assert (
                actual_string.fts_index is not None
            ), f"Key '{key}' fts_index should exist"
            assert (
                actual_string.fts_index.enabled == expected_enabled
            ), f"Key '{key}' fts_index enabled mismatch: expected {expected_enabled}, got {actual_string.fts_index.enabled}"

        if "vector_index" in key_expected:
            expected_enabled = key_expected["vector_index"]["enabled"]
            actual_float_list = actual_value_types.float_list
            assert actual_float_list is not None, f"Key '{key}' float_list should exist"
            assert (
                actual_float_list.vector_index is not None
            ), f"Key '{key}' vector_index should exist"
            assert (
                actual_float_list.vector_index.enabled == expected_enabled
            ), f"Key '{key}' vector_index enabled mismatch: expected {expected_enabled}, got {actual_float_list.vector_index.enabled}"


@given(
    name=strategies.collection_name(),
    optional_fields=strategies.metadata_configuration_schema_strategy(),
)
def test_vector_index_configuration_create_collection(
    client: ClientAPI,
    name: str,
    optional_fields: strategies.CollectionInputCombination,
) -> None:
    metadata = optional_fields.metadata
    configuration = optional_fields.configuration
    schema = optional_fields.schema

    reset(client)
    collection = client.create_collection(
        name=name,
        metadata=metadata,
        configuration=configuration,
        schema=schema,
    )

    if metadata is None:
        assert collection.metadata in (None, {})
    else:
        check_metadata(metadata, collection.metadata)

    coll_config = collection.configuration
    spann_active = not is_spann_disabled_mode
    active_key = "spann" if spann_active else "hnsw"
    inactive_key = "hnsw" if spann_active else "spann"

    active_block = coll_config.get(active_key)
    inactive_block = coll_config.get(inactive_key)

    assert active_block is not None, f"{active_key} configuration missing"
    assert inactive_block in (
        None,
        {},
    ), f"{inactive_key} configuration should be absent"

    expected = _compute_expected_config(
        spann_active=spann_active,
        metadata=metadata,
        configuration=configuration,
        schema_vector_index_config=optional_fields.schema_vector_info,
    )

    _assert_config_values(cast(Dict[str, Any], active_block), expected, spann_active)

    # Check embedding function name if one was provided
    if configuration and configuration.get("embedding_function") is not None:
        ef = configuration["embedding_function"]
        if ef is not None:
            coll_ef = coll_config.get("embedding_function")
            if coll_ef is not None:
                ef_config = coll_ef.get_config()
                if ef_config and ef_config.get("type") == "known":
                    assert hasattr(
                        ef, "name"
                    ), "embedding function should have name method"
                    assert ef_config.get("name") == ef.name(), (
                        f"embedding function name mismatch: "
                        f"expected {ef.name()}, got {ef_config.get('name')}"
                    )

    schema_result = collection.schema
    assert schema_result is not None
    defaults_cfg, embedding_cfg = _extract_vector_configs_from_schema(schema_result)

    if spann_active:
        assert defaults_cfg["hnsw"] is None
        assert embedding_cfg["hnsw"] is None
        assert defaults_cfg["spann"] is not None
        assert embedding_cfg["spann"] is not None
    else:
        assert defaults_cfg["spann"] is None
        assert embedding_cfg["spann"] is None
        assert defaults_cfg["hnsw"] is not None
        assert embedding_cfg["hnsw"] is not None

    _assert_schema_values(defaults_cfg, expected, spann_active)
    _assert_schema_values(embedding_cfg, expected, spann_active)

    # Check embedding function name in schema if one was provided
    if configuration and configuration.get("embedding_function") is not None:
        ef = configuration["embedding_function"]
        if ef is not None:
            # Check defaults vector index
            defaults_ef = schema_result.defaults.float_list.vector_index.config.embedding_function  # type: ignore[union-attr]
            if defaults_ef is not None and hasattr(defaults_ef, "name"):
                assert defaults_ef.name() == ef.name(), (
                    f"defaults embedding function name mismatch: "
                    f"expected {ef.name()}, got {defaults_ef.name()}"
                )
            # Check embedding key vector index
            embedding_ef = schema_result.keys[EMBEDDING_KEY].float_list.vector_index.config.embedding_function  # type: ignore[union-attr]
            if embedding_ef is not None and hasattr(embedding_ef, "name"):
                assert embedding_ef.name() == ef.name(), (
                    f"embedding key embedding function name mismatch: "
                    f"expected {ef.name()}, got {embedding_ef.name()}"
                )


@given(
    name=strategies.collection_name(),
    schema=strategies.schema_strategy(),
)
def test_schema_create_and_get_collection(
    client: ClientAPI,
    name: str,
    schema: Optional[Schema],
) -> None:
    """
    Test that schema-only components (inverted indexes, sparse vector indexes)
    are correctly created and persisted when creating a collection.
    """
    reset(client)

    if schema is None:
        expected_indexes = _get_default_schema_indexes()
    else:
        expected_indexes = _extract_expected_schema_indexes(schema)

    collection = client.create_collection(name=name, schema=schema)

    # Get the returned schema
    schema_result = collection.schema
    assert schema_result is not None, "Schema should not be None"

    _assert_schema_indexes(schema_result, expected_indexes)

    collection = client.get_collection(name)
    schema_result = collection.schema
    assert schema_result is not None, "Schema should not be None"
    _assert_schema_indexes(schema_result, expected_indexes)
