"""Validation of collection names and metadata types at the client boundary."""

import pytest

from chromadb.api.types import (
    validate_collection_metadata,
    validate_collection_name,
)


def test_validate_collection_name_accepts_str():
    assert validate_collection_name("my_collection") == "my_collection"


@pytest.mark.parametrize("name", [123, None, ["a"], {"a": 1}])
def test_validate_collection_name_rejects_non_str(name):
    with pytest.raises(ValueError, match="collection name to be a str"):
        validate_collection_name(name)


def test_validate_collection_metadata_accepts_scalars_and_none():
    metadata = {"a": "x", "b": 1, "c": 1.5, "d": True, "e": None}
    assert validate_collection_metadata(metadata) is metadata
    assert validate_collection_metadata(None) is None


@pytest.mark.parametrize("metadata", [[1, 2], "nope", 3])
def test_validate_collection_metadata_rejects_non_dict(metadata):
    with pytest.raises(ValueError, match="metadata to be a dict or None"):
        validate_collection_metadata(metadata)


@pytest.mark.parametrize("metadata", [{"a": [1, 2]}, {"a": {"b": 1}}, {"a": object()}])
def test_validate_collection_metadata_rejects_non_scalar_values(metadata):
    with pytest.raises(ValueError, match="metadata value for key"):
        validate_collection_metadata(metadata)


def test_validate_collection_metadata_rejects_non_str_keys():
    with pytest.raises(TypeError, match="metadata key to be a str"):
        validate_collection_metadata({1: "x"})
