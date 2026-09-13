"""Tests for delete_collection cleaning up array metadata.

Regression tests for https://github.com/chroma-core/chroma/issues/7594
"""
import pytest
from chromadb.api import ClientAPI


def test_delete_collection_cleans_array_metadata(client: ClientAPI) -> None:
    """delete_collection should remove list-valued metadata, not just scalar."""
    client.reset()

    # Create collection and add record with list-valued metadata
    collection = client.get_or_create_collection("test_delete_array")
    collection.add(
        ids=["id-1"],
        embeddings=[[0.1, 0.2, 0.3]],
        metadatas=[{"tags": ["important", "review"]}],
    )

    # Delete the collection
    client.delete_collection("test_delete_array")

    # Recreate with same name
    collection = client.get_or_create_collection("test_delete_array")
    assert collection.count() == 0

    # Add new record with different metadata (no list values)
    collection.add(
        ids=["id-1"],
        embeddings=[[0.4, 0.5, 0.6]],
        metadatas=[{"category": "new"}],
    )

    # Verify only the new metadata is returned - no leaked array metadata
    result = collection.get(ids=["id-1"], include=["metadatas"])
    metadata = result["metadatas"][0]
    assert metadata == {"category": "new"}, f"Expected only new metadata, got: {metadata}"
    assert "tags" not in metadata, f"Array metadata leaked: {metadata}"


def test_delete_collection_cleans_array_metadata_different_name(client: ClientAPI) -> None:
    """Array metadata leak should not cross collection names."""
    client.reset()

    # Create collection and add record with list-valued metadata
    collection = client.get_or_create_collection("original")
    collection.add(
        ids=["id-1"],
        embeddings=[[0.1, 0.2, 0.3]],
        metadatas=[{"tags": ["leaked"]}],
    )

    # Delete the collection
    client.delete_collection("original")

    # Create a DIFFERENT collection
    collection = client.get_or_create_collection("replacement")
    assert collection.count() == 0

    # Add new record
    collection.add(
        ids=["id-1"],
        embeddings=[[0.4, 0.5, 0.6]],
        metadatas=[{"category": "clean"}],
    )

    # Verify no leaked array metadata
    result = collection.get(ids=["id-1"], include=["metadatas"])
    metadata = result["metadatas"][0]
    assert metadata == {"category": "clean"}, f"Expected clean metadata, got: {metadata}"
    assert "tags" not in metadata, f"Array metadata leaked across collections: {metadata}"


def test_delete_collection_scalar_metadata_still_cleaned(client: ClientAPI) -> None:
    """Scalar metadata cleanup should continue to work correctly."""
    client.reset()

    collection = client.get_or_create_collection("test_scalar")
    collection.add(
        ids=["id-1"],
        embeddings=[[0.1, 0.2, 0.3]],
        metadatas=[{"key": "value"}],
    )

    client.delete_collection("test_scalar")

    collection = client.get_or_create_collection("test_scalar")
    assert collection.count() == 0

    collection.add(
        ids=["id-1"],
        embeddings=[[0.4, 0.5, 0.6]],
        metadatas=[{"new_key": "new_value"}],
    )

    result = collection.get(ids=["id-1"], include=["metadatas"])
    metadata = result["metadatas"][0]
    assert metadata == {"new_key": "new_value"}, f"Scalar metadata leaked: {metadata}"
    assert "key" not in metadata, f"Old scalar metadata leaked: {metadata}"
