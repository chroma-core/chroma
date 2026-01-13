"""Tests for the Search API endpoint."""

from typing import Tuple
from uuid import uuid4

import pytest

from chromadb.api import ClientAPI
from chromadb.api.models.Collection import Collection
from chromadb.api.types import Embeddings, ReadLevel
from chromadb.execution.expression import Knn, Search
from chromadb.test.conftest import (
    ClientFactories,
    is_spann_disabled_mode,
    skip_reason_spann_disabled,
)


def _create_test_collection(
    client_factories: ClientFactories,
) -> Tuple[Collection, ClientAPI]:
    """Create a test collection with some data."""
    client = client_factories.create_client_from_system()
    client.reset()

    collection_name = f"search_api_test_{uuid4().hex}"
    collection = client.get_or_create_collection(name=collection_name)

    return collection, client


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_search_with_read_level_index_and_wal(
    client_factories: ClientFactories,
) -> None:
    """Test search with ReadLevel.INDEX_AND_WAL (default) returns results."""
    collection, _ = _create_test_collection(client_factories)

    # Add some data
    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["apple fruit", "banana fruit", "car vehicle"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.2, 0.3, 0.4, 0.5], [0.9, 0.8, 0.7, 0.6]],
    )

    # Search with explicit INDEX_AND_WAL (default behavior)
    search = Search().rank(Knn(query=[0.1, 0.2, 0.3, 0.4], limit=10))
    results = collection.search(search, read_level=ReadLevel.INDEX_AND_WAL)

    assert results["ids"] is not None
    assert len(results["ids"]) == 1
    assert len(results["ids"][0]) > 0


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_search_with_read_level_index_only(
    client_factories: ClientFactories,
) -> None:
    """Test search with ReadLevel.INDEX_ONLY returns results."""
    collection, _ = _create_test_collection(client_factories)

    # Add some data
    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["apple fruit", "banana fruit", "car vehicle"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.2, 0.3, 0.4, 0.5], [0.9, 0.8, 0.7, 0.6]],
    )

    # Search with INDEX_ONLY - this skips the WAL
    # Note: Results may or may not include recent writes depending on compaction state
    search = Search().rank(Knn(query=[0.1, 0.2, 0.3, 0.4], limit=10))
    results = collection.search(search, read_level=ReadLevel.INDEX_ONLY)

    # Just verify the API works and returns a valid response structure
    assert results["ids"] is not None
    assert len(results["ids"]) == 1
    # Results may be empty if data hasn't been compacted yet, which is expected behavior


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_search_default_read_level(
    client_factories: ClientFactories,
) -> None:
    """Test search without explicit read_level uses default (INDEX_AND_WAL)."""
    collection, _ = _create_test_collection(client_factories)

    # Add some data
    collection.add(
        ids=["doc1", "doc2"],
        documents=["hello world", "goodbye world"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]],
    )

    # Search without specifying read_level (should use default)
    search = Search().rank(Knn(query=[0.1, 0.2, 0.3, 0.4], limit=10))
    results = collection.search(search)

    # Should return results since default is INDEX_AND_WAL (full consistency)
    assert results["ids"] is not None
    assert len(results["ids"]) == 1
    assert len(results["ids"][0]) > 0

