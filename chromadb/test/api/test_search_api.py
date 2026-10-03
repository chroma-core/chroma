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
def test_search_with_read_level_index_and_bounded_wal(
    client_factories: ClientFactories,
) -> None:
    """Test search with ReadLevel.INDEX_AND_BOUNDED_WAL returns results."""
    collection, _ = _create_test_collection(client_factories)

    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["apple fruit", "banana fruit", "car vehicle"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.2, 0.3, 0.4, 0.5], [0.9, 0.8, 0.7, 0.6]],
    )

    # Search with INDEX_AND_BOUNDED_WAL reads up to a server-configured number of WAL entries
    search = Search().rank(Knn(query=[0.1, 0.2, 0.3, 0.4], limit=10))
    results = collection.search(search, read_level=ReadLevel.INDEX_AND_BOUNDED_WAL)

    assert results["ids"] is not None
    assert len(results["ids"]) == 1


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


def test_search_knn_dict_form_with_string_query() -> None:
    """A $knn dict carrying a string query matches Knn(query="...").

    Knn accepts a string query and embeds it with the collection's embedding
    function, so the dict form of the same expression has to accept it too.
    """
    from_dict = Search(rank={"$knn": {"query": "quantum mechanics"}})
    from_object = Search(rank=Knn(query="quantum mechanics"))

    assert from_dict.to_dict()["rank"] == from_object.to_dict()["rank"]


def test_search_knn_dict_form_with_string_query_and_options() -> None:
    """Optional $knn fields survive the dict to Knn conversion."""
    rank = {
        "$knn": {
            "query": "quantum mechanics",
            "key": "custom_embedding",
            "limit": 8,
            "return_rank": True,
        }
    }

    assert Search(rank=rank).to_dict()["rank"] == Knn(
        query="quantum mechanics",
        key="custom_embedding",
        limit=8,
        return_rank=True,
    ).to_dict()


def test_search_knn_dict_form_rejects_unsupported_query_types() -> None:
    """Accepting a string query must not widen the check to any type.

    Anything outside the documented set (string, dense vector, sparse vector)
    still has to be rejected instead of being passed through to Knn.
    """
    for unsupported in (1, 1.5, None, True, object()):
        with pytest.raises(TypeError):
            Search(rank={"$knn": {"query": unsupported}})
