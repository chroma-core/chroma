"""Tests for the Count API endpoint with read_level support."""

from typing import Tuple
from uuid import uuid4

import pytest

from chromadb.api import ClientAPI
from chromadb.api.models.Collection import Collection
from chromadb.api.types import ReadLevel
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

    collection_name = f"count_api_test_{uuid4().hex}"
    collection = client.get_or_create_collection(name=collection_name)

    return collection, client


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_count_with_read_level_index_and_wal(
    client_factories: ClientFactories,
) -> None:
    """Test count with ReadLevel.INDEX_AND_WAL (default) returns correct count."""
    collection, _ = _create_test_collection(client_factories)

    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["apple fruit", "banana fruit", "car vehicle"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.2, 0.3, 0.4, 0.5], [0.9, 0.8, 0.7, 0.6]],
    )

    count = collection.count(read_level=ReadLevel.INDEX_AND_WAL)
    assert count == 3


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_count_with_read_level_index_only(
    client_factories: ClientFactories,
) -> None:
    """Test count with ReadLevel.INDEX_ONLY returns a valid count."""
    collection, _ = _create_test_collection(client_factories)

    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["apple fruit", "banana fruit", "car vehicle"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.2, 0.3, 0.4, 0.5], [0.9, 0.8, 0.7, 0.6]],
    )

    # Count with INDEX_ONLY skips the WAL.
    # Result may be less than 3 if data hasn't been compacted yet.
    count = collection.count(read_level=ReadLevel.INDEX_ONLY)
    assert isinstance(count, int)
    assert count >= 0


@pytest.mark.skipif(is_spann_disabled_mode, reason=skip_reason_spann_disabled)
def test_count_default_read_level(
    client_factories: ClientFactories,
) -> None:
    """Test count without explicit read_level uses default (INDEX_AND_WAL)."""
    collection, _ = _create_test_collection(client_factories)

    collection.add(
        ids=["doc1", "doc2"],
        documents=["hello world", "goodbye world"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]],
    )

    count = collection.count()
    assert count == 2
