"""Tests for the Fork Count API endpoint."""

from typing import Tuple
from uuid import uuid4


from chromadb.api import ClientAPI
from chromadb.api.models.Collection import Collection
from chromadb.test.conftest import (
    ClientFactories,
    skip_if_not_cluster,
)


def _create_test_collection(
    client_factories: ClientFactories,
) -> Tuple[Collection, ClientAPI]:
    """Create a test collection with some data."""
    client = client_factories.create_client_from_system()
    client.reset()

    collection_name = f"fork_count_api_test_{uuid4().hex}"
    collection = client.get_or_create_collection(name=collection_name)

    return collection, client


@skip_if_not_cluster()
def test_fork_count_no_forks(
    client_factories: ClientFactories,
) -> None:
    """Test fork_count returns 0 for a collection with no forks."""
    collection, _ = _create_test_collection(client_factories)

    # Add some data to the collection
    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["apple fruit", "banana fruit", "car vehicle"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.2, 0.3, 0.4, 0.5], [0.9, 0.8, 0.7, 0.6]],
    )

    fork_count = collection.fork_count()
    assert fork_count == 0


@skip_if_not_cluster()
def test_fork_count_after_single_fork(
    client_factories: ClientFactories,
) -> None:
    """Test fork_count returns 1 after creating one fork."""
    collection, client = _create_test_collection(client_factories)

    # Add some data to the collection
    collection.add(
        ids=["doc1", "doc2"],
        documents=["hello world", "goodbye world"],
        embeddings=[[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]],
    )

    # Fork the collection
    fork_name = f"forked_collection_{uuid4().hex}"
    forked_collection = collection.fork(fork_name)

    # The source collection should now have 1 fork
    fork_count = collection.fork_count()
    assert fork_count == 1

    # The forked collection should also have 1 fork (it shares the lineage)
    forked_fork_count = forked_collection.fork_count()
    assert forked_fork_count == 1


@skip_if_not_cluster()
def test_fork_count_after_multiple_forks(
    client_factories: ClientFactories,
) -> None:
    """Test fork_count returns correct count after creating multiple forks."""
    collection, client = _create_test_collection(client_factories)

    # Add some data to the collection
    collection.add(
        ids=["doc1"],
        documents=["test document"],
        embeddings=[[0.1, 0.2, 0.3, 0.4]],
    )

    # Create 5 forks
    num_forks = 5
    forked_collections = []
    for _ in range(num_forks):
        fork_name = f"forked_collection_{uuid4().hex}"
        forked_collection = collection.fork(fork_name)
        forked_collections.append(forked_collection)

    # The source collection should have 5 forks
    fork_count = collection.fork_count()
    assert fork_count == num_forks

    # Each forked collection should also report 5 forks (same lineage)
    for forked in forked_collections:
        assert forked.fork_count() == num_forks


@skip_if_not_cluster()
def test_fork_count_empty_collection(
    client_factories: ClientFactories,
) -> None:
    """Test fork_count works on an empty collection."""
    collection, _ = _create_test_collection(client_factories)

    # Don't add any data - collection is empty
    fork_count = collection.fork_count()
    assert fork_count == 0


@skip_if_not_cluster()
def test_fork_count_returns_integer(
    client_factories: ClientFactories,
) -> None:
    """Test fork_count returns an integer type."""
    collection, _ = _create_test_collection(client_factories)

    fork_count = collection.fork_count()
    assert isinstance(fork_count, int)
    assert fork_count >= 0
