"""
Integration test for the Collection statistics wrapper methods
"""

import pytest
import json
import time
from chromadb.api.client import Client as ClientCreator
from chromadb.config import System
from chromadb.test.utils.wait_for_version_increase import (
    get_collection_version,
    wait_for_version_increase,
)


def test_statistics_wrapper(basic_http_client: System) -> None:
    """Test the statistics wrapper methods on Collection"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a collection
    collection = client.get_or_create_collection(
        name="test_collection",
        metadata={"description": "Test collection for statistics"},
    )

    # Enable statistics
    attached_fn = collection.enable_statistics()
    assert attached_fn is not None
    assert attached_fn.function_name == "statistics"
    assert attached_fn.output_collection == "test_collection_statistics"

    initial_version = get_collection_version(client, collection.name)

    # Add some documents with metadata
    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["test document 1", "test document 2", "test document 3"],
        metadatas=[
            {"category": "A", "score": 10, "active": True},
            {"category": "B", "score": 10, "active": False},
            {"category": "A", "score": 20, "active": True},
        ],
    )

    # Wait for statistics to be computed
    wait_for_version_increase(client, collection.name, initial_version)

    # Get statistics
    stats = collection.statistics()
    print("\nStatistics output:")
    print(json.dumps(stats, indent=2))

    # Verify the structure
    assert "statistics" in stats
    assert "summary" in stats

    # Verify summary
    assert stats["summary"]["total_count"] == 3

    # Verify category statistics
    assert "category" in stats["statistics"]
    assert "A" in stats["statistics"]["category"]
    assert "B" in stats["statistics"]["category"]
    assert stats["statistics"]["category"]["A"]["count"] == 2
    assert stats["statistics"]["category"]["B"]["count"] == 1

    # Verify score statistics
    assert "score" in stats["statistics"]
    assert "10" in stats["statistics"]["score"]
    assert "20" in stats["statistics"]["score"]
    assert stats["statistics"]["score"]["10"]["count"] == 2
    assert stats["statistics"]["score"]["20"]["count"] == 1

    # Verify active statistics
    assert "active" in stats["statistics"]
    assert "true" in stats["statistics"]["active"]
    assert "false" in stats["statistics"]["active"]
    assert stats["statistics"]["active"]["true"]["count"] == 2
    assert stats["statistics"]["active"]["false"]["count"] == 1

    # Test get_attached_function
    stats_fn = collection.get_attached_function(collection._get_statistics_fn_name())
    assert stats_fn.function_name == "statistics"

    # Disable statistics (keep the collection)
    success = collection.disable_statistics(delete_stats_collection=False)
    assert success is True

    # Verify the statistics collection still exists
    stats_collection = client.get_collection("test_collection_statistics")
    assert stats_collection is not None

def test_backfill_statistics(basic_http_client: System) -> None:
    """Test backfill statistics"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="my_collection")

    initial_version = get_collection_version(client, collection.name)

    # Add some documents with metadata
    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["test document 1", "test document 2", "test document 3"],
        metadatas=[
            {"category": "A", "score": 10, "active": True},
            {"category": "B", "score": 10, "active": False},
            {"category": "A", "score": 20, "active": True},
        ],
    )

    # Let this all be compacted
    wait_for_version_increase(client, collection.name, initial_version)
    initial_version = get_collection_version(client, collection.name)

    # Enable statistics
    attached_fn = collection.enable_statistics()
    assert attached_fn.function_name == "statistics"
    assert attached_fn.output_collection == "my_collection_statistics"

    # Wait for statistics to be computed
    wait_for_version_increase(client, collection.name, initial_version)

    stats = collection.statistics()
    assert stats is not None
    assert "statistics" in stats
    assert "summary" in stats

     # Verify summary
    assert stats["summary"]["total_count"] == 3

    # Verify category statistics
    assert "category" in stats["statistics"]
    assert "A" in stats["statistics"]["category"]
    assert "B" in stats["statistics"]["category"]
    assert stats["statistics"]["category"]["A"]["count"] == 2
    assert stats["statistics"]["category"]["B"]["count"] == 1

    # Verify score statistics
    assert "score" in stats["statistics"]
    assert "10" in stats["statistics"]["score"]
    assert "20" in stats["statistics"]["score"]
    assert stats["statistics"]["score"]["10"]["count"] == 2
    assert stats["statistics"]["score"]["20"]["count"] == 1

    # Verify active statistics
    assert "active" in stats["statistics"]
    assert "true" in stats["statistics"]["active"]
    assert "false" in stats["statistics"]["active"]
    assert stats["statistics"]["active"]["true"]["count"] == 2
    assert stats["statistics"]["active"]["false"]["count"] == 1


    # Disable statistics
    success = collection.disable_statistics(delete_stats_collection=True)
    assert success is True

def test_statistics_wrapper_custom_output_collection(basic_http_client: System) -> None:
    """Test statistics with custom output collection name"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="my_collection")

    # Enable statistics with custom output collection name
    attached_fn = collection.enable_statistics(stats_collection_name="my_custom_stats")
    assert attached_fn.output_collection == "my_custom_stats"

    initial_version = get_collection_version(client, collection.name)

    # Add data
    collection.add(
        ids=["id1"],
        documents=["doc1"],
        metadatas=[{"key": "value"}],
    )

    wait_for_version_increase(client, collection.name, initial_version)

    # Get statistics
    stats = collection.statistics()
    assert "statistics" in stats
    assert "key" in stats["statistics"]

    # Disable and delete the custom collection
    collection.disable_statistics(delete_stats_collection=True)

# commenting out for now as waiting for query cache invalidateion slows down the test suite
def test_statistics_wrapper_incremental_updates(basic_http_client: System) -> None:
    """Test that statistics are updated incrementally"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="incremental_test")
    collection.enable_statistics()

    initial_version = get_collection_version(client, collection.name)

    # Add initial batch
    collection.add(
        ids=["id1", "id2"],
        documents=["doc1", "doc2"],
        metadatas=[{"category": "A"}, {"category": "A"}],
    )

    wait_for_version_increase(client, collection.name, initial_version)
    next_version = get_collection_version(client, collection.name)

    # Check initial statistics
    stats = collection.statistics()
    assert stats["statistics"]["category"]["A"]["count"] == 2
    assert stats["summary"]["total_count"] == 2

    # Add more data
    collection.add(
        ids=["id3", "id4"],
        documents=["doc3", "doc4"],
        metadatas=[{"category": "B"}, {"category": "A"}],
    )

    wait_for_version_increase(client, collection.name, next_version)
    # TODO(tanujnay112): Remove this sleep once query cache invalidation is solidified
    # or figure out a different testing harness where we don't have to wait for query cache invalidation
    time.sleep(70)

    # Check updated statistics
    stats = collection.statistics()
    assert stats["statistics"]["category"]["A"]["count"] == 3
    assert stats["statistics"]["category"]["B"]["count"] == 1
    assert stats["summary"]["total_count"] == 4

    collection.disable_statistics(delete_stats_collection=True)
