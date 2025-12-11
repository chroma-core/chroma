"""
Integration test for the Collection statistics wrapper methods
"""

import json
import time
from typing import Any

import pytest

from chromadb.api.client import Client as ClientCreator
from chromadb.base_types import SparseVector
from chromadb.config import System
from chromadb.test.utils.wait_for_version_increase import (
    get_collection_version,
    wait_for_version_increase,
)
from chromadb.utils.statistics import (
    attach_statistics_function,
    detach_statistics_function,
    get_statistics,
    get_statistics_fn_name,
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
    attached_fn, created = attach_statistics_function(collection, "test_collection_statistics")
    assert attached_fn is not None
    assert created is True
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
    time.sleep(60)

    # Get statistics
    stats = get_statistics(collection, "test_collection_statistics")
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
    stats_fn = collection.get_attached_function(get_statistics_fn_name(collection))
    assert stats_fn.function_name == "statistics"

    # Disable statistics (keep the collection)
    success = detach_statistics_function(collection, delete_stats_collection=False)
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
    attached_fn, created = attach_statistics_function(collection, "my_collection_statistics")
    assert created is True
    assert attached_fn.function_name == "statistics"
    assert attached_fn.output_collection == "my_collection_statistics"

    # Wait for statistics to be computed
    wait_for_version_increase(client, collection.name, initial_version)

    stats = get_statistics(collection, "my_collection_statistics")
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
    success = detach_statistics_function(collection, delete_stats_collection=True)
    assert success is True


def test_statistics_wrapper_custom_output_collection(basic_http_client: System) -> None:
    """Test statistics with custom output collection name"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="my_collection")

    # Enable statistics with custom output collection name
    attached_fn, created = attach_statistics_function(
        collection, stats_collection_name="my_custom_stats"
    )
    assert created is True
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
    stats = get_statistics(collection, "my_custom_stats")
    assert "statistics" in stats
    assert "key" in stats["statistics"]

    # Disable and delete the custom collection
    detach_statistics_function(collection, delete_stats_collection=True)


def test_statistics_wrapper_key_filter(basic_http_client: System) -> None:
    """Test get_statistics with key filter parameter"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="key_filter_test")

    # Enable statistics
    _, created = attach_statistics_function(collection, "key_filter_test_statistics")
    assert created is True

    initial_version = get_collection_version(client, collection.name)

    # Add documents with multiple metadata keys
    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=["test document 1", "test document 2", "test document 3"],
        metadatas=[
            {"category": "A", "score": 10, "active": True},
            {"category": "B", "score": 10, "active": False},
            {"category": "A", "score": 20, "active": True},
        ],
    )

    wait_for_version_increase(client, collection.name, initial_version)
    time.sleep(60)

    # Get all statistics (no key filter)
    all_stats = get_statistics(collection, "key_filter_test_statistics")
    assert "category" in all_stats["statistics"]
    assert "score" in all_stats["statistics"]
    assert "active" in all_stats["statistics"]

    # Get statistics filtered by "category" key only
    category_stats = get_statistics(
        collection, "key_filter_test_statistics", keys=["category"]
    )
    assert "category" in category_stats["statistics"]
    assert "score" not in category_stats["statistics"]
    assert "active" not in category_stats["statistics"]
    assert category_stats["statistics"]["category"]["A"]["count"] == 2
    assert category_stats["statistics"]["category"]["B"]["count"] == 1
    # Summary should still be present when filtering by key
    assert "summary" in category_stats
    assert category_stats["summary"]["total_count"] == 3

    # Get statistics filtered by "score" key only
    score_stats = get_statistics(
        collection, "key_filter_test_statistics", keys=["score"]
    )
    assert "score" in score_stats["statistics"]
    assert "category" not in score_stats["statistics"]
    assert "active" not in score_stats["statistics"]
    assert score_stats["statistics"]["score"]["10"]["count"] == 2
    assert score_stats["statistics"]["score"]["20"]["count"] == 1
    # Summary should still be present when filtering by key
    assert "summary" in score_stats
    assert score_stats["summary"]["total_count"] == 3

    # Cleanup
    detach_statistics_function(collection, delete_stats_collection=True)


def test_statistics_wrapper_key_filter_too_many_keys(basic_http_client: System) -> None:
    """Test that get_statistics raises ValueError when more than 30 keys are provided"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="too_many_keys_test")

    # Enable statistics
    attach_statistics_function(collection, "too_many_keys_test_statistics")

    # Generate more than 30 keys
    too_many_keys = [f"key_{i}" for i in range(31)]

    # Should raise ValueError when more than 30 keys are provided
    with pytest.raises(ValueError) as exc_info:
        get_statistics(collection, "too_many_keys_test_statistics", keys=too_many_keys)

    assert "Too many keys provided: 31" in str(exc_info.value)
    assert "Maximum allowed is 30" in str(exc_info.value)

    # Cleanup
    detach_statistics_function(collection, delete_stats_collection=True)


# commenting out for now as waiting for query cache invalidateion slows down the test suite
def test_statistics_wrapper_incremental_updates(basic_http_client: System) -> None:
    """Test that statistics are updated incrementally"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="incremental_test")
    _, created = attach_statistics_function(collection, "incremental_test_statistics")
    assert created is True

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
    stats = get_statistics(collection, "incremental_test_statistics")
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
    stats = get_statistics(collection, "incremental_test_statistics")
    assert stats["statistics"]["category"]["A"]["count"] == 3
    assert stats["statistics"]["category"]["B"]["count"] == 1
    assert stats["summary"]["total_count"] == 4

    detach_statistics_function(collection, delete_stats_collection=True)


def test_sparse_vector_statistics(basic_http_client: System) -> None:
    """Test statistics with sparse vector that includes labels"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="sparse_vector_test1")

    # Create sparse vectors with labels
    sparse_vec1 = SparseVector(
        indices=[100, 200, 300],
        values=[1.0, 2.0, 3.0],
        labels=["apple", "banana", "cherry"],
    )
    sparse_vec2 = SparseVector(
        indices=[100, 400], values=[1.5, 2.5], labels=["apple", "date"]
    )
    sparse_vec3 = SparseVector(
        indices=[200, 300], values=[2.0, 3.0], labels=["banana", "cherry"]
    )

    # Add data with sparse vectors
    collection.add(
        ids=["id1", "id2", "id3"],
        documents=["doc1", "doc2", "doc3"],
        metadatas=[
            {"category": "A", "vec": sparse_vec1},
            {"category": "B", "vec": sparse_vec2},
            {"category": "A", "vec": sparse_vec3},
        ],
    )
    _, created = attach_statistics_function(collection, "sparse_vector_test1_statistics")
    assert created is True

    initial_version = get_collection_version(client, collection.name)

    wait_for_version_increase(client, collection.name, initial_version)

    # Get statistics
    stats = get_statistics(collection, "sparse_vector_test1_statistics")
    print("\nSparse vector statistics output:")
    print(json.dumps(stats, indent=2))

    assert "statistics" in stats
    assert "summary" in stats
    assert stats["summary"]["total_count"] == 3

    # Verify category statistics
    assert "category" in stats["statistics"]
    assert "A" in stats["statistics"]["category"]
    assert "B" in stats["statistics"]["category"]
    assert stats["statistics"]["category"]["A"]["count"] == 2
    assert stats["statistics"]["category"]["B"]["count"] == 1

    # Verify sparse vector statistics use labels instead of hash IDs
    assert "vec" in stats["statistics"]
    assert "apple" in stats["statistics"]["vec"], "Should use label 'apple' not hash ID"
    assert (
        "banana" in stats["statistics"]["vec"]
    ), "Should use label 'banana' not hash ID"
    assert (
        "cherry" in stats["statistics"]["vec"]
    ), "Should use label 'cherry' not hash ID"
    assert "date" in stats["statistics"]["vec"], "Should use label 'date' not hash ID"

    # Verify counts
    assert stats["statistics"]["vec"]["apple"]["count"] == 2  # in id1 and id2
    assert stats["statistics"]["vec"]["banana"]["count"] == 2  # in id1 and id3
    assert stats["statistics"]["vec"]["cherry"]["count"] == 2  # in id1 and id3
    assert stats["statistics"]["vec"]["date"]["count"] == 1  # in id2 only


def test_statistics_high_cardinality(basic_http_client: System) -> None:
    """Test statistics with high cardinality metadata"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="high_cardinality_test")

    # Generate 500 documents with 10 metadata fields each
    num_docs = 500
    num_fields = 10
    ids = [f"id{i}" for i in range(num_docs)]
    documents = [f"doc{i}" for i in range(num_docs)]

    metadatas: list[dict[str, Any]] = []
    for i in range(num_docs):
        meta: dict[str, Any] = {}
        for j in range(num_fields):
            meta[f"field_{j}"] = f"value_{j}_{i}"
        metadatas.append(meta)

    # Add in batches to avoid hitting request size limits
    batch_size = 100
    initial_version = get_collection_version(client, collection.name)

    for i in range(0, num_docs, batch_size):
        collection.add(
            ids=ids[i : i + batch_size],
            documents=documents[i : i + batch_size],
            metadatas=metadatas[i : i + batch_size],  # type: ignore[arg-type]
        )

    # Let all data be compacted
    wait_for_version_increase(client, collection.name, initial_version)
    initial_version = get_collection_version(client, collection.name)

    # Enable statistics
    _, created = attach_statistics_function(collection, "high_cardinality_test_statistics")
    assert created is True

    # Wait for statistics to be computed
    wait_for_version_increase(client, collection.name, initial_version)

    # Get statistics
    stats = get_statistics(collection, "high_cardinality_test_statistics")

    assert "statistics" in stats

    # Verify we have stats for all fields
    for j in range(num_fields):
        field_key = f"field_{j}"
        assert field_key in stats["statistics"]

        field_stats = stats["statistics"][field_key]
        assert len(field_stats) == num_docs

        # Verify each value has count 1
        for i in range(num_docs):
            value = f"value_{j}_{i}"
            assert value in field_stats
            assert field_stats[value]["count"] == 1

    # Verify total count
    assert stats["summary"]["total_count"] == num_docs

    detach_statistics_function(collection, delete_stats_collection=True)
