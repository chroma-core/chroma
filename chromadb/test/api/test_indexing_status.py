from time import sleep
import threading
from chromadb.api import ClientAPI
from chromadb.api.types import IndexingStatus
from chromadb.test.conftest import skip_if_not_cluster
from chromadb.test.utils.wait_for_version_increase import (
    get_collection_version,
    wait_for_version_increase,
)


@skip_if_not_cluster()
def test_indexing_status_empty_collection(client: ClientAPI) -> None:
    """Test indexing status on empty collection"""
    client.reset()

    collection = client.create_collection(name="test_collection")
    status = collection.get_indexing_status()

    assert isinstance(status, IndexingStatus)
    assert status.num_indexed_ops == 0
    assert status.num_unindexed_ops == 0
    assert status.total_ops == 0
    assert status.op_indexing_progress == 1.0


@skip_if_not_cluster()
def test_indexing_status_after_add(client: ClientAPI) -> None:
    """Test indexing status after adding embeddings"""
    client.reset()

    collection = client.create_collection(name="test_collection")

    ids = [f"id_{i}" for i in range(300)]
    embeddings = [[float(i), float(i + 1), float(i + 2)] for i in range(300)]
    initial_version = get_collection_version(client, collection.name)
    collection.add(ids=ids, embeddings=embeddings)  # type: ignore

    status = collection.get_indexing_status()
    assert status.total_ops == 300

    if initial_version == get_collection_version(client, collection.name):
        assert isinstance(status, IndexingStatus)
        assert status.num_unindexed_ops == 300
        assert status.num_indexed_ops == 0
        assert status.op_indexing_progress == 0.0
        wait_for_version_increase(client, collection.name, initial_version)
        # Give some time to invalidate the frontend query cache
        sleep(60)

        # Check status after indexing completes
        final_status = collection.get_indexing_status()
        assert isinstance(final_status, IndexingStatus)
        assert final_status.num_indexed_ops == 300
        assert final_status.num_unindexed_ops == 0
        assert final_status.op_indexing_progress == 1.0


@skip_if_not_cluster()
def test_indexing_status_after_upsert(client: ClientAPI) -> None:
    """Test indexing status after upsert operations"""
    client.reset()

    collection = client.create_collection(name="test_collection")
    initial_version = get_collection_version(client, collection.name)

    collection.upsert(ids=["id1", "id2"], embeddings=[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # type: ignore

    status = collection.get_indexing_status()
    assert status.total_ops == 2

    if initial_version == get_collection_version(client, collection.name):
        assert isinstance(status, IndexingStatus)
        assert status.num_unindexed_ops == 2
        assert status.num_indexed_ops == 0
        assert status.op_indexing_progress == 0.0
        wait_for_version_increase(client, collection.name, initial_version)
        sleep(60)

    collection.upsert(ids=["id1", "id3"], embeddings=[[1.1, 2.1, 3.1], [7.0, 8.0, 9.0]])  # type: ignore

    status = collection.get_indexing_status()
    assert status.total_ops == 4


@skip_if_not_cluster()
def test_indexing_status_after_delete(client: ClientAPI) -> None:
    """Test indexing status after delete operations"""
    client.reset()

    collection = client.create_collection(name="test_collection")
    initial_version = get_collection_version(client, collection.name)

    collection.add(
        ids=["id1", "id2", "id3"],
        embeddings=[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],  # type: ignore
    )

    if initial_version == get_collection_version(client, collection.name):
        status = collection.get_indexing_status()
        assert isinstance(status, IndexingStatus)
        assert status.num_unindexed_ops == 3
        assert status.num_indexed_ops == 0
        assert status.op_indexing_progress == 0.0
        wait_for_version_increase(client, collection.name, initial_version)
        sleep(60)

    initial_status = collection.get_indexing_status()
    assert initial_status.total_ops == 3

    collection.delete(ids=["id1", "id2"])

    # Delete adds operations to the log, so total_ops increases
    status_after_delete = collection.get_indexing_status()
    assert status_after_delete.total_ops == 5


@skip_if_not_cluster()
def test_indexing_status_field_types(client: ClientAPI) -> None:
    """Test that indexing status returns correct field types"""
    client.reset()

    collection = client.create_collection(name="field_types_collection")
    initial_version = get_collection_version(client, collection.name)

    collection.add(ids=["type_test_id"], embeddings=[[1.0, 2.0, 3.0]])  # type: ignore

    status = collection.get_indexing_status()

    if initial_version == get_collection_version(client, collection.name):
        assert isinstance(status, IndexingStatus)
        assert status.num_unindexed_ops == 1
        assert status.num_indexed_ops == 0
        assert status.op_indexing_progress == 0.0
        wait_for_version_increase(client, collection.name, initial_version)
        sleep(60)

    final_status = collection.get_indexing_status()

    assert isinstance(final_status.num_indexed_ops, int)
    assert isinstance(final_status.num_unindexed_ops, int)
    assert isinstance(final_status.total_ops, int)
    assert isinstance(final_status.op_indexing_progress, float)

    assert final_status.num_indexed_ops >= 0
    assert final_status.num_unindexed_ops >= 0
    assert final_status.total_ops >= 0
    assert 0.0 <= final_status.op_indexing_progress <= 1.0


@skip_if_not_cluster()
def test_indexing_status_batch_progression(client: ClientAPI) -> None:
    """Test indexing status with 2000 records based on index version progression"""
    client.reset()

    collection = client.create_collection(name="batch_test_collection")
    get_collection_version(client, collection.name)

    # Insert 2000 records in two batches of 1000 (max batch size)
    ids_1 = [f"batch_id_{i}" for i in range(1000)]
    embeddings_1 = [[float(i), float(i + 1), float(i + 2)] for i in range(1000)]
    collection.add(ids=ids_1, embeddings=embeddings_1)  # type: ignore

    ids_2 = [f"batch_id_{i}" for i in range(1000, 2000)]
    embeddings_2 = [[float(i), float(i + 1), float(i + 2)] for i in range(1000, 2000)]
    collection.add(ids=ids_2, embeddings=embeddings_2)  # type: ignore

    current_version = get_collection_version(client, collection.name)

    allowed_statuses = [
        IndexingStatus(
            num_indexed_ops=0,
            num_unindexed_ops=2000,
            total_ops=2000,
            op_indexing_progress=0.0,
        ),
        IndexingStatus(
            num_indexed_ops=1000,
            num_unindexed_ops=1000,
            total_ops=2000,
            op_indexing_progress=0.5,
        ),
        IndexingStatus(
            num_indexed_ops=2000,
            num_unindexed_ops=0,
            total_ops=2000,
            op_indexing_progress=1.0,
        ),
    ]

    ops_indexed = 0
    while ops_indexed < 2000:
        status = collection.get_indexing_status()
        assert status in allowed_statuses
        print("witnessed status: ", status)
        ops_indexed = status.num_indexed_ops
        wait_for_version_increase(client, collection.name, current_version)
        sleep(60)


@skip_if_not_cluster()
def test_indexing_status_not_found(client: ClientAPI) -> None:
    """Test indexing status on non-existent collection"""
    client.reset()

    collection = client.create_collection(name="temp_collection")
    client.delete_collection("temp_collection")

    try:
        collection.get_indexing_status()
        assert False, "Expected exception for non-existent collection"
    except Exception as e:
        assert (
            "not found" in str(e).lower()
            or "does not exist" in str(e).lower()
            or "soft deleted" in str(e).lower()
            or "collection not found" in str(e).lower()
        )


@skip_if_not_cluster()
def test_indexing_status_concurrent_progress_variation(client: ClientAPI) -> None:
    """Test that progress values vary during concurrent operations"""
    client.reset()

    collection = client.create_collection(name="concurrent_test_collection")

    progress_values = []
    stop_monitoring = threading.Event()

    def progress_monitor() -> None:
        """Thread that continuously monitors indexing progress"""
        while not stop_monitoring.is_set():
            try:
                status = collection.get_indexing_status()
                progress_values.append(status.op_indexing_progress)
                sleep(0.1)  # Poll every 100ms
            except Exception:
                break

    def record_adder() -> None:
        """Thread that adds records one at a time"""
        for i in range(50):  # Add 50 records one by one
            collection.add(
                ids=[f"concurrent_id_{i}"],
                embeddings=[[float(i), float(i + 1), float(i + 2)]],  # type: ignore
            )
            sleep(0.05)  # Small delay between additions

    # Start both threads
    monitor_thread = threading.Thread(target=progress_monitor)
    adder_thread = threading.Thread(target=record_adder)

    monitor_thread.start()
    adder_thread.start()

    # Wait for record addition to complete
    adder_thread.join()

    # Give a moment for final progress updates
    sleep(1.0)

    # Stop monitoring and wait for thread to finish
    stop_monitoring.set()
    monitor_thread.join()

    # Assert that we collected progress values
    assert len(progress_values) > 0, "No progress values were collected"

    # Assert that not all progress values are 0
    non_zero_progress = [p for p in progress_values if p > 0.0]
    assert len(non_zero_progress) > 0, "All progress values remained at 0"

    # Assert that there's variation in progress values
    unique_progress = set(progress_values)
    assert (
        len(unique_progress) > 1
    ), f"Expected variation in progress values, but only got: {unique_progress}"

    print(f"Collected {len(progress_values)} progress values")
    print(f"Unique progress values: {sorted(unique_progress)}")
    print(f"Progress range: {min(progress_values):.3f} - {max(progress_values):.3f}")
