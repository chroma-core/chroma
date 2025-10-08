"""
Integration test for Chroma's Task API

Tests the task creation, execution, and removal functionality
for automatically processing collections.
"""

import pytest
from chromadb.api.client import Client as ClientCreator
from chromadb.config import System
from chromadb.errors import ChromaError, NotFoundError


def test_task_create_and_remove(basic_http_client: System) -> None:
    """Test creating and removing a task with the record_counter operator"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a collection
    collection = client.get_or_create_collection(
        name="my_document",
        metadata={"description": "Sample documents for task processing"},
    )

    # Add initial documents
    collection.add(
        ids=["doc1", "doc2", "doc3"],
        documents=[
            "The quick brown fox jumps over the lazy dog",
            "Machine learning is a subset of artificial intelligence",
            "Python is a popular programming language",
        ],
        metadatas=[{"source": "proverb"}, {"source": "tech"}, {"source": "tech"}],
    )

    # Verify collection has documents
    assert collection.count() == 3

    # Create a task that counts records in the collection
    success, task_id = collection.create_task(
        task_name="count_my_docs",
        operator_name="record_counter",  # Built-in operator that counts records
        output_collection_name="my_documents_counts",
        params=None,
    )

    # Verify task creation succeeded
    assert success is True
    assert task_id is not None
    assert len(task_id) > 0

    # Add more documents
    collection.add(
        ids=["doc4", "doc5"],
        documents=[
            "Chroma is a vector database",
            "Tasks automate data processing",
        ],
    )

    # Verify documents were added
    assert collection.count() == 5

    # Remove the task
    success = collection.remove_task(
        task_name="count_my_docs",
        delete_output=True,
    )

    # Verify task removal succeeded
    assert success is True


def test_task_with_invalid_operator(basic_http_client: System) -> None:
    """Test that creating a task with an invalid operator raises an error"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.get_or_create_collection(name="test_invalid_operator")
    collection.add(ids=["id1"], documents=["test document"])

    # Attempt to create task with non-existent operator should raise ChromaError
    with pytest.raises(ChromaError, match="operator not found"):
        collection.create_task(
            task_name="invalid_task",
            operator_name="nonexistent_operator",
            output_collection_name="output_collection",
            params=None,
        )


def test_task_multiple_collections(basic_http_client: System) -> None:
    """Test creating tasks on multiple collections"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create first collection and task
    collection1 = client.create_collection(name="collection_1")
    collection1.add(ids=["id1", "id2"], documents=["doc1", "doc2"])

    success1, task_id1 = collection1.create_task(
        task_name="task_1",
        operator_name="record_counter",
        output_collection_name="output_1",
        params=None,
    )

    assert success1 is True
    assert task_id1 is not None

    # Create second collection and task
    collection2 = client.create_collection(name="collection_2")
    collection2.add(ids=["id3", "id4"], documents=["doc3", "doc4"])

    success2, task_id2 = collection2.create_task(
        task_name="task_2",
        operator_name="record_counter",
        output_collection_name="output_2",
        params=None,
    )

    assert success2 is True
    assert task_id2 is not None

    # Task IDs should be different
    assert task_id1 != task_id2

    # Clean up
    assert collection1.remove_task(task_name="task_1", delete_output=True) is True
    assert collection2.remove_task(task_name="task_2", delete_output=True) is True


def test_task_multiple_tasks(basic_http_client: System) -> None:
    """Test creating multiple tasks on the same collection"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a single collection
    collection = client.create_collection(name="multi_task_collection")
    collection.add(ids=["id1", "id2", "id3"], documents=["doc1", "doc2", "doc3"])

    # Create first task on the collection
    success1, task_id1 = collection.create_task(
        task_name="task_1",
        operator_name="record_counter",
        output_collection_name="output_1",
        params=None,
    )

    assert success1 is True
    assert task_id1 is not None

    # Create second task on the SAME collection with a different name
    success2, task_id2 = collection.create_task(
        task_name="task_2",
        operator_name="record_counter",
        output_collection_name="output_2",
        params=None,
    )

    assert success2 is True
    assert task_id2 is not None

    # Task IDs should be different even though they're on the same collection
    assert task_id1 != task_id2

    # Create third task on the same collection
    success3, task_id3 = collection.create_task(
        task_name="task_3",
        operator_name="record_counter",
        output_collection_name="output_3",
        params=None,
    )

    assert success3 is True
    assert task_id3 is not None
    assert task_id3 != task_id1
    assert task_id3 != task_id2

    # Attempt to create a task with duplicate name on same collection should fail
    with pytest.raises(ChromaError, match="already exists"):
        collection.create_task(
            task_name="task_1",  # Duplicate name
            operator_name="record_counter",
            output_collection_name="output_duplicate",
            params=None,
        )

    # Clean up - remove each task individually
    assert collection.remove_task(task_name="task_1", delete_output=True) is True
    assert collection.remove_task(task_name="task_2", delete_output=True) is True
    assert collection.remove_task(task_name="task_3", delete_output=True) is True


def test_task_remove_nonexistent(basic_http_client: System) -> None:
    """Test removing a task that doesn't exist raises NotFoundError"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="test_collection")
    collection.add(ids=["id1"], documents=["test"])

    # Try to remove a task that was never created should raise NotFoundError
    with pytest.raises(NotFoundError, match="does not exist"):
        collection.remove_task(
            task_name="nonexistent_task",
            delete_output=False,
        )
