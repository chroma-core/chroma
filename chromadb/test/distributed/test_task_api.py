"""
Integration test for Chroma's Task API

Tests the task creation, execution, and removal functionality
for automatically processing collections.
"""

import pytest
from chromadb.api.client import Client as ClientCreator
from chromadb.config import System
from chromadb.errors import ChromaError, NotFoundError


def test_function_attach_and_detach(basic_http_client: System) -> None:
    """Test creating and removing a function with the record_counter operator"""
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
    attached_fn = collection.attach_function(
        name="count_my_docs",
        function_id="record_counter",  # Built-in operator that counts records
        output_collection="my_documents_counts",
        params=None,
    )

    # Verify task creation succeeded
    assert attached_fn is not None

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
    success = attached_fn.detach(
        delete_output_collection=True,
    )

    # Verify task removal succeeded
    assert success is True


def test_task_with_invalid_function(basic_http_client: System) -> None:
    """Test that creating a task with an invalid function raises an error"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.get_or_create_collection(name="test_invalid_function")
    collection.add(ids=["id1"], documents=["test document"])

    # Attempt to create task with non-existent function should raise ChromaError
    with pytest.raises(ChromaError, match="function not found"):
        collection.attach_function(
            name="invalid_task",
            function_id="nonexistent_function",
            output_collection="output_collection",
            params=None,
        )


def test_function_multiple_collections(basic_http_client: System) -> None:
    """Test attaching functions on multiple collections"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create first collection and task
    collection1 = client.create_collection(name="collection_1")
    collection1.add(ids=["id1", "id2"], documents=["doc1", "doc2"])

    attached_fn1 = collection1.attach_function(
        name="task_1",
        function_id="record_counter",
        output_collection="output_1",
        params=None,
    )

    assert attached_fn1 is not None

    # Create second collection and task
    collection2 = client.create_collection(name="collection_2")
    collection2.add(ids=["id3", "id4"], documents=["doc3", "doc4"])

    attached_fn2 = collection2.attach_function(
        name="task_2",
        function_id="record_counter",
        output_collection="output_2",
        params=None,
    )

    assert attached_fn2 is not None

    # Task IDs should be different
    assert attached_fn1.id != attached_fn2.id

    # Clean up
    assert attached_fn1.detach(delete_output_collection=True) is True
    assert attached_fn2.detach(delete_output_collection=True) is True


def test_functions_multiple_attached_functions(basic_http_client: System) -> None:
    """Test attaching multiple functions on the same collection"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a single collection
    collection = client.create_collection(name="multi_task_collection")
    collection.add(ids=["id1", "id2", "id3"], documents=["doc1", "doc2", "doc3"])

    # Create first task on the collection
    attached_fn1 = collection.attach_function(
        name="task_1",
        function_id="record_counter",
        output_collection="output_1",
        params=None,
    )

    assert attached_fn1 is not None

    # Create second task on the SAME collection with a different name
    attached_fn2 = collection.attach_function(
        name="task_2",
        function_id="record_counter",
        output_collection="output_2",
        params=None,
    )

    assert attached_fn2 is not None

    # Task IDs should be different even though they're on the same collection
    assert attached_fn1.id != attached_fn2.id

    # Create third task on the same collection
    attached_fn3 = collection.attach_function(
        name="task_3",
        function_id="record_counter",
        output_collection="output_3",
        params=None,
    )

    assert attached_fn3 is not None
    assert attached_fn3.id != attached_fn1.id
    assert attached_fn3.id != attached_fn2.id

    # Attempt to create a task with duplicate name on same collection should fail
    with pytest.raises(ChromaError, match="already exists"):
        collection.attach_function(
            name="task_1",  # Duplicate name
            function_id="record_counter",
            output_collection="output_duplicate",
            params=None,
        )

    # Clean up - remove each task individually
    assert attached_fn1.detach(delete_output_collection=True) is True
    assert attached_fn2.detach(delete_output_collection=True) is True
    assert attached_fn3.detach(delete_output_collection=True) is True


def test_function_remove_nonexistent(basic_http_client: System) -> None:
    """Test removing a task that doesn't exist raises NotFoundError"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="test_collection")
    collection.add(ids=["id1"], documents=["test"])
    attached_fn = collection.attach_function(
        name="test_function",
        function_id="record_counter",
        output_collection="output_collection",
        params=None,
    )

    attached_fn.detach(delete_output_collection=True)

    # Trying to detach this function again should raise NotFoundError
    with pytest.raises(NotFoundError, match="does not exist"):
        attached_fn.detach(delete_output_collection=True)
