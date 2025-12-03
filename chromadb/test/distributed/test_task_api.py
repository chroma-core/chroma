"""
Integration test for Chroma's Task API

Tests the task creation, execution, and removal functionality
for automatically processing collections.
"""

import pytest
from chromadb.api.client import Client as ClientCreator
from chromadb.config import System
from chromadb.errors import ChromaError, NotFoundError
from chromadb.test.utils.wait_for_version_increase import (
    get_collection_version,
    wait_for_version_increase,
)
from time import sleep


def test_count_function_attach_and_detach(basic_http_client: System) -> None:
    """Test creating and removing a function with the record_counter operator"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a collection
    collection = client.get_or_create_collection(
        name="my_document",
        metadata={"description": "Sample documents for task processing"},
    )

    # Create a task that counts records in the collection
    attached_fn = collection.attach_function(
        name="count_my_docs",
        function_id="record_counter",  # Built-in operator that counts records
        output_collection="my_documents_counts",
        params=None,
    )

    # Verify task creation succeeded
    assert attached_fn is not None
    initial_version = get_collection_version(client, collection.name)

    # Add documents
    collection.add(
        ids=["doc_{}".format(i) for i in range(0, 300)],
        documents=["test document"] * 300,
    )

    # Verify documents were added
    assert collection.count() == 300

    wait_for_version_increase(client, collection.name, initial_version)
    # Give some time to invalidate the frontend query cache
    sleep(60)

    result = client.get_collection("my_documents_counts").get("function_output")
    assert result["metadatas"] is not None
    assert result["metadatas"][0]["total_count"] == 300

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


def test_attach_function_returns_function_name(basic_http_client: System) -> None:
    """Test that attach_function and get_attached_function return function_name field instead of UUID"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="test_function_name")
    collection.add(ids=["id1"], documents=["doc1"])

    # Attach a function and verify function_name field in response
    attached_fn = collection.attach_function(
        name="my_counter",
        function_id="record_counter",
        output_collection="output_collection",
        params=None,
    )

    # Verify the attached function has function_name (not function_id UUID)
    assert attached_fn.function_name == "record_counter"
    assert attached_fn.name == "my_counter"

    # Get the attached function and verify function_name field is also present
    retrieved_fn = collection.get_attached_function("my_counter")
    assert retrieved_fn == attached_fn

    # Clean up
    attached_fn.detach(delete_output_collection=True)


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


def test_functions_one_attached_function_per_collection(
    basic_http_client: System,
) -> None:
    """Test that only one attached function is allowed per collection"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a single collection
    collection = client.create_collection(name="single_task_collection")
    collection.add(ids=["id1", "id2", "id3"], documents=["doc1", "doc2", "doc3"])

    # Create first task on the collection
    attached_fn1 = collection.attach_function(
        name="task_1",
        function_id="record_counter",
        output_collection="output_1",
        params=None,
    )

    assert attached_fn1 is not None

    # Attempt to create a second task with a different name should fail
    # (only one attached function allowed per collection)
    with pytest.raises(ChromaError, match="already has an attached function"):
        collection.attach_function(
            name="task_2",
            function_id="record_counter",
            output_collection="output_2",
            params=None,
        )

    # Attempt to create a task with the same name but different params should also fail
    with pytest.raises(ChromaError, match="already exists"):
        collection.attach_function(
            name="task_1",
            function_id="record_counter",
            output_collection="output_different",  # Different output collection
            params=None,
        )

    # Detach the first function
    assert attached_fn1.detach(delete_output_collection=True) is True

    # Now we should be able to attach a new function
    attached_fn2 = collection.attach_function(
        name="task_2",
        function_id="record_counter",
        output_collection="output_2",
        params=None,
    )

    assert attached_fn2 is not None
    assert attached_fn2.id != attached_fn1.id

    # Clean up
    assert attached_fn2.detach(delete_output_collection=True) is True


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
