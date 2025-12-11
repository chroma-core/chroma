"""
Integration test for Chroma's Task API

Tests the task creation, execution, and removal functionality
for automatically processing collections.
"""

import pytest
from chromadb.api.client import Client as ClientCreator
from chromadb.api.functions import (
    RECORD_COUNTER_FUNCTION,
    STATISTICS_FUNCTION,
    Function,
)
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
    attached_fn, created = collection.attach_function(
        name="count_my_docs",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="my_documents_counts",
        params=None,
    )

    # Verify task creation succeeded
    assert attached_fn is not None
    assert created is True
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
    success = collection.detach_function(
        attached_fn.name,
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
            function=Function._NONEXISTENT_TEST_ONLY,
            name="invalid_task",
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
    attached_fn, created = collection.attach_function(
        function=RECORD_COUNTER_FUNCTION,
        name="my_counter",
        output_collection="output_collection",
        params=None,
    )

    # Verify the attached function has function_name (not function_id UUID)
    assert created is True
    assert attached_fn.function_name == "record_counter"
    assert attached_fn.name == "my_counter"

    # Get the attached function and verify function_name field is also present
    retrieved_fn = collection.get_attached_function("my_counter")
    assert retrieved_fn == attached_fn

    # Clean up
    collection.detach_function(attached_fn.name, delete_output_collection=True)


def test_function_multiple_collections(basic_http_client: System) -> None:
    """Test attaching functions on multiple collections"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create first collection and task
    collection1 = client.create_collection(name="collection_1")
    collection1.add(ids=["id1", "id2"], documents=["doc1", "doc2"])

    attached_fn1, created1 = collection1.attach_function(
        function=RECORD_COUNTER_FUNCTION,
        name="task_1",
        output_collection="output_1",
        params=None,
    )

    assert attached_fn1 is not None
    assert created1 is True

    # Create second collection and task
    collection2 = client.create_collection(name="collection_2")
    collection2.add(ids=["id3", "id4"], documents=["doc3", "doc4"])

    attached_fn2, created2 = collection2.attach_function(
        function=RECORD_COUNTER_FUNCTION,
        name="task_2",
        output_collection="output_2",
        params=None,
    )

    assert attached_fn2 is not None
    assert created2 is True

    # Task IDs should be different
    assert attached_fn1.id != attached_fn2.id

    # Clean up
    assert (
        collection1.detach_function(attached_fn1.name, delete_output_collection=True)
        is True
    )
    assert (
        collection2.detach_function(attached_fn2.name, delete_output_collection=True)
        is True
    )


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
    attached_fn1, created = collection.attach_function(
        function=RECORD_COUNTER_FUNCTION,
        name="task_1",
        output_collection="output_1",
        params=None,
    )

    assert attached_fn1 is not None
    assert created is True

    # Attempt to create a second task with a different name should fail
    # (only one attached function allowed per collection)
    with pytest.raises(
        ChromaError,
        match="collection already has an attached function: name=task_1, function=record_counter, output_collection=output_1",
    ):
        collection.attach_function(
            function=RECORD_COUNTER_FUNCTION,
            name="task_2",
            output_collection="output_2",
            params=None,
        )

    # Attempt to create a task with the same name but different function_id should also fail
    with pytest.raises(
        ChromaError,
        match=r"collection already has an attached function: name=task_1, function=record_counter, output_collection=output_1",
    ):
        collection.attach_function(
            function=STATISTICS_FUNCTION,
            name="task_1",
            output_collection="output_different",  # Different output collection
            params=None,
        )

    # Detach the first function
    assert (
        collection.detach_function(attached_fn1.name, delete_output_collection=True)
        is True
    )

    # Now we should be able to attach a new function
    attached_fn2, created2 = collection.attach_function(
        function=RECORD_COUNTER_FUNCTION,
        name="task_2",
        output_collection="output_2",
        params=None,
    )

    assert attached_fn2 is not None
    assert created2 is True
    assert attached_fn2.id != attached_fn1.id

    # Clean up
    assert (
        collection.detach_function(attached_fn2.name, delete_output_collection=True)
        is True
    )


def test_attach_function_with_invalid_params(basic_http_client: System) -> None:
    """Test that attach_function with non-empty params raises an error"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="test_invalid_params")
    collection.add(ids=["id1"], documents=["test document"])

    # Attempt to create task with non-empty params should fail
    # (no functions currently accept parameters)
    with pytest.raises(
        ChromaError,
        match="params must be empty - no functions currently accept parameters",
    ):
        collection.attach_function(
            name="invalid_params_task",
            function=RECORD_COUNTER_FUNCTION,
            output_collection="output_collection",
            params={"some_key": "some_value"},
        )


def test_attach_function_output_collection_already_exists(
    basic_http_client: System,
) -> None:
    """Test that attach_function fails when output collection name already exists"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a collection that will be used as input
    input_collection = client.create_collection(name="input_collection")
    input_collection.add(ids=["id1"], documents=["test document"])

    # Create another collection with the name we want to use for output
    client.create_collection(name="existing_output_collection")

    # Attempt to create task with output collection name that already exists
    with pytest.raises(
        ChromaError,
        match=r"Output collection \[existing_output_collection\] already exists",
    ):
        input_collection.attach_function(
            name="my_task",
            function=RECORD_COUNTER_FUNCTION,
            output_collection="existing_output_collection",
            params=None,
        )


def test_function_remove_nonexistent(basic_http_client: System) -> None:
    """Test removing a task that doesn't exist raises NotFoundError"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="test_collection")
    collection.add(ids=["id1"], documents=["test"])
    attached_fn, _ = collection.attach_function(
        function=RECORD_COUNTER_FUNCTION,
        name="test_function",
        output_collection="output_collection",
        params=None,
    )

    collection.detach_function(attached_fn.name, delete_output_collection=True)

    # Trying to detach this function again should raise NotFoundError
    with pytest.raises(NotFoundError, match="does not exist"):
        collection.detach_function(attached_fn.name, delete_output_collection=True)


def test_attach_to_output_collection_fails(basic_http_client: System) -> None:
    """Test that attaching a function to an output collection fails"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create input collection
    input_collection = client.create_collection(name="input_collection")
    input_collection.add(ids=["id1"], documents=["test"])

    _, _ = input_collection.attach_function(
        name="test_function",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="output_collection",
        params=None,
    )
    output_collection = client.get_collection(name="output_collection")

    with pytest.raises(
        ChromaError, match="cannot attach function to an output collection"
    ):
        _ = output_collection.attach_function(
            name="test_function_2",
            function=RECORD_COUNTER_FUNCTION,
            output_collection="output_collection_2",
            params=None,
        )


def test_delete_output_collection_detaches_function(basic_http_client: System) -> None:
    """Test that deleting an output collection also detaches the attached function"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create input collection and attach a function
    input_collection = client.create_collection(name="input_collection")
    input_collection.add(ids=["id1"], documents=["test"])

    attached_fn, created = input_collection.attach_function(
        name="my_function",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="output_collection",
        params=None,
    )
    assert attached_fn is not None
    assert created is True

    # Delete the output collection directly
    client.delete_collection("output_collection")

    # The attached function should now be gone - trying to get it should raise NotFoundError
    with pytest.raises(NotFoundError):
        input_collection.get_attached_function("my_function")


def test_delete_orphaned_output_collection(basic_http_client: System) -> None:
    """Test that deleting an output collection from a recently detached function works"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create input collection and attach a function
    input_collection = client.create_collection(name="input_collection")
    input_collection.add(ids=["id1"], documents=["test"])

    attached_fn, created = input_collection.attach_function(
        name="my_function",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="output_collection",
        params=None,
    )
    assert attached_fn is not None
    assert created is True

    input_collection.detach_function(attached_fn.name, delete_output_collection=False)

    # Delete the output collection directly
    client.delete_collection("output_collection")

    # The attached function should still exist but be marked as detached
    with pytest.raises(NotFoundError):
        input_collection.get_attached_function("my_function")

    with pytest.raises(NotFoundError):
        # Try to use the function - it should fail since it's detached
        client.get_collection("output_collection")

def test_partial_attach_function_repair(
    basic_http_client: System,
) -> None:
    """Test creating and removing a function with the record_counter operator"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a collection
    collection = client.get_or_create_collection(
        name="my_document",
    )

    # Create a task that counts records in the collection
    attached_fn, created = collection.attach_function(
        name="count_my_docs",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="my_documents_counts",
        params=None,
    )
    assert created is True

    # Verify task creation succeeded
    assert attached_fn is not None

    collection2 = client.get_or_create_collection(
        name="my_document2",
    )

    # Create a task that counts records in the collection
    # This should fail
    with pytest.raises(
        ChromaError, match=r"Output collection \[my_documents_counts\] already exists"
    ):
        attached_fn, _ = collection2.attach_function(
            name="count_my_docs",
            function=RECORD_COUNTER_FUNCTION,
            output_collection="my_documents_counts",
            params=None,
        )

    # Detach the function
    assert (
        collection.detach_function(attached_fn.name, delete_output_collection=True)
        is True
    )

    # Create a task that counts records in the collection
    attached_fn, created = collection2.attach_function(
        name="count_my_docs",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="my_documents_counts",
        params=None,
    )
    assert attached_fn is not None
    assert created is True


def test_output_collection_created_with_schema(basic_http_client: System) -> None:
    """Test that output collections are created with the source_attached_function_id in the schema"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create input collection and attach a function
    input_collection = client.create_collection(name="input_collection")
    input_collection.add(ids=["id1"], documents=["test"])

    attached_fn, created = input_collection.attach_function(
        name="my_function",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="output_collection",
        params=None,
    )
    assert attached_fn is not None
    assert created is True

    # Get the output collection - it should exist
    output_collection = client.get_collection(name="output_collection")
    assert output_collection is not None

    # The source_attached_function_id is stored in the schema (not metadata)
    # We can't directly access the schema from the client, but we verify the collection exists
    # and the attached function orchestrator will use this field internally
    assert "source_attached_function_id" in output_collection._model.pretty_schema()

    # Clean up
    input_collection.detach_function(attached_fn.name, delete_output_collection=True)


def test_count_function_attach_and_detach_attach_attach(
    basic_http_client: System,
) -> None:
    """Test creating and removing a function with the record_counter operator"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a collection
    collection = client.get_or_create_collection(
        name="my_document",
        metadata={"description": "Sample documents for task processing"},
    )

    # Create a task that counts records in the collection
    attached_fn, created = collection.attach_function(
        name="count_my_docs",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="my_documents_counts",
        params=None,
    )

    # Verify task creation succeeded
    assert created is True
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
    success = collection.detach_function(
        attached_fn.name, delete_output_collection=True
    )

    # Verify task removal succeeded
    assert success is True

    # Attach a function that counts records in the collection
    attached_fn, created = collection.attach_function(
        name="count_my_docs",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="my_documents_counts",
        params=None,
    )
    assert attached_fn is not None
    assert created is True

    # Attach a function that counts records in the collection
    attached_fn, created = collection.attach_function(
        name="count_my_docs",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="my_documents_counts",
        params=None,
    )
    assert created is False
    assert attached_fn is not None

def test_attach_function_idempotency(basic_http_client: System) -> None:
    """Test that attach_function is idempotent - calling it twice with same params returns created=False"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="idempotency_test")
    collection.add(ids=["id1"], documents=["test document"])

    # First attach - should be newly created
    attached_fn1, created1 = collection.attach_function(
        name="my_function",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="output_collection",
        params=None,
    )
    assert attached_fn1 is not None
    assert created1 is True

    # Second attach with identical params - should be idempotent (created=False)
    attached_fn2, created2 = collection.attach_function(
        name="my_function",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="output_collection",
        params=None,
    )
    assert attached_fn2 is not None
    assert created2 is False

    # Both should return the same function ID
    assert attached_fn1.id == attached_fn2.id

    # Clean up
    collection.detach_function(attached_fn1.name, delete_output_collection=True)
