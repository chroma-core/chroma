"""
Integration test for Chroma's Task API

Tests the task creation, execution, and removal functionality
for automatically processing collections.
"""

import functools
import pytest
import time
import urllib.parse
import uuid
from typing import Any, Optional, cast
from chromadb.api.client import Client as ClientCreator
from chromadb.api.functions import (
    COUNT_TO_FILE_ASYNC_FUNCTION,
    DUMMY_ASYNC_FUNCTION,
    RECORD_COUNTER_FUNCTION,
    STATISTICS_FUNCTION,
    Function,
)
from chromadb.api.models.Collection import Collection
from chromadb.config import System
from chromadb.errors import ChromaError, NotFoundError
from chromadb.test.conftest import skip_if_not_cluster
from chromadb.test.utils.wait_for_version_increase import (
    get_collection_version,
    wait_for_version_increase,
)
from time import sleep

pytestmark = [skip_if_not_cluster()]

MINIO_S3_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_REGION = "us-east-1"
MINIO_BUCKET = "chroma-storage"


@functools.lru_cache(maxsize=1)
def _minio_client() -> Any:
    try:
        import boto3
    except ImportError as e:
        pytest.fail(f"count_to_file_async test requires boto3: {e}")
    return boto3.client(
        "s3",
        endpoint_url=MINIO_S3_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )


def _minio_get_object(bucket: str, key: str) -> Optional[bytes]:
    try:
        import botocore.exceptions as botocore_exceptions
    except ImportError as e:
        pytest.fail(f"count_to_file_async test requires botocore: {e}")

    try:
        response = _minio_client().get_object(Bucket=bucket, Key=key)
    except botocore_exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey"}:
            return None
        pytest.fail(f"Failed to read MinIO object s3://{bucket}/{key}: {e}")

    return cast(bytes, response["Body"].read())


def _wait_for_minio_count(
    s3_path: str, expected_count: int, timeout_seconds: float = 180.0
) -> None:
    parsed = urllib.parse.urlparse(s3_path)
    assert parsed.scheme == "s3"
    assert parsed.netloc
    key = parsed.path.lstrip("/")

    deadline = time.monotonic() + timeout_seconds
    last_body = None
    while time.monotonic() < deadline:
        body = _minio_get_object(parsed.netloc, key)
        if body is not None:
            last_body = body.decode("utf-8").strip()
            if last_body == str(expected_count):
                return
        sleep(5)

    pytest.fail(
        f"Timed out waiting for {s3_path} to contain {expected_count}. Last observed body={last_body!r}"
    )


def _wait_for_record_counter_count(
    client: Any,
    output_collection_name: str,
    expected_count: int,
    timeout_seconds: float = 180.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_count = None
    while time.monotonic() < deadline:
        result = client.get_collection(output_collection_name).get("function_output")
        metadatas = result.get("metadatas")
        if metadatas:
            last_count = metadatas[0].get("total_count")
            if last_count == expected_count:
                return
        sleep(5)

    pytest.fail(
        f"Timed out waiting for {output_collection_name} to contain total_count={expected_count}. "
        f"Last observed total_count={last_count!r}"
    )


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


def test_functions_allow_one_sync_and_multiple_async_per_collection(
    basic_http_client: System,
) -> None:
    """Test that a collection can have one sync and multiple async attached functions"""
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

    # A second sync function should fail because the sync slot is already occupied.
    with pytest.raises(
        ChromaError,
        match="collection already has a sync attached function: name=task_1, function=record_counter, output_collection=output_1",
    ):
        collection.attach_function(
            function=RECORD_COUNTER_FUNCTION,
            name="task_2",
            output_collection="output_2",
            params=None,
        )

    # An async function should still be allowed on the same input collection.
    attached_async_fn, async_created = collection.attach_function(
        function=DUMMY_ASYNC_FUNCTION,
        name="task_async",
        output_collection="output_async",
        params=None,
    )

    assert attached_async_fn is not None
    assert async_created is True

    # Additional async functions should be allowed up to the configured limit.
    attached_async_fn_2, async_created_2 = collection.attach_function(
        function=DUMMY_ASYNC_FUNCTION,
        name="task_async_2",
        output_collection="output_async_2",
        params=None,
    )
    assert attached_async_fn_2 is not None
    assert async_created_2 is True

    attached_async_fn_3, async_created_3 = collection.attach_function(
        function=DUMMY_ASYNC_FUNCTION,
        name="task_async_3",
        output_collection="output_async_3",
        params=None,
    )
    assert attached_async_fn_3 is not None
    assert async_created_3 is True

    # Detach all functions.
    assert (
        collection.detach_function(attached_fn1.name, delete_output_collection=True)
        is True
    )
    assert (
        collection.detach_function(
            attached_async_fn.name, delete_output_collection=True
        )
        is True
    )
    assert (
        collection.detach_function(
            attached_async_fn_2.name, delete_output_collection=True
        )
        is True
    )
    assert (
        collection.detach_function(
            attached_async_fn_3.name, delete_output_collection=True
        )
        is True
    )

    # Now we should be able to attach a new sync function.
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


# TODO(tanujnay112): We take params now but there needs to be validation
# of the params in a later change.
# def test_attach_function_with_invalid_params(basic_http_client: System) -> None:
#     """Test that attach_function with non-empty params raises an error"""
#     client = ClientCreator.from_system(basic_http_client)
#     client.reset()

#     collection = client.create_collection(name="test_invalid_params")
#     collection.add(ids=["id1"], documents=["test document"])

#     # Attempt to create task with non-empty params should fail
#     # (no functions currently accept parameters)
#     with pytest.raises(
#         ChromaError,
#         match="params must be empty - no functions currently accept parameters",
#     ):
#         collection.attach_function(
#             name="invalid_params_task",
#             function=RECORD_COUNTER_FUNCTION,
#             output_collection="output_collection",
#             params={"some_key": "some_value"},
#         )


def test_attach_function_output_collection_already_exists(
    basic_http_client: System,
) -> None:
    """Test that attach_function can reuse any existing collection as output collection"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create a collection that will be used as input
    input_collection = client.create_collection(name="input_collection")
    input_collection.add(ids=["id1"], documents=["test document"])

    # Create another collection with the name we want to use for output
    client.create_collection(name="existing_output_collection")

    # Attempt to create task with output collection name that already exists - should succeed
    attached_fn, created = input_collection.attach_function(
        name="my_task",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="existing_output_collection",
        params=None,
    )
    assert attached_fn is not None
    assert created is True  # We can now reuse any existing collection


def test_multiple_functions_can_share_output_collection(
    basic_http_client: System,
) -> None:
    """Test that multiple functions can share an output collection, including different function types"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    # Create three input collections
    input_collection1 = client.create_collection(name="input_collection_1")
    input_collection1.add(ids=["id1", "id2"], documents=["doc1", "doc2"])

    input_collection2 = client.create_collection(name="input_collection_2")
    input_collection2.add(ids=["id3", "id4"], documents=["doc3", "doc4"])

    input_collection3 = client.create_collection(name="input_collection_3")
    input_collection3.add(ids=["id5", "id6"], documents=["doc5", "doc6"])

    # Attach first record_counter function
    attached_fn1, created1 = input_collection1.attach_function(
        name="counter_1",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="shared_counter_output",
        params=None,
    )
    assert attached_fn1 is not None
    assert created1 is True

    # Attach second record_counter function to the same output collection - should succeed
    attached_fn2, created2 = input_collection2.attach_function(
        name="counter_2",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="shared_counter_output",
        params=None,
    )
    assert attached_fn2 is not None
    assert created2 is True

    # Now try to attach a different function type to the same output collection - should succeed
    # The validation has been removed, so different function types can share output collections
    attached_fn3, created3 = input_collection3.attach_function(
        name="statistics_1",
        function=STATISTICS_FUNCTION,
        output_collection="shared_counter_output",
        params=None,
    )
    assert attached_fn3 is not None
    assert created3 is True


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


def test_attach_to_output_collection_fails_for_sync_upstream(
    basic_http_client: System,
) -> None:
    """Test that attaching a function to an output collection still fails when an upstream function is sync"""
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


def test_attach_to_output_collection_succeeds_for_async_upstream(
    basic_http_client: System,
) -> None:
    """Test that attaching a function to an output collection succeeds when all upstream functions are async"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    input_collection = client.create_collection(name="async_input_collection")
    input_collection.add(ids=["id1"], documents=["test"])

    _, _ = input_collection.attach_function(
        name="async_test_function",
        function=DUMMY_ASYNC_FUNCTION,
        output_collection="async_output_collection",
        params=None,
    )
    output_collection = client.get_collection(name="async_output_collection")

    attached_fn, created = output_collection.attach_function(
        name="downstream_test_function",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="downstream_output_collection",
        params=None,
    )

    assert attached_fn is not None
    assert created is True


def test_async_attached_function_can_add_multiple_inputs(
    basic_http_client: System,
) -> None:
    """Test that an async attached function can add another input collection through the client handle."""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    input_collection_1 = client.create_collection(name="multi_input_collection_1")
    input_collection_1.add(ids=["id1"], documents=["doc1"])

    input_collection_2 = client.create_collection(name="multi_input_collection_2")
    input_collection_2.add(ids=["id2"], documents=["doc2"])

    attached_fn, created = input_collection_1.attach_function(
        name="multi_input_async_function",
        function=DUMMY_ASYNC_FUNCTION,
        output_collection="multi_input_output_collection",
        params=None,
    )

    assert created is True
    assert attached_fn.input_collection_id == input_collection_1.id

    added_input_fn = attached_fn.add_input(input_collection_2)
    assert added_input_fn.id == attached_fn.id
    assert added_input_fn.name == attached_fn.name
    assert added_input_fn.function_name == attached_fn.function_name
    assert added_input_fn.input_collection_id == input_collection_2.id
    assert added_input_fn.output_collection == attached_fn.output_collection

    retrieved_from_first_input = input_collection_1.get_attached_function(
        attached_fn.name
    )
    assert retrieved_from_first_input == attached_fn

    retrieved_from_second_input = input_collection_2.get_attached_function(
        attached_fn.name
    )
    assert retrieved_from_second_input == added_input_fn

    # Re-adding the same input should be idempotent and return the same handle shape.
    assert added_input_fn.add_input(input_collection_2) == added_input_fn


def test_attach_to_output_collection_fails_for_mixed_sync_and_async_upstream(
    basic_http_client: System,
) -> None:
    """Test that attaching to an output collection fails when upstream functions are a mix of sync and async"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    async_input_collection = client.create_collection(
        name="mixed_async_input_collection"
    )
    async_input_collection.add(ids=["id1"], documents=["test"])

    sync_input_collection = client.create_collection(name="mixed_sync_input_collection")
    sync_input_collection.add(ids=["id2"], documents=["test"])

    _, _ = async_input_collection.attach_function(
        name="mixed_async_upstream",
        function=DUMMY_ASYNC_FUNCTION,
        output_collection="mixed_output_collection",
        params=None,
    )

    _, _ = sync_input_collection.attach_function(
        name="mixed_sync_upstream",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="mixed_output_collection",
        params=None,
    )

    output_collection = client.get_collection(name="mixed_output_collection")

    with pytest.raises(
        ChromaError, match="cannot attach function to an output collection"
    ):
        _ = output_collection.attach_function(
            name="mixed_downstream_test_function",
            function=RECORD_COUNTER_FUNCTION,
            output_collection="mixed_downstream_output_collection",
            params=None,
        )


def test_count_to_file_async_attached_function_counts_late_inputs(
    basic_http_client: System,
) -> None:
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    def add_records(collection: Collection, start: int, count: int) -> None:
        collection.add(
            ids=[f"{collection.name}_doc_{i}" for i in range(start, start + count)],
            documents=["test document"] * count,
        )

    file_key = f"task-api/count-to-file-{uuid.uuid4()}.txt"
    s3_path = f"s3://{MINIO_BUCKET}/{file_key}"

    input_collection_1 = client.create_collection(name="count_to_file_async_input_1")
    input_collection_2 = client.create_collection(name="count_to_file_async_input_2")

    # This function currently writes its result to object storage and does not
    # populate the attached output collection yet.
    attached_fn, created = input_collection_1.attach_function(
        name="count_to_file_async_function",
        function=COUNT_TO_FILE_ASYNC_FUNCTION,
        output_collection="count_to_file_async_output",
        params={"s3_path": s3_path},
    )
    assert created is True

    attached_fn_input_2 = attached_fn.add_input(input_collection_2.id)
    assert attached_fn_input_2 is not None

    input_collection_1_version = get_collection_version(client, input_collection_1.name)
    input_collection_2_version = get_collection_version(client, input_collection_2.name)
    add_records(input_collection_1, 0, 300)
    add_records(input_collection_2, 0, 300)
    wait_for_version_increase(
        client, input_collection_1.name, input_collection_1_version
    )
    wait_for_version_increase(
        client, input_collection_2.name, input_collection_2_version
    )
    _wait_for_minio_count(s3_path, 600)

    input_collection_2_version = get_collection_version(client, input_collection_2.name)
    add_records(input_collection_2, 300, 300)
    wait_for_version_increase(
        client, input_collection_2.name, input_collection_2_version
    )
    _wait_for_minio_count(s3_path, 900)

    input_collection_3 = client.create_collection(name="count_to_file_async_input_3")
    input_collection_3_version = get_collection_version(client, input_collection_3.name)
    add_records(input_collection_3, 0, 300)
    wait_for_version_increase(
        client, input_collection_3.name, input_collection_3_version
    )
    attached_fn_input_3 = attached_fn.add_input(input_collection_3.id)
    assert attached_fn_input_3 is not None
    _wait_for_minio_count(s3_path, 1200)


def test_record_counter_attached_late_counts_existing_and_new_inputs(
    basic_http_client: System,
) -> None:
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="late_sync_count_input_collection")
    collection.add(
        ids=[f"pre_attach_doc_{i}" for i in range(300)],
        documents=["test document"] * 300,
    )

    attached_fn, created = collection.attach_function(
        name="late_sync_counter",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="late_sync_counter_output",
        params=None,
    )
    assert attached_fn is not None
    assert created is True

    _wait_for_record_counter_count(client, "late_sync_counter_output", 300)

    collection.add(
        ids=[f"post_attach_doc_{i}" for i in range(300)],
        documents=["test document"] * 300,
    )

    _wait_for_record_counter_count(client, "late_sync_counter_output", 600)


def test_count_to_file_async_attached_late_counts_existing_and_new_inputs(
    basic_http_client: System,
) -> None:
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="late_async_count_input_collection")
    collection.add(
        ids=[f"pre_attach_doc_{i}" for i in range(300)],
        documents=["test document"] * 300,
    )

    file_key = f"task-api/late-async-count-{uuid.uuid4()}.txt"
    s3_path = f"s3://{MINIO_BUCKET}/{file_key}"

    attached_fn, created = collection.attach_function(
        name="late_async_counter",
        function=COUNT_TO_FILE_ASYNC_FUNCTION,
        output_collection="late_async_counter_output",
        params={"s3_path": s3_path},
    )
    assert attached_fn is not None
    assert created is True

    _wait_for_minio_count(s3_path, 300)

    collection.add(
        ids=[f"post_attach_doc_{i}" for i in range(300)],
        documents=["test document"] * 300,
    )

    _wait_for_minio_count(s3_path, 600)


def test_multiple_count_to_file_async_functions_can_share_one_input_collection(
    basic_http_client: System,
) -> None:
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="multi_async_count_input_collection")
    collection.add(ids=["seed"], documents=["seed document"])

    file_key_1 = f"task-api/multi-async-count-1-{uuid.uuid4()}.txt"
    file_key_2 = f"task-api/multi-async-count-2-{uuid.uuid4()}.txt"
    s3_path_1 = f"s3://{MINIO_BUCKET}/{file_key_1}"
    s3_path_2 = f"s3://{MINIO_BUCKET}/{file_key_2}"

    attached_fn_1, created_1 = collection.attach_function(
        name="multi_async_counter_1",
        function=COUNT_TO_FILE_ASYNC_FUNCTION,
        output_collection="multi_async_counter_output_1",
        params={"s3_path": s3_path_1},
    )
    assert attached_fn_1 is not None
    assert created_1 is True

    attached_fn_2, created_2 = collection.attach_function(
        name="multi_async_counter_2",
        function=COUNT_TO_FILE_ASYNC_FUNCTION,
        output_collection="multi_async_counter_output_2",
        params={"s3_path": s3_path_2},
    )
    assert attached_fn_2 is not None
    assert created_2 is True

    initial_version = get_collection_version(client, collection.name)

    collection.add(
        ids=[f"doc_{i}" for i in range(300)],
        documents=["test document"] * 300,
    )

    wait_for_version_increase(client, collection.name, initial_version)
    _wait_for_minio_count(s3_path_1, 301)
    _wait_for_minio_count(s3_path_2, 301)

    assert (
        collection.detach_function(attached_fn_1.name, delete_output_collection=True)
        is True
    )
    assert (
        collection.detach_function(attached_fn_2.name, delete_output_collection=True)
        is True
    )


def test_sync_and_async_count_functions_can_share_one_input_collection(
    basic_http_client: System,
) -> None:
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection = client.create_collection(name="shared_count_input_collection")
    collection.add(ids=["seed"], documents=["seed document"])

    file_key = f"task-api/shared-count-{uuid.uuid4()}.txt"
    s3_path = f"s3://{MINIO_BUCKET}/{file_key}"

    sync_attached_fn, sync_created = collection.attach_function(
        name="shared_sync_counter",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="shared_sync_counter_output",
        params=None,
    )
    assert sync_attached_fn is not None
    assert sync_created is True

    async_attached_fn, async_created = collection.attach_function(
        name="shared_async_counter",
        function=COUNT_TO_FILE_ASYNC_FUNCTION,
        output_collection="shared_async_counter_output",
        params={"s3_path": s3_path},
    )
    assert async_attached_fn is not None
    assert async_created is True

    initial_version = get_collection_version(client, collection.name)

    collection.add(
        ids=[f"doc_{i}" for i in range(300)],
        documents=["test document"] * 300,
    )

    wait_for_version_increase(client, collection.name, initial_version)
    _wait_for_minio_count(s3_path, 301)

    # Give some time to invalidate the frontend query cache for the sync output.
    sleep(60)

    result = client.get_collection("shared_sync_counter_output").get("function_output")
    assert result["metadatas"] is not None
    assert result["metadatas"][0]["total_count"] == 301

    assert (
        collection.detach_function(sync_attached_fn.name, delete_output_collection=True)
        is True
    )
    assert (
        collection.detach_function(
            async_attached_fn.name, delete_output_collection=True
        )
        is True
    )


def test_attach_to_existing_output_collection_rejects_cycle(
    basic_http_client: System,
) -> None:
    """Test that attaching to an existing output collection rejects a cycle like A -> B -> C -> A"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    collection_a = client.create_collection(name="cycle_collection_a")
    collection_a.add(ids=["id1"], documents=["doc1"])

    _, _ = collection_a.attach_function(
        name="a_to_b",
        function=DUMMY_ASYNC_FUNCTION,
        output_collection="cycle_collection_b",
        params=None,
    )

    collection_b = client.get_collection(name="cycle_collection_b")

    _, _ = collection_b.attach_function(
        name="b_to_c",
        function=DUMMY_ASYNC_FUNCTION,
        output_collection="cycle_collection_c",
        params=None,
    )

    collection_c = client.get_collection(name="cycle_collection_c")

    with pytest.raises(
        ChromaError, match="cannot attach function to an output collection"
    ):
        collection_c.attach_function(
            name="c_to_a",
            function=RECORD_COUNTER_FUNCTION,
            output_collection="cycle_collection_a",
            params=None,
        )


def test_attach_function_rejects_depth_above_maximum(
    basic_http_client: System,
) -> None:
    """Test that attach_function rejects chains deeper than the configured maximum depth"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    current_collection = client.create_collection(name="depth_collection_0")
    current_collection.add(ids=["id0"], documents=["doc0"])

    for i in range(1, 6):
        _, _ = current_collection.attach_function(
            name=f"depth_edge_{i}",
            function=DUMMY_ASYNC_FUNCTION,
            output_collection=f"depth_collection_{i}",
            params=None,
        )
        current_collection = client.get_collection(name=f"depth_collection_{i}")

    with pytest.raises(
        ChromaError, match="attached function depth exceeds maximum of 5"
    ):
        current_collection.attach_function(
            name="depth_edge_6",
            function=RECORD_COUNTER_FUNCTION,
            output_collection="depth_collection_6",
            params=None,
        )


def test_attach_function_rejects_when_connecting_two_chains_exceeds_maximum_depth(
    basic_http_client: System,
) -> None:
    """Test that attach_function rejects connecting two valid chains if the combined path would exceed the maximum depth"""
    client = ClientCreator.from_system(basic_http_client)
    client.reset()

    left_current = client.create_collection(name="left_depth_collection_0")
    left_current.add(ids=["left_id0"], documents=["left_doc0"])

    for i in range(1, 3):
        _, _ = left_current.attach_function(
            name=f"left_depth_edge_{i}",
            function=DUMMY_ASYNC_FUNCTION,
            output_collection=f"left_depth_collection_{i}",
            params=None,
        )
        left_current = client.get_collection(name=f"left_depth_collection_{i}")

    right_current = client.create_collection(name="right_depth_collection_0")
    right_current.add(ids=["right_id0"], documents=["right_doc0"])

    for i in range(1, 4):
        _, _ = right_current.attach_function(
            name=f"right_depth_edge_{i}",
            function=DUMMY_ASYNC_FUNCTION,
            output_collection=f"right_depth_collection_{i}",
            params=None,
        )
        right_current = client.get_collection(name=f"right_depth_collection_{i}")

    with pytest.raises(
        ChromaError, match="attached function depth exceeds maximum of 5"
    ):
        left_current.attach_function(
            name="bridge_two_chains",
            function=RECORD_COUNTER_FUNCTION,
            output_collection="right_depth_collection_0",
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

    # The attached function should be deleted due to cascade delete (database query finds all functions using this output collection)
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
    # This should succeed since both are record_counter functions
    attached_fn2, created2 = collection2.attach_function(
        name="count_my_docs2",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="my_documents_counts",
        params=None,
    )
    assert attached_fn2 is not None
    assert created2 is True  # Both functions can share the same output collection

    # Detach the function with delete_output_collection=True
    # This will delete the output collection and cascade delete ALL attached functions
    assert (
        collection.detach_function(attached_fn.name, delete_output_collection=True)
        is True
    )

    # The second function should be deleted due to cascade delete
    with pytest.raises(NotFoundError):
        collection2.get_attached_function(attached_fn2.name)

    # Create a task that counts records in the collection
    attached_fn, created = collection2.attach_function(
        name="count_my_docs",
        function=RECORD_COUNTER_FUNCTION,
        output_collection="my_documents_counts",
        params=None,
    )
    assert attached_fn is not None
    assert created is True


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
