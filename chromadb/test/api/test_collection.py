from chromadb.api import ClientAPI
from chromadb.test.conftest import NOT_CLUSTER_ONLY
import pytest


def duplicate_collection_create(
    client: ClientAPI,
) -> None:
    collection = client.create_collection(
        name="test",
        metadata={"hnsw:construction_ef": 128, "hnsw:search_ef": 128, "hnsw:M": 128},
    )

    try:
        client.create_collection(
            name="test",
            metadata={
                "hnsw:construction_ef": 128,
                "hnsw:search_ef": 128,
                "hnsw:M": 128,
            },
        )
        assert False, "Expected exception"
    except Exception as e:
        print("Collection creation failed as expected with error ", e)
        assert "already exists" in e.args[0]


def not_existing_collection_delete(
    client: ClientAPI,
) -> None:
    try:
        collection = client.delete_collection(
            name="test101",
        )
        assert False, "Expected exception"
    except Exception as e:
        print("Collection deletion failed as expected with error ", e)
        assert "does not exist" in e.args[0]


def test_duplicate_collection_create_local(client: ClientAPI) -> None:
    duplicate_collection_create(client)


def test_not_existing_collection_delete_local(client: ClientAPI) -> None:
    not_existing_collection_delete(client)


def test_duplicate_collection_create_distributed(http_client: ClientAPI) -> None:
    if NOT_CLUSTER_ONLY:
        pytest.skip("Skipping test for non-cluster environment")
    duplicate_collection_create(http_client)


def test_not_existing_collection_delete_distributed(http_client: ClientAPI) -> None:
    if NOT_CLUSTER_ONLY:
        pytest.skip("Skipping test for non-cluster environment")
    not_existing_collection_delete(http_client)
