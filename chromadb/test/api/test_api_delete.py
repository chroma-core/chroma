import pytest
from chromadb.api import ClientAPI
from chromadb.test.api.utils import batch_records
from chromadb.errors import InvalidCollectionException


# test to make sure delete error on invalid id input
def test_delete_invalid_id(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_invalid_id")

    # Delete with malformed ids
    with pytest.raises(ValueError) as e:
        collection.delete(ids=["valid", 0])  # type: ignore[list-item]
    assert "ID" in str(e.value)


def test_collection_delete_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.delete(ids=["id1"])


def test_delete(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)  # type: ignore[arg-type]
    assert collection.count() == 2

    with pytest.raises(Exception):
        collection.delete()
