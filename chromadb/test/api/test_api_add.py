import pytest
from chromadb.api import ClientAPI
from chromadb.test.api.utils import (
    minimal_records,
    bad_dimensionality_records,
    batch_records,
    bad_metadata_records,
)
from chromadb.errors import InvalidCollectionException


def test_collection_add_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.add(**batch_records)  # type: ignore[arg-type]


def test_dimensionality_validation_add(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_dimensionality_validation")
    collection.add(**minimal_records)  # type: ignore[arg-type]

    with pytest.raises(Exception) as e:
        collection.add(**bad_dimensionality_records)  # type: ignore[arg-type]
    assert "dimensionality" in str(e.value)


# test to make sure add error on invalid id input
def test_invalid_id(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_invalid_id")
    # Add with non-string id
    with pytest.raises(ValueError) as e:
        collection.add(embeddings=[0, 0, 0], ids=[1], metadatas=[{}])  # type: ignore
    assert "ID" in str(e.value)


# test to make sure add, and query error on invalid embeddings input
def test_add_invalid_embeddings(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_invalid_embeddings")

    # Add with string embeddings
    invalid_records = {
        "embeddings": [["0", "0", "0"], ["1.2", "2.24", "3.2"]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.add(**invalid_records)  # type: ignore[arg-type]
    assert "embedding" in str(e.value)


def test_metadata_validation_add(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_metadata_validation")
    with pytest.raises(ValueError, match="metadata"):
        collection.add(**bad_metadata_records)  # type: ignore[arg-type]
