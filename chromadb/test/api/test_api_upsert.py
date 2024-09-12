import pytest
from chromadb.api import ClientAPI
from chromadb.errors import InvalidCollectionException
from chromadb.test.api.utils import (
    initial_records,
    minimal_records,
    bad_dimensionality_records,
)


def test_collection_upsert_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.upsert(**initial_records)  # type: ignore[arg-type]


def test_upsert_invalid_embeddings(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_invalid_embeddings")

    # Upsert with invalid embeddings
    invalid_records = {
        "embeddings": [[[1.1, 2.3, 3.2]], [[1.2, 2.24, 3.2]]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.upsert(**invalid_records)  # type: ignore[arg-type]
    assert "embedding" in str(e.value)


# test to make sure upsert shows exception for bad dimensionality
def test_dimensionality_exception_upsert(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_dimensionality_upsert_exception")
    collection.add(**minimal_records)  # type: ignore[arg-type]

    with pytest.raises(Exception) as e:
        collection.upsert(**bad_dimensionality_records)  # type: ignore[arg-type]
    assert "dimensionality" in str(e.value)
