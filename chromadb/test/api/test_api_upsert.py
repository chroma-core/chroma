import pytest
from typing import cast, List
from chromadb.api import ClientAPI
from chromadb.errors import InvalidCollectionException
from chromadb.api.types import IncludeEnum
from chromadb.test.api.utils import (
    vector_approx_equal,
    initial_records,
    new_records,
    minimal_records,
    bad_dimensionality_records,
)


def test_upsert(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")

    collection.add(**initial_records)  # type: ignore[arg-type]
    assert collection.count() == 3

    collection.upsert(**new_records)  # type: ignore[arg-type]
    assert collection.count() == 4

    get_result = collection.get(
        include=cast(List[IncludeEnum], ["embeddings", "metadatas", "documents"]),
        ids=new_records["ids"][0],  # type: ignore[index]
    )
    assert vector_approx_equal(
        (get_result["embeddings"])[0], new_records["embeddings"][0]  # type: ignore[index]
    )
    assert (get_result["metadatas"])[0] == new_records["metadatas"][0]  # type: ignore[index]
    assert (get_result["documents"])[0] == new_records["documents"][0]  # type: ignore[index]

    query_result = collection.query(
        query_embeddings=get_result["embeddings"],
        n_results=1,
        include=cast(List[IncludeEnum], ["embeddings", "metadatas", "documents"]),
    )
    assert vector_approx_equal(
        (query_result["embeddings"])[0][0], new_records["embeddings"][0]  # type: ignore[index]
    )
    assert (query_result["metadatas"])[0][0] == new_records["metadatas"][0]  # type: ignore
    assert (query_result["documents"])[0][0] == new_records["documents"][0]  # type: ignore[index]

    collection.delete(ids=initial_records["ids"][2])  # type: ignore[index]
    collection.upsert(
        ids=initial_records["ids"][2],  # type: ignore[index]
        embeddings=[[1.1, 0.99, 2.21]],  # type: ignore[arg-type]
        metadatas=[{"string_value": "a new string value"}],
    )
    assert collection.count() == 4

    get_result = collection.get(
        include=cast(List[IncludeEnum], ["embeddings", "metadatas", "documents"]),
        ids=["id3"],
    )
    assert vector_approx_equal((get_result["embeddings"])[0], [1.1, 0.99, 2.21])  # type: ignore[index]
    assert (get_result["metadatas"])[0] == {  # type: ignore[index]
        "string_value": "a new string value"
    }

    assert (get_result["documents"])[0] is None  # type: ignore[index]


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
