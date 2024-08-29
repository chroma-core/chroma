import pytest
from typing import List, cast

from chromadb.errors import InvalidCollectionException
from chromadb.api import ClientAPI
from chromadb.api.types import IncludeEnum
from chromadb.test.api.utils import (
    vector_approx_equal,
    records,
    minimal_records,
    bad_dimensionality_records,
    metadata_records,
)


def test_metadata_update_get_int_float(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)  # type: ignore[arg-type]

    collection.update(
        ids=["id1"],
        metadatas=[{"int_value": 2, "string_value": "two", "float_value": 2.002}],
    )
    items = collection.get(ids=["id1"])
    assert (items["metadatas"])[0]["int_value"] == 2  # type: ignore[index]
    assert (items["metadatas"])[0]["string_value"] == "two"  # type: ignore[index]
    assert (items["metadatas"])[0]["float_value"] == 2.002  # type: ignore[index]


def test_metadata_validation_update(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_metadata_validation")
    collection.add(**metadata_records)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="metadata"):
        collection.update(ids=["id1"], metadatas={"value": {"nested": "5"}})  # type: ignore[dict-item]


def test_update_query(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_update_query")
    collection.add(**records)  # type: ignore[arg-type]

    updated_records = {
        "ids": [records["ids"][0]],  # type: ignore[index]
        "embeddings": [[0.1, 0.2, 0.3]],
        "documents": ["updated document"],
        "metadatas": [{"foo": "bar"}],
    }

    collection.update(**updated_records)

    # test query
    results = collection.query(
        query_embeddings=updated_records["embeddings"],
        n_results=1,
        include=cast(List[IncludeEnum], ["embeddings", "documents", "metadatas"]),
    )
    assert len(results["ids"][0]) == 1
    assert results["ids"][0][0] == updated_records["ids"][0]
    assert (results["documents"])[0][0] == updated_records["documents"][0]  # type: ignore[index]
    assert (results["metadatas"])[0][0]["foo"] == "bar"  # type: ignore[index]
    assert vector_approx_equal(
        (results["embeddings"])[0][0], updated_records["embeddings"][0]  # type: ignore[index]
    )


# test to make sure update error on invalid embeddings input
def test_update_invalid_embeddings(client: ClientAPI) -> None:
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

    # Query with invalid embeddings
    with pytest.raises(ValueError) as e:
        collection.query(
            query_embeddings=[["1.1", "2.3", "3.2"]],  # type: ignore[arg-type]
            n_results=1,
        )
    assert "embedding" in str(e.value)

    # Update with invalid embeddings
    invalid_records = {
        "embeddings": [[[0], [0], [0]], [[1.2], [2.24], [3.2]]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.update(**invalid_records)  # type: ignore[arg-type]
    assert "embedding" in str(e.value)


# test to make sure update shows exception for bad dimensionality
def test_dimensionality_exception_update(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_dimensionality_update_exception")
    collection.add(**minimal_records)  # type: ignore[arg-type]

    with pytest.raises(Exception) as e:
        collection.update(**bad_dimensionality_records)  # type: ignore[arg-type]
    assert "dimensionality" in str(e.value)


def test_collection_update_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.update(ids=["id1"], documents=["test"])
