# type: ignore
import pytest

from chromadb.errors import NotFoundError

from .utils import vector_approx_equal

initial_records = {
    "embeddings": [[0, 0, 0], [1.2, 2.24, 3.2], [2.2, 3.24, 4.2]],
    "ids": ["id1", "id2", "id3"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
        {"string_value": "three"},
    ],
    "documents": [
        "this document is first",
        "this document is second",
        "this document is third",
    ],
}

new_records = {
    "embeddings": [[3.0, 3.0, 1.1], [3.2, 4.24, 5.2]],
    "ids": ["id1", "id4"],
    "metadatas": [
        {"int_value": 1, "string_value": "one_of_one", "float_value": 1.001},
        {"int_value": 4},
    ],
    "documents": [
        "this document is even more first",
        "this document is new and fourth",
    ],
}


def test_upsert(client):
    client.reset()
    collection = client.create_collection("test")
    collection.add(**initial_records)
    assert collection.count() == 3
    collection.upsert(**new_records)
    assert collection.count() == 4
    get_result = collection.get(
        include=["embeddings", "metadatas", "documents"], ids=new_records["ids"][0]
    )
    assert vector_approx_equal(
        get_result["embeddings"][0], new_records["embeddings"][0]
    )
    assert get_result["metadatas"][0] == new_records["metadatas"][0]
    assert get_result["documents"][0] == new_records["documents"][0]
    query_result = collection.query(
        query_embeddings=get_result["embeddings"],
        n_results=1,
        include=["embeddings", "metadatas", "documents"],
    )
    assert vector_approx_equal(
        query_result["embeddings"][0][0], new_records["embeddings"][0]
    )
    assert query_result["metadatas"][0][0] == new_records["metadatas"][0]
    assert query_result["documents"][0][0] == new_records["documents"][0]
    collection.delete(ids=initial_records["ids"][2])
    collection.upsert(
        ids=initial_records["ids"][2],
        embeddings=[[1.1, 0.99, 2.21]],
        metadatas=[{"string_value": "a new string value"}],
    )
    assert collection.count() == 4
    get_result = collection.get(
        include=["embeddings", "metadatas", "documents"], ids=["id3"]
    )
    assert vector_approx_equal(get_result["embeddings"][0], [1.1, 0.99, 2.21])
    assert get_result["metadatas"][0] == {"string_value": "a new string value"}
    assert get_result["documents"][0] is None


def test_collection_upsert_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")
    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.upsert(**initial_records)
