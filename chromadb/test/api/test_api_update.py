# type: ignore
import numpy as np
import pytest

from chromadb.errors import NotFoundError

from .utils import vector_approx_equal

records = {
    "embeddings": [[0, 0, 0], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
    "documents": ["this document is first", "this document is second"],
}

metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
}


def test_update_query(client):
    client.reset()
    collection = client.create_collection("test_update_query")
    collection.add(**records)
    updated_records = {
        "ids": [records["ids"][0]],
        "embeddings": [[0.1, 0.2, 0.3]],
        "documents": ["updated document"],
        "metadatas": [{"foo": "bar"}],
    }
    collection.update(**updated_records)
    results = collection.query(
        query_embeddings=updated_records["embeddings"],
        n_results=1,
        include=["embeddings", "documents", "metadatas"],
    )
    assert len(results["ids"][0]) == 1
    assert results["ids"][0][0] == updated_records["ids"][0]
    assert results["documents"][0][0] == updated_records["documents"][0]
    assert results["metadatas"][0][0]["foo"] == "bar"
    assert vector_approx_equal(
        results["embeddings"][0][0], updated_records["embeddings"][0]
    )


def test_modify(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.modify(name="testspace2")
    assert collection.name == "testspace2"


def test_collection_modify_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")
    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.modify(name="test2")


def test_modify_error_on_existing_name(client):
    client.reset()
    client.create_collection("testspace")
    c2 = client.create_collection("testspace2")
    with pytest.raises(Exception):
        c2.modify(name="testspace")


def test_modify_warn_on_DF_change(client, caplog):
    client.reset()
    collection = client.create_collection("testspace")
    with pytest.raises(Exception, match="not supported"):
        collection.modify(metadata={"hnsw:space": "cosine"})


def test_metadata_update_get_int_float(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)
    collection.update(
        ids=["id1"],
        metadatas=[{"int_value": 2, "string_value": "two", "float_value": 2.002}],
    )
    items = collection.get(ids=["id1"])
    assert items["metadatas"][0]["int_value"] == 2
    assert items["metadatas"][0]["string_value"] == "two"
    assert items["metadatas"][0]["float_value"] == 2.002
