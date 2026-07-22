# type: ignore
import numpy as np
import pytest

from chromadb.errors import InvalidArgumentError

minimal_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}

records = {
    "embeddings": [[0, 0, 0], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
    "documents": ["this document is first", "this document is second"],
}

bad_dimensionality_records = {
    "embeddings": [[1.1, 2.3, 3.2, 4.5], [1.2, 2.24, 3.2, 4.5]],
    "ids": ["id1", "id2"],
}

bad_dimensionality_query = {
    "query_embeddings": [[1.1, 2.3, 3.2, 4.5], [1.2, 2.24, 3.2, 4.5]],
}

bad_metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [{"value": {"nested": "5"}}, {"value": [1, 2, 3]}],
}

metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
}


def test_dimensionality_validation_add(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_validation")
    collection.add(**minimal_records)
    with pytest.raises(Exception) as e:
        collection.add(**bad_dimensionality_records)
    assert "dimension" in str(e.value)


def test_dimensionality_validation_query(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_validation_query")
    collection.add(**minimal_records)
    with pytest.raises(Exception) as e:
        collection.query(**bad_dimensionality_query)
    assert "dimension" in str(e.value)


def test_invalid_embeddings(client):
    client.reset()
    collection = client.create_collection("test_invalid_embeddings")
    invalid_records = {
        "embeddings": [["0", "0", "0"], ["1.2", "2.24", "3.2"]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.add(**invalid_records)
    assert "embedding" in str(e.value)
    with pytest.raises(ValueError) as e:
        collection.query(
            query_embeddings=[["1.1", "2.3", "3.2"]],
            n_results=1,
        )
    assert "embedding" in str(e.value)
    invalid_records = {
        "embeddings": [[[0], [0], [0]], [[1.2], [2.24], [3.2]]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.update(**invalid_records)
    assert "embedding" in str(e.value)
    invalid_records = {
        "embeddings": [[[1.1, 2.3, 3.2]], [[1.2, 2.24, 3.2]]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.upsert(**invalid_records)
    assert "embedding" in str(e.value)


def test_dimensionality_exception_update(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_update_exception")
    collection.add(**minimal_records)
    with pytest.raises(Exception) as e:
        collection.update(**bad_dimensionality_records)
    assert "dimension" in str(e.value)


def test_dimensionality_exception_upsert(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_upsert_exception")
    collection.add(**minimal_records)
    with pytest.raises(Exception) as e:
        collection.upsert(**bad_dimensionality_records)
    assert "dimension" in str(e.value)


def test_invalid_id(client):
    client.reset()
    collection = client.create_collection("test_invalid_id")
    with pytest.raises(ValueError) as e:
        collection.add(embeddings=[0, 0, 0], ids=[1], metadatas=[{}])
    assert "ID" in str(e.value)
    with pytest.raises(ValueError) as e:
        collection.get(ids=1)
    assert "ID" in str(e.value)
    with pytest.raises(ValueError) as e:
        collection.delete(ids=["valid", 0])
    assert "ID" in str(e.value)


def test_index_params(client):
    EPS = 1e-12
    client.reset()
    collection = client.create_collection(name="test_index_params")
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] > 4
    client.reset()
    collection = client.create_collection(
        name="test_index_params",
        metadata={"hnsw:space": "cosine", "hnsw:construction_ef": 20, "hnsw:M": 5},
    )
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] > 0 - EPS
    assert items["distances"][0][0] < 1 + EPS
    client.reset()
    collection = client.create_collection(
        name="test_index_params", metadata={"hnsw:space": "ip"}
    )
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] < -5


def test_invalid_index_params(client):
    client.reset()
    with pytest.raises(InvalidArgumentError):
        collection = client.create_collection(
            name="test_index_params", metadata={"hnsw:space": "foobar"}
        )
        collection.add(**records)


def test_metadata_validation_add(client):
    client.reset()
    collection = client.create_collection("test_metadata_validation")
    with pytest.raises(ValueError, match="metadata"):
        collection.add(**bad_metadata_records)


def test_metadata_validation_update(client):
    client.reset()
    collection = client.create_collection("test_metadata_validation")
    collection.add(**metadata_records)
    with pytest.raises(ValueError, match="metadata"):
        collection.update(ids=["id1"], metadatas={"value": {"nested": "5"}})
