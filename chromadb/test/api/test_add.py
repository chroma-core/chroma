import pytest
import numpy as np

from chromadb.test.api.utils import minimal_records, bad_dimensionality_records

def test_add_minimal(client):
    client.reset()

    collection = client.create_collection("testspace")

    collection.add(**minimal_records)

    assert collection.count() == 2


def test_dimensionality_validation_add(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_validation")
    collection.add(**minimal_records)

    with pytest.raises(Exception) as e:
        collection.add(**bad_dimensionality_records)
    assert "dimensionality" in str(e.value)

# test to make sure add error on invalid id input
def test_invalid_id(client):
    client.reset()
    collection = client.create_collection("test_invalid_id")
    # Add with non-string id
    with pytest.raises(ValueError) as e:
        collection.add(embeddings=[0, 0, 0], ids=[1], metadatas=[{}])
    assert "ID" in str(e.value)

def test_add_large(client):
    client.reset()

    collection = client.create_collection("testspace")

    # Test adding a large number of records
    large_records = np.random.rand(2000, 512).astype(np.float32).tolist()

    collection.add(
        embeddings=large_records,
        ids=[f"http://example.com/{i}" for i in range(len(large_records))],
    )

    assert collection.count() == len(large_records)

# test to make sure add, and query error on invalid embeddings input
def test_add_invalid_embeddings(client):
    client.reset()
    collection = client.create_collection("test_invalid_embeddings")

    # Add with string embeddings
    invalid_records = {
        "embeddings": [["0", "0", "0"], ["1.2", "2.24", "3.2"]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.add(**invalid_records)
    assert "embedding" in str(e.value)