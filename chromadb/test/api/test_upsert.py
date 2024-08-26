import pytest

minimal_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
}

bad_dimensionality_records = {
    "embeddings": [[1.1, 2.3], [1.2, 2.24]],  # Different dimensionality
    "ids": ["id1", "id2"],
}

def test_dimensionality_exception_update(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_update_exception")
    collection.add(**minimal_records)

    with pytest.raises(Exception) as e:
        collection.update(**bad_dimensionality_records)
    assert "dimensionality" in str(e.value)

