import pytest
from chromadb.test.test_api import contains_records

# test to make sure delete error on invalid id input
def test_delete_invalid_id(client):
    client.reset()
    collection = client.create_collection("test_invalid_id")

    # Delete with malformed ids
    with pytest.raises(ValueError) as e:
        collection.delete(ids=["valid", 0])
    assert "ID" in str(e.value)
    

def test_delete_where_document(client):
    client.reset()
    collection = client.create_collection("test_delete_where_document")
    collection.add(**contains_records)

    collection.delete(where_document={"$contains": "doc1"})
    assert collection.count() == 1

    collection.delete(where_document={"$contains": "bad"})
    assert collection.count() == 1

    collection.delete(where_document={"$contains": "great"})
    assert collection.count() == 0
