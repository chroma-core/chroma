import pytest
from chromadb.api import ClientAPI
from chromadb.test.api.utils import contains_records, batch_records


# test to make sure delete error on invalid id input
def test_delete_invalid_id(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_invalid_id")

    # Delete with malformed ids
    with pytest.raises(ValueError) as e:
        collection.delete(ids=["valid", 0])  # type: ignore[list-item]
    assert "ID" in str(e.value)


def test_delete_where_document(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_delete_where_document")
    collection.add(**contains_records)  # type: ignore[arg-type]

    collection.delete(where_document={"$contains": "doc1"})
    assert collection.count() == 1

    collection.delete(where_document={"$contains": "bad"})
    assert collection.count() == 1

    collection.delete(where_document={"$contains": "great"})
    assert collection.count() == 0


def test_delete(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)  # type: ignore[arg-type]
    assert collection.count() == 2

    with pytest.raises(Exception):
        collection.delete()
