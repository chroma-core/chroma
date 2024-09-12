import pytest
from typing import List, cast
from chromadb.errors import InvalidCollectionException
from chromadb.api.types import IncludeEnum
from chromadb.api import ClientAPI
from chromadb.test.api.utils import (
    operator_records,
)


def test_collection_get_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.get()


def test_get_with_invalid_include(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_get_include")

    with pytest.raises(ValueError, match="include"):
        collection.get(include=cast(List[IncludeEnum], ["metadatas", "undefined"]))

    with pytest.raises(ValueError, match="include"):
        collection.get(include=None)  # type: ignore[arg-type]


# test to make sure get error on invalid id input
def test_invalid_id(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_invalid_id")

    # Get with non-list id
    with pytest.raises(ValueError) as e:
        collection.get(ids=1)  # type: ignore[arg-type]
    assert "ID" in str(e.value)


def test_get_document_valid_operators(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_valid_operators")
    collection.add(**operator_records)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$lt": {"$nested": 2}})  # type: ignore[dict-item]

    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$contains": []})

    # Test invalid $and, $or
    with pytest.raises(ValueError):
        collection.get(where_document={"$and": {"$unsupported": "doc"}})  # type: ignore[dict-item]

    with pytest.raises(ValueError):
        collection.get(
            where_document={"$or": [{"$unsupported": "doc"}, {"$unsupported": "doc"}]}  # type: ignore[dict-item]
        )

    with pytest.raises(ValueError):
        collection.get(where_document={"$or": [{"$contains": "doc"}]})

    with pytest.raises(ValueError):
        collection.get(where_document={"$or": []})

    with pytest.raises(ValueError):
        collection.get(
            where_document={
                "$or": [{"$and": [{"$contains": "doc"}]}, {"$contains": "doc"}]
            }
        )


def test_where_valid_operators(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_valid_operators")
    collection.add(**operator_records)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$invalid": 2}})  # type: ignore[dict-item]

    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$lt": "2"}})  # type: ignore[dict-item]

    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$lt": 2, "$gt": 1}})  # type: ignore[dict-item]

    # Test invalid $and, $or
    with pytest.raises(ValueError):
        collection.get(where={"$and": {"int_value": {"$lt": 2}}})  # type: ignore[dict-item]

    with pytest.raises(ValueError):
        collection.get(
            where={"int_value": {"$lt": 2}, "$or": {"int_value": {"$gt": 1}}}  # type: ignore[dict-item]
        )

    with pytest.raises(ValueError):
        collection.get(
            where={"$gt": [{"int_value": {"$lt": 2}}, {"int_value": {"$gt": 1}}]}  # type: ignore[dict-item]
        )

    with pytest.raises(ValueError):
        collection.get(where={"$or": [{"int_value": {"$lt": 2}}]})  # type: ignore[dict-item]

    with pytest.raises(ValueError):
        collection.get(where={"$or": []})

    with pytest.raises(ValueError):
        collection.get(where={"a": {"$contains": "test"}})  # type: ignore[dict-item]

    with pytest.raises(ValueError):
        collection.get(
            where={
                "$or": [
                    {"a": {"$contains": "first"}},  # type: ignore[dict-item]
                    {"$contains": "second"},
                ]
            }
        )


def test_where_validation_get(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_validation")
    with pytest.raises(ValueError, match="where"):
        collection.get(where={"value": {"nested": "5"}})  # type: ignore[dict-item]
