import pytest
from typing import List, cast
from chromadb.errors import InvalidCollectionException
from chromadb.api.types import QueryResult, IncludeEnum
from chromadb.api import ClientAPI
from chromadb.test.api.utils import (
    approx_equal,
    metadata_records,
    contains_records,
    logical_operator_records,
    records,
    batch_records,
    operator_records,
)


def test_get_from_db(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)  # type: ignore[arg-type]
    includes: List[IncludeEnum] = cast(
        List[IncludeEnum], ["embeddings", "documents", "metadatas"]
    )
    records = collection.get(include=includes)
    for key in records.keys():
        if (key in includes) or (key == "ids"):
            assert len(records[key]) == 2  # type: ignore[literal-required]
        elif key == "included":
            assert set(records[key]) == set(includes)  # type: ignore[literal-required]
        else:
            assert records[key] is None  # type: ignore[literal-required]


def test_collection_get_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.get()


def test_get_where_document(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_get_where_document")
    collection.add(**contains_records)  # type: ignore[arg-type]

    items = collection.get(where_document={"$contains": "doc1"})
    assert len(items["metadatas"]) == 1  # type: ignore[arg-type]

    items = collection.get(where_document={"$contains": "great"})
    assert len(items["metadatas"]) == 2  # type: ignore[arg-type]
    items = collection.get(where_document={"$contains": "bad"})
    assert len(items["metadatas"]) == 0  # type: ignore[arg-type]


# TEST METADATA AND METADATA FILTERING
# region
def test_where_logical_operators(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_logical_operators")
    collection.add(**logical_operator_records)  # type: ignore[arg-type]

    items = collection.get(
        where={
            "$and": [
                {"$or": [{"int_value": {"$gte": 3}}, {"float_value": {"$lt": 1.9}}]},  # type: ignore[dict-item]
                {"is": "doc"},
            ]
        }
    )
    assert len(items["metadatas"]) == 3  # type: ignore[arg-type]
    items = collection.get(
        where={
            "$or": [
                {
                    "$and": [
                        {"int_value": {"$eq": 3}},  # type: ignore[dict-item]
                        {"string_value": {"$eq": "three"}},  # type: ignore[dict-item]
                    ]
                },
                {
                    "$and": [
                        {"int_value": {"$eq": 4}},  # type: ignore[dict-item]
                        {"string_value": {"$eq": "four"}},  # type: ignore[dict-item]
                    ]
                },
            ]
        }
    )
    assert len(items["metadatas"]) == 2  # type: ignore[arg-type]

    items = collection.get(
        where={
            "$and": [
                {
                    "$or": [
                        {"int_value": {"$eq": 1}},  # type: ignore[dict-item]
                        {"string_value": {"$eq": "two"}},  # type: ignore[dict-item]
                    ]
                },
                {
                    "$or": [
                        {"int_value": {"$eq": 2}},  # type: ignore[dict-item]
                        {"string_value": {"$eq": "one"}},  # type: ignore[dict-item]
                    ]
                },
            ]
        }
    )
    assert len(items["metadatas"]) == 2  # type: ignore[arg-type]


def test_where_document_logical_operators(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_document_logical_operators")
    collection.add(**logical_operator_records)  # type: ignore[arg-type]

    items = collection.get(
        where_document={
            "$and": [
                {"$contains": "first"},
                {"$contains": "doc"},
            ]
        }
    )
    assert len(items["metadatas"]) == 1  # type: ignore[arg-type]

    items = collection.get(
        where_document={
            "$or": [
                {"$contains": "first"},
                {"$contains": "second"},
            ]
        }
    )
    assert len(items["metadatas"]) == 2  # type: ignore[arg-type]

    items = collection.get(
        where_document={
            "$or": [
                {"$contains": "first"},
                {"$contains": "second"},
            ]
        },
        where={
            "int_value": {"$ne": 2},  # type: ignore[dict-item]
        },
    )
    assert len(items["metadatas"]) == 1  # type: ignore[arg-type]


def test_get_include(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_get_include")
    collection.add(**records)  # type: ignore[arg-type]

    include: List[IncludeEnum] = cast(List[IncludeEnum], ["metadatas", "documents"])
    items = collection.get(include=include, where={"int_value": 1})
    assert items["embeddings"] is None
    assert items["ids"][0] == "id1"
    assert (items["metadatas"])[0]["int_value"] == 1  # type: ignore[index]
    assert (items["documents"])[0] == "this document is first"  # type: ignore[index]
    assert set(items["included"]) == set(include)

    include = cast(List[IncludeEnum], ["embeddings", "documents"])
    items = collection.get(include=include)
    assert items["metadatas"] is None
    assert items["ids"][0] == "id1"
    assert approx_equal((items["embeddings"])[1][0], 1.2)  # type: ignore[index]
    assert set(items["included"]) == set(include)

    items = collection.get(include=[])
    assert items["documents"] is None
    assert items["metadatas"] is None
    assert items["embeddings"] is None
    assert items["ids"][0] == "id1"
    assert items["included"] == []

    with pytest.raises(ValueError, match="include"):
        items = collection.get(
            include=cast(List[IncludeEnum], ["metadatas", "undefined"])
        )

    with pytest.raises(ValueError, match="include"):
        items = collection.get(include=None)  # type: ignore[arg-type]


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


def test_where_lt(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_lt")
    collection.add(**operator_records)  # type: ignore[arg-type]
    items = collection.get(where={"int_value": {"$lt": 2}})  # type: ignore[dict-item]
    assert len(items["metadatas"]) == 1  # type: ignore[arg-type]


def test_where_lte(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)  # type: ignore[arg-type]
    items = collection.get(where={"int_value": {"$lte": 2.0}})  # type: ignore[dict-item]
    assert len(items["metadatas"]) == 2  # type: ignore[arg-type]


def test_where_gt(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)  # type: ignore[arg-type]
    items = collection.get(where={"float_value": {"$gt": -1.4}})  # type: ignore[dict-item]
    assert len(items["metadatas"]) == 2  # type: ignore[arg-type]


def test_where_gte(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)  # type: ignore[arg-type]
    items = collection.get(where={"float_value": {"$gte": 2.002}})  # type: ignore[dict-item]
    assert len(items["metadatas"]) == 1  # type: ignore[arg-type]


def test_where_ne_string(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)  # type: ignore[arg-type]
    items = collection.get(where={"string_value": {"$ne": "two"}})  # type: ignore[dict-item]
    assert len(items["metadatas"]) == 1  # type: ignore[arg-type]


def test_where_ne_eq_number(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)  # type: ignore[arg-type]
    items = collection.get(where={"int_value": {"$ne": 1}})  # type: ignore[dict-item]
    assert len(items["metadatas"]) == 1  # type: ignore[arg-type]
    items = collection.get(where={"float_value": {"$eq": 2.002}})  # type: ignore[dict-item]
    assert len(items["metadatas"]) == 1  # type: ignore[arg-type]


def test_where_validation_get(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_where_validation")
    with pytest.raises(ValueError, match="where"):
        collection.get(where={"value": {"nested": "5"}})  # type: ignore[dict-item]


def test_metadata_get_where_int(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)  # type: ignore[arg-type]

    items = collection.get(where={"int_value": 1})
    assert (items["metadatas"])[0]["int_value"] == 1  # type: ignore[index]
    assert (items["metadatas"])[0]["string_value"] == "one"  # type: ignore[index]


def test_metadata_get_where_float(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)  # type: ignore[arg-type]

    items = collection.get(where={"float_value": 1.001})
    assert (items["metadatas"])[0]["int_value"] == 1  # type: ignore[index]
    assert (items["metadatas"])[0]["string_value"] == "one"  # type: ignore[index]
    assert (items["metadatas"])[0]["float_value"] == 1.001  # type: ignore[index]


def test_metadata_get_where_string(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)  # type: ignore[arg-type]

    items = collection.get(where={"string_value": "one"})
    assert (items["metadatas"])[0]["int_value"] == 1  # type: ignore[index]
    assert (items["metadatas"])[0]["string_value"] == "one"  # type: ignore[index]


def test_metadata_add_get_int_float(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)  # type: ignore[arg-type]

    items = collection.get(ids=["id1", "id2"])
    assert (items["metadatas"])[0]["int_value"] == 1  # type: ignore[index]
    assert (items["metadatas"])[0]["float_value"] == 1.001  # type: ignore[index]
    assert (items["metadatas"])[1]["int_value"] == 2  # type: ignore[index]
    assert isinstance((items["metadatas"])[0]["int_value"], int)  # type: ignore[index]
    assert isinstance((items["metadatas"])[0]["float_value"], float)  # type: ignore[index]


def test_metadata_add_query_int_float(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)  # type: ignore[arg-type]

    items: QueryResult = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]], n_results=1  # type: ignore[arg-type]
    )
    assert items["metadatas"] is not None
    assert items["metadatas"][0][0]["int_value"] == 1
    assert items["metadatas"][0][0]["float_value"] == 1.001
    assert isinstance(items["metadatas"][0][0]["int_value"], int)
    assert isinstance(items["metadatas"][0][0]["float_value"], float)
