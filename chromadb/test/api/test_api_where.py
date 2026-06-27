# type: ignore
import pytest

operator_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}

logical_operator_records = {
    "embeddings": [
        [1.1, 2.3, 3.2],
        [1.2, 2.24, 3.2],
        [1.3, 2.25, 3.2],
        [1.4, 2.26, 3.2],
    ],
    "ids": ["id1", "id2", "id3", "id4"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001, "is": "doc"},
        {"int_value": 2, "float_value": 2.002, "string_value": "two", "is": "doc"},
        {"int_value": 3, "float_value": 3.003, "string_value": "three", "is": "doc"},
        {"int_value": 4, "float_value": 4.004, "string_value": "four", "is": "doc"},
    ],
    "documents": [
        "this document is first and great",
        "this document is second and great",
        "this document is third and great",
        "this document is fourth and great",
    ],
}


def test_where_lt(client):
    client.reset()
    collection = client.create_collection("test_where_lt")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$lt": 2}})
    assert len(items["metadatas"]) == 1


def test_where_lte(client):
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$lte": 2.0}})
    assert len(items["metadatas"]) == 2


def test_where_gt(client):
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"float_value": {"$gt": -1.4}})
    assert len(items["metadatas"]) == 2


def test_where_gte(client):
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"float_value": {"$gte": 2.002}})
    assert len(items["metadatas"]) == 1


def test_where_ne_string(client):
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"string_value": {"$ne": "two"}})
    assert len(items["metadatas"]) == 1


def test_where_ne_eq_number(client):
    client.reset()
    collection = client.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$ne": 1}})
    assert len(items["metadatas"]) == 1
    items = collection.get(where={"float_value": {"$eq": 2.002}})
    assert len(items["metadatas"]) == 1


def test_where_valid_operators(client):
    client.reset()
    collection = client.create_collection("test_where_valid_operators")
    collection.add(**operator_records)
    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$invalid": 2}})
    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$lt": "2"}})
    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$lt": 2, "$gt": 1}})
    with pytest.raises(ValueError):
        collection.get(where={"$and": {"int_value": {"$lt": 2}}})
    with pytest.raises(ValueError):
        collection.get(
            where={"int_value": {"$lt": 2}, "$or": {"int_value": {"$gt": 1}}}
        )
    with pytest.raises(ValueError):
        collection.get(
            where={"$gt": [{"int_value": {"$lt": 2}}, {"int_value": {"$gt": 1}}]}
        )
    with pytest.raises(ValueError):
        collection.get(where={"$or": [{"int_value": {"$lt": 2}}]})
    with pytest.raises(ValueError):
        collection.get(where={"$or": []})
    with pytest.raises(ValueError):
        collection.get(where={"$contains": "test"})


def test_where_logical_operators(client):
    client.reset()
    collection = client.create_collection("test_logical_operators")
    collection.add(**logical_operator_records)
    items = collection.get(
        where={
            "$and": [
                {"$or": [{"int_value": {"$gte": 3}}, {"float_value": {"$lt": 1.9}}]},
                {"is": "doc"},
            ]
        }
    )
    assert len(items["metadatas"]) == 3
    items = collection.get(
        where={
            "$or": [
                {
                    "$and": [
                        {"int_value": {"$eq": 3}},
                        {"string_value": {"$eq": "three"}},
                    ]
                },
                {
                    "$and": [
                        {"int_value": {"$eq": 4}},
                        {"string_value": {"$eq": "four"}},
                    ]
                },
            ]
        }
    )
    assert len(items["metadatas"]) == 2
    items = collection.get(
        where={
            "$and": [
                {
                    "$or": [
                        {"int_value": {"$eq": 1}},
                        {"string_value": {"$eq": "two"}},
                    ]
                },
                {
                    "$or": [
                        {"int_value": {"$eq": 2}},
                        {"string_value": {"$eq": "one"}},
                    ]
                },
            ]
        }
    )
    assert len(items["metadatas"]) == 2


def test_where_document_logical_operators(client):
    client.reset()
    collection = client.create_collection("test_document_logical_operators")
    collection.add(**logical_operator_records)
    items = collection.get(
        where_document={
            "$and": [
                {"$contains": "first"},
                {"$contains": "doc"},
            ]
        }
    )
    assert len(items["metadatas"]) == 1
    items = collection.get(
        where_document={
            "$or": [
                {"$contains": "first"},
                {"$contains": "second"},
            ]
        }
    )
    assert len(items["metadatas"]) == 2
    items = collection.get(
        where_document={
            "$or": [
                {"$contains": "first"},
                {"$contains": "second"},
            ]
        },
        where={
            "int_value": {"$ne": 2},
        },
    )
    assert len(items["metadatas"]) == 1


def test_where_validation_get(client):
    client.reset()
    collection = client.create_collection("test_where_validation")
    with pytest.raises(ValueError, match="where"):
        collection.get(where={"value": {"nested": "5"}})


def test_where_validation_query(client):
    client.reset()
    collection = client.create_collection("test_where_validation")
    with pytest.raises(ValueError, match="where"):
        collection.query(query_embeddings=[0, 0, 0], where={"value": {"nested": "5"}})


def test_where_contains_validation():
    from chromadb.api.types import validate_where

    validate_where({"tags": {"$contains": "action"}})
    validate_where({"scores": {"$contains": 42}})
    validate_where({"ratings": {"$contains": 4.5}})
    validate_where({"flags": {"$contains": True}})
    validate_where({"tags": {"$not_contains": "draft"}})
    validate_where({"scores": {"$not_contains": 0}})
    with pytest.raises(ValueError):
        validate_where({"tags": {"$contains": ["a", "b"]}})
    with pytest.raises(ValueError):
        validate_where({"tags": {"$contains": {"nested": True}}})
    validate_where(
        {"$and": [{"tags": {"$contains": "action"}}, {"year": {"$gt": 2020}}]}
    )
