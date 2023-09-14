# type: ignore

import chromadb
from chromadb.api.types import QueryResult
from chromadb.config import Settings
import chromadb.server.fastapi
import pytest
import tempfile
import numpy as np
import os
import shutil
from datetime import datetime, timedelta
from chromadb.utils.embedding_functions import (
    DefaultEmbeddingFunction,
)

persist_dir = tempfile.mkdtemp()


@pytest.fixture
def local_persist_api():
    yield chromadb.Client(
        Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            allow_reset=True,
            is_persistent=True,
            persist_directory=persist_dir,
        ),
    )
    if os.path.exists(persist_dir):
        shutil.rmtree(persist_dir, ignore_errors=True)


# https://docs.pytest.org/en/6.2.x/fixture.html#fixtures-can-be-requested-more-than-once-per-test-return-values-are-cached
@pytest.fixture
def local_persist_api_cache_bust():
    yield chromadb.Client(
        Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            allow_reset=True,
            is_persistent=True,
            persist_directory=persist_dir,
        ),
    )
    if os.path.exists(persist_dir):
        shutil.rmtree(persist_dir, ignore_errors=True)


def approx_equal(a, b, tolerance=1e-6) -> bool:
    return abs(a - b) < tolerance


def vector_approx_equal(a, b, tolerance: float = 1e-6) -> bool:
    if len(a) != len(b):
        return False
    return all([approx_equal(a, b, tolerance) for a, b in zip(a, b)])


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist_index_loading(api_fixture, request):
    api = request.getfixturevalue("local_persist_api")
    api.reset()
    collection = api.create_collection("test")
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection("test")

    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        assert len(nn[key]) == 1


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist_index_loading_embedding_function(api_fixture, request):
    embedding_function = lambda x: [[1, 2, 3] for _ in range(len(x))]  # noqa E731
    api = request.getfixturevalue("local_persist_api")
    api.reset()
    collection = api.create_collection("test", embedding_function=embedding_function)
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection("test", embedding_function=embedding_function)

    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        assert len(nn[key]) == 1


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist_index_get_or_create_embedding_function(api_fixture, request):
    embedding_function = lambda x: [[1, 2, 3] for _ in range(len(x))]  # noqa E731
    api = request.getfixturevalue("local_persist_api")
    api.reset()
    collection = api.get_or_create_collection(
        "test", embedding_function=embedding_function
    )
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_or_create_collection(
        "test", embedding_function=embedding_function
    )

    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )

    for key in nn.keys():
        assert len(nn[key]) == 1

    assert nn["ids"] == [["id1"]]
    assert nn["embeddings"] == [[[1, 2, 3]]]
    assert nn["documents"] == [["hello"]]
    assert nn["distances"] == [[0]]


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()

    collection = api.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2

    api = request.getfixturevalue(api_fixture.__name__)
    collection = api.get_collection("testspace")
    assert collection.count() == 2

    api.delete_collection("testspace")

    api = request.getfixturevalue(api_fixture.__name__)
    assert api.list_collections() == []


def test_heartbeat(api):
    heartbeat_ns = api.heartbeat()
    assert isinstance(heartbeat_ns, int)

    heartbeat_s = heartbeat_ns // 10**9
    heartbeat = datetime.fromtimestamp(heartbeat_s)
    assert heartbeat > datetime.now() - timedelta(seconds=10)


batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


def test_add(api):
    api.reset()

    collection = api.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2


def test_get_or_create(api):
    api.reset()

    collection = api.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2

    with pytest.raises(Exception):
        collection = api.create_collection("testspace")

    collection = api.get_or_create_collection("testspace")

    assert collection.count() == 2


minimal_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


def test_add_minimal(api):
    api.reset()

    collection = api.create_collection("testspace")

    collection.add(**minimal_records)

    assert collection.count() == 2


def test_get_from_db(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    records = collection.get(include=["embeddings", "documents", "metadatas"])
    for key in records.keys():
        assert len(records[key]) == 2


def test_reset_db(api):
    api.reset()

    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    api.reset()
    assert len(api.list_collections()) == 0


def test_get_nearest_neighbors(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)

    nn = collection.query(
        query_embeddings=[1.1, 2.3, 3.2],
        n_results=1,
        where={},
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        assert len(nn[key]) == 1

    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=1,
        where={},
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        assert len(nn[key]) == 1

    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2], [0.1, 2.3, 4.5]],
        n_results=1,
        where={},
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        assert len(nn[key]) == 2


def test_delete(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    with pytest.raises(Exception):
        collection.delete()


def test_delete_with_index(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1)


def test_count(api):
    api.reset()
    collection = api.create_collection("testspace")
    assert collection.count() == 0
    collection.add(**batch_records)
    assert collection.count() == 2


def test_modify(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.modify(name="testspace2")

    # collection name is modify
    assert collection.name == "testspace2"


def test_modify_error_on_existing_name(api):
    api.reset()

    api.create_collection("testspace")
    c2 = api.create_collection("testspace2")

    with pytest.raises(Exception):
        c2.modify(name="testspace")


def test_metadata_cru(api):
    api.reset()
    metadata_a = {"a": 1, "b": 2}
    # Test create metatdata
    collection = api.create_collection("testspace", metadata=metadata_a)
    assert collection.metadata is not None
    assert collection.metadata["a"] == 1
    assert collection.metadata["b"] == 2

    # Test get metatdata
    collection = api.get_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 1
    assert collection.metadata["b"] == 2

    # Test modify metatdata
    collection.modify(metadata={"a": 2, "c": 3})
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3
    assert "b" not in collection.metadata

    # Test get after modify metatdata
    collection = api.get_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3
    assert "b" not in collection.metadata

    # Test name exists get_or_create_metadata
    collection = api.get_or_create_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3

    # Test name exists create metadata
    collection = api.get_or_create_collection("testspace2")
    assert collection.metadata is None

    # Test list collections
    collections = api.list_collections()
    for collection in collections:
        if collection.name == "testspace":
            assert collection.metadata is not None
            assert collection.metadata["a"] == 2
            assert collection.metadata["c"] == 3
        elif collection.name == "testspace2":
            assert collection.metadata is None


def test_increment_index_on(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    # increment index
    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        assert len(nn[key]) == 1


def test_add_a_collection(api):
    api.reset()
    api.create_collection("testspace")

    # get collection does not throw an error
    collection = api.get_collection("testspace")
    assert collection.name == "testspace"

    # get collection should throw an error if collection does not exist
    with pytest.raises(Exception):
        collection = api.get_collection("testspace2")


def test_list_collections(api):
    api.reset()
    api.create_collection("testspace")
    api.create_collection("testspace2")

    # get collection does not throw an error
    collections = api.list_collections()
    assert len(collections) == 2


def test_reset(api):
    api.reset()
    api.create_collection("testspace")
    api.create_collection("testspace2")

    # get collection does not throw an error
    collections = api.list_collections()
    assert len(collections) == 2

    api.reset()
    collections = api.list_collections()
    assert len(collections) == 0


def test_peek(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    # peek
    peek = collection.peek()
    for key in peek.keys():
        assert len(peek[key]) == 2


# TEST METADATA AND METADATA FILTERING
# region

metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
}


def test_metadata_add_get_int_float(api):
    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(ids=["id1", "id2"])
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["float_value"] == 1.001
    assert items["metadatas"][1]["int_value"] == 2
    assert type(items["metadatas"][0]["int_value"]) == int
    assert type(items["metadatas"][0]["float_value"]) == float


def test_metadata_add_query_int_float(api):
    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items: QueryResult = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]], n_results=1
    )
    assert items["metadatas"] is not None
    assert items["metadatas"][0][0]["int_value"] == 1
    assert items["metadatas"][0][0]["float_value"] == 1.001
    assert type(items["metadatas"][0][0]["int_value"]) == int
    assert type(items["metadatas"][0][0]["float_value"]) == float


def test_metadata_get_where_string(api):
    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"string_value": "one"})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"


def test_metadata_get_where_int(api):
    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"int_value": 1})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"


def test_metadata_get_where_float(api):
    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"float_value": 1.001})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"
    assert items["metadatas"][0]["float_value"] == 1.001


def test_metadata_update_get_int_float(api):
    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    collection.update(
        ids=["id1"],
        metadatas=[{"int_value": 2, "string_value": "two", "float_value": 2.002}],
    )
    items = collection.get(ids=["id1"])
    assert items["metadatas"][0]["int_value"] == 2
    assert items["metadatas"][0]["string_value"] == "two"
    assert items["metadatas"][0]["float_value"] == 2.002


bad_metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [{"value": {"nested": "5"}}, {"value": [1, 2, 3]}],
}


def test_metadata_validation_add(api):
    api.reset()
    collection = api.create_collection("test_metadata_validation")
    with pytest.raises(ValueError, match="metadata"):
        collection.add(**bad_metadata_records)


def test_metadata_validation_update(api):
    api.reset()
    collection = api.create_collection("test_metadata_validation")
    collection.add(**metadata_records)
    with pytest.raises(ValueError, match="metadata"):
        collection.update(ids=["id1"], metadatas={"value": {"nested": "5"}})


def test_where_validation_get(api):
    api.reset()
    collection = api.create_collection("test_where_validation")
    with pytest.raises(ValueError, match="where"):
        collection.get(where={"value": {"nested": "5"}})


def test_where_validation_query(api):
    api.reset()
    collection = api.create_collection("test_where_validation")
    with pytest.raises(ValueError, match="where"):
        collection.query(query_embeddings=[0, 0, 0], where={"value": {"nested": "5"}})


operator_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}


def test_where_lt(api):
    api.reset()
    collection = api.create_collection("test_where_lt")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$lt": 2}})
    assert len(items["metadatas"]) == 1


def test_where_lte(api):
    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$lte": 2.0}})
    assert len(items["metadatas"]) == 2


def test_where_gt(api):
    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"float_value": {"$gt": -1.4}})
    assert len(items["metadatas"]) == 2


def test_where_gte(api):
    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"float_value": {"$gte": 2.002}})
    assert len(items["metadatas"]) == 1


def test_where_ne_string(api):
    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"string_value": {"$ne": "two"}})
    assert len(items["metadatas"]) == 1


def test_where_ne_eq_number(api):
    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$ne": 1}})
    assert len(items["metadatas"]) == 1
    items = collection.get(where={"float_value": {"$eq": 2.002}})
    assert len(items["metadatas"]) == 1


def test_where_valid_operators(api):
    api.reset()
    collection = api.create_collection("test_where_valid_operators")
    collection.add(**operator_records)
    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$invalid": 2}})

    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$lt": "2"}})

    with pytest.raises(ValueError):
        collection.get(where={"int_value": {"$lt": 2, "$gt": 1}})

    # Test invalid $and, $or
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
        collection.get(where={"a": {"$contains": "test"}})

    with pytest.raises(ValueError):
        collection.get(
            where={
                "$or": [
                    {"a": {"$contains": "first"}},  # invalid
                    {"$contains": "second"},  # valid
                ]
            }
        )


# TODO: Define the dimensionality of these embeddingds in terms of the default record
bad_dimensionality_records = {
    "embeddings": [[1.1, 2.3, 3.2, 4.5], [1.2, 2.24, 3.2, 4.5]],
    "ids": ["id1", "id2"],
}

bad_dimensionality_query = {
    "query_embeddings": [[1.1, 2.3, 3.2, 4.5], [1.2, 2.24, 3.2, 4.5]],
}

bad_number_of_results_query = {
    "query_embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "n_results": 100,
}


def test_dimensionality_validation_add(api):
    api.reset()
    collection = api.create_collection("test_dimensionality_validation")
    collection.add(**minimal_records)

    with pytest.raises(Exception) as e:
        collection.add(**bad_dimensionality_records)
    assert "dimensionality" in str(e.value)


def test_dimensionality_validation_query(api):
    api.reset()
    collection = api.create_collection("test_dimensionality_validation_query")
    collection.add(**minimal_records)

    with pytest.raises(Exception) as e:
        collection.query(**bad_dimensionality_query)
    assert "dimensionality" in str(e.value)


def test_query_document_valid_operators(api):
    api.reset()
    collection = api.create_collection("test_where_valid_operators")
    collection.add(**operator_records)
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$lt": {"$nested": 2}})

    with pytest.raises(ValueError, match="where document"):
        collection.query(query_embeddings=[0, 0, 0], where_document={"$contains": 2})

    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$contains": []})

    # Test invalid $and, $or
    with pytest.raises(ValueError):
        collection.get(where_document={"$and": {"$unsupported": "doc"}})

    with pytest.raises(ValueError):
        collection.get(
            where_document={"$or": [{"$unsupported": "doc"}, {"$unsupported": "doc"}]}
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


contains_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "documents": ["this is doc1 and it's great!", "doc2 is also great!"],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}


def test_get_where_document(api):
    api.reset()
    collection = api.create_collection("test_get_where_document")
    collection.add(**contains_records)

    items = collection.get(where_document={"$contains": "doc1"})
    assert len(items["metadatas"]) == 1

    items = collection.get(where_document={"$contains": "great"})
    assert len(items["metadatas"]) == 2

    items = collection.get(where_document={"$contains": "bad"})
    assert len(items["metadatas"]) == 0


def test_query_where_document(api):
    api.reset()
    collection = api.create_collection("test_query_where_document")
    collection.add(**contains_records)

    items = collection.query(
        query_embeddings=[1, 0, 0], where_document={"$contains": "doc1"}, n_results=1
    )
    assert len(items["metadatas"][0]) == 1

    items = collection.query(
        query_embeddings=[0, 0, 0], where_document={"$contains": "great"}, n_results=2
    )
    assert len(items["metadatas"][0]) == 2

    with pytest.raises(Exception) as e:
        items = collection.query(
            query_embeddings=[0, 0, 0], where_document={"$contains": "bad"}, n_results=1
        )
        assert "datapoints" in str(e.value)


def test_delete_where_document(api):
    api.reset()
    collection = api.create_collection("test_delete_where_document")
    collection.add(**contains_records)

    collection.delete(where_document={"$contains": "doc1"})
    assert collection.count() == 1

    collection.delete(where_document={"$contains": "bad"})
    assert collection.count() == 1

    collection.delete(where_document={"$contains": "great"})
    assert collection.count() == 0


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


def test_where_logical_operators(api):
    api.reset()
    collection = api.create_collection("test_logical_operators")
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


def test_where_document_logical_operators(api):
    api.reset()
    collection = api.create_collection("test_document_logical_operators")
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


# endregion

records = {
    "embeddings": [[0, 0, 0], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
    "documents": ["this document is first", "this document is second"],
}


def test_query_include(api):
    api.reset()
    collection = api.create_collection("test_query_include")
    collection.add(**records)

    items = collection.query(
        query_embeddings=[0, 0, 0],
        include=["metadatas", "documents", "distances"],
        n_results=1,
    )
    assert items["embeddings"] is None
    assert items["ids"][0][0] == "id1"
    assert items["metadatas"][0][0]["int_value"] == 1

    items = collection.query(
        query_embeddings=[0, 0, 0],
        include=["embeddings", "documents", "distances"],
        n_results=1,
    )
    assert items["metadatas"] is None
    assert items["ids"][0][0] == "id1"

    items = collection.query(
        query_embeddings=[[0, 0, 0], [1, 2, 1.2]],
        include=[],
        n_results=2,
    )
    assert items["documents"] is None
    assert items["metadatas"] is None
    assert items["embeddings"] is None
    assert items["distances"] is None
    assert items["ids"][0][0] == "id1"
    assert items["ids"][0][1] == "id2"


def test_get_include(api):
    api.reset()
    collection = api.create_collection("test_get_include")
    collection.add(**records)

    items = collection.get(include=["metadatas", "documents"], where={"int_value": 1})
    assert items["embeddings"] is None
    assert items["ids"][0] == "id1"
    assert items["metadatas"][0]["int_value"] == 1
    assert items["documents"][0] == "this document is first"

    items = collection.get(include=["embeddings", "documents"])
    assert items["metadatas"] is None
    assert items["ids"][0] == "id1"
    assert approx_equal(items["embeddings"][1][0], 1.2)

    items = collection.get(include=[])
    assert items["documents"] is None
    assert items["metadatas"] is None
    assert items["embeddings"] is None
    assert items["ids"][0] == "id1"

    with pytest.raises(ValueError, match="include"):
        items = collection.get(include=["metadatas", "undefined"])

    with pytest.raises(ValueError, match="include"):
        items = collection.get(include=None)


# make sure query results are returned in the right order


def test_query_order(api):
    api.reset()
    collection = api.create_collection("test_query_order")
    collection.add(**records)

    items = collection.query(
        query_embeddings=[1.2, 2.24, 3.2],
        include=["metadatas", "documents", "distances"],
        n_results=2,
    )

    assert items["documents"][0][0] == "this document is second"
    assert items["documents"][0][1] == "this document is first"


# test to make sure add, get, delete error on invalid id input


def test_invalid_id(api):
    api.reset()
    collection = api.create_collection("test_invalid_id")
    # Add with non-string id
    with pytest.raises(ValueError) as e:
        collection.add(embeddings=[0, 0, 0], ids=[1], metadatas=[{}])
    assert "ID" in str(e.value)

    # Get with non-list id
    with pytest.raises(ValueError) as e:
        collection.get(ids=1)
    assert "ID" in str(e.value)

    # Delete with malformed ids
    with pytest.raises(ValueError) as e:
        collection.delete(ids=["valid", 0])
    assert "ID" in str(e.value)


def test_index_params(api):
    EPS = 1e-12
    # first standard add
    api.reset()
    collection = api.create_collection(name="test_index_params")
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] > 4

    # cosine
    api.reset()
    collection = api.create_collection(
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

    # ip
    api.reset()
    collection = api.create_collection(
        name="test_index_params", metadata={"hnsw:space": "ip"}
    )
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] < -5


def test_invalid_index_params(api):
    api.reset()

    with pytest.raises(Exception):
        collection = api.create_collection(
            name="test_index_params", metadata={"hnsw:foobar": "blarg"}
        )
        collection.add(**records)

    with pytest.raises(Exception):
        collection = api.create_collection(
            name="test_index_params", metadata={"hnsw:space": "foobar"}
        )
        collection.add(**records)


def test_persist_index_loading_params(api, request):
    api = request.getfixturevalue("local_persist_api")
    api.reset()
    collection = api.create_collection(
        "test",
        metadata={"hnsw:space": "ip"},
    )
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection(
        "test",
    )

    assert collection.metadata["hnsw:space"] == "ip"

    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        assert len(nn[key]) == 1


def test_add_large(api):
    api.reset()

    collection = api.create_collection("testspace")

    # Test adding a large number of records
    large_records = np.random.rand(2000, 512).astype(np.float32).tolist()

    collection.add(
        embeddings=large_records,
        ids=[f"http://example.com/{i}" for i in range(len(large_records))],
    )

    assert collection.count() == len(large_records)


# test get_version
def test_get_version(api):
    api.reset()
    version = api.get_version()

    # assert version matches the pattern x.y.z
    import re

    assert re.match(r"\d+\.\d+\.\d+", version)


# test delete_collection
def test_delete_collection(api):
    api.reset()
    collection = api.create_collection("test_delete_collection")
    collection.add(**records)

    assert len(api.list_collections()) == 1
    api.delete_collection("test_delete_collection")
    assert len(api.list_collections()) == 0


# test default embedding function
def test_default_embedding():
    embedding_function = DefaultEmbeddingFunction()
    docs = ["this is a test" for _ in range(64)]
    embeddings = embedding_function(docs)
    assert len(embeddings) == 64


def test_multiple_collections(api):
    embeddings1 = np.random.rand(10, 512).astype(np.float32).tolist()
    embeddings2 = np.random.rand(10, 512).astype(np.float32).tolist()
    ids1 = [f"http://example.com/1/{i}" for i in range(len(embeddings1))]
    ids2 = [f"http://example.com/2/{i}" for i in range(len(embeddings2))]

    api.reset()
    coll1 = api.create_collection("coll1")
    coll1.add(embeddings=embeddings1, ids=ids1)

    coll2 = api.create_collection("coll2")
    coll2.add(embeddings=embeddings2, ids=ids2)

    assert len(api.list_collections()) == 2
    assert coll1.count() == len(embeddings1)
    assert coll2.count() == len(embeddings2)

    results1 = coll1.query(query_embeddings=embeddings1[0], n_results=1)
    results2 = coll2.query(query_embeddings=embeddings2[0], n_results=1)

    assert results1["ids"][0][0] == ids1[0]
    assert results2["ids"][0][0] == ids2[0]


def test_update_query(api):
    api.reset()
    collection = api.create_collection("test_update_query")
    collection.add(**records)

    updated_records = {
        "ids": [records["ids"][0]],
        "embeddings": [[0.1, 0.2, 0.3]],
        "documents": ["updated document"],
        "metadatas": [{"foo": "bar"}],
    }

    collection.update(**updated_records)

    # test query
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


def test_get_nearest_neighbors_where_n_results_more_than_element(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**records)

    results1 = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=5,
        where={},
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in results1.keys():
        assert len(results1[key][0]) == 2


def test_invalid_n_results_param(api):
    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**records)
    with pytest.raises(TypeError) as exc:
        collection.query(
            query_embeddings=[[1.1, 2.3, 3.2]],
            n_results=-1,
            where={},
            include=["embeddings", "documents", "metadatas", "distances"],
        )
    assert "Number of requested results -1, cannot be negative, or zero." in str(
        exc.value
    )
    assert exc.type == TypeError

    with pytest.raises(ValueError) as exc:
        collection.query(
            query_embeddings=[[1.1, 2.3, 3.2]],
            n_results="one",
            where={},
            include=["embeddings", "documents", "metadatas", "distances"],
        )
    assert "int" in str(exc.value)
    assert exc.type == ValueError


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


def test_upsert(api):
    api.reset()
    collection = api.create_collection("test")

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


# test to make sure add, query, update, upsert error on invalid embeddings input


def test_invalid_embeddings(api):
    api.reset()
    collection = api.create_collection("test_invalid_embeddings")

    # Add with string embeddings
    invalid_records = {
        "embeddings": [["0", "0", "0"], ["1.2", "2.24", "3.2"]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.add(**invalid_records)
    assert "embedding" in str(e.value)

    # Query with invalid embeddings
    with pytest.raises(ValueError) as e:
        collection.query(
            query_embeddings=[["1.1", "2.3", "3.2"]],
            n_results=1,
        )
    assert "embedding" in str(e.value)

    # Update with invalid embeddings
    invalid_records = {
        "embeddings": [[[0], [0], [0]], [[1.2], [2.24], [3.2]]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.update(**invalid_records)
    assert "embedding" in str(e.value)

    # Upsert with invalid embeddings
    invalid_records = {
        "embeddings": [[[1.1, 2.3, 3.2]], [[1.2, 2.24, 3.2]]],
        "ids": ["id1", "id2"],
    }
    with pytest.raises(ValueError) as e:
        collection.upsert(**invalid_records)
    assert "embedding" in str(e.value)
