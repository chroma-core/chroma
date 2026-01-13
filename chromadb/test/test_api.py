# type: ignore
import os
import shutil
import sys
import tempfile
import traceback
from datetime import datetime, timedelta
from typing import Any

import httpx
import numpy as np
import pytest

import chromadb
import chromadb.server.fastapi
from chromadb.api.fastapi import FastAPI
from chromadb.api.types import (
    Document,
    EmbeddingFunction,
    QueryResult,
    TYPE_KEY,
    SPARSE_VECTOR_TYPE_VALUE,
)
from chromadb.config import Settings
from chromadb.errors import (
    ChromaError,
    NotFoundError,
    InvalidArgumentError,
)
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction


@pytest.fixture
def persist_dir():
    return tempfile.mkdtemp()


@pytest.fixture
def local_persist_api(persist_dir):
    client = chromadb.Client(
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
    yield client
    client.clear_system_cache()
    if os.path.exists(persist_dir):
        shutil.rmtree(persist_dir, ignore_errors=True)


# https://docs.pytest.org/en/6.2.x/fixture.html#fixtures-can-be-requested-more-than-once-per-test-return-values-are-cached
@pytest.fixture
def local_persist_api_cache_bust(persist_dir):
    client = chromadb.Client(
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
    yield client
    client.clear_system_cache()
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
    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection("test")
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection("test")

    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist_index_loading_embedding_function(api_fixture, request):
    class TestEF(EmbeddingFunction[Document]):
        def __call__(self, input):
            return [np.array([1, 2, 3]) for _ in range(len(input))]

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)

        def name(self) -> str:
            return "test"

        def build_from_config(self, config: dict[str, Any]) -> None:
            pass

        def get_config(self) -> dict[str, Any]:
            return {}

    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection("test", embedding_function=TestEF())
    collection.add(ids="id1", documents="hello")

    client2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = client2.get_collection("test", embedding_function=TestEF())

    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist_index_get_or_create_embedding_function(api_fixture, request):
    class TestEF(EmbeddingFunction[Document]):
        def __call__(self, input):
            return [np.array([1, 2, 3]) for _ in range(len(input))]

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)

        def name(self) -> str:
            return "test"

        def build_from_config(self, config: dict[str, Any]) -> None:
            pass

        def get_config(self) -> dict[str, Any]:
            return {}

    api = request.getfixturevalue("local_persist_api")
    api.reset()
    collection = api.get_or_create_collection("test", embedding_function=TestEF())
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_or_create_collection("test", embedding_function=TestEF())

    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=includes,
    )

    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None

    assert nn["ids"] == [["id1"]]
    assert nn["embeddings"][0][0].tolist() == [1, 2, 3]
    assert nn["documents"] == [["hello"]]
    assert nn["distances"] == [[0]]


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist(api_fixture, request):
    client = request.getfixturevalue(api_fixture.__name__)

    client.reset()

    collection = client.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2

    client = request.getfixturevalue(api_fixture.__name__)
    collection = client.get_collection("testspace")
    assert collection.count() == 2

    client.delete_collection("testspace")

    client = request.getfixturevalue(api_fixture.__name__)
    assert client.list_collections() == []


def test_heartbeat(client):
    heartbeat_ns = client.heartbeat()
    assert isinstance(heartbeat_ns, int)

    heartbeat_s = heartbeat_ns // 10**9
    heartbeat = datetime.fromtimestamp(heartbeat_s)
    assert heartbeat > datetime.now() - timedelta(seconds=10)


def test_max_batch_size(client):
    batch_size = client.get_max_batch_size()
    assert batch_size > 0


def test_supports_base64_encoding(client):
    if not isinstance(client, FastAPI):
        pytest.skip("Not a FastAPI instance")

    client.reset()

    supports_base64_encoding = client.supports_base64_encoding()
    assert supports_base64_encoding is True


def test_supports_base64_encoding_legacy(client):
    if not isinstance(client, FastAPI):
        pytest.skip("Not a FastAPI instance")

    client.reset()

    # legacy server does not give back supports_base64_encoding
    client.pre_flight_checks = {
        "max_batch_size": 100,
    }

    assert client.supports_base64_encoding() is False
    assert client.get_max_batch_size() == 100


def test_pre_flight_checks(client):
    if not isinstance(client, FastAPI):
        pytest.skip("Not a FastAPI instance")

    resp = httpx.get(f"{client._api_url}/pre-flight-checks")
    assert resp.status_code == 200
    assert resp.json() is not None
    assert "max_batch_size" in resp.json().keys()
    assert "supports_base64_encoding" in resp.json().keys()


batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


def test_add(client):
    client.reset()

    collection = client.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2


def test_collection_add_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.add(**batch_records)


def test_get_or_create(client):
    client.reset()

    collection = client.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2

    with pytest.raises(Exception):
        collection = client.create_collection("testspace")

    collection = client.get_or_create_collection("testspace")

    assert collection.count() == 2


minimal_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


def test_add_minimal(client):
    client.reset()

    collection = client.create_collection("testspace")

    collection.add(**minimal_records)

    assert collection.count() == 2


def test_get_from_db(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    includes = ["embeddings", "documents", "metadatas"]
    records = collection.get(include=includes)
    for key in records.keys():
        if (key in includes) or (key == "ids"):
            assert len(records[key]) == 2
        elif key == "included":
            assert set(records[key]) == set(includes)
        else:
            assert records[key] is None


def test_collection_get_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.get()


def test_reset_db(client):
    client.reset()

    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    client.reset()
    assert len(client.list_collections()) == 0


def test_get_nearest_neighbors(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)

    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_embeddings=[1.1, 2.3, 3.2],
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None

    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None

    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2], [0.1, 2.3, 4.5]],
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 2
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


def test_delete(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    with pytest.raises(Exception):
        collection.delete()


def test_delete_returns_none(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    assert collection.delete(ids=batch_records["ids"]) is None


def test_delete_with_index(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1)


def test_collection_delete_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.delete(ids=["id1"])


def test_count(client):
    client.reset()
    collection = client.create_collection("testspace")
    assert collection.count() == 0
    collection.add(**batch_records)
    assert collection.count() == 2


def test_collection_count_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.count()


def test_modify(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.modify(name="testspace2")

    # collection name is modify
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


def test_metadata_cru(client):
    client.reset()
    metadata_a = {"a": 1, "b": 2}
    # Test create metadata
    collection = client.create_collection("testspace", metadata=metadata_a)
    assert collection.metadata is not None
    assert collection.metadata["a"] == 1
    assert collection.metadata["b"] == 2

    # Test get metadata
    collection = client.get_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 1
    assert collection.metadata["b"] == 2

    # Test modify metadata
    collection.modify(metadata={"a": 2, "c": 3})
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3
    assert "b" not in collection.metadata

    # Test get after modify metadata
    collection = client.get_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3
    assert "b" not in collection.metadata

    # Test name exists get_or_create_metadata
    collection = client.get_or_create_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3

    # Test name exists create metadata
    collection = client.get_or_create_collection("testspace2")
    assert collection.metadata is None

    # Test list collections
    collections = client.list_collections()
    for collection in collections:
        if collection.name == "testspace":
            assert collection.metadata is not None
            assert collection.metadata["a"] == 2
            assert collection.metadata["c"] == 3
        elif collection.name == "testspace2":
            assert collection.metadata is None


def test_increment_index_on(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    includes = ["embeddings", "documents", "metadatas", "distances"]
    # increment index
    nn = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


def test_add_a_collection(client):
    client.reset()
    client.create_collection("testspace")

    # get collection does not throw an error
    collection = client.get_collection("testspace")
    assert collection.name == "testspace"

    # get collection should throw an error if collection does not exist
    with pytest.raises(Exception):
        collection = client.get_collection("testspace2")


def test_error_includes_trace_id(http_client):
    http_client.reset()

    with pytest.raises(ChromaError) as error:
        http_client.get_collection("testspace2")

    assert error.value.trace_id is not None


def test_list_collections(client):
    client.reset()
    client.create_collection("testspace")
    client.create_collection("testspace2")

    # get collection does not throw an error
    collections = client.list_collections()
    assert len(collections) == 2


def test_reset(client):
    client.reset()
    client.create_collection("testspace")
    client.create_collection("testspace2")

    # get collection does not throw an error
    collections = client.list_collections()
    assert len(collections) == 2

    client.reset()
    collections = client.list_collections()
    assert len(collections) == 0


def test_peek(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    # peek
    peek = collection.peek()
    print(peek)
    for key in peek.keys():
        if key in ["embeddings", "documents", "metadatas"] or key == "ids":
            assert len(peek[key]) == 2
        elif key == "included":
            assert set(peek[key]) == set(["embeddings", "metadatas", "documents"])
        else:
            assert peek[key] is None


def test_collection_peek_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.peek()


def test_collection_query_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.query(query_texts=["test"])


def test_collection_update_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.update(ids=["id1"], documents=["test"])


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


def test_metadata_add_get_int_float(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(ids=["id1", "id2"])
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["float_value"] == 1.001
    assert items["metadatas"][1]["int_value"] == 2
    assert isinstance(items["metadatas"][0]["int_value"], int)
    assert isinstance(items["metadatas"][0]["float_value"], float)


def test_metadata_add_query_int_float(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)

    items: QueryResult = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]], n_results=1
    )
    assert items["metadatas"] is not None
    assert items["metadatas"][0][0]["int_value"] == 1
    assert items["metadatas"][0][0]["float_value"] == 1.001
    assert isinstance(items["metadatas"][0][0]["int_value"], int)
    assert isinstance(items["metadatas"][0][0]["float_value"], float)


def test_metadata_get_where_string(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"string_value": "one"})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"


def test_metadata_get_where_int(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"int_value": 1})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"


def test_metadata_get_where_float(client):
    client.reset()
    collection = client.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"float_value": 1.001})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"
    assert items["metadatas"][0]["float_value"] == 1.001


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


bad_metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [{"value": {"nested": "5"}}, {"value": [1, 2, 3]}],
}


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


operator_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
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


def test_query_document_valid_operators(client):
    client.reset()
    collection = client.create_collection("test_where_valid_operators")
    collection.add(**operator_records)
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$lt": {"$nested": 2}})

    with pytest.raises(ValueError, match="where document"):
        collection.query(query_embeddings=[0, 0, 0], where_document={"$contains": 2})

    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$contains": []})

    # Test invalid $contains
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$contains": {"text": "hello"}})

    # Test invalid $not_contains
    with pytest.raises(ValueError, match="where document"):
        collection.get(where_document={"$not_contains": {"text": "hello"}})

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


def test_get_where_document(client):
    client.reset()
    collection = client.create_collection("test_get_where_document")
    collection.add(**contains_records)

    items = collection.get(where_document={"$contains": "doc1"})
    assert len(items["metadatas"]) == 1

    items = collection.get(where_document={"$contains": "great"})
    assert len(items["metadatas"]) == 2

    items = collection.get(where_document={"$contains": "bad"})
    assert len(items["metadatas"]) == 0


def test_query_where_document(client):
    client.reset()
    collection = client.create_collection("test_query_where_document")
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


def test_query_include(client):
    client.reset()
    collection = client.create_collection("test_query_include")
    collection.add(**records)

    include = ["metadatas", "documents", "distances"]
    items = collection.query(
        query_embeddings=[0, 0, 0],
        include=include,
        n_results=1,
    )
    assert items["embeddings"] is None
    assert items["ids"][0][0] == "id1"
    assert items["metadatas"][0][0]["int_value"] == 1
    assert set(items["included"]) == set(include)

    include = ["embeddings", "documents", "distances"]
    items = collection.query(
        query_embeddings=[0, 0, 0],
        include=include,
        n_results=1,
    )
    assert items["metadatas"] is None
    assert items["ids"][0][0] == "id1"
    assert set(items["included"]) == set(include)

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


def test_get_include(client):
    client.reset()
    collection = client.create_collection("test_get_include")
    collection.add(**records)

    include = ["metadatas", "documents"]
    items = collection.get(include=include, where={"int_value": 1})
    assert items["embeddings"] is None
    assert items["ids"][0] == "id1"
    assert items["metadatas"][0]["int_value"] == 1
    assert items["documents"][0] == "this document is first"
    assert set(items["included"]) == set(include)

    include = ["embeddings", "documents"]
    items = collection.get(include=include)
    assert items["metadatas"] is None
    assert items["ids"][0] == "id1"
    assert approx_equal(items["embeddings"][1][0], 1.2)
    assert set(items["included"]) == set(include)

    items = collection.get(include=[])
    assert items["documents"] is None
    assert items["metadatas"] is None
    assert items["embeddings"] is None
    assert items["ids"][0] == "id1"
    assert items["included"] == []

    with pytest.raises(ValueError, match="include"):
        items = collection.get(include=["metadatas", "undefined"])

    with pytest.raises(ValueError, match="include"):
        items = collection.get(include=None)


# make sure query results are returned in the right order


def test_query_order(client):
    client.reset()
    collection = client.create_collection("test_query_order")
    collection.add(**records)

    items = collection.query(
        query_embeddings=[1.2, 2.24, 3.2],
        include=["metadatas", "documents", "distances"],
        n_results=2,
    )

    assert items["documents"][0][0] == "this document is second"
    assert items["documents"][0][1] == "this document is first"


# test to make sure add, get, delete error on invalid id input


def test_invalid_id(client):
    client.reset()
    collection = client.create_collection("test_invalid_id")
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


def test_index_params(client):
    EPS = 1e-12
    # first standard add
    client.reset()
    collection = client.create_collection(name="test_index_params")
    collection.add(**records)
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert items["distances"][0][0] > 4

    # cosine
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

    # ip
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


def test_persist_index_loading_params(client, request):
    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection(
        "test",
        metadata={"hnsw:space": "ip"},
    )
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection(
        "test",
    )

    assert collection.metadata["hnsw:space"] == "ip"
    includes = ["embeddings", "documents", "metadatas", "distances"]
    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


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


# test get_version
def test_get_version(client):
    client.reset()
    version = client.get_version()

    # assert version matches the pattern x.y.z
    import re

    assert re.match(r"\d+\.\d+\.\d+", version)


# test delete_collection
def test_delete_collection(client):
    client.reset()
    collection = client.create_collection("test_delete_collection")
    collection.add(**records)

    assert len(client.list_collections()) == 1
    client.delete_collection("test_delete_collection")
    assert len(client.list_collections()) == 0


# test default embedding function
def test_default_embedding():
    embedding_function = DefaultEmbeddingFunction()
    docs = ["this is a test" for _ in range(64)]
    embeddings = embedding_function(docs)
    assert len(embeddings) == 64


def test_multiple_collections(client):
    embeddings1 = np.random.rand(10, 512).astype(np.float32).tolist()
    embeddings2 = np.random.rand(10, 512).astype(np.float32).tolist()
    ids1 = [f"http://example.com/1/{i}" for i in range(len(embeddings1))]
    ids2 = [f"http://example.com/2/{i}" for i in range(len(embeddings2))]

    client.reset()
    coll1 = client.create_collection("coll1")
    coll1.add(embeddings=embeddings1, ids=ids1)

    coll2 = client.create_collection("coll2")
    coll2.add(embeddings=embeddings2, ids=ids2)

    assert len(client.list_collections()) == 2
    assert coll1.count() == len(embeddings1)
    assert coll2.count() == len(embeddings2)

    results1 = coll1.query(query_embeddings=embeddings1[0], n_results=1)
    results2 = coll2.query(query_embeddings=embeddings2[0], n_results=1)

    # progressively check the results are what we expect so we can debug when/if flakes happen
    assert len(results1["ids"]) > 0
    assert len(results2["ids"]) > 0
    assert len(results1["ids"][0]) > 0
    assert len(results2["ids"][0]) > 0

    assert results1["ids"][0][0] == ids1[0]
    assert results2["ids"][0][0] == ids2[0]


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


def test_get_nearest_neighbors_where_n_results_more_than_element(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**records)

    includes = ["embeddings", "documents", "metadatas", "distances"]
    results = collection.query(
        query_embeddings=[[1.1, 2.3, 3.2]],
        n_results=5,
        include=includes,
    )
    for key in results.keys():
        if key in includes or key == "ids":
            assert len(results[key][0]) == 2
        elif key == "included":
            assert set(results[key]) == set(includes)
        else:
            assert results[key] is None


def test_invalid_n_results_param(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**records)
    with pytest.raises(TypeError) as exc:
        collection.query(
            query_embeddings=[[1.1, 2.3, 3.2]],
            n_results=-1,
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


def test_upsert(client):
    client.reset()
    collection = client.create_collection("test")

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


def test_collection_upsert_with_invalid_collection_throws(client):
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(NotFoundError, match=r"Collection .* does not exist"):
        collection.upsert(**initial_records)


# test to make sure add, query, update, upsert error on invalid embeddings input


def test_invalid_embeddings(client):
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


# test to make sure update shows exception for bad dimensionality


def test_dimensionality_exception_update(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_update_exception")
    collection.add(**minimal_records)

    with pytest.raises(Exception) as e:
        collection.update(**bad_dimensionality_records)
    assert "dimension" in str(e.value)


# test to make sure upsert shows exception for bad dimensionality


def test_dimensionality_exception_upsert(client):
    client.reset()
    collection = client.create_collection("test_dimensionality_upsert_exception")
    collection.add(**minimal_records)

    with pytest.raises(Exception) as e:
        collection.upsert(**bad_dimensionality_records)
    assert "dimension" in str(e.value)


# this may be flaky on windows, so we rerun it
@pytest.mark.flaky(reruns=3, condition=sys.platform.startswith("win32"))
def test_ssl_self_signed(client_ssl):
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping test for integration test")
    client_ssl.heartbeat()


# this may be flaky on windows, so we rerun it
@pytest.mark.flaky(reruns=3, condition=sys.platform.startswith("win32"))
def test_ssl_self_signed_without_ssl_verify(client_ssl):
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping test for integration test")
    client_ssl.heartbeat()
    _port = client_ssl._server._settings.chroma_server_http_port
    with pytest.raises(ValueError) as e:
        chromadb.HttpClient(ssl=True, port=_port)
    stack_trace = traceback.format_exception(
        type(e.value), e.value, e.value.__traceback__
    )
    client_ssl.clear_system_cache()
    assert "CERTIFICATE_VERIFY_FAILED" in "".join(stack_trace)


def test_query_id_filtering_small_dataset(client):
    client.reset()
    collection = client.create_collection("test_query_id_filtering_small")

    num_vectors = 100
    dim = 512
    small_records = np.random.rand(100, 512).astype(np.float32).tolist()
    ids = [f"{i}" for i in range(num_vectors)]

    collection.add(
        embeddings=small_records,
        ids=ids,
    )

    query_ids = [f"{i}" for i in range(0, num_vectors, 10)]
    query_embedding = np.random.rand(dim).astype(np.float32).tolist()
    results = collection.query(
        query_embeddings=query_embedding,
        ids=query_ids,
        n_results=num_vectors,
        include=[],
    )

    all_returned_ids = [item for sublist in results["ids"] for item in sublist]
    assert all(id in query_ids for id in all_returned_ids)


def test_query_id_filtering_medium_dataset(client):
    client.reset()
    collection = client.create_collection("test_query_id_filtering_medium")

    num_vectors = 1000
    dim = 512
    medium_records = np.random.rand(num_vectors, dim).astype(np.float32).tolist()
    ids = [f"{i}" for i in range(num_vectors)]

    collection.add(
        embeddings=medium_records,
        ids=ids,
    )

    query_ids = [f"{i}" for i in range(0, num_vectors, 10)]

    query_embedding = np.random.rand(dim).astype(np.float32).tolist()
    results = collection.query(
        query_embeddings=query_embedding,
        ids=query_ids,
        n_results=num_vectors,
        include=[],
    )

    all_returned_ids = [item for sublist in results["ids"] for item in sublist]
    assert all(id in query_ids for id in all_returned_ids)

    multi_query_embeddings = [
        np.random.rand(dim).astype(np.float32).tolist() for _ in range(3)
    ]
    multi_results = collection.query(
        query_embeddings=multi_query_embeddings,
        ids=query_ids,
        n_results=10,
        include=[],
    )

    for result_set in multi_results["ids"]:
        assert all(id in query_ids for id in result_set)


def test_query_id_filtering_e2e(client):
    client.reset()
    collection = client.create_collection("test_query_id_filtering_e2e")

    dim = 512
    num_vectors = 100
    embeddings = np.random.rand(num_vectors, dim).astype(np.float32).tolist()
    ids = [f"{i}" for i in range(num_vectors)]
    metadatas = [{"index": i} for i in range(num_vectors)]

    collection.add(
        embeddings=embeddings,
        ids=ids,
        metadatas=metadatas,
    )

    ids_to_delete = [f"{i}" for i in range(10, 30)]
    collection.delete(ids=ids_to_delete)

    # modify some existing ids, and add some new ones to check query returns updated metadata
    ids_to_upsert_existing = [f"{i}" for i in range(30, 50)]
    new_num_vectors = num_vectors + 20
    ids_to_upsert_new = [f"{i}" for i in range(num_vectors, new_num_vectors)]

    upsert_embeddings = (
        np.random.rand(len(ids_to_upsert_existing) + len(ids_to_upsert_new), dim)
        .astype(np.float32)
        .tolist()
    )
    upsert_metadatas = [
        {"index": i, "upserted": True} for i in range(len(upsert_embeddings))
    ]

    collection.upsert(
        embeddings=upsert_embeddings,
        ids=ids_to_upsert_existing + ids_to_upsert_new,
        metadatas=upsert_metadatas,
    )

    valid_query_ids = (
        [f"{i}" for i in range(5, 10)]  # subset of existing ids
        + [f"{i}" for i in range(35, 45)]  # subset of existing, but upserted
        + [
            f"{i}" for i in range(num_vectors + 5, num_vectors + 15)
        ]  # subset of new upserted ids
    )

    includes = ["metadatas"]
    query_embedding = np.random.rand(dim).astype(np.float32).tolist()
    results = collection.query(
        query_embeddings=query_embedding,
        ids=valid_query_ids,
        n_results=new_num_vectors,
        include=includes,
    )

    all_returned_ids = [item for sublist in results["ids"] for item in sublist]
    assert all(id in valid_query_ids for id in all_returned_ids)

    for result_index, id_list in enumerate(results["ids"]):
        for item_index, item_id in enumerate(id_list):
            if item_id in ids_to_upsert_existing or item_id in ids_to_upsert_new:
                # checks if metadata correctly has upserted flag
                assert results["metadatas"][result_index][item_index]["upserted"]

    upserted_id = ids_to_upsert_existing[0]
    # test single id filtering
    results = collection.query(
        query_embeddings=query_embedding,
        ids=upserted_id,
        n_results=1,
        include=includes,
    )
    assert results["metadatas"][0][0]["upserted"]

    deleted_id = ids_to_delete[0]
    # test deleted id filter raises
    with pytest.raises(Exception) as error:
        collection.query(
            query_embeddings=query_embedding,
            ids=deleted_id,
            n_results=1,
            include=includes,
        )
    assert "Error finding id" in str(error.value)


def test_validate_sparse_vector():
    """Test SparseVector validation in __post_init__."""
    from chromadb.base_types import SparseVector

    # Test 1: Valid sparse vector - should not raise
    SparseVector(indices=[0, 2, 5], values=[0.1, 0.5, 0.9])

    # Test 2: Valid sparse vector with empty lists - should not raise
    SparseVector(indices=[], values=[])

    # Test 4: Invalid - indices not a list
    with pytest.raises(ValueError, match="Expected SparseVector indices to be a list"):
        SparseVector(indices="not_a_list", values=[0.1, 0.2])  # type: ignore

    # Test 5: Invalid - values not a list
    with pytest.raises(ValueError, match="Expected SparseVector values to be a list"):
        SparseVector(indices=[0, 1], values="not_a_list")  # type: ignore

    # Test 6: Invalid - mismatched lengths
    with pytest.raises(
        ValueError, match="indices and values must have the same length"
    ):
        SparseVector(indices=[0, 1, 2], values=[0.1, 0.2])

    # Test 7: Invalid - non-integer index
    with pytest.raises(ValueError, match="SparseVector indices must be integers"):
        SparseVector(indices=[0, "not_int", 2], values=[0.1, 0.2, 0.3])  # type: ignore

    # Test 8: Invalid - negative index
    with pytest.raises(ValueError, match="SparseVector indices must be non-negative"):
        SparseVector(indices=[0, -1, 2], values=[0.1, 0.2, 0.3])

    # Test 9: Invalid - non-numeric value
    with pytest.raises(ValueError, match="SparseVector values must be numbers"):
        SparseVector(indices=[0, 1, 2], values=[0.1, "not_number", 0.3])  # type: ignore

    # Test 10: Invalid - float indices (not integers)
    with pytest.raises(ValueError, match="SparseVector indices must be integers"):
        SparseVector(indices=[0.0, 1.0, 2.0], values=[0.1, 0.2, 0.3])  # type: ignore

    # Test 11: Valid - integer values (not just floats)
    SparseVector(indices=[0, 1, 2], values=[1, 2, 3])

    # Test 12: Valid - mixed int and float values
    SparseVector(indices=[0, 1, 2], values=[1, 2.5, 3])

    # Test 13: Valid - large indices
    SparseVector(indices=[100, 1000, 10000], values=[0.1, 0.2, 0.3])

    # Test 14: Invalid - None as value
    with pytest.raises(ValueError, match="SparseVector values must be numbers"):
        SparseVector(indices=[0, 1], values=[0.1, None])  # type: ignore

    # Test 15: Invalid - None as index
    with pytest.raises(ValueError, match="SparseVector indices must be integers"):
        SparseVector(indices=[0, None], values=[0.1, 0.2])  # type: ignore

    # Test 16: Valid - single element
    SparseVector(indices=[42], values=[3.14])

    # Test 17: Boolean values are actually valid (bool is subclass of int in Python)
    SparseVector(indices=[0, 1], values=[True, False])  # True=1, False=0

    # Test 18: Invalid - unsorted indices
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        SparseVector(indices=[0, 2, 1], values=[0.1, 0.2, 0.3])

    # Test 19: Invalid - duplicate indices (not strictly ascending)
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        SparseVector(indices=[0, 1, 1, 2], values=[0.1, 0.2, 0.3, 0.4])

    # Test 20: Invalid - descending order
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        SparseVector(indices=[5, 3, 1], values=[0.5, 0.3, 0.1])


def test_sparse_vector_in_metadata_validation():
    """Test that sparse vectors are properly validated in metadata."""
    from chromadb.api.types import validate_metadata
    from chromadb.base_types import SparseVector

    # Test 1: Valid metadata with sparse vectors
    sparse_vector_1 = SparseVector(indices=[0, 2, 5], values=[0.1, 0.5, 0.9])
    sparse_vector_2 = SparseVector(indices=[1, 3, 4], values=[0.2, 0.4, 0.6])

    metadata_1 = {
        "text": "document 1",
        "sparse_embedding": sparse_vector_1,
        "score": 0.5,
    }
    metadata_2 = {
        "text": "document 2",
        "sparse_embedding": sparse_vector_2,
        "score": 0.8,
    }
    validate_metadata(metadata_1)
    validate_metadata(metadata_2)

    # Test 2: Valid metadata with empty sparse vector
    metadata_empty = {
        "text": "empty sparse",
        "sparse_vec": SparseVector(indices=[], values=[]),
    }
    validate_metadata(metadata_empty)

    # Test 3: Invalid sparse vector in metadata (construction fails)
    with pytest.raises(
        ValueError, match="indices and values must have the same length"
    ):
        invalid_metadata = {
            "text": "invalid",
            "sparse_embedding": SparseVector(indices=[0, 1], values=[0.1]),
        }

    # Test 4: Invalid dict in metadata (not a SparseVector dataclass)
    invalid_metadata_2 = {
        "text": "missing indices",
        "sparse_embedding": {"values": [0.1, 0.2]},
    }
    with pytest.raises(
        ValueError,
        match="Expected metadata value to be a str, int, float, bool, SparseVector, or None",
    ):
        validate_metadata(invalid_metadata_2)

    # Test 5: Invalid sparse vector - negative index (construction fails)
    with pytest.raises(ValueError, match="SparseVector indices must be non-negative"):
        invalid_metadata_3 = {
            "text": "negative index",
            "sparse_embedding": SparseVector(
                indices=[0, -1, 2], values=[0.1, 0.2, 0.3]
            ),
        }

    # Test 6: Invalid sparse vector - non-numeric value (construction fails)
    with pytest.raises(ValueError, match="SparseVector values must be numbers"):
        invalid_metadata_4 = {
            "text": "non-numeric value",
            "sparse_embedding": SparseVector(
                indices=[0, 1], values=[0.1, "not_a_number"]
            ),  # type: ignore
        }

    # Test 7: Multiple sparse vectors in metadata
    metadata_multiple = {
        "text": "multiple sparse vectors",
        "sparse_1": SparseVector(indices=[0, 1], values=[0.1, 0.2]),
        "sparse_2": SparseVector(indices=[2, 3, 4], values=[0.3, 0.4, 0.5]),
        "regular_field": 42,
    }
    validate_metadata(metadata_multiple)

    # Test 8: Regular dict (not SparseVector) should be rejected
    metadata_nested = {
        "config": "some_config",
        "sparse_vector": {"indices": [0, 1, 2], "values": [1.0, 2.0, 3.0]},
    }
    with pytest.raises(
        ValueError,
        match="Expected metadata value to be a str, int, float, bool, SparseVector, or None",
    ):
        validate_metadata(metadata_nested)

    # Test 9: Large sparse vector
    large_sparse = SparseVector(
        indices=list(range(1000)),
        values=[float(i) * 0.001 for i in range(1000)],
    )
    metadata_large = {"text": "large sparse", "large_sparse_vec": large_sparse}
    validate_metadata(metadata_large)


def test_sparse_vector_dict_format_normalization():
    """Test that dict-format sparse vectors are normalized to SparseVector instances."""
    from chromadb.api.types import normalize_metadata, validate_metadata
    from chromadb.base_types import SparseVector

    # Test 1: Dict format with #type='sparse_vector' should be converted
    metadata_dict_format = {
        "text": "test document",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 2, 5],
            "values": [1.0, 2.0, 3.0],
        },
    }
    normalized = normalize_metadata(metadata_dict_format)

    assert isinstance(normalized["sparse"], SparseVector)
    assert normalized["sparse"].indices == [0, 2, 5]
    assert normalized["sparse"].values == [1.0, 2.0, 3.0]

    # Should pass validation after normalization
    validate_metadata(normalized)

    # Test 2: SparseVector instance should pass through unchanged
    sparse_instance = SparseVector(indices=[1, 3, 4], values=[0.5, 1.5, 2.5])
    metadata_instance_format = {
        "text": "test document",
        "sparse": sparse_instance,
    }
    normalized2 = normalize_metadata(metadata_instance_format)

    assert normalized2["sparse"] is sparse_instance  # Same object
    validate_metadata(normalized2)

    # Test 3: Dict format with unsorted indices should be rejected during normalization
    metadata_unsorted = {
        "text": "unsorted",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [5, 0, 2],
            "values": [3.0, 1.0, 2.0],
        },
    }
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        normalize_metadata(metadata_unsorted)

    # Test 4: Dict format with duplicate indices should be rejected
    metadata_duplicates = {
        "text": "duplicates",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 2, 2],
            "values": [1.0, 2.0, 3.0],
        },
    }
    with pytest.raises(
        ValueError, match="indices must be sorted in strictly ascending order"
    ):
        normalize_metadata(metadata_duplicates)

    # Test 5: Dict format with negative indices should be rejected
    metadata_negative = {
        "text": "negative",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [-1, 0, 2],
            "values": [1.0, 2.0, 3.0],
        },
    }
    with pytest.raises(ValueError, match="indices must be non-negative"):
        normalize_metadata(metadata_negative)

    # Test 6: Dict format with length mismatch should be rejected
    metadata_mismatch = {
        "text": "mismatch",
        "sparse": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 2],
            "values": [1.0, 2.0, 3.0],
        },
    }
    with pytest.raises(
        ValueError, match="indices and values must have the same length"
    ):
        normalize_metadata(metadata_mismatch)

    # Test 7: Regular dict without #type should not be converted
    metadata_regular_dict = {
        "text": "regular",
        "config": {"key": "value"},
    }
    normalized3 = normalize_metadata(metadata_regular_dict)
    assert isinstance(normalized3["config"], dict)
    assert normalized3["config"]["key"] == "value"

    # Test 8: Empty sparse vector in dict format
    metadata_empty = {
        "text": "empty",
        "sparse": {TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE, "indices": [], "values": []},
    }
    normalized4 = normalize_metadata(metadata_empty)
    assert isinstance(normalized4["sparse"], SparseVector)
    assert normalized4["sparse"].indices == []
    assert normalized4["sparse"].values == []

    # Test 9: Multiple sparse vectors in dict format
    metadata_multiple = {
        "sparse1": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 1],
            "values": [1.0, 2.0],
        },
        "sparse2": {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [2, 3],
            "values": [3.0, 4.0],
        },
        "regular": 42,
    }
    normalized5 = normalize_metadata(metadata_multiple)
    assert isinstance(normalized5["sparse1"], SparseVector)
    assert isinstance(normalized5["sparse2"], SparseVector)
    assert normalized5["regular"] == 42


def test_sparse_vector_dict_format_in_record_set():
    """Test that dict-format sparse vectors work in normalize_insert_record_set."""
    from chromadb.api.types import (
        normalize_insert_record_set,
        validate_insert_record_set,
    )
    from chromadb.base_types import SparseVector

    # Test 1: Mix of dict format and SparseVector instances
    record_set = normalize_insert_record_set(
        ids=["doc1", "doc2", "doc3"],
        embeddings=None,
        metadatas=[
            {
                "text": "test1",
                "sparse": {
                    TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
                    "indices": [0, 2],
                    "values": [1.0, 2.0],
                },
            },
            {
                "text": "test2",
                "sparse": SparseVector(indices=[1, 3], values=[1.5, 2.5]),
            },
            {"text": "test3"},  # No sparse vector
        ],
        documents=["doc one", "doc two", "doc three"],
    )

    # Both should be converted to SparseVector instances
    assert isinstance(record_set["metadatas"][0]["sparse"], SparseVector)
    assert isinstance(record_set["metadatas"][1]["sparse"], SparseVector)
    assert "sparse" not in record_set["metadatas"][2]

    # Validation should pass
    validate_insert_record_set(record_set)

    # Test 2: Verify values are correct after normalization
    assert record_set["metadatas"][0]["sparse"].indices == [0, 2]
    assert record_set["metadatas"][0]["sparse"].values == [1.0, 2.0]
    assert record_set["metadatas"][1]["sparse"].indices == [1, 3]
    assert record_set["metadatas"][1]["sparse"].values == [1.5, 2.5]


def test_search_result_rows() -> None:
    """Test the SearchResult.rows() method for converting column-major to row-major format."""
    from chromadb.api.types import SearchResult

    # Test 1: Basic single payload with all fields
    result = SearchResult(
        {
            "ids": [["id1", "id2", "id3"]],
            "documents": [["doc1", "doc2", "doc3"]],
            "embeddings": [[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]],
            "metadatas": [[{"key": "a"}, {"key": "b"}, {"key": "c"}]],
            "scores": [[0.9, 0.8, 0.7]],
            "select": [["document", "score", "metadata"]],
        }
    )

    rows = result.rows()
    assert len(rows) == 1  # One payload
    assert len(rows[0]) == 3  # Three results

    # Check first row
    assert rows[0][0]["id"] == "id1"
    assert rows[0][0]["document"] == "doc1"
    assert rows[0][0]["embedding"] == [1.0, 2.0]
    assert rows[0][0]["metadata"] == {"key": "a"}
    assert rows[0][0]["score"] == 0.9

    # Check all rows have all fields
    for row in rows[0]:
        assert "id" in row
        assert "document" in row
        assert "embedding" in row
        assert "metadata" in row
        assert "score" in row

    # Test 2: Multiple payloads
    result = SearchResult(
        {
            "ids": [["a1", "a2"], ["b1", "b2", "b3"]],
            "documents": [["doc_a1", "doc_a2"], ["doc_b1", "doc_b2", "doc_b3"]],
            "embeddings": [
                None,
                [[1.0], [2.0], [3.0]],
            ],  # First payload has no embeddings
            "metadatas": [[{"x": 1}, {"x": 2}], None],  # Second payload has no metadata
            "scores": [[0.5, 0.4], [0.9, 0.8, 0.7]],
            "select": [["document", "score"], ["embedding", "score"]],
        }
    )

    rows = result.rows()
    assert len(rows) == 2  # Two payloads
    assert len(rows[0]) == 2  # First payload has 2 results
    assert len(rows[1]) == 3  # Second payload has 3 results

    # First payload - has docs, metadata, scores but no embeddings
    assert rows[0][0] == {
        "id": "a1",
        "document": "doc_a1",
        "metadata": {"x": 1},
        "score": 0.5,
    }
    assert rows[0][1] == {
        "id": "a2",
        "document": "doc_a2",
        "metadata": {"x": 2},
        "score": 0.4,
    }

    # Second payload - has docs, embeddings, scores but no metadata
    assert rows[1][0] == {
        "id": "b1",
        "document": "doc_b1",
        "embedding": [1.0],
        "score": 0.9,
    }
    assert rows[1][1] == {
        "id": "b2",
        "document": "doc_b2",
        "embedding": [2.0],
        "score": 0.8,
    }
    assert rows[1][2] == {
        "id": "b3",
        "document": "doc_b3",
        "embedding": [3.0],
        "score": 0.7,
    }

    # Test 3: Empty result
    result = SearchResult(
        {
            "ids": [],
            "documents": [],
            "embeddings": [],
            "metadatas": [],
            "scores": [],
            "select": [],
        }
    )

    rows = result.rows()
    assert rows == []

    # Test 4: Sparse data with None values in lists
    result = SearchResult(
        {
            "ids": [["id1", "id2", "id3"]],
            "documents": [[None, "doc2", None]],  # Sparse documents
            "embeddings": None,  # No embeddings at all
            "metadatas": [[{"a": 1}, None, {"c": 3}]],  # Sparse metadata
            "scores": [[0.9, None, 0.7]],  # Sparse scores
            "select": [["document", "metadata", "score"]],
        }
    )

    rows = result.rows()
    assert len(rows) == 1
    assert len(rows[0]) == 3

    # First row - only has metadata and score
    assert rows[0][0] == {"id": "id1", "metadata": {"a": 1}, "score": 0.9}

    # Second row - only has document
    assert rows[0][1] == {"id": "id2", "document": "doc2"}

    # Third row - has metadata and score
    assert rows[0][2] == {"id": "id3", "metadata": {"c": 3}, "score": 0.7}

    # Test 5: Only IDs (minimal result)
    result = SearchResult(
        {
            "ids": [["id1", "id2"]],
            "documents": None,
            "embeddings": None,
            "metadatas": None,
            "scores": None,
            "select": [[]],
        }
    )

    rows = result.rows()
    assert len(rows) == 1
    assert len(rows[0]) == 2
    assert rows[0][0] == {"id": "id1"}
    assert rows[0][1] == {"id": "id2"}

    # Test 6: SearchResult works as dict (backward compatibility)
    result = SearchResult(
        {
            "ids": [["test"]],
            "documents": [["test doc"]],
            "metadatas": [[{"test": True}]],
            "embeddings": [[[0.1, 0.2]]],
            "scores": [[0.99]],
            "select": [["all"]],
        }
    )

    # Should work as dict
    assert result["ids"] == [["test"]]
    assert result.get("documents") == [["test doc"]]
    assert "metadatas" in result
    assert len(result) == 6  # Should have 6 keys

    # Should also have rows() method
    rows = result.rows()
    assert len(rows[0]) == 1
    assert rows[0][0]["id"] == "test"

    print("All SearchResult.rows() tests passed!")


def test_rrf_to_dict() -> None:
    """Test the Rrf (Reciprocal Rank Fusion) to_dict conversion."""
    # Note: In these tests, "sparse_embedding" is just an example metadata field name.
    # Users can store any data in metadata fields and reference them by name (without # prefix).
    # The "#embedding" key refers to the special main embedding field.

    import pytest
    from chromadb.execution.expression.operator import Rrf, Knn, Val

    # Test 1: Basic RRF with two KNN rankings (equal weight)
    rrf = Rrf(
        [
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], key="sparse_embedding", return_rank=True),
        ]
    )

    result = rrf.to_dict()

    # RRF formula: -sum(weight_i / (k + rank_i))
    # With default k=60 and equal weights (1.0 each)
    # Expected: -(1.0/(60 + knn1) + 1.0/(60 + knn2))
    expected = {
        "$mul": [
            {"$val": -1},
            {
                "$sum": [
                    {
                        "$div": {
                            "left": {"$val": 1.0},
                            "right": {
                                "$sum": [
                                    {"$val": 60},
                                    {
                                        "$knn": {
                                            "query": [0.1, 0.2],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                    {
                        "$div": {
                            "left": {"$val": 1.0},
                            "right": {
                                "$sum": [
                                    {"$val": 60},
                                    {
                                        "$knn": {
                                            "query": [0.3, 0.4],
                                            "key": "sparse_embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                ]
            },
        ]
    }

    assert result == expected

    # Test 2: RRF with custom weights and k
    rrf_weighted = Rrf(
        ranks=[
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], key="sparse_embedding", return_rank=True),
        ],
        weights=[2.0, 1.0],  # Dense is 2x more important
        k=100,
    )

    result_weighted = rrf_weighted.to_dict()

    # Expected: -(2.0/(100 + knn1) + 1.0/(100 + knn2))
    expected_weighted = {
        "$mul": [
            {"$val": -1},
            {
                "$sum": [
                    {
                        "$div": {
                            "left": {"$val": 2.0},
                            "right": {
                                "$sum": [
                                    {"$val": 100},
                                    {
                                        "$knn": {
                                            "query": [0.1, 0.2],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                    {
                        "$div": {
                            "left": {"$val": 1.0},
                            "right": {
                                "$sum": [
                                    {"$val": 100},
                                    {
                                        "$knn": {
                                            "query": [0.3, 0.4],
                                            "key": "sparse_embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                ]
            },
        ]
    }

    assert result_weighted == expected_weighted

    # Test 3: RRF with three rankings
    rrf_three = Rrf(
        [
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], key="sparse_embedding", return_rank=True),
            Val(5.0),  # Can also include constant rank
        ]
    )

    result_three = rrf_three.to_dict()

    # Verify it has three terms in the sum
    assert "$mul" in result_three
    assert "$sum" in result_three["$mul"][1]
    terms = result_three["$mul"][1]["$sum"]
    assert len(terms) == 3  # Three ranking strategies

    # Test 4: Error case - mismatched weights
    with pytest.raises(
        ValueError, match="Number of weights .* must match number of ranks"
    ):
        rrf_bad = Rrf(
            ranks=[
                Knn(query=[0.1, 0.2], return_rank=True),
                Knn(query=[0.3, 0.4], return_rank=True),
            ],
            weights=[1.0],  # Only one weight for two ranks
        )
        rrf_bad.to_dict()

    # Test 5: Error case - negative weights
    with pytest.raises(ValueError, match="All weights must be non-negative"):
        rrf_negative = Rrf(
            ranks=[
                Knn(query=[0.1, 0.2], return_rank=True),
                Knn(query=[0.3, 0.4], return_rank=True),
            ],
            weights=[1.0, -1.0],  # Negative weight
        )
        rrf_negative.to_dict()

    # Test 6: Error case - empty ranks list
    with pytest.raises(ValueError, match="RRF requires at least one rank"):
        rrf_empty = Rrf([])
        rrf_empty.to_dict()  # Validation happens in to_dict()

    # Test 7: Error case - negative k value
    with pytest.raises(ValueError, match="k must be positive"):
        rrf_neg_k = Rrf([Val(1.0)], k=-5)
        rrf_neg_k.to_dict()  # Validation happens in to_dict()

    # Test 8: Error case - zero k value
    with pytest.raises(ValueError, match="k must be positive"):
        rrf_zero_k = Rrf([Val(1.0)], k=0)
        rrf_zero_k.to_dict()  # Validation happens in to_dict()
    # Test 9: Normalize flag with weights
    rrf_normalized = Rrf(
        ranks=[
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], key="sparse_embedding", return_rank=True),
        ],
        weights=[3.0, 1.0],  # Will be normalized to [0.75, 0.25]
        normalize=True,
        k=100,
    )

    result_normalized = rrf_normalized.to_dict()

    # Expected: -(0.75/(100 + knn1) + 0.25/(100 + knn2))
    expected_normalized = {
        "$mul": [
            {"$val": -1},
            {
                "$sum": [
                    {
                        "$div": {
                            "left": {"$val": 0.75},
                            "right": {
                                "$sum": [
                                    {"$val": 100},
                                    {
                                        "$knn": {
                                            "query": [0.1, 0.2],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                    {
                        "$div": {
                            "left": {"$val": 0.25},
                            "right": {
                                "$sum": [
                                    {"$val": 100},
                                    {
                                        "$knn": {
                                            "query": [0.3, 0.4],
                                            "key": "sparse_embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                ]
            },
        ]
    }

    assert result_normalized == expected_normalized

    # Test 10: Normalize flag without weights (should work with defaults)
    rrf_normalize_defaults = Rrf(
        ranks=[
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=[0.3, 0.4], return_rank=True),
        ],
        normalize=True,  # Will normalize [1.0, 1.0] to [0.5, 0.5]
    )

    result_defaults = rrf_normalize_defaults.to_dict()

    # Both weights should be 0.5 after normalization
    expected_defaults = {
        "$mul": [
            {"$val": -1},
            {
                "$sum": [
                    {
                        "$div": {
                            "left": {"$val": 0.5},
                            "right": {
                                "$sum": [
                                    {"$val": 60},  # Default k=60
                                    {
                                        "$knn": {
                                            "query": [0.1, 0.2],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                    {
                        "$div": {
                            "left": {"$val": 0.5},
                            "right": {
                                "$sum": [
                                    {"$val": 60},
                                    {
                                        "$knn": {
                                            "query": [0.3, 0.4],
                                            "key": "#embedding",
                                            "limit": 16,
                                            "return_rank": True,
                                        }
                                    },
                                ]
                            },
                        }
                    },
                ]
            },
        ]
    }

    assert result_defaults == expected_defaults

    # Test 11: Error case - normalize with all zero weights
    with pytest.raises(ValueError, match="Sum of weights must be positive"):
        rrf_zero_weights = Rrf(
            ranks=[
                Knn(query=[0.1, 0.2], return_rank=True),
                Knn(query=[0.3, 0.4], return_rank=True),
            ],
            weights=[0.0, 0.0],
            normalize=True,
        )
        rrf_zero_weights.to_dict()

    print("All RRF tests passed!")


def test_group_by_serialization() -> None:
    """Test GroupBy, MinK, and MaxK serialization and deserialization."""
    import pytest
    from chromadb.execution.expression.operator import (
        GroupBy,
        MinK,
        MaxK,
        Key,
        Aggregate,
    )

    # to_dict with OneOrMany keys
    group_by = GroupBy(keys=Key("category"), aggregate=MinK(keys=Key.SCORE, k=3))
    assert group_by.to_dict() == {
        "keys": ["category"],
        "aggregate": {"$min_k": {"keys": ["#score"], "k": 3}},
    }

    # to_dict with multiple keys and MaxK
    group_by = GroupBy(
        keys=[Key("year"), Key("category")],
        aggregate=MaxK(keys=[Key.SCORE, Key("priority")], k=5),
    )
    assert group_by.to_dict() == {
        "keys": ["year", "category"],
        "aggregate": {"$max_k": {"keys": ["#score", "priority"], "k": 5}},
    }

    # Round-trip
    original = GroupBy(keys=[Key("category")], aggregate=MinK(keys=[Key.SCORE], k=3))
    assert GroupBy.from_dict(original.to_dict()).to_dict() == original.to_dict()

    # Empty GroupBy serializes to {} and from_dict({}) returns default GroupBy
    empty_group_by = GroupBy()
    assert empty_group_by.to_dict() == {}
    assert GroupBy.from_dict({}).to_dict() == {}

    # Error cases
    with pytest.raises(ValueError, match="requires 'keys' field"):
        GroupBy.from_dict({"aggregate": {"$min_k": {"keys": ["#score"], "k": 3}}})

    with pytest.raises(ValueError, match="requires 'aggregate' field"):
        GroupBy.from_dict({"keys": ["category"]})

    with pytest.raises(ValueError, match="keys cannot be empty"):
        GroupBy.from_dict(
            {"keys": [], "aggregate": {"$min_k": {"keys": ["#score"], "k": 3}}}
        )

    with pytest.raises(ValueError, match="Unknown aggregate operator"):
        Aggregate.from_dict({"$unknown": {"keys": ["#score"], "k": 3}})


# Expression API Tests - Testing dict support and from_dict methods
class TestSearchDictSupport:
    """Test Search class dict input support."""

    def test_search_with_dict_where(self):
        """Test Search accepts dict for where parameter."""
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Where

        # Simple equality
        search = Search(where={"status": "active"})
        assert search._where is not None
        assert isinstance(search._where, Where)

        # Complex where with operators
        search = Search(where={"$and": [{"status": "active"}, {"score": {"$gt": 0.5}}]})
        assert search._where is not None

    def test_search_with_dict_rank(self):
        """Test Search accepts dict for rank parameter."""
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Rank

        # KNN ranking
        search = Search(rank={"$knn": {"query": [0.1, 0.2]}})
        assert search._rank is not None
        assert isinstance(search._rank, Rank)

        # Val ranking
        search = Search(rank={"$val": 0.5})
        assert search._rank is not None

    def test_search_with_dict_limit(self):
        """Test Search accepts dict and int for limit parameter."""
        from chromadb.execution.expression.plan import Search

        # Dict limit
        search = Search(limit={"limit": 10, "offset": 5})
        assert search._limit.limit == 10
        assert search._limit.offset == 5

        # Int limit (creates Limit with offset=0)
        search = Search(limit=10)
        assert search._limit.limit == 10
        assert search._limit.offset == 0

    def test_search_with_dict_select(self):
        """Test Search accepts dict, list, and set for select parameter."""
        from chromadb.execution.expression.plan import Search

        # Dict select
        search = Search(select={"keys": ["#document", "#score"]})
        assert search._select is not None

        # List select
        search = Search(select=["#document", "#metadata"])
        assert search._select is not None

        # Set select
        search = Search(select={"#document", "#embedding"})
        assert search._select is not None

    def test_search_mixed_inputs(self):
        """Test Search with mixed expression and dict inputs."""
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Key

        search = Search(
            where=Key("status") == "active",  # Expression
            rank={"$knn": {"query": [0.1, 0.2]}},  # Dict
            limit=10,  # Int
            select=["#document"],  # List
        )
        assert search._where is not None
        assert search._rank is not None
        assert search._limit.limit == 10
        assert search._select is not None

    def test_search_builder_methods_with_dicts(self):
        """Test Search builder methods accept dicts."""
        from chromadb.execution.expression.plan import Search

        search = Search().where({"status": "active"}).rank({"$val": 0.5})
        assert search._where is not None
        assert search._rank is not None

    def test_search_invalid_inputs(self):
        """Test Search rejects invalid input types."""
        import pytest
        from chromadb.execution.expression.plan import Search

        with pytest.raises(TypeError, match="where must be"):
            Search(where="invalid")

        with pytest.raises(TypeError, match="rank must be"):
            Search(rank=0.5)  # Primitive numbers not allowed

        with pytest.raises(TypeError, match="limit must be"):
            Search(limit="10")

        with pytest.raises(TypeError, match="select must be"):
            Search(select=123)

    def test_search_with_group_by(self):
        """Test Search accepts group_by as dict, object, and builder method."""
        import pytest
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import GroupBy, MinK, Key

        # Dict input
        search = Search(
            group_by={
                "keys": ["category"],
                "aggregate": {"$min_k": {"keys": ["#score"], "k": 3}},
            }
        )
        assert isinstance(search._group_by, GroupBy)

        # Object input and builder method
        group_by = GroupBy(keys=Key("category"), aggregate=MinK(keys=Key.SCORE, k=3))
        assert Search(group_by=group_by)._group_by is group_by
        assert Search().group_by(group_by)._group_by.aggregate is not None

        # Invalid inputs
        with pytest.raises(TypeError, match="group_by must be"):
            Search(group_by="invalid")
        with pytest.raises(ValueError, match="requires 'aggregate' field"):
            Search(group_by={"keys": ["category"]})

    def test_search_group_by_serialization(self):
        """Test Search serializes group_by correctly."""
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import GroupBy, MinK, Key, Knn

        # Without group_by - empty dict
        search = Search().rank(Knn(query=[0.1, 0.2])).limit(10)
        assert search.to_dict()["group_by"] == {}

        # With group_by - has keys and aggregate
        search = Search().group_by(
            GroupBy(keys=Key("category"), aggregate=MinK(keys=Key.SCORE, k=3))
        )
        result = search.to_dict()["group_by"]
        assert result["keys"] == ["category"]
        assert result["aggregate"] == {"$min_k": {"keys": ["#score"], "k": 3}}


class TestWhereFromDict:
    """Test Where.from_dict() conversion."""

    def test_simple_equality(self):
        """Test simple equality conversion."""
        from chromadb.execution.expression.operator import Where, Eq

        # Shorthand for equality
        where = Where.from_dict({"status": "active"})
        assert isinstance(where, Eq)

        # Explicit $eq
        where = Where.from_dict({"status": {"$eq": "active"}})
        assert isinstance(where, Eq)

    def test_comparison_operators(self):
        """Test comparison operator conversions."""
        from chromadb.execution.expression.operator import Where, Ne, Gt, Gte, Lt, Lte

        # $ne
        where = Where.from_dict({"status": {"$ne": "inactive"}})
        assert isinstance(where, Ne)

        # $gt
        where = Where.from_dict({"score": {"$gt": 0.5}})
        assert isinstance(where, Gt)

        # $gte
        where = Where.from_dict({"score": {"$gte": 0.5}})
        assert isinstance(where, Gte)

        # $lt
        where = Where.from_dict({"score": {"$lt": 1.0}})
        assert isinstance(where, Lt)

        # $lte
        where = Where.from_dict({"score": {"$lte": 1.0}})
        assert isinstance(where, Lte)

    def test_membership_operators(self):
        """Test membership operator conversions."""
        from chromadb.execution.expression.operator import Where, In, Nin

        # $in
        where = Where.from_dict({"status": {"$in": ["active", "pending"]}})
        assert isinstance(where, In)

        # $nin (not in)
        where = Where.from_dict({"status": {"$nin": ["deleted", "archived"]}})
        assert isinstance(where, Nin)

    def test_string_operators(self):
        """Test string operator conversions."""
        from chromadb.execution.expression.operator import (
            Where,
            Contains,
            NotContains,
            Regex,
            NotRegex,
        )

        # $contains
        where = Where.from_dict({"text": {"$contains": "hello"}})
        assert isinstance(where, Contains)

        # $not_contains
        where = Where.from_dict({"text": {"$not_contains": "spam"}})
        assert isinstance(where, NotContains)

        # $regex
        where = Where.from_dict({"text": {"$regex": "^test.*"}})
        assert isinstance(where, Regex)

        # $not_regex
        where = Where.from_dict({"text": {"$not_regex": r"\d+"}})
        assert isinstance(where, NotRegex)

    def test_logical_operators(self):
        """Test logical operator conversions."""
        from chromadb.execution.expression.operator import Where, And, Or

        # $and
        where = Where.from_dict(
            {"$and": [{"status": "active"}, {"score": {"$gt": 0.5}}]}
        )
        assert isinstance(where, And)

        # $or
        where = Where.from_dict({"$or": [{"status": "active"}, {"status": "pending"}]})
        assert isinstance(where, Or)

    def test_nested_logical_operators(self):
        """Test nested logical operations."""
        from chromadb.execution.expression.operator import Where, And

        where = Where.from_dict(
            {
                "$and": [
                    {"$or": [{"status": "active"}, {"status": "pending"}]},
                    {"score": {"$gte": 0.5}},
                ]
            }
        )
        assert isinstance(where, And)

    def test_special_keys(self):
        """Test special key handling."""
        from chromadb.execution.expression.operator import Where, In

        # ID key
        where = Where.from_dict({"#id": {"$in": ["id1", "id2"]}})
        assert isinstance(where, In)

    def test_invalid_where_dicts(self):
        """Test invalid Where dict inputs."""
        import pytest
        from chromadb.execution.expression.operator import Where

        with pytest.raises(TypeError, match="Expected dict"):
            Where.from_dict("not a dict")

        with pytest.raises(ValueError, match="cannot be empty"):
            Where.from_dict({})

        with pytest.raises(ValueError, match="requires at least one condition"):
            Where.from_dict({"$and": []})


class TestRankFromDict:
    """Test Rank.from_dict() conversion."""

    def test_val_conversion(self):
        """Test Val conversion."""
        from chromadb.execution.expression.operator import Rank, Val

        rank = Rank.from_dict({"$val": 0.5})
        assert isinstance(rank, Val)
        assert rank.value == 0.5

    def test_knn_conversion(self):
        """Test KNN conversion."""
        import numpy as np
        from chromadb.execution.expression.operator import Rank, Knn

        # Basic KNN with defaults
        rank = Rank.from_dict({"$knn": {"query": [0.1, 0.2]}})
        assert isinstance(rank, Knn)
        # Handle both list and numpy array cases
        if isinstance(rank.query, np.ndarray):
            # Use allclose for floating point comparison with dtype tolerance
            assert np.allclose(rank.query, np.array([0.1, 0.2]))
        else:
            assert rank.query == [0.1, 0.2]
        assert rank.key == "#embedding"  # default
        assert rank.limit == 16  # default

        # KNN with custom parameters
        rank = Rank.from_dict(
            {
                "$knn": {
                    "query": [0.1, 0.2],
                    "key": "sparse_embedding",
                    "limit": 256,
                    "return_rank": True,
                }
            }
        )
        assert rank.key == "sparse_embedding"
        assert rank.limit == 256
        assert rank.return_rank

    def test_arithmetic_operators(self):
        """Test arithmetic operator conversions."""
        from chromadb.execution.expression.operator import Rank, Sum, Sub, Mul, Div

        # $sum
        rank = Rank.from_dict({"$sum": [{"$val": 0.5}, {"$val": 0.3}]})
        assert isinstance(rank, Sum)

        # $sub
        rank = Rank.from_dict({"$sub": {"left": {"$val": 1.0}, "right": {"$val": 0.3}}})
        assert isinstance(rank, Sub)

        # $mul
        rank = Rank.from_dict({"$mul": [{"$val": 2.0}, {"$val": 0.5}]})
        assert isinstance(rank, Mul)

        # $div
        rank = Rank.from_dict({"$div": {"left": {"$val": 1.0}, "right": {"$val": 2.0}}})
        assert isinstance(rank, Div)

    def test_math_functions(self):
        """Test math function conversions."""
        from chromadb.execution.expression.operator import Rank, Abs, Exp, Log

        # $abs
        rank = Rank.from_dict({"$abs": {"$val": -0.5}})
        assert isinstance(rank, Abs)

        # $exp
        rank = Rank.from_dict({"$exp": {"$val": 1.0}})
        assert isinstance(rank, Exp)

        # $log
        rank = Rank.from_dict({"$log": {"$val": 2.0}})
        assert isinstance(rank, Log)

    def test_aggregation_functions(self):
        """Test min/max conversions."""
        from chromadb.execution.expression.operator import Rank, Max, Min

        # $max
        rank = Rank.from_dict({"$max": [{"$val": 0.5}, {"$val": 0.8}]})
        assert isinstance(rank, Max)

        # $min
        rank = Rank.from_dict({"$min": [{"$val": 0.5}, {"$val": 0.8}]})
        assert isinstance(rank, Min)

    def test_complex_rank_expression(self):
        """Test complex nested rank expressions."""
        from chromadb.execution.expression.operator import Rank, Sum

        rank = Rank.from_dict(
            {
                "$sum": [
                    {"$mul": [{"$knn": {"query": [0.1, 0.2]}}, {"$val": 0.8}]},
                    {"$mul": [{"$val": 0.5}, {"$val": 0.2}]},
                ]
            }
        )
        assert isinstance(rank, Sum)

    def test_invalid_rank_dicts(self):
        """Test invalid Rank dict inputs."""
        import pytest
        from chromadb.execution.expression.operator import Rank

        with pytest.raises(TypeError, match="Expected dict"):
            Rank.from_dict("not a dict")

        with pytest.raises(ValueError, match="cannot be empty"):
            Rank.from_dict({})

        with pytest.raises(ValueError, match="exactly one operator"):
            Rank.from_dict({"$val": 0.5, "$knn": {"query": [0.1]}})

        with pytest.raises(TypeError, match="requires a number"):
            Rank.from_dict({"$val": "not a number"})


class TestLimitFromDict:
    """Test Limit.from_dict() conversion."""

    def test_limit_only(self):
        """Test limit without offset."""
        from chromadb.execution.expression.operator import Limit

        limit = Limit.from_dict({"limit": 20})
        assert limit.limit == 20
        assert limit.offset == 0  # default

    def test_offset_only(self):
        """Test offset without limit."""
        from chromadb.execution.expression.operator import Limit

        limit = Limit.from_dict({"offset": 10})
        assert limit.offset == 10
        assert limit.limit is None

    def test_limit_and_offset(self):
        """Test both limit and offset."""
        from chromadb.execution.expression.operator import Limit

        limit = Limit.from_dict({"limit": 20, "offset": 10})
        assert limit.limit == 20
        assert limit.offset == 10

    def test_validation(self):
        """Test Limit validation."""
        import pytest
        from chromadb.execution.expression.operator import Limit

        # Negative limit
        with pytest.raises(ValueError, match="must be positive"):
            Limit.from_dict({"limit": -1})

        # Zero limit
        with pytest.raises(ValueError, match="must be positive"):
            Limit.from_dict({"limit": 0})

        # Negative offset
        with pytest.raises(ValueError, match="must be non-negative"):
            Limit.from_dict({"offset": -1})

    def test_invalid_types(self):
        """Test type validation."""
        import pytest
        from chromadb.execution.expression.operator import Limit

        with pytest.raises(TypeError, match="Expected dict"):
            Limit.from_dict("not a dict")

        with pytest.raises(TypeError, match="must be an integer"):
            Limit.from_dict({"limit": "20"})

        with pytest.raises(TypeError, match="must be an integer"):
            Limit.from_dict({"offset": 10.5})

    def test_unexpected_keys(self):
        """Test rejection of unexpected keys."""
        import pytest
        from chromadb.execution.expression.operator import Limit

        with pytest.raises(ValueError, match="Unexpected keys"):
            Limit.from_dict({"limit": 10, "invalid": "key"})


class TestSelectFromDict:
    """Test Select.from_dict() conversion."""

    def test_special_keys(self):
        """Test special key conversion."""
        from chromadb.execution.expression.operator import Select, Key

        select = Select.from_dict(
            {"keys": ["#document", "#embedding", "#metadata", "#score"]}
        )
        assert Key.DOCUMENT in select.keys
        assert Key.EMBEDDING in select.keys
        assert Key.METADATA in select.keys
        assert Key.SCORE in select.keys

    def test_metadata_keys(self):
        """Test regular metadata field keys."""
        from chromadb.execution.expression.operator import Select, Key

        select = Select.from_dict({"keys": ["title", "author", "date"]})
        assert Key("title") in select.keys
        assert Key("author") in select.keys
        assert Key("date") in select.keys

    def test_mixed_keys(self):
        """Test mix of special and metadata keys."""
        from chromadb.execution.expression.operator import Select, Key

        select = Select.from_dict({"keys": ["#document", "title", "#score"]})
        assert Key.DOCUMENT in select.keys
        assert Key("title") in select.keys
        assert Key.SCORE in select.keys

    def test_empty_keys(self):
        """Test empty keys list."""
        from chromadb.execution.expression.operator import Select

        select = Select.from_dict({"keys": []})
        assert len(select.keys) == 0

    def test_validation(self):
        """Test Select validation."""
        import pytest
        from chromadb.execution.expression.operator import Select

        with pytest.raises(TypeError, match="Expected dict"):
            Select.from_dict("not a dict")

        with pytest.raises(TypeError, match="must be a list/tuple/set"):
            Select.from_dict({"keys": "not a list"})

        with pytest.raises(TypeError, match="must be a string"):
            Select.from_dict({"keys": [123]})

    def test_unexpected_keys(self):
        """Test rejection of unexpected keys."""
        import pytest
        from chromadb.execution.expression.operator import Select

        with pytest.raises(ValueError, match="Unexpected keys"):
            Select.from_dict({"keys": [], "invalid": "key"})


class TestRoundTripConversion:
    """Test that to_dict() and from_dict() round-trip correctly."""

    def test_where_round_trip(self):
        """Test Where round-trip conversion."""
        from chromadb.execution.expression.operator import Where, And, Key

        original = And([Key("status") == "active", Key("score") > 0.5])
        dict_form = original.to_dict()
        restored = Where.from_dict(dict_form)
        assert restored.to_dict() == dict_form

    def test_rank_round_trip(self):
        """Test Rank round-trip conversion."""
        import numpy as np
        from chromadb.execution.expression.operator import Rank, Knn, Val

        original = Knn(query=[0.1, 0.2]) * 0.8 + Val(0.5) * 0.2
        dict_form = original.to_dict()
        restored = Rank.from_dict(dict_form)
        restored_dict = restored.to_dict()

        # Compare with float32 precision tolerance for KNN queries
        # The normalize_embeddings function converts to float32, causing precision differences
        def compare_dicts(d1, d2):
            if isinstance(d1, dict) and isinstance(d2, dict):
                if "$knn" in d1 and "$knn" in d2:
                    # Special handling for KNN queries
                    knn1, knn2 = d1["$knn"], d2["$knn"]
                    if "query" in knn1 and "query" in knn2:
                        # Compare queries with float32 precision
                        q1 = np.array(knn1["query"], dtype=np.float32)
                        q2 = np.array(knn2["query"], dtype=np.float32)
                        if not np.allclose(q1, q2):
                            return False
                        # Compare other fields exactly
                        for key in knn1:
                            if key != "query" and knn1[key] != knn2.get(key):
                                return False
                        return True

                # Recursively compare other dict structures
                if set(d1.keys()) != set(d2.keys()):
                    return False
                for key in d1:
                    if not compare_dicts(d1[key], d2[key]):
                        return False
                return True
            elif isinstance(d1, list) and isinstance(d2, list):
                if len(d1) != len(d2):
                    return False
                return all(compare_dicts(a, b) for a, b in zip(d1, d2))
            else:
                return d1 == d2

        assert compare_dicts(restored_dict, dict_form)

    def test_limit_round_trip(self):
        """Test Limit round-trip conversion."""
        from chromadb.execution.expression.operator import Limit

        original = Limit(limit=20, offset=10)
        dict_form = original.to_dict()
        restored = Limit.from_dict(dict_form)
        assert restored.to_dict() == dict_form

    def test_select_round_trip(self):
        """Test Select round-trip conversion."""
        from chromadb.execution.expression.operator import Select, Key

        original = Select(keys={Key.DOCUMENT, Key("title"), Key.SCORE})
        dict_form = original.to_dict()
        restored = Select.from_dict(dict_form)
        # Note: Set order might differ, so compare sets
        assert set(restored.to_dict()["keys"]) == set(dict_form["keys"])

    def test_search_round_trip(self):
        """Test Search round-trip through dict inputs."""
        import numpy as np
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Key, Knn, Limit, Select

        original_search = Search(
            where=Key("status") == "active",
            rank=Knn(query=[0.1, 0.2]),
            limit=Limit(limit=10),
            select=Select(keys={Key.DOCUMENT}),
        )

        # Convert to dict
        search_dict = original_search.to_dict()

        # Create new Search from dicts
        new_search = Search(
            where=search_dict["filter"] if search_dict["filter"] else None,
            rank=search_dict["rank"] if search_dict["rank"] else None,
            limit=search_dict["limit"],
            select=search_dict["select"],
        )

        # Get new dict
        new_dict = new_search.to_dict()

        # Compare with float32 tolerance for KNN queries
        # Use the same comparison function as test_rank_round_trip
        def compare_search_dicts(d1, d2):
            if isinstance(d1, dict) and isinstance(d2, dict):
                # Special handling for rank field with KNN
                if "rank" in d1 and "rank" in d2:
                    rank1, rank2 = d1["rank"], d2["rank"]
                    if isinstance(rank1, dict) and isinstance(rank2, dict):
                        if "$knn" in rank1 and "$knn" in rank2:
                            knn1, knn2 = rank1["$knn"], rank2["$knn"]
                            if "query" in knn1 and "query" in knn2:
                                q1 = np.array(knn1["query"], dtype=np.float32)
                                q2 = np.array(knn2["query"], dtype=np.float32)
                                if not np.allclose(q1, q2):
                                    return False
                                # Compare other KNN fields
                                for key in knn1:
                                    if key != "query" and knn1[key] != knn2.get(key):
                                        return False
                                # Compare other fields in the dict
                                for key in d1:
                                    if key != "rank" and d1[key] != d2.get(key):
                                        return False
                                return True

                # Normal dict comparison
                if set(d1.keys()) != set(d2.keys()):
                    return False
                for key in d1:
                    if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                        if not compare_search_dicts(d1[key], d2[key]):
                            return False
                    elif d1[key] != d2[key]:
                        return False
                return True
            else:
                return d1 == d2

        assert compare_search_dicts(new_dict, search_dict)

    def test_search_round_trip_with_group_by(self):
        """Test Search round-trip with group_by."""
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Key, GroupBy, MinK

        original = Search(
            where=Key("status") == "active",
            group_by=GroupBy(
                keys=[Key("category")],
                aggregate=MinK(keys=[Key.SCORE], k=3),
            ),
        )

        # Verify group_by round-trip
        search_dict = original.to_dict()
        assert search_dict["group_by"]["keys"] == ["category"]
        assert search_dict["group_by"]["aggregate"] == {
            "$min_k": {"keys": ["#score"], "k": 3}
        }

        # Reconstruct and compare group_by
        restored = Search(group_by=GroupBy.from_dict(search_dict["group_by"]))
        assert restored.to_dict()["group_by"] == search_dict["group_by"]
