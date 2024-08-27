# type: ignore
import traceback
import httpx

import chromadb
from chromadb.errors import ChromaError
from chromadb.api.fastapi import FastAPI
from chromadb.api.types import QueryResult, EmbeddingFunction, Document
from chromadb.config import Settings
from chromadb.errors import InvalidCollectionException
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

# https://docs.pytest.org/en/6.2.x/fixture.html#fixtures-can-be-requested-more-than-once-per-test-return-values-are-cached
@pytest.fixture
def local_persist_api_cache_bust():
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


def test_heartbeat(client):
    heartbeat_ns = client.heartbeat()
    assert isinstance(heartbeat_ns, int)

    heartbeat_s = heartbeat_ns // 10**9
    heartbeat = datetime.fromtimestamp(heartbeat_s)
    assert heartbeat > datetime.now() - timedelta(seconds=10)


def test_max_batch_size(client):
    print(client)
    batch_size = client.get_max_batch_size()
    assert batch_size > 0


def test_pre_flight_checks(client):
    if not isinstance(client, FastAPI):
        pytest.skip("Not a FastAPI instance")

    resp = httpx.get(f"{client._api_url}/pre-flight-checks")
    assert resp.status_code == 200
    assert resp.json() is not None
    assert "max_batch_size" in resp.json().keys()


batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


minimal_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


def test_reset_db(client):
    client.reset()

    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    client.reset()
    assert len(client.list_collections()) == 0


def test_delete(client):
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    with pytest.raises(Exception):
        collection.delete()


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

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
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

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
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

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
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
    # Test create metatdata
    collection = client.create_collection("testspace", metadata=metadata_a)
    assert collection.metadata is not None
    assert collection.metadata["a"] == 1
    assert collection.metadata["b"] == 2

    # Test get metatdata
    collection = client.get_collection("testspace")
    assert collection.metadata is not None
    assert collection.metadata["a"] == 1
    assert collection.metadata["b"] == 2

    # Test modify metatdata
    collection.modify(metadata={"a": 2, "c": 3})
    assert collection.metadata["a"] == 2
    assert collection.metadata["c"] == 3
    assert "b" not in collection.metadata

    # Test get after modify metatdata
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
    for key in peek.keys():
        if key in ["embeddings", "documents", "metadatas"] or key == "ids":
            assert len(peek[key]) == 2
        elif key == "included":
            assert set(peek[key]) == set(["embeddings", "metadatas", "documents"])
        else:
            assert peek[key] is None

metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2},
    ],
}


bad_metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [{"value": {"nested": "5"}}, {"value": [1, 2, 3]}],
}

operator_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}

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

contains_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "documents": ["this is doc1 and it's great!", "doc2 is also great!"],
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