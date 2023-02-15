import chromadb
from chromadb.api.types import QueryResult
from chromadb.config import Settings
from chromadb.errors import NoDatapointsException
import chromadb.server.fastapi
import pytest
import time
import tempfile
import copy
import os
from multiprocessing import Process
import uvicorn
from requests.exceptions import ConnectionError
from chromadb.api.models import Collection


@pytest.fixture
def local_api():
    return chromadb.Client(
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb",
            persist_directory=tempfile.gettempdir(),
        )
    )


@pytest.fixture
def local_persist_api():
    return chromadb.Client(
        Settings(
            chroma_api_impl="local",
            chroma_db_impl="duckdb+parquet",
            persist_directory=tempfile.gettempdir() + "/test_server",
        )
    )


@pytest.fixture
def fastapi_integration_api():
    return chromadb.Client()  # configured by environment variables


def _build_fastapi_api():
    return chromadb.Client(
        Settings(
            chroma_api_impl="rest", chroma_server_host="localhost", chroma_server_http_port="6666"
        )
    )


@pytest.fixture
def fastapi_api():
    return _build_fastapi_api()


def run_server():
    settings = Settings(
        chroma_api_impl="local",
        chroma_db_impl="duckdb",
        persist_directory=tempfile.gettempdir() + "/test_server",
    )
    server = chromadb.server.fastapi.FastAPI(settings)
    uvicorn.run(server.app(), host="0.0.0.0", port=6666, log_level="info")


def await_server(attempts=0):
    api = _build_fastapi_api()

    try:
        api.heartbeat()
    except ConnectionError as e:
        if attempts > 10:
            raise e
        else:
            time.sleep(2)
            await_server(attempts + 1)


@pytest.fixture(scope="module", autouse=True)
def fastapi_server():
    proc = Process(target=run_server, args=(), daemon=True)
    proc.start()
    await_server()
    yield
    proc.kill()


test_apis = [local_api, fastapi_api]

if "CHROMA_INTEGRATION_TEST" in os.environ:
    print("Including integration tests")
    test_apis.append(fastapi_integration_api)


@pytest.mark.parametrize("api_fixture", [local_persist_api])
def test_persist(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()

    collection = api.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2

    api.persist()
    del api

    api = request.getfixturevalue(api_fixture.__name__)
    collection = api.get_collection("testspace")
    assert collection.count() == 2

    api.delete_collection("testspace")
    api.persist()
    del api

    api = request.getfixturevalue(api_fixture.__name__)
    assert api.list_collections() == []


@pytest.mark.parametrize("api_fixture", test_apis)
def test_heartbeat(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    assert isinstance(api.heartbeat(), int)


batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com", "https://example.com"],
}


@pytest.mark.parametrize("api_fixture", test_apis)
def test_add(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()

    collection = api.create_collection("testspace")

    collection.add(**batch_records)

    assert collection.count() == 2


minimal_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com", "https://example.com"],
}


@pytest.mark.parametrize("api_fixture", test_apis)
def test_add_minimal(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()

    collection = api.create_collection("testspace")

    collection.add(**minimal_records)

    assert collection.count() == 2


@pytest.mark.parametrize("api_fixture", test_apis)
def test_get_from_db(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    records = collection.get()
    for key in records.keys():
        assert len(records[key]) == 2


@pytest.mark.parametrize("api_fixture", test_apis)
def test_reset_db(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()

    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    assert api.reset()
    assert len(api.list_collections()) == 0


@pytest.mark.parametrize("api_fixture", test_apis)
def test_get_nearest_neighbors(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    # assert api.create_index(collection_name="testspace") # default is auto now

    nn = collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1, where={})

    for key in nn.keys():
        assert len(nn[key]) == 1


@pytest.mark.parametrize("api_fixture", test_apis)
def test_get_nearest_neighbors_filter(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)

    # assert api.create_index(collection_name="testspace") # default is auto now

    with pytest.raises(Exception) as e:
        nn = collection.query(
            query_embeddings=[[1.1, 2.3, 3.2]], n_results=1, where={"distance": "false"}
        )

    assert str(e.value).__contains__("found")


@pytest.mark.parametrize("api_fixture", test_apis)
def test_delete(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    # generic delete on collection not working yet
    # assert collection.delete() == []
    # assert collection.count() == 2
    # assert collection.delete()
    # assert collection.count() == 0


@pytest.mark.parametrize("api_fixture", test_apis)
def test_delete_with_index(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2
    # api.create_index()
    nn = collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1)

    # assert nn['embeddings']['inference_class'][0] == 'knife'

    # assert api.delete(where={"inference_class": "knife"})

    # nn2 = api.get_nearest_neighbors(embedding=[1.1, 2.3, 3.2],
    #                                 n_results=1)
    # assert nn2['embeddings']['inference_class'][0] == 'person'


@pytest.mark.parametrize("api_fixture", test_apis)
def test_count(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    assert collection.count() == 0
    collection.add(**batch_records)
    assert collection.count() == 2


@pytest.mark.parametrize("api_fixture", test_apis)
def test_modify(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.modify(name="testspace2")

    # collection name is modify
    assert collection.name == "testspace2"


@pytest.mark.parametrize("api_fixture", test_apis)
def test_increment_index_on(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    # increment index
    # collection.create_index(index_type="hnsw", index_params={"M": 16, "efConstruction": 200})
    nn = collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1)
    for key in nn.keys():
        assert len(nn[key]) == 1


@pytest.mark.parametrize("api_fixture", test_apis)
def test_increment_index_off(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records, increment_index=False)
    assert collection.count() == 2

    # incremental index
    collection.create_index()
    nn = collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1)
    for key in nn.keys():
        assert len(nn[key]) == 1


@pytest.mark.parametrize("api_fixture", test_apis)
def skipping_indexing_will_fail(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records, increment_index=False)
    assert collection.count() == 2

    # incremental index
    with pytest.raises(Exception) as e:
        nn = collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1)
    assert str(e.value).__contains__("index not found")


@pytest.mark.parametrize("api_fixture", test_apis)
def test_add_a_collection(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    api.create_collection("testspace")

    # get collection does not throw an error
    collection = api.get_collection("testspace")
    assert collection.name == "testspace"


@pytest.mark.parametrize("api_fixture", test_apis)
def test_list_collections(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    api.create_collection("testspace")
    api.create_collection("testspace2")

    # get collection does not throw an error
    collections = api.list_collections()
    assert len(collections) == 2


@pytest.mark.parametrize("api_fixture", test_apis)
def test_reset(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    api.create_collection("testspace")
    api.create_collection("testspace2")

    # get collection does not throw an error
    collections = api.list_collections()
    assert len(collections) == 2

    api.reset()
    collections = api.list_collections()
    assert len(collections) == 0


@pytest.mark.parametrize("api_fixture", test_apis)
def test_peek(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("testspace")
    collection.add(**batch_records)
    assert collection.count() == 2

    # peek
    peek = collection.peek()
    for key in peek.keys():
        assert len(peek[key]) == 2


#### TEST METADATA AND METADATA FILTERING ####
# region

metadata_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [{"int_value": 1, "string_value": "one", "float_value": 1.001}, {"int_value": 2}],
}


@pytest.mark.parametrize("api_fixture", test_apis)
def test_metadata_add_get_int_float(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(ids=["id1", "id2"])
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["float_value"] == 1.001
    assert items["metadatas"][1]["int_value"] == 2
    assert type(items["metadatas"][0]["int_value"]) == int
    assert type(items["metadatas"][0]["float_value"]) == float


@pytest.mark.parametrize("api_fixture", test_apis)
def test_metadata_add_query_int_float(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items: QueryResult = collection.query(query_embeddings=[[1.1, 2.3, 3.2]], n_results=1)
    assert items["metadatas"][0][0]["int_value"] == 1
    assert items["metadatas"][0][0]["float_value"] == 1.001
    assert type(items["metadatas"][0][0]["int_value"]) == int
    assert type(items["metadatas"][0][0]["float_value"]) == float


@pytest.mark.parametrize("api_fixture", test_apis)
def test_metadata_get_where_string(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"string_value": "one"})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"


@pytest.mark.parametrize("api_fixture", test_apis)
def test_metadata_get_where_int(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"int_value": 1})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"


@pytest.mark.parametrize("api_fixture", test_apis)
def test_metadata_get_where_float(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    items = collection.get(where={"float_value": 1.001})
    assert items["metadatas"][0]["int_value"] == 1
    assert items["metadatas"][0]["string_value"] == "one"
    assert items["metadatas"][0]["float_value"] == 1.001


@pytest.mark.parametrize("api_fixture", test_apis)
def test_metadata_update_get_int_float(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_int")
    collection.add(**metadata_records)

    collection.update(
        ids=["id1"], metadatas=[{"int_value": 2, "string_value": "two", "float_value": 2.002}]
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


@pytest.mark.parametrize("api_fixture", test_apis)
def test_metadata_validation_add(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_metadata_validation")
    with pytest.raises(ValueError) as e:
        collection.add(**bad_metadata_records)
    assert "Metadata" in str(e.value)


@pytest.mark.parametrize("api_fixture", test_apis)
def test_metadata_validation_update(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_metadata_validation")
    collection.add(**metadata_records)
    with pytest.raises(ValueError) as e:
        collection.update(ids=["id1"], metadatas={"value": {"nested": "5"}})
    assert "Metadata" in str(e.value)


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_validation_get(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_validation")
    with pytest.raises(ValueError) as e:
        collection.get(where={"value": {"nested": "5"}})
    assert "Where" in str(e.value)


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_validation_query(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_validation")
    with pytest.raises(ValueError) as e:
        collection.query(query_embeddings=[0, 0, 0], where={"value": {"nested": "5"}})
    assert "Where" in str(e.value)


operator_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["id1", "id2"],
    "metadatas": [
        {"int_value": 1, "string_value": "one", "float_value": 1.001},
        {"int_value": 2, "float_value": 2.002, "string_value": "two"},
    ],
}


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_lt(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_lt")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$lt": 2}})
    assert len(items["metadatas"]) == 1


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_lte(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$lte": 2.0}})
    assert len(items["metadatas"]) == 2


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_gt(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"float_value": {"$gt": -1.4}})
    assert len(items["metadatas"]) == 2


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_gte(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"float_value": {"$gte": 2.002}})
    assert len(items["metadatas"]) == 1


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_ne_string(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"string_value": {"$ne": "two"}})
    assert len(items["metadatas"]) == 1


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_ne_eq_number(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_lte")
    collection.add(**operator_records)
    items = collection.get(where={"int_value": {"$ne": 1}})
    assert len(items["metadatas"]) == 1
    items = collection.get(where={"float_value": {"$eq": 2.002}})
    assert len(items["metadatas"]) == 1


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_valid_operators(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_valid_operators")
    collection.add(**operator_records)
    with pytest.raises(ValueError) as e:
        collection.get(where={"int_value": {"$invalid": 2}})

    with pytest.raises(ValueError) as e:
        collection.get(where={"int_value": {"$lt": "2"}})

    with pytest.raises(ValueError) as e:
        collection.get(where={"int_value": {"$lt": 2, "$gt": 1}})

    # Test invalid $and, $or
    with pytest.raises(ValueError) as e:
        collection.get(where={"$and": {"int_value": {"$lt": 2}}})

    with pytest.raises(ValueError) as e:
        collection.get(where={"int_value": {"$lt": 2}, "$or": {"int_value": {"$gt": 1}}})

    with pytest.raises(ValueError) as e:
        collection.get(where={"$gt": [{"int_value": {"$lt": 2}}, {"int_value": {"$gt": 1}}]})

    with pytest.raises(ValueError) as e:
        collection.get(where={"$or": [{"int_value": {"$lt": 2}}]})

    with pytest.raises(ValueError) as e:
        collection.get(where={"$or": []})

    with pytest.raises(ValueError) as e:
        collection.get(where={"a": {"$contains": "test"}})

    with pytest.raises(ValueError) as e:
        collection.get(
            where={
                "$or": [
                    {"a": {"$contains": "first"}},  # invalid
                    {"$contains": "second"},  # valid
                ]
            }
        )


@pytest.mark.parametrize("api_fixture", test_apis)
def test_query_document_valid_operators(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_where_valid_operators")
    collection.add(**operator_records)
    with pytest.raises(ValueError) as e:
        collection.get(where_document={"$lt": {"$nested": 2}})
    assert "Where document" in str(e.value)

    with pytest.raises(ValueError) as e:
        collection.query(query_embeddings=[0, 0, 0], where_document={"$contains": 2})
    assert "Where document" in str(e.value)

    with pytest.raises(ValueError) as e:
        collection.get(where_document={"$contains": []})
    assert "Where document" in str(e.value)

    # Test invalid $and, $or
    with pytest.raises(ValueError) as e:
        collection.get(where_document={"$and": {"$unsupported": "doc"}})

    with pytest.raises(ValueError) as e:
        collection.get(where_document={"$or": [{"$unsupported": "doc"}, {"$unsupported": "doc"}]})

    with pytest.raises(ValueError) as e:
        collection.get(where_document={"$or": [{"$contains": "doc"}]})

    with pytest.raises(ValueError) as e:
        collection.get(where_document={"$or": []})

    with pytest.raises(ValueError) as e:
        collection.get(
            where_document={"$or": [{"$and": [{"$contains": "doc"}]}, {"$contains": "doc"}]}
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


@pytest.mark.parametrize("api_fixture", test_apis)
def test_get_where_document(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_get_where_document")
    collection.add(**contains_records)

    items = collection.get(where_document={"$contains": "doc1"})
    assert len(items["metadatas"]) == 1

    items = collection.get(where_document={"$contains": "great"})
    assert len(items["metadatas"]) == 2

    items = collection.get(where_document={"$contains": "bad"})
    assert len(items["metadatas"]) == 0


@pytest.mark.parametrize("api_fixture", test_apis)
def test_query_where_document(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

    api.reset()
    collection = api.create_collection("test_query_where_document")
    collection.add(**contains_records)

    items = collection.query(
        query_embeddings=[0, 0, 0], where_document={"$contains": "doc1"}, n_results=1
    )
    assert len(items["metadatas"][0]) == 1

    items = collection.query(
        query_embeddings=[0, 0, 0], where_document={"$contains": "great"}, n_results=2
    )
    assert len(items["metadatas"][0]) == 2

    with pytest.raises(NoDatapointsException) as e:
        items = collection.query(
            query_embeddings=[0, 0, 0], where_document={"$contains": "bad"}, n_results=1
        )


@pytest.mark.parametrize("api_fixture", test_apis)
def test_delete_where_document(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

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
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2], [1.3, 2.25, 3.2], [1.4, 2.26, 3.2]],
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


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_logical_operators(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

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
                {"$and": [{"int_value": {"$eq": 3}}, {"string_value": {"$eq": "three"}}]},
                {"$and": [{"int_value": {"$eq": 4}}, {"string_value": {"$eq": "four"}}]},
            ]
        }
    )
    assert len(items["metadatas"]) == 2

    items = collection.get(
        where={
            "$or": [
                {"$and": [{"int_value": {"$eq": 3}}, {"string_value": {"$eq": "three"}}]},
                {"$and": [{"int_value": {"$eq": 4}}, {"string_value": {"$eq": "four"}}]},
            ],
            "$and": [{"is": "doc"}, {"string_value": "four"}],
        }
    )
    assert len(items["metadatas"]) == 1


@pytest.mark.parametrize("api_fixture", test_apis)
def test_where_document_logical_operators(api_fixture, request):
    api = request.getfixturevalue(api_fixture.__name__)

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
