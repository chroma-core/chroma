import pytest
import numpy as np

from chromadb.api import ClientAPI
from chromadb.api.types import EmbeddingFunction, Documents
from chromadb.test.api.utils import batch_records, records
from chromadb.errors import InvalidCollectionException, ChromaError
from chromadb.test.api.utils import (
    local_persist_api,
    local_persist_api_cache_bust,
)


@pytest.mark.parametrize(
    "api_fixture", [local_persist_api, local_persist_api_cache_bust]
)
def test_persist_index_get_or_create_embedding_function(api_fixture, request):  # type: ignore[no-untyped-def]
    class TestEF(EmbeddingFunction[Documents]):
        def __call__(self, input):  # type: ignore[no-untyped-def]
            return [[1, 2, 3] for _ in range(len(input))]

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
    assert nn["embeddings"] == [[[1, 2, 3]]]
    assert nn["documents"] == [["hello"]]
    assert nn["distances"] == [[0]]


@pytest.mark.parametrize("api_fixture", [local_persist_api, local_persist_api_cache_bust])  # type: ignore[no-untyped-def]
def test_persist(api_fixture, request) -> None:
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


def test_get_or_create(client: ClientAPI) -> None:
    client.reset()

    collection = client.create_collection("testspace")

    collection.add(**batch_records)  # type: ignore[arg-type]

    assert collection.count() == 2

    with pytest.raises(Exception):
        collection = client.create_collection("testspace")

    collection = client.get_or_create_collection("testspace")

    assert collection.count() == 2


# test delete_collection
def test_delete_collection(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test_delete_collection")
    collection.add(**records)  # type: ignore[arg-type]

    assert len(client.list_collections()) == 1
    client.delete_collection("test_delete_collection")
    assert len(client.list_collections()) == 0


def test_multiple_collections(client: ClientAPI) -> None:
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

    assert results1["ids"][0][0] == ids1[0]
    assert results2["ids"][0][0] == ids2[0]


def test_collection_peek_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.peek()


def test_add_a_collection(client: ClientAPI) -> None:
    client.reset()
    client.create_collection("testspace")

    # get collection does not throw an error
    collection = client.get_collection("testspace")
    assert collection.name == "testspace"

    # get collection should throw an error if collection does not exist
    with pytest.raises(Exception):
        collection = client.get_collection("testspace2")


def test_error_includes_trace_id(http_client: ClientAPI) -> None:
    http_client.reset()

    with pytest.raises(ChromaError) as error:
        http_client.get_collection("testspace2")

    assert error.value.trace_id is not None


def test_list_collections(client: ClientAPI) -> None:
    client.reset()
    client.create_collection("testspace")
    client.create_collection("testspace2")

    # get collection does not throw an error
    collections = client.list_collections()
    assert len(collections) == 2


def test_peek(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.add(**batch_records)  # type: ignore[arg-type]
    assert collection.count() == 2

    # peek
    peek = collection.peek()
    for key in peek.keys():
        if key in ["embeddings", "documents", "metadatas"] or key == "ids":
            assert len(peek[key]) == 2  # type: ignore[literal-required]
        elif key == "included":
            assert set(peek[key]) == set(["embeddings", "metadatas", "documents"])  # type: ignore[literal-required]
        else:
            assert peek[key] is None  # type: ignore[literal-required]


def test_metadata_cru(client: ClientAPI) -> None:
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


def test_modify_error_on_existing_name(client: ClientAPI) -> None:
    client.reset()

    client.create_collection("testspace")
    c2 = client.create_collection("testspace2")

    with pytest.raises(Exception):
        c2.modify(name="testspace")


def test_collection_modify_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.modify(name="test2")


def test_modify(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    collection.modify(name="testspace2")

    # collection name is modify
    assert collection.name == "testspace2"


def test_collection_delete_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.delete(ids=["id1"])


def test_count(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("testspace")
    assert collection.count() == 0
    collection.add(**batch_records)  # type: ignore[arg-type]
    assert collection.count() == 2


def test_collection_count_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.count()
