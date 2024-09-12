import pytest

from chromadb.api import ClientAPI
from chromadb.api.types import EmbeddingFunction, Documents
from chromadb.test.api.utils import batch_records
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


def test_collection_peek_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.peek()


def test_get_a_nonexistent_collection(client: ClientAPI) -> None:
    client.reset()

    # get collection should throw an error if collection does not exist
    with pytest.raises(Exception):
        client.get_collection("testspace2")


def test_error_includes_trace_id(http_client: ClientAPI) -> None:
    http_client.reset()

    with pytest.raises(ChromaError) as error:
        http_client.get_collection("testspace2")

    assert error.value.trace_id is not None


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


def test_collection_count_with_invalid_collection_throws(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    client.delete_collection("test")

    with pytest.raises(
        InvalidCollectionException, match=r"Collection .* does not exist."
    ):
        collection.count()
