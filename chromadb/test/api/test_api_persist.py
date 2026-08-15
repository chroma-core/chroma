# type: ignore
from typing import Any

import numpy as np
import pytest

from chromadb.api.types import Document, EmbeddingFunction

batch_records = {
    "embeddings": [[1.1, 2.3, 3.2], [1.2, 2.24, 3.2]],
    "ids": ["https://example.com/1", "https://example.com/2"],
}


@pytest.mark.parametrize("api_fixture", ["local_persist_api"])
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


@pytest.mark.parametrize("api_fixture", ["local_persist_api"])
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


@pytest.mark.parametrize("api_fixture", ["local_persist_api"])
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


@pytest.mark.parametrize("api_fixture", ["local_persist_api"])
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


def test_persist_index_loading_params(client, request):
    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection(
        "test",
        metadata={"hnsw:space": "ip"},
    )
    collection.add(ids="id1", documents="hello")
    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection("test")
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
