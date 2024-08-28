import pytest
import tempfile

from typing import List, cast
from chromadb.api import ClientAPI
from chromadb.test.api.utils import records
from chromadb.api.types import EmbeddingFunction, Documents, IncludeEnum
import chromadb
from chromadb.config import Settings
from typing import Generator
import os
import shutil

persist_dir = tempfile.mkdtemp()

@pytest.fixture
def local_persist_api():
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
def local_persist_api_cache_bust() -> Generator[ClientAPI, None, None]:
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

@pytest.mark.parametrize("api_fixture", [local_persist_api])
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

@pytest.mark.parametrize("api_fixture", [local_persist_api])  # type: ignore[no-untyped-def]
def test_persist_index_loading(api_fixture, request):
    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection("test")
    collection.add(ids="id1", documents="hello")

    api2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = api2.get_collection("test")

    includes: List[IncludeEnum] = cast(
        List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
    )
    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=cast(
            List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
        ),
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1
        elif key == "included":
            assert set(nn[key]) == set(includes)
        else:
            assert nn[key] is None


@pytest.mark.parametrize("api_fixture", [local_persist_api])  # type: ignore[no-untyped-def]
def test_persist_index_loading_embedding_function(api_fixture, request):
    class TestEF(EmbeddingFunction[Documents]):
        def __call__(self, input):  # type: ignore[no-untyped-def]
            return [[1, 2, 3] for _ in range(len(input))]

    client = request.getfixturevalue("local_persist_api")
    client.reset()
    collection = client.create_collection("test", embedding_function=TestEF())
    collection.add(ids="id1", documents="hello")

    client2 = request.getfixturevalue("local_persist_api_cache_bust")
    collection = client2.get_collection("test", embedding_function=TestEF())

    includes: List[IncludeEnum] = cast(
        List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
    )
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


def test_index_params(client: ClientAPI) -> None:
    EPS = 1e-12
    # first standard add
    client.reset()
    collection = client.create_collection(name="test_index_params")
    collection.add(**records)  # type: ignore[arg-type]
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert (items["distances"] or [])[0][0] > 4

    # cosine
    client.reset()
    collection = client.create_collection(
        name="test_index_params",
        metadata={"hnsw:space": "cosine", "hnsw:construction_ef": 20, "hnsw:M": 5},
    )
    collection.add(**records)  # type: ignore[arg-type]
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert (items["distances"] or [])[0][0] > 0 - EPS
    assert (items["distances"] or [])[0][0] < 1 + EPS

    # ip
    client.reset()
    collection = client.create_collection(
        name="test_index_params", metadata={"hnsw:space": "ip"}
    )
    collection.add(**records)  # type: ignore[arg-type]
    items = collection.query(
        query_embeddings=[0.6, 1.12, 1.6],
        n_results=1,
    )
    assert (items["distances"] or [])[0][0] < -5


def test_invalid_index_params(client: ClientAPI) -> None:
    client.reset()

    with pytest.raises(Exception):
        collection = client.create_collection(
            name="test_index_params", metadata={"hnsw:foobar": "blarg"}
        )
        collection.add(**records)  # type: ignore[arg-type]

    with pytest.raises(Exception):
        collection = client.create_collection(
            name="test_index_params", metadata={"hnsw:space": "foobar"}
        )
        collection.add(**records)  # type: ignore[arg-type]


def test_persist_index_loading_params(
    client: ClientAPI, request: pytest.FixtureRequest
) -> None:
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
    includes: List[IncludeEnum] = cast(
        List[IncludeEnum], ["embeddings", "documents", "metadatas", "distances"]
    )
    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=includes,
    )
    for key in nn.keys():
        if (key in includes) or (key == "ids"):
            assert len(nn[key]) == 1  # type: ignore[literal-required]
        elif key == "included":
            assert set(nn[key]) == set(includes)  # type: ignore[literal-required]
        else:
            assert nn[key] is None  # type: ignore[literal-required]


def test_modify_warn_on_DF_change(client: ClientAPI) -> None:
    client.reset()

    collection = client.create_collection("testspace")

    with pytest.raises(Exception, match="not supported"):
        collection.modify(metadata={"hnsw:space": "cosine"})
