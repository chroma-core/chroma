# type: ignore
import chromadb
import chromadb.server.fastapi
import pytest
import tempfile


@pytest.fixture
def ephemeral_api():
    return chromadb.EphemeralClient()


@pytest.fixture
def persistent_api():
    return chromadb.PersistentClient(
        path=tempfile.gettempdir() + "/test_server",
    )


@pytest.fixture
def persistent_api_cache_bust():
    return chromadb.PersistentClient(
        path=tempfile.gettempdir() + "/test_server",
    )


@pytest.fixture
def http_api():
    return chromadb.HttpClient()


# verify the ephemeral api is set to ephemeral mode (can't persist)
@pytest.mark.parametrize("api_fixture", [ephemeral_api])
def test_ephemeral_client(api_fixture, request):
    api = request.getfixturevalue("ephemeral_api")
    api.reset()
    collection = api.create_collection("test")
    collection.add(ids="id1", documents="hello")

    # should raise a NotImplementedError when calling persist
    with pytest.raises(NotImplementedError):
        api.persist()


# verify the persistent api is set to persistent mode (can persist)
@pytest.mark.parametrize("api_fixture", [persistent_api])
def test_persistent_client(api_fixture, request):
    api = request.getfixturevalue("persistent_api")
    api.reset()
    collection = api.create_collection("test")
    collection.add(ids="id1", documents="hello")

    api.persist()
    del api

    api2 = request.getfixturevalue("persistent_api_cache_bust")
    collection = api2.get_collection("test")

    nn = collection.query(
        query_texts="hello",
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )
    for key in nn.keys():
        assert len(nn[key]) == 1


# verify the http api is set to http mode (can't persist)
@pytest.mark.parametrize("api_fixture", [http_api])
def test_http_client(api_fixture, request):
    api = request.getfixturevalue("http_api")
    api.reset()
    collection = api.create_collection("test")
    collection.add(ids="id1", documents="hello")

    # should raise a NotImplementedError when calling persist
    # Exception: {"error":"NotImplementedError('Clickhouse is a persistent database, this method is not needed')"}
    with pytest.raises(Exception):
        api.persist()
