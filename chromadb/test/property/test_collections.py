import pytest
from hypothesis import given
import chromadb
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from chromadb.test.configurations import configurations
import chromadb.test.property.strategies as strategies


@pytest.fixture(scope="module", params=configurations())
def api(request):
    configuration = request.param
    return chromadb.Client(configuration)


@given(coll=strategies.collections())
def test_create_collection(api: API, coll: strategies.Collection):
    api.reset()
    c = api.create_collection(coll["name"], metadata=coll["metadata"])
    assert isinstance(c, Collection)
    assert c.name == coll["name"]
    assert c.metadata == coll["metadata"]
