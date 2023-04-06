import pytest
from hypothesis import given, settings
import hypothesis.strategies as st
import chromadb
from chromadb.api.models.Collection import Collection
from chromadb.test.configurations import configurations
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants


@pytest.fixture(scope="module", params=configurations())
def api(request):
    configuration = request.param
    return chromadb.Client(configuration)


@given(collection=strategies.collections(), embeddings=strategies.embeddings())
def test_update(api, collection, embeddings):
    api.reset()

    # Implement by using a custom composite strategy that generates the embeddings
    # along with a selection of values to update
    raise NotImplementedError("TODO: Implement this test")
