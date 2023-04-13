import pytest
from hypothesis import given
import chromadb
from chromadb.test.configurations import configurations
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants


@pytest.fixture(scope="module", params=configurations())
def api(request):
    configuration = request.param
    return chromadb.Client(configuration)


@given(collection=strategies.collections(), embeddings=strategies.embedding_set())
def test_add(api, collection, embeddings):

    api.reset()

    # TODO: Generative embedding functions
    coll = api.create_collection(**collection, embedding_function=lambda x: None)
    coll.add(**embeddings)

    invariants.count(
        api,
        coll.name,
        len(embeddings["ids"]),
    )
    invariants.ann_accuracy(coll, embeddings)
