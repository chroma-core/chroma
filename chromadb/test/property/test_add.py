import pytest
from hypothesis import given
import chromadb
from chromadb.api import API
from chromadb.test.configurations import configurations
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants


@pytest.fixture(scope="module", params=configurations())
def api(request):
    configuration = request.param
    return chromadb.Client(configuration)


@given(collection=strategies.collections(), embeddings=strategies.embedding_set())
def test_add(
    api: API, collection: strategies.Collection, embeddings: strategies.EmbeddingSet
):
    api.reset()

    # TODO: Generative embedding functions
    coll = api.create_collection(**collection, embedding_function=lambda x: None)
    coll.add(**embeddings)

    invariants.count(
        api,
        coll.name,
        len(embeddings["ids"]),
    )
    invariants.ann_accuracy(coll, embeddings, n_results=len(embeddings["ids"]))


# TODO: This test fails right now because the ids are not sorted by the input order
@pytest.mark.xfail(
    reason="This is expected to fail right now. We should change the API to sort the \
    ids by input order."
)
def test_out_of_order_ids(api: API):
    api.reset()
    ooo_ids = [
        "40",
        "05",
        "8",
        "6",
        "10",
        "01",
        "00",
        "3",
        "04",
        "20",
        "02",
        "9",
        "30",
        "11",
        "13",
        "2",
        "0",
        "7",
        "06",
        "5",
        "50",
        "12",
        "03",
        "4",
        "1",
    ]
    coll = api.create_collection("test", embedding_function=lambda x: [1, 2, 3])
    coll.add(ids=ooo_ids, embeddings=[[1, 2, 3] for _ in range(len(ooo_ids))])
    get_ids = coll.get(ids=ooo_ids)["ids"]
    assert get_ids == ooo_ids
