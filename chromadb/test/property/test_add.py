import pytest
import hypothesis.strategies as st
from hypothesis import given, settings
from chromadb.api import API
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants

collection_st = st.shared(strategies.collections(with_hnsw_params=True), key="coll")


@given(collection=collection_st, embeddings=strategies.recordsets(collection_st))
@settings(deadline=None)
def test_add(
    api: API, collection: strategies.Collection, embeddings: strategies.RecordSet
):
    api.reset()

    # TODO: Generative embedding functions
    coll = api.create_collection(
        name=collection.name,
        metadata=collection.metadata,
        embedding_function=collection.embedding_function,
    )
    coll.add(**embeddings)

    embeddings = invariants.wrap_all(embeddings)
    invariants.count(coll, embeddings)
    n_results = max(1, (len(embeddings["ids"]) // 10))
    invariants.ann_accuracy(
        coll,
        embeddings,
        n_results=n_results,
        embedding_function=collection.embedding_function,
    )


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
