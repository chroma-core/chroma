import random
import uuid
from random import randint
from typing import cast, List, Any, Dict
import pytest
import hypothesis.strategies as st
from hypothesis import given, settings
from chromadb.api import ServerAPI
from chromadb.api.types import Embeddings, Metadatas
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.utils.batch_utils import create_batches

collection_st = st.shared(strategies.collections(with_hnsw_params=True), key="coll")


@given(collection=collection_st, record_set=strategies.recordsets(collection_st))
@settings(deadline=None)
def test_add(
    api: ServerAPI,
    collection: strategies.Collection,
    record_set: strategies.RecordSet,
) -> None:
    api.reset()

    # TODO: Generative embedding functions
    coll = api.create_collection(
        name=collection.name,
        metadata=collection.metadata,  # type: ignore
        embedding_function=collection.embedding_function,
    )
    normalized_record_set = invariants.wrap_all(record_set)

    if not invariants.is_metadata_valid(normalized_record_set):
        with pytest.raises(Exception):
            coll.add(**normalized_record_set)
        return

    coll.add(**record_set)

    invariants.count(coll, cast(strategies.RecordSet, normalized_record_set))
    n_results = max(1, (len(normalized_record_set["ids"]) // 10))
    invariants.ann_accuracy(
        coll,
        cast(strategies.RecordSet, normalized_record_set),
        n_results=n_results,
        embedding_function=collection.embedding_function,
    )


def create_large_recordset(
    min_size: int = 45000,
    max_size: int = 50000,
) -> strategies.RecordSet:
    size = randint(min_size, max_size)

    ids = [str(uuid.uuid4()) for _ in range(size)]
    metadatas = [{"some_key": f"{i}"} for i in range(size)]
    documents = [f"Document {i}" for i in range(size)]
    embeddings = [[1, 2, 3] for _ in range(size)]
    record_set: Dict[str, List[Any]] = {
        "ids": ids,
        "embeddings": cast(Embeddings, embeddings),
        "metadatas": metadatas,
        "documents": documents,
    }
    return cast(strategies.RecordSet, record_set)


@given(collection=collection_st)
@settings(deadline=None, max_examples=1)
def test_add_large(api: ServerAPI, collection: strategies.Collection) -> None:
    api.reset()
    record_set = create_large_recordset(
        min_size=api.max_batch_size,
        max_size=api.max_batch_size + int(api.max_batch_size * random.random()),
    )
    coll = api.create_collection(
        name=collection.name,
        metadata=collection.metadata,  # type: ignore
        embedding_function=collection.embedding_function,
    )
    normalized_record_set = invariants.wrap_all(record_set)

    if not invariants.is_metadata_valid(normalized_record_set):
        with pytest.raises(Exception):
            coll.add(**normalized_record_set)
        return
    for batch in create_batches(
        api=api,
        ids=cast(List[str], record_set["ids"]),
        embeddings=cast(Embeddings, record_set["embeddings"]),
        metadatas=cast(Metadatas, record_set["metadatas"]),
        documents=cast(List[str], record_set["documents"]),
    ):
        coll.add(*batch)
    invariants.count(coll, cast(strategies.RecordSet, normalized_record_set))


@given(collection=collection_st)
@settings(deadline=None, max_examples=1)
def test_add_large_exceeding(api: ServerAPI, collection: strategies.Collection) -> None:
    api.reset()
    record_set = create_large_recordset(
        min_size=api.max_batch_size,
        max_size=api.max_batch_size + int(api.max_batch_size * random.random()),
    )
    coll = api.create_collection(
        name=collection.name,
        metadata=collection.metadata,  # type: ignore
        embedding_function=collection.embedding_function,
    )
    normalized_record_set = invariants.wrap_all(record_set)

    if not invariants.is_metadata_valid(normalized_record_set):
        with pytest.raises(Exception):
            coll.add(**normalized_record_set)
        return
    with pytest.raises(Exception) as e:
        coll.add(**record_set)
    assert "exceeds maximum batch size" in str(e.value)


# TODO: This test fails right now because the ids are not sorted by the input order
@pytest.mark.xfail(
    reason="This is expected to fail right now. We should change the API to sort the \
    ids by input order."
)
def test_out_of_order_ids(api: ServerAPI) -> None:
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

    coll = api.create_collection(
        "test", embedding_function=lambda input: [[1, 2, 3] for _ in input]  # type: ignore
    )
    embeddings: Embeddings = [[1, 2, 3] for _ in ooo_ids]
    coll.add(ids=ooo_ids, embeddings=embeddings)
    get_ids = coll.get(ids=ooo_ids)["ids"]
    assert get_ids == ooo_ids


def test_add_partial(api: ServerAPI) -> None:
    """Tests adding a record set with some of the fields set to None."""

    api.reset()

    coll = api.create_collection("test")
    # TODO: We need to clean up the api types to support this typing
    coll.add(
        ids=["1", "2", "3"],
        embeddings=[[1, 2, 3], [1, 2, 3], [1, 2, 3]],  # type: ignore
        metadatas=[{"a": 1}, None, {"a": 3}],  # type: ignore
        documents=["a", "b", None],  # type: ignore
    )

    results = coll.get()
    assert results["ids"] == ["1", "2", "3"]
    assert results["metadatas"] == [{"a": 1}, None, {"a": 3}]
    assert results["documents"] == ["a", "b", None]
