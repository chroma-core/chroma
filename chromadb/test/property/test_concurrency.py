from concurrent.futures import Future, ThreadPoolExecutor, wait
import multiprocessing
from typing import Any, List, cast
import hypothesis.strategies as st
from hypothesis import given, settings
import pytest
import random

from chromadb.api import API
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants

collection_st = st.shared(strategies.collections(with_hnsw_params=True), key="coll")


@given(
    collection=collection_st,
    record_set=strategies.recordsets(collection_st, min_size=10),
    num_workers=st.integers(min_value=2, max_value=multiprocessing.cpu_count() * 2),
)
@settings(deadline=None)
def test_many_threads_add(
    api: API,
    collection: strategies.Collection,
    record_set: strategies.RecordSet,
    num_workers: int,
) -> None:
    api.reset()

    coll = api.create_collection(
        name=collection.name,
        metadata=collection.metadata,
        embedding_function=collection.embedding_function,
    )
    normalized_record_set = invariants.wrap_all(record_set)

    if not invariants.is_metadata_valid(normalized_record_set):
        with pytest.raises(Exception):
            coll.add(**normalized_record_set)
        return

    with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() // 2) as executor:
        # Submit to the executor random batches of the record set until we have sent all of it
        # to the executor.
        total_sent = 0
        futures: List[Future[Any]] = []
        while total_sent < len(normalized_record_set["ids"]):
            to_send = random.randint(
                1, (len(record_set["ids"]) - total_sent) // num_workers
            )
            start = total_sent
            end = total_sent + to_send + 1
            future = executor.submit(
                coll.add,
                ids=normalized_record_set["ids"][start:end],
                embeddings=normalized_record_set["embeddings"][start:end]
                if normalized_record_set["embeddings"] is not None
                else None,
                metadatas=normalized_record_set["metadatas"][start:end]
                if normalized_record_set["metadatas"] is not None
                else None,
                documents=normalized_record_set["documents"][start:end]
                if normalized_record_set["documents"] is not None
                else None,
            )
            futures.append(future)
            total_sent += to_send
        wait(futures, timeout=120, return_when="FIRST_EXCEPTION")

    for future in futures:
        exception = future.exception()
        if exception is not None:
            raise exception

    invariants.count(coll, cast(strategies.RecordSet, normalized_record_set))
    n_results = max(1, (len(normalized_record_set["ids"]) // 10))
    invariants.ann_accuracy(
        coll,
        cast(strategies.RecordSet, normalized_record_set),
        n_results=n_results,
        embedding_function=collection.embedding_function,
    )
