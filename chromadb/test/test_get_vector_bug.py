import uuid
from dataclasses import dataclass
from typing import Sequence

import pytest
from hypothesis import given

from chromadb.api import ServerAPI
from chromadb.api.fastapi import FastAPI
from chromadb.segment import MetadataReader, VectorReader
from chromadb.types import MetadataEmbeddingRecord, VectorEmbeddingRecord
import hypothesis.strategies as st


@dataclass
class BatchParams:
    batch_size: int
    sync_threshold: int
    item_size: int

@st.composite
def batching_params(draw: st.DrawFn) -> BatchParams:
    batch_size = draw(st.integers(min_value=3, max_value=100))
    sync_threshold = draw(st.integers(min_value=batch_size, max_value=batch_size * 2))
    item_size = draw(st.integers(min_value=batch_size + 1, max_value=(batch_size * 2) + 1))
    return BatchParams(batch_size=batch_size, sync_threshold=sync_threshold, item_size=item_size)


@given(batching_params=batching_params())
def test_update_path(batching_params: BatchParams, api: ServerAPI) -> None:
    error_distribution = {"IndexError": 0, "TypeError": 0, "NoError": 0}
    rounds = 100
    if isinstance(api, FastAPI) or not api.get_settings().is_persistent:
        pytest.skip("FastAPI does not support this test")
    for _ in range(rounds):
        # with tempfile.TemporaryDirectory(ignore_cleanup_errors=False) as tmp:
        # client = chromadb.PersistentClient(tmp)
        print(batching_params)
        api.reset()
        collection = api.get_or_create_collection('test',
                                                  metadata={"hnsw:batch_size": batching_params.batch_size, "hnsw:sync_threshold": batching_params.sync_threshold})
        items = [(f"{uuid.uuid4()}", i, [0.1] * 2) for i in range(batching_params.item_size)]  # we want to exceed the batch size by at least 1
        ids = [item[0] for item in items]
        embeddings = [item[2] for item in items]
        collection.add(ids=ids, embeddings=embeddings)
        collection.delete(ids=[ids[0]])
        collection.add(ids=[ids[0]], embeddings=[[1] * 2])
        # with pytest.raises(IndexError, match="list assignment index out of range"): # TypeError: 'NoneType' object is not subscriptable
        try:
            collection.get(include=['embeddings'])
            error_distribution["NoError"] += 1
        except IndexError as e:
            if "list assignment index out of range" in str(e):
                error_distribution["IndexError"] += 1
        except TypeError as e:
            if "'NoneType' object is not subscriptable" in str(e):
                error_distribution["TypeError"] += 1
        segment_manager = api._manager
        metadata_segment: MetadataReader = segment_manager.get_segment(collection.id, MetadataReader)
        vector_segment: VectorReader = segment_manager.get_segment(collection.id, VectorReader)
        metadata_records: Sequence[MetadataEmbeddingRecord] = metadata_segment.get_metadata()
        vector_records: Sequence[VectorEmbeddingRecord] = vector_segment.get_vectors()
        assert len(metadata_records) == len(vector_records)

    assert error_distribution["NoError"] == rounds
    assert error_distribution["IndexError"] == 0
    assert error_distribution["TypeError"] == 0