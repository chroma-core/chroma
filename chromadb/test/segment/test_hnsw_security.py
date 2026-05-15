from types import SimpleNamespace

import numpy as np
import pytest

from chromadb.segment.impl.vector.hnsw_params import HnswParams, PersistentHnswParams
from chromadb.segment.impl.vector.local_hnsw import LocalHnswSegment
from chromadb.segment.impl.vector.local_persistent_hnsw import (
    PersistentLocalHnswSegment,
)
from chromadb.types import RequestVersionContext
from chromadb.utils.read_write_lock import ReadWriteLock


class InspectableReadWriteLock(ReadWriteLock):
    @property
    def readers(self) -> int:
        return self._readers


class AssertingIndex:
    def __init__(self, lock: InspectableReadWriteLock) -> None:
        self._lock = lock
        self.labels = []

    def get_items(self, labels):
        assert self._lock.readers == 1
        self.labels = list(labels)
        return [[1.0, 2.0]]


REQUEST_VERSION_CONTEXT = RequestVersionContext(collection_version=0, log_position=0)


def test_local_hnsw_get_vectors_holds_read_lock() -> None:
    lock = InspectableReadWriteLock()
    index = AssertingIndex(lock)
    segment = object.__new__(LocalHnswSegment)
    segment._lock = lock
    segment._index = index
    segment._id_to_label = {"id1": 1}
    segment._label_to_id = {1: "id1"}

    results = segment.get_vectors(REQUEST_VERSION_CONTEXT, ids=["id1"])

    assert index.labels == [1]
    assert len(results) == 1
    assert results[0]["id"] == "id1"
    np.testing.assert_array_equal(results[0]["embedding"], np.array([1.0, 2.0]))
    assert lock.readers == 0


def test_persistent_local_hnsw_get_vectors_holds_read_lock() -> None:
    lock = InspectableReadWriteLock()
    index = AssertingIndex(lock)
    segment = object.__new__(PersistentLocalHnswSegment)
    segment._lock = lock
    segment._index = index
    segment._id_to_label = {"id1": 1}
    segment._label_to_id = {1: "id1"}
    segment._brute_force_index = None
    segment._curr_batch = SimpleNamespace(
        _deleted_ids=set(), get_written_ids=lambda: []
    )

    results = segment.get_vectors(REQUEST_VERSION_CONTEXT, ids=["id1"])

    assert index.labels == [1]
    assert len(results) == 1
    assert results[0]["id"] == "id1"
    np.testing.assert_array_equal(results[0]["embedding"], np.array([1.0, 2.0]))
    assert lock.readers == 0


@pytest.mark.parametrize("params", [HnswParams, PersistentHnswParams])
@pytest.mark.parametrize(
    "value", [0.0, -1.0, 5.1, float("inf"), float("nan"), 10**1000, True]
)
def test_hnsw_params_reject_unsafe_resize_factor(params, value) -> None:
    with pytest.raises(ValueError, match="Invalid value for HNSW parameter"):
        params.extract({"hnsw:resize_factor": value})


@pytest.mark.parametrize(
    "param",
    ["hnsw:construction_ef", "hnsw:search_ef", "hnsw:M", "hnsw:num_threads"],
)
@pytest.mark.parametrize("value", [0, -1, True])
def test_hnsw_params_reject_non_positive_integer_params(param, value) -> None:
    with pytest.raises(ValueError, match="Invalid value for HNSW parameter"):
        HnswParams.extract({param: value})
