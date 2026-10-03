import logging
from typing import Any, Optional, Sequence, Tuple, cast
from unittest.mock import MagicMock

import numpy as np

from chromadb.segment.impl.vector.batch import Batch
from chromadb.segment.impl.vector.local_hnsw import LocalHnswSegment
from chromadb.segment.impl.vector.local_persistent_hnsw import (
    PersistentLocalHnswSegment,
)
from chromadb.types import Vector, VectorQuery
from chromadb.utils.read_write_lock import ReadWriteLock


class FakeHnswIndex:
    def knn_query(
        self, vectors: np.ndarray, k: int, filter: Optional[object] = None
    ) -> Tuple[np.ndarray, np.ndarray]:
        return np.array([[1]]), np.array([[0.0]])


class FakeBruteForceIndex:
    def query(self, query: VectorQuery) -> Sequence[Sequence[Any]]:
        return [[]]

    def has_id(self, id: str) -> bool:
        return False


def make_query() -> VectorQuery:
    vector = cast(Vector, np.array([0.0, 0.0, 0.0], dtype=np.float32))
    return VectorQuery(
        vectors=[vector],
        k=10,
        allowed_ids=None,
        include_embeddings=False,
        options=None,
        request_version_context={"collection_version": 0, "log_position": 0},
    )


def test_query_with_fewer_elements_logs_at_debug(caplog: Any) -> None:
    segment = object.__new__(LocalHnswSegment)
    segment._index = FakeHnswIndex()
    segment._id_to_label = {"one": 1}
    segment._label_to_id = {1: "one"}
    segment._lock = ReadWriteLock()

    with caplog.at_level(
        logging.DEBUG, logger="chromadb.segment.impl.vector.local_hnsw"
    ):
        results = segment.query_vectors(make_query())

    assert results[0][0]["id"] == "one"
    matching_records = [
        record
        for record in caplog.records
        if "Number of requested results" in record.getMessage()
    ]
    assert len(matching_records) == 1
    assert matching_records[0].levelno == logging.DEBUG


def test_persistent_query_with_fewer_elements_logs_at_debug(
    caplog: Any,
) -> None:
    segment = object.__new__(PersistentLocalHnswSegment)
    segment._index = FakeHnswIndex()
    segment._brute_force_index = MagicMock()
    segment._brute_force_index.query.side_effect = FakeBruteForceIndex().query
    segment._brute_force_index.has_id.return_value = False
    segment._id_to_label = {"one": 1}
    segment._label_to_id = {1: "one"}
    segment._lock = ReadWriteLock()
    segment._curr_batch = Batch()

    with caplog.at_level(
        logging.DEBUG, logger="chromadb.segment.impl.vector.local_persistent_hnsw"
    ):
        results = segment.query_vectors(make_query())

    assert results[0][0]["id"] == "one"
    matching_records = [
        record
        for record in caplog.records
        if "Number of requested results" in record.getMessage()
    ]
    assert len(matching_records) == 1
    assert matching_records[0].levelno == logging.DEBUG
