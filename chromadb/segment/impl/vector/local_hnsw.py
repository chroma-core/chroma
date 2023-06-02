from overrides import override
from typing import Optional, Sequence, Dict, Set, List, Callable, Union, cast
from uuid import UUID
from chromadb.segment import VectorReader
from chromadb.ingest import Consumer
from chromadb.config import Component, System, Settings
from chromadb.types import (
    EmbeddingRecord,
    VectorEmbeddingRecord,
    VectorQuery,
    VectorQueryResult,
    SeqId,
    Segment,
    Metadata,
    Operation,
    Vector,
)
from chromadb.errors import InvalidDimensionException
import re
import multiprocessing
import hnswlib
from threading import Lock
import logging

logger = logging.getLogger(__name__)

DEFAULT_CAPACITY = 1000

Validator = Callable[[Union[str, int, float]], bool]

param_validators: Dict[str, Validator] = {
    "hnsw:space": lambda p: bool(re.match(r"^(l2|cosine|ip)$", str(p))),
    "hnsw:construction_ef": lambda p: isinstance(p, int),
    "hnsw:search_ef": lambda p: isinstance(p, int),
    "hnsw:M": lambda p: isinstance(p, int),
    "hnsw:num_threads": lambda p: isinstance(p, int),
    "hnsw:resize_factor": lambda p: isinstance(p, (int, float)),
}


class HnswParams:
    space: str
    construction_ef: int
    search_ef: int
    M: int
    num_threads: int
    resize_factor: float

    def __init__(self, metadata: Metadata):
        metadata = metadata or {}

        for param, value in metadata.items():
            if param.startswith("hnsw:"):
                if param not in param_validators:
                    raise ValueError(f"Unknown HNSW parameter: {param}")
                if not param_validators[param](value):
                    raise ValueError(
                        f"Invalid value for HNSW parameter: {param} = {value}"
                    )

        self.space = str(metadata.get("hnsw:space", "l2"))
        self.construction_ef = int(metadata.get("hnsw:construction_ef", 100))
        self.search_ef = int(metadata.get("hnsw:search_ef", 10))
        self.M = int(metadata.get("hnsw:M", 16))
        self.num_threads = int(
            metadata.get("hnsw:num_threads", multiprocessing.cpu_count())
        )
        self.resize_factor = float(metadata.get("hnsw:resize_factor", 1.2))


class Batch:
    """Used to model the set of changes as an atomic operation"""

    labels: List[Optional[int]]
    vectors: List[Vector]
    seq_ids: List[SeqId]
    ids: List[str]
    delete_labels: List[int]
    delete_ids: List[str]
    add_count: int
    delete_count: int

    def __init__(self) -> None:
        self.labels = []
        self.vectors = []
        self.seq_ids = []
        self.ids = []
        self.delete_labels = []
        self.delete_ids = []
        self.add_count = 0
        self.delete_count = 0

    def add(self, label: Optional[int], record: EmbeddingRecord) -> None:
        self.labels.append(label)
        self.vectors.append(cast(Vector, record["embedding"]))
        self.seq_ids.append(record["seq_id"])
        self.ids.append(record["id"])
        if not label:
            self.add_count += 1

    def delete(self, label: int, id: str) -> None:
        self.delete_labels.append(label)
        self.delete_ids.append(id)
        self.delete_count += 1


class LocalHnswSegment(Component, VectorReader):
    _id: UUID
    _consumer: Consumer
    _topic: Optional[str]
    _subscription: UUID
    _settings: Settings
    _params: HnswParams

    _index: Optional[hnswlib.Index]
    _dimensionality: Optional[int]
    _elements: int
    _max_seq_id: SeqId

    _lock: Lock

    _id_to_label: Dict[str, int]
    _label_to_id: Dict[int, str]
    _id_to_seq_id: Dict[str, SeqId]

    def __init__(self, system: System, segment: Segment):
        self._consumer = system.instance(Consumer)
        self._id = segment["id"]
        self._topic = segment["topic"]
        self._settings = system.settings
        self._params = HnswParams(segment["metadata"] or {})

        self._index = None
        self._dimensionality = None
        self._total_elements_added = 0
        self._max_seq_id = self._consumer.min_seqid()

        self._id_to_seq_id = {}
        self._id_to_label = {}
        self._label_to_id = {}

        self._lock = Lock()
        super().__init__(system)

    @override
    def start(self) -> None:
        super().start()
        if self._topic:
            seq_id = self.max_seqid()
            self._subscription = self._consumer.subscribe(
                self._topic, self._write_records, start=seq_id
            )

    @override
    def stop(self) -> None:
        super().stop()
        if self._subscription:
            self._consumer.unsubscribe(self._subscription)

    @override
    def get_vectors(
        self, ids: Optional[Sequence[str]] = None
    ) -> Sequence[VectorEmbeddingRecord]:
        if ids is None:
            labels = list(self._label_to_id.keys())
        else:
            labels = []
            for id in ids:
                if id in self._id_to_label:
                    labels.append(self._id_to_label[id])

        results = []
        if self._index is not None:
            vectors = cast(Sequence[Vector], self._index.get_items(labels))

            for label, vector in zip(labels, vectors):
                id = self._label_to_id[label]
                seq_id = self._id_to_seq_id[id]
                results.append(
                    VectorEmbeddingRecord(id=id, seq_id=seq_id, embedding=vector)
                )

        return results

    @override
    def query_vectors(
        self, query: VectorQuery
    ) -> Sequence[Sequence[VectorQueryResult]]:
        if self._index is None:
            return [[] for _ in range(len(query["vectors"]))]

        k = query["k"]
        size = len(self._id_to_label)

        if k > size:
            logger.warning(
                f"Number of requested results {k} is greater than number of elements in index {size}, updating n_results = {size}"
            )
            k = size

        labels: Set[int] = set()
        ids = query["allowed_ids"]
        if ids is not None:
            labels = {self._id_to_label[id] for id in ids}
            if len(labels) < k:
                k = len(labels)

        def filter_function(label: int) -> bool:
            return label in labels

        query_vectors = query["vectors"]

        result_labels, distances = self._index.knn_query(
            query_vectors, k=k, filter=filter_function if ids else None
        )

        distances = cast(List[List[float]], distances)
        result_labels = cast(List[List[int]], result_labels)

        all_results: List[List[VectorQueryResult]] = []
        for result_i in range(len(result_labels)):
            results: List[VectorQueryResult] = []
            for label, distance in zip(result_labels[result_i], distances[result_i]):
                id = self._label_to_id[label]
                seq_id = self._id_to_seq_id[id]
                results.append(
                    VectorQueryResult(id=id, seq_id=seq_id, distance=distance)
                )
            all_results.append(results)

        return all_results

    @override
    def max_seqid(self) -> SeqId:
        return self._max_seq_id

    @override
    def count(self) -> int:
        return len(self._id_to_label)

    def _init_index(self, dimensionality: int) -> None:
        # more comments available at the source: https://github.com/nmslib/hnswlib

        index = hnswlib.Index(
            space=self._params.space, dim=dimensionality
        )  # possible options are l2, cosine or ip
        index.init_index(
            max_elements=DEFAULT_CAPACITY,
            ef_construction=self._params.construction_ef,
            M=self._params.M,
        )
        index.set_ef(self._params.search_ef)
        index.set_num_threads(self._params.num_threads)

        self._index = index
        self._dimensionality = dimensionality

    def _ensure_index(self, n: int, dim: int) -> None:
        """Create or resize the index as necessary to accomodate N new records"""
        if not self._index:
            self._dimensionality = dim
            self._init_index(dim)
        else:
            if dim != self._dimensionality:
                raise InvalidDimensionException(
                    f"Dimensionality of ({dim}) does not match index"
                    + f"dimensionality ({self._dimensionality})"
                )

        index = cast(hnswlib.Index, self._index)

        if (self._total_elements_added + n) > index.get_max_elements():
            new_size = int(
                (self._total_elements_added + n) * self._params.resize_factor
            )
            index.resize_index(max(new_size, DEFAULT_CAPACITY))

    def _apply_batch(self, batch: Batch) -> None:
        """Apply a batch of changes, as atomically as possible."""

        if batch.delete_ids:
            index = cast(hnswlib.Index, self._index)
            for i in range(len(batch.delete_ids)):
                label = batch.delete_labels[i]
                id = batch.delete_ids[i]

                index.mark_deleted(label)
                del self._id_to_label[id]
                del self._label_to_id[label]
                del self._id_to_seq_id[id]

        if batch.ids:
            self._ensure_index(batch.add_count, len(batch.vectors[0]))

            next_label = self._total_elements_added + 1
            for i in range(len(batch.labels)):
                if batch.labels[i] is None:
                    batch.labels[i] = next_label
                    next_label += 1

            labels = cast(List[int], batch.labels)

            index = cast(hnswlib.Index, self._index)

            # First, update the index
            index.add_items(batch.vectors, labels)

            # If that succeeds, update the mappings
            for id, label, seq_id in zip(batch.ids, labels, batch.seq_ids):
                self._id_to_seq_id[id] = seq_id
                self._id_to_label[id] = label
                self._label_to_id[label] = id

            # If that succeeds, update the total count
            self._total_elements_added += batch.add_count

            # If that succeeds, finally the seq ID
            self._max_seq_id = max(self._max_seq_id, max(batch.seq_ids))

    def _write_records(self, records: Sequence[EmbeddingRecord]) -> None:
        """Add a batch of embeddings to the index"""
        if not self._running:
            raise RuntimeError("Cannot add embeddings to stopped component")

        # Avoid all sorts of potential problems by ensuring single-threaded access
        with self._lock:
            batch = Batch()

            for record in records:
                self._max_seq_id = max(self._max_seq_id, record["seq_id"])
                id = record["id"]
                op = record["operation"]
                label = self._id_to_label.get(id, None)

                if op == Operation.DELETE:
                    if label:
                        batch.delete(label, id)
                    else:
                        logger.warning(f"Delete of nonexisting embedding ID: {id}")

                elif op == Operation.UPDATE:
                    if record["embedding"] is not None:
                        if label is not None:
                            batch.add(label, record)
                        else:
                            logger.warning(
                                f"Update of nonexisting embedding ID: {record['id']}"
                            )
                elif op == Operation.ADD:
                    if not label:
                        batch.add(label, record)
                    else:
                        logger.warning(f"Add of existing embedding ID: {id}")
                elif op == Operation.UPSERT:
                    batch.add(label, record)

            self._apply_batch(batch)


# TODO: Implement this as a performance improvement, if rebuilding the
# index on startup is too slow. But test this first.
class PersistentLocalHnswSegment(LocalHnswSegment):
    pass
