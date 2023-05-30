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
            labels = [self._id_to_label[id] for id in ids]

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

    def _check_dimensionality(self, data: Vector) -> None:
        """Assert that the given data matches the index dimensionality"""
        if len(data) != self._dimensionality:
            raise InvalidDimensionException(
                f"Dimensionality of ({len(data)}) does not match index"
                + f"dimensionality ({self._dimensionality})"
            )

    def _resize_index(self) -> None:
        """Resize the index (if necessary)"""
        index = cast(hnswlib.Index, self._index)

        if self._total_elements_added > index.get_max_elements():
            new_size = int(self._total_elements_added * self._params.resize_factor)
            index.resize_index(max(new_size, DEFAULT_CAPACITY))

    def _delete_vector_from_index(self, id: str) -> None:
        """Delete a vector from the index"""
        index = cast(hnswlib.Index, self._index)

        label = self._id_to_label[id]
        index.delete_items([label])
        del self._id_to_label[id]
        del self._label_to_id[label]
        del self._id_to_seq_id[id]

    def _add_vector_to_index(self, id: str, seq_id: SeqId, embedding: Vector) -> None:
        """Add a vector to the index"""
        if not self._index:
            self._init_index(len(embedding))

        index = cast(hnswlib.Index, self._index)

        self._check_dimensionality(embedding)

        if id in self._id_to_label:
            label = self._id_to_label[id]
        else:
            self._total_elements_added += 1
            label = self._total_elements_added
            self._id_to_label[id] = label
            self._label_to_id[label] = id
            self._id_to_seq_id[id] = seq_id

        self._resize_index()
        index.add_items([embedding], [label])

    def _write_record(self, record: EmbeddingRecord) -> None:
        """Add a single embedding to the index"""

        # let's be safe, just in case
        with self._lock:
            self._max_seq_id = max(self._max_seq_id, record["seq_id"])

            if record["operation"] == Operation.DELETE:
                if record["id"] in self._id_to_label:
                    return self._delete_vector_from_index(record["id"])
                else:
                    logger.warning(
                        f"Delete of nonexisting embedding ID: {record['id']}"
                    )
                    return

            if record["id"] in self._id_to_label:
                if record["operation"] == Operation.ADD:
                    logger.warning(f"Insert of existing embedding ID: {record['id']}")
                    return
            else:
                if record["operation"] == Operation.UPDATE:
                    logger.warning(
                        f"Update of nonexisting embedding ID: {record['id']}"
                    )
                    return

            # Might be false for updates
            if record["embedding"]:
                self._add_vector_to_index(
                    record["id"], record["seq_id"], record["embedding"]
                )

    def _write_records(self, records: Sequence[EmbeddingRecord]) -> None:
        """Add a batch of embeddings to the index"""
        if not self._running:
            raise RuntimeError("Cannot add embeddings to stopped component")

        for record in records:
            self._write_record(record)


# TODO: Implement this
class PersistentLocalHnswSegment(LocalHnswSegment):
    pass
