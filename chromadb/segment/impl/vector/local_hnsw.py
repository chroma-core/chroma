from overrides import override
from typing import Any, Optional, Sequence, Dict, Set, List, Callable, Union, cast
from uuid import UUID
from chromadb.segment import VectorReader
from chromadb.ingest import Consumer
from chromadb.config import System, Settings
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
import os
import pickle
import numpy as np
import numpy.typing as npt

from chromadb.utils import distance_functions

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

    _ids_to_records: Dict[str, EmbeddingRecord]
    _deleted_ids: Set[str]
    _written_ids: Set[str]
    _upsert_add_ids: Set[str]  # IDs that are being added in an upsert
    add_count: int
    update_count: int
    delete_count: int
    max_seq_id: SeqId

    def __init__(self) -> None:
        self._ids_to_records = {}
        self._deleted_ids = set()
        self._written_ids = set()
        self._upsert_add_ids = set()
        self.add_count = 0
        self.update_count = 0
        self.delete_count = 0
        self.max_seq_id = 0

    def __len__(self) -> int:
        """Get the number of changes in this batch"""
        return len(self._written_ids) + len(self._deleted_ids)

    def count(self) -> int:
        """Get the net number of embeddings in this batch"""
        return len(self._written_ids)

    def get_deleted_ids(self) -> List[str]:
        """Get the list of deleted embeddings in this batch"""
        return list(self._deleted_ids)

    def get_written_ids(self) -> List[str]:
        """Get the list of written embeddings in this batch"""
        return list(self._written_ids)

    def get_written_vectors(self) -> List[Vector]:
        """Get the list of vectors to write in this batch"""
        return [
            cast(Vector, self._ids_to_records[id]["embedding"])
            for id in self.get_written_ids()
        ]

    def get_record(self, id: str) -> EmbeddingRecord:
        """Get the record for a given ID"""
        return self._ids_to_records[id]

    def is_new(self, id: str) -> bool:
        """Returns true if the id is a new addition to the index"""
        record = self._ids_to_records[id]
        return record["operation"] == Operation.ADD or (
            record["operation"] == Operation.UPSERT and id in self._upsert_add_ids
        )

    def apply(self, record: EmbeddingRecord, is_add: bool = False) -> None:
        """
        Apply an embedding record to this batch. Records passed to this method are assumed to be validated for correctness.
        For example, a delete or update presumes the ID exists in the index. An add presumes the ID does not exist in the index.
        In the case of upsert, the is_add flag should be set to True if the ID does not exist in the index, and False otherwise.
        """

        id = record["id"]
        if record["operation"] == Operation.DELETE:
            self._deleted_ids.add(id)

            # If the ID was previously written, remove it from the written set
            # And update the add/update/delete counts
            if id in self._written_ids:
                self._written_ids.remove(id)
                if self._ids_to_records[id]["operation"] == Operation.ADD:
                    self.add_count -= 1
                elif self._ids_to_records[id]["operation"] == Operation.UPDATE:
                    self.update_count -= 1
                elif self._ids_to_records[id]["operation"] == Operation.UPSERT:
                    if id in self._upsert_add_ids:
                        self.add_count -= 1
                        self._upsert_add_ids.remove(id)
                    else:
                        self.update_count -= 1

            # Remove the record from the batch
            if id in self._ids_to_records:
                del self._ids_to_records[id]

            self.delete_count += 1
        else:
            self._ids_to_records[id] = record
            self._written_ids.add(id)

            # If the ID was previously deleted, remove it from the deleted set
            # And update the delete count
            if id in self._deleted_ids:
                self._deleted_ids.remove(id)
                self.delete_count -= 1

            # Update the add/update counts
            if record["operation"] == Operation.UPSERT:
                if is_add:
                    self.add_count += 1
                    self._upsert_add_ids.add(id)
                else:
                    self.update_count += 1
            elif record["operation"] == Operation.ADD:
                self.add_count += 1
            elif record["operation"] == Operation.UPDATE:
                self.update_count += 1

        self.max_seq_id = max(self.max_seq_id, record["seq_id"])


class BruteForceIndex:
    """A lightweight, numpy based brute force index that is used for batches that have not been indexed into hnsw yet"""

    # TODO: mark internal
    id_to_index: Dict[str, int]
    index_to_id: Dict[int, str]
    id_to_seq_id: Dict[str, int]
    deleted_ids: Set[str]
    curr_index: int
    size: int
    dimensionality: int
    distance_fn: Callable[[npt.NDArray[Any], npt.NDArray[Any]], float]
    vectors: npt.NDArray[Any]

    def __init__(self, size: int, dimensionality: int, space: str = "l2"):
        if space == "l2":
            self.distance_fn = distance_functions.l2
        elif space == "ip":
            self.distance_fn = distance_functions.ip
        elif space == "cosine":
            self.distance_fn = distance_functions.cosine
        else:
            raise Exception(f"Unknown distance function: {space}")

        self.id_to_index = {}
        self.index_to_id = {}
        self.id_to_seq_id = {}
        self.deleted_ids = set()
        self.curr_index = 0
        self.size = size
        self.dimensionality = dimensionality
        self.vectors = np.zeros((size, dimensionality))

    def __len__(self) -> int:
        return len(self.id_to_index)

    def flush(self) -> None:
        self.id_to_index = {}
        self.index_to_id = {}
        self.id_to_seq_id = {}
        self.deleted_ids.clear()
        self.curr_index = 0
        self.vectors.fill(0)

    # TODO: thread safety
    def upsert(self, records: List[EmbeddingRecord]) -> None:
        if len(records) + len(self) > self.size:
            raise Exception(
                "Index with capacity {} and {} current entries cannot add {} records".format(
                    self.size, len(self), len(records)
                )
            )

        for i, record in enumerate(records):
            id = record["id"]
            vector = record["embedding"]
            self.id_to_seq_id[id] = record["seq_id"]
            if id in self.deleted_ids:
                self.deleted_ids.remove(id)

            if id in self.id_to_index:
                # Update
                index = self.id_to_index[id]
                self.vectors[index] = vector
            else:
                # Add
                self.id_to_index[id] = self.curr_index
                self.index_to_id[self.curr_index] = id
                self.vectors[self.curr_index] = vector
                self.curr_index += 1

    # TODO: use id type?
    def delete(self, records: List[EmbeddingRecord]) -> None:
        for record in records:
            id = record["id"]
            if id in self.id_to_index:
                index = self.id_to_index[id]
                self.deleted_ids.add(id)
                del self.id_to_index[id]
                del self.index_to_id[index]
                del self.id_to_seq_id[id]
                self.vectors[index].fill(0)
            else:
                logger.warning(f"Delete of nonexisting embedding ID: {id}")

    def has_id(self, id: str) -> bool:
        """Returns whether the index contains the given ID"""
        return id in self.id_to_index and id not in self.deleted_ids

    def get_vectors(
        self, ids: Sequence[str] | None = None
    ) -> Sequence[VectorEmbeddingRecord]:
        target_ids = ids or self.id_to_index.keys()

        return [
            VectorEmbeddingRecord(
                id=id,
                embedding=self.vectors[self.id_to_index[id]].tolist(),
                seq_id=self.id_to_seq_id[id],
            )
            for id in target_ids
        ]

    def query(self, query: VectorQuery) -> Sequence[Sequence[VectorQueryResult]]:
        np_query = np.array(query["vectors"])
        allowed_ids = None if not query["allowed_ids"] else set(query["allowed_ids"])
        distances = np.apply_along_axis(
            lambda query: np.apply_along_axis(self.distance_fn, 1, self.vectors, query),
            1,
            np_query,
        )

        indices = np.argsort(distances).tolist()
        # Filter out deleted labels
        filtered_results = []
        for i, index_list in enumerate(indices):
            curr_results = []
            for j in index_list:
                # If the index is in the index_to_id map, then it has been added
                if j in self.index_to_id:
                    id = self.index_to_id[j]
                    if id not in self.deleted_ids and (
                        allowed_ids is None or id in allowed_ids
                    ):
                        curr_results.append(
                            VectorQueryResult(
                                id=id,
                                distance=distances[i][j],
                                seq_id=self.id_to_seq_id[id],
                                embedding=self.vectors[j].tolist(),
                            )
                        )
            filtered_results.append(curr_results)
        return filtered_results


class LocalHnswSegment(VectorReader):
    _id: UUID
    _consumer: Consumer
    _topic: Optional[str]
    _subscription: UUID
    _settings: Settings
    _params: HnswParams

    _index: Optional[hnswlib.Index]
    _dimensionality: Optional[int]
    _total_elements_added: int
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
        super().__init__(system, segment)

    @staticmethod
    @override
    def propagate_collection_metadata(metadata: Metadata) -> Optional[Metadata]:
        # Extract relevant metadata
        segment_metadata = {}
        for param, value in metadata.items():
            if param.startswith("hnsw:"):
                segment_metadata[param] = value

        # Validate it
        for param, value in segment_metadata.items():
            if param not in param_validators:
                raise ValueError(f"Unknown HNSW parameter: {param}")
            if not param_validators[param](value):
                raise ValueError(f"Invalid value for HNSW parameter: {param} = {value}")

        return segment_metadata

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
            labels = {self._id_to_label[id] for id in ids if id in self._id_to_label}
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
                if query["include_embeddings"]:
                    embedding = self._index.get_items([label])[0]
                else:
                    embedding = None
                results.append(
                    VectorQueryResult(
                        id=id, seq_id=seq_id, distance=distance, embedding=embedding
                    )
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
        deleted_ids = batch.get_deleted_ids()
        written_ids = batch.get_written_ids()
        vectors_to_write = batch.get_written_vectors()
        labels_to_write = [0] * len(vectors_to_write)

        if len(deleted_ids) > 0:
            index = cast(hnswlib.Index, self._index)
            for i in range(len(deleted_ids)):
                id = deleted_ids[i]
                label = self._id_to_label[id]

                index.mark_deleted(label)
                del self._id_to_label[id]
                del self._label_to_id[label]
                del self._id_to_seq_id[id]

        if len(written_ids) > 0:
            self._ensure_index(batch.add_count, len(vectors_to_write[0]))

            next_label = self._total_elements_added + 1
            for i in range(len(written_ids)):
                if batch.is_new(written_ids[i]):
                    labels_to_write[i] = next_label
                    next_label += 1
                else:
                    labels_to_write[i] = self._id_to_label[written_ids[i]]

            index = cast(hnswlib.Index, self._index)

            # First, update the index
            index.add_items(vectors_to_write, labels_to_write)

            # If that succeeds, update the mappings
            for i, id in enumerate(written_ids):
                self._id_to_seq_id[id] = batch.get_record(id)["seq_id"]
                self._id_to_label[id] = labels_to_write[i]
                self._label_to_id[labels_to_write[i]] = id

            # If that succeeds, update the total count
            self._total_elements_added += batch.add_count

            # If that succeeds, finally the seq ID
            self._max_seq_id = batch.max_seq_id

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
                        batch.apply(record)
                    else:
                        logger.warning(f"Delete of nonexisting embedding ID: {id}")

                elif op == Operation.UPDATE:
                    if record["embedding"] is not None:
                        if label is not None:
                            batch.apply(record)
                        else:
                            logger.warning(
                                f"Update of nonexisting embedding ID: {record['id']}"
                            )
                elif op == Operation.ADD:
                    if not label:
                        batch.apply(record, True)
                    else:
                        logger.warning(f"Add of existing embedding ID: {id}")
                elif op == Operation.UPSERT:
                    batch.apply(record, is_add=label is None)

            self._apply_batch(batch)


class PersistentData:
    """Stores the data and metadata needed for a PersistentLocalHnswSegment"""

    dimensionality: Optional[int]
    total_elements_added: int
    max_seq_id: SeqId

    id_to_label: Dict[str, int]
    label_to_id: Dict[int, str]
    id_to_seq_id: Dict[str, SeqId]

    def __init__(
        self,
        dimensionality: Optional[int],
        total_elements_added: int,
        max_seq_id: int,
        id_to_label: Dict[str, int],
        label_to_id: Dict[int, str],
        id_to_seq_id: Dict[str, SeqId],
    ):
        self.dimensionality = dimensionality
        self.total_elements_added = total_elements_added
        self.max_seq_id = max_seq_id
        self.id_to_label = id_to_label
        self.label_to_id = label_to_id
        self.id_to_seq_id = id_to_seq_id

    @staticmethod
    def load_from_file(filename: str) -> "PersistentData":
        """Load persistent data from a file"""
        with open(filename, "rb") as f:
            ret = cast(PersistentData, pickle.load(f))
            return ret


class PersistentLocalHnswSegment(LocalHnswSegment):
    METADATA_FILE: str = "index_metadata.pickle"
    # How many records to add to index at once, we do this because crossing the python/c++ boundary is expensive (for add())
    # When records are not added to the c++ index, they are buffered in memory and served
    # via brute force search.
    _batch_size: int = 1000
    _brute_force_index: BruteForceIndex
    _curr_batch: Batch
    # How many records to add to index before syncing to disk
    _sync_threshold: int = 1000
    _persist_data: PersistentData
    _persist_directory: str

    def __init__(self, system: System, segment: Segment):
        super().__init__(system, segment)
        self._persist_directory = system.settings.require("persist_directory")
        self._curr_batch = Batch()
        if not os.path.exists(self._get_storage_folder()):
            os.makedirs(self._get_storage_folder())
        # Load persist data if it exists already, otherwise create it
        if self._index_exists():
            self._persist_data = PersistentData.load_from_file(
                self._get_metadata_file()
            )
            self._dimensionality = self._persist_data.dimensionality
            self._total_elements_added = self._persist_data.total_elements_added
            self._max_seq_id = self._persist_data.max_seq_id
            self._id_to_label = self._persist_data.id_to_label
            self._label_to_id = self._persist_data.label_to_id
            self._id_to_seq_id = self._persist_data.id_to_seq_id
        else:
            self._persist_data = PersistentData(
                self._dimensionality,
                self._total_elements_added,
                self._max_seq_id,
                self._id_to_label,
                self._label_to_id,
                self._id_to_seq_id,
            )

    def _index_exists(self) -> bool:
        """Check if the index exists via the metadata file"""
        return os.path.exists(self._get_metadata_file())

    def _get_metadata_file(self) -> str:
        """Get the metadata file path"""
        return os.path.join(self._get_storage_folder(), self.METADATA_FILE)

    def _get_storage_folder(self) -> str:
        """Get the storage folder path"""
        folder = os.path.join(self._persist_directory, str(self._id))
        return folder

    @override
    def _init_index(self, dimensionality: int) -> None:
        index = hnswlib.Index(space=self._params.space, dim=dimensionality)
        self._brute_force_index = BruteForceIndex(
            size=self._batch_size,
            dimensionality=dimensionality,
            space=self._params.space,
        )

        # Check if index exists and load it if it does
        if self._index_exists():
            index.load_index(
                self._get_storage_folder(),
                is_persistent_index=True,
                max_elements=int(
                    max(self.count() * self._params.resize_factor, DEFAULT_CAPACITY)
                ),
            )
        else:
            index.init_index(
                max_elements=DEFAULT_CAPACITY,
                ef_construction=self._params.construction_ef,
                M=self._params.M,
                is_persistent_index=True,
                persistence_location=self._get_storage_folder(),
            )

        index.set_ef(self._params.search_ef)
        index.set_num_threads(self._params.num_threads)

        self._index = index
        self._dimensionality = dimensionality

    def _persist(self) -> None:
        """Persist the index and data to disk"""
        index = cast(hnswlib.Index, self._index)

        # Persist the index
        index.persist_dirty()

        # Persist the metadata
        self._persist_data.dimensionality = self._dimensionality
        self._persist_data.total_elements_added = self._total_elements_added
        self._persist_data.max_seq_id = self._max_seq_id

        # TODO: This should really be stored in sqlite or the index itself
        self._persist_data.id_to_label = self._id_to_label
        self._persist_data.label_to_id = self._label_to_id
        self._persist_data.id_to_seq_id = self._id_to_seq_id

        with open(self._get_metadata_file(), "wb") as metadata_file:
            pickle.dump(self._persist_data, metadata_file, pickle.HIGHEST_PROTOCOL)

    @override
    def _apply_batch(self, batch: Batch) -> None:
        super()._apply_batch(batch)
        if (
            self._total_elements_added - self._persist_data.total_elements_added
            >= self._sync_threshold
        ):
            self._persist()

    @override
    def _write_records(self, records: Sequence[EmbeddingRecord]) -> None:
        """Add a batch of embeddings to the index"""
        if not self._running:
            raise RuntimeError("Cannot add embeddings to stopped component")

        # TODO: THREAD SAFETY
        for record in records:
            if record["embedding"] is not None:
                self._ensure_index(len(records), len(record["embedding"]))

            self._max_seq_id = max(self._max_seq_id, record["seq_id"])
            id = record["id"]
            op = record["operation"]
            exists_in_index = self._id_to_label.get(
                id, None
            ) or self._brute_force_index.has_id(id)

            if op == Operation.DELETE:
                if exists_in_index:
                    self._curr_batch.apply(record)
                    self._brute_force_index.delete([record])
                else:
                    logger.warning(f"Delete of nonexisting embedding ID: {id}")

            elif op == Operation.UPDATE:
                if record["embedding"] is not None:
                    if exists_in_index:
                        self._curr_batch.apply(record)
                        self._brute_force_index.upsert([record])
                    else:
                        logger.warning(
                            f"Update of nonexisting embedding ID: {record['id']}"
                        )
            elif op == Operation.ADD:
                if record["embedding"] is not None:
                    if not exists_in_index:
                        self._curr_batch.apply(record, is_add=True)
                        self._brute_force_index.upsert([record])
                    else:
                        logger.warning(f"Add of existing embedding ID: {id}")
            elif op == Operation.UPSERT:
                if record["embedding"] is not None:
                    self._curr_batch.apply(record, is_add=exists_in_index is False)
                    self._brute_force_index.upsert([record])

            if len(self._curr_batch) >= self._batch_size:
                self._apply_batch(self._curr_batch)
                self._curr_batch = Batch()
                self._brute_force_index.flush()

    @override
    def count(self) -> int:
        return len(self._id_to_label) + self._curr_batch.count()

    @override
    def get_vectors(
        self, ids: Sequence[str] | None = None
    ) -> Sequence[VectorEmbeddingRecord]:
        """Get the embeddings from the HNSW index and layered brute force batch index"""
        results = []
        ids_hnsw = set(self._id_to_label.keys())
        ids_bf = set(self._curr_batch.get_written_ids())
        target_ids = ids or list(ids_hnsw.union(ids_bf))
        hnsw_labels = []

        for id in target_ids:
            if id in ids_bf:
                results.append(self._brute_force_index.get_vectors([id])[0])
            elif id in ids_hnsw:
                hnsw_labels.append(self._id_to_label[id])

        if len(hnsw_labels) > 0 and self._index is not None:
            vectors = cast(Sequence[Vector], self._index.get_items(hnsw_labels))

            for label, vector in zip(hnsw_labels, vectors):
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
        k = query["k"]
        if k > self.count():
            logger.warning(
                f"Number of requested results {k} is greater than number of elements in index {self.count()}, updating n_results = {self.count()}"
            )
            k = self.count()

        # Overquery by updated elements amount because they may
        # hide the real nearest neighbors in the hnsw index
        hnsw_query = VectorQuery(
            vectors=query["vectors"],
            k=k + self._curr_batch.update_count,
            allowed_ids=query["allowed_ids"],
            include_embeddings=query["include_embeddings"],
            options=query["options"],
        )
        results = []
        bf_results = self._brute_force_index.query(query)
        hnsw_results = super().query_vectors(hnsw_query)
        # For each query vector, we want to take the top k results from the
        # combined results of the brute force and hnsw index
        for i in range(len(query["vectors"])):
            # Merge results into a single list of size k
            bf_pointer: int = 0
            hnsw_pointer: int = 0
            curr_bf_result: Sequence[VectorQueryResult] = bf_results[i]
            curr_hnsw_result: Sequence[VectorQueryResult] = hnsw_results[i]
            curr_results: List[VectorQueryResult] = []
            while len(curr_results) < k:
                if bf_pointer < len(curr_bf_result) and hnsw_pointer < len(
                    curr_hnsw_result
                ):
                    bf_dist = curr_bf_result[bf_pointer]["distance"]
                    hnsw_dist = curr_hnsw_result[hnsw_pointer]["distance"]
                    if bf_dist <= hnsw_dist:
                        curr_results.append(curr_bf_result[bf_pointer])
                        bf_pointer += 1
                    else:
                        curr_results.append(curr_hnsw_result[hnsw_pointer])
                        hnsw_pointer += 1
                if bf_pointer >= len(curr_bf_result):
                    remaining = k - len(curr_results)
                    curr_results.extend(
                        curr_hnsw_result[hnsw_pointer : hnsw_pointer + remaining]
                    )
                if hnsw_pointer >= len(curr_hnsw_result):
                    remaining = k - len(curr_results)
                    curr_results.extend(
                        curr_bf_result[bf_pointer : bf_pointer + remaining]
                    )
            results.append(curr_results)
        return results
