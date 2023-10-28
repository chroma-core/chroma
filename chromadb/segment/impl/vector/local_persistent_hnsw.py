import os
import shutil
from overrides import override
import pickle
from typing import Dict, List, Optional, Sequence, Set, cast
from chromadb.config import System
from chromadb.segment.impl.vector.batch import Batch
from chromadb.segment.impl.vector.hnsw_params import PersistentHnswParams
from chromadb.segment.impl.vector.local_hnsw import (
    DEFAULT_CAPACITY,
    LocalHnswSegment,
)
from chromadb.segment.impl.vector.brute_force_index import BruteForceIndex
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.types import (
    EmbeddingRecord,
    Metadata,
    Operation,
    Segment,
    SeqId,
    Vector,
    VectorEmbeddingRecord,
    VectorQuery,
    VectorQueryResult,
)
import hnswlib
import logging

from chromadb.utils.read_write_lock import ReadRWLock, WriteRWLock


logger = logging.getLogger(__name__)


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
    _batch_size: int
    _brute_force_index: Optional[BruteForceIndex]
    _index_initialized: bool = False
    _curr_batch: Batch
    # How many records to add to index before syncing to disk
    _sync_threshold: int
    _persist_data: PersistentData
    _persist_directory: str
    _allow_reset: bool

    _opentelemtry_client: OpenTelemetryClient

    def __init__(self, system: System, segment: Segment):
        super().__init__(system, segment)

        self._opentelemtry_client = system.require(OpenTelemetryClient)

        self._params = PersistentHnswParams(segment["metadata"] or {})
        self._batch_size = self._params.batch_size
        self._sync_threshold = self._params.sync_threshold
        self._allow_reset = system.settings.allow_reset
        self._persist_directory = system.settings.require("persist_directory")
        self._curr_batch = Batch()
        self._brute_force_index = None
        if not os.path.exists(self._get_storage_folder()):
            os.makedirs(self._get_storage_folder(), exist_ok=True)
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
            # If the index was written to, we need to re-initialize it
            if len(self._id_to_label) > 0:
                self._dimensionality = cast(int, self._dimensionality)
                self._init_index(self._dimensionality)
        else:
            self._persist_data = PersistentData(
                self._dimensionality,
                self._total_elements_added,
                self._max_seq_id,
                self._id_to_label,
                self._label_to_id,
                self._id_to_seq_id,
            )

    @staticmethod
    @override
    def propagate_collection_metadata(metadata: Metadata) -> Optional[Metadata]:
        # Extract relevant metadata
        segment_metadata = PersistentHnswParams.extract(metadata)
        return segment_metadata

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

    @trace_method(
        "PersistentLocalHnswSegment._init_index", OpenTelemetryGranularity.ALL
    )
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
        self._index_initialized = True

    @trace_method("PersistentLocalHnswSegment._persist", OpenTelemetryGranularity.ALL)
    def _persist(self) -> None:
        """Persist the index and data to disk"""
        index = cast(hnswlib.Index, self._index)

        # Persist the index
        index.persist_dirty()

        # Persist the metadata
        self._persist_data.dimensionality = self._dimensionality
        self._persist_data.total_elements_added = self._total_elements_added
        self._persist_data.max_seq_id = self._max_seq_id

        # TODO: This should really be stored in sqlite, the index itself, or a better
        # storage format
        self._persist_data.id_to_label = self._id_to_label
        self._persist_data.label_to_id = self._label_to_id
        self._persist_data.id_to_seq_id = self._id_to_seq_id

        with open(self._get_metadata_file(), "wb") as metadata_file:
            pickle.dump(self._persist_data, metadata_file, pickle.HIGHEST_PROTOCOL)

    @trace_method(
        "PersistentLocalHnswSegment._apply_batch", OpenTelemetryGranularity.ALL
    )
    @override
    def _apply_batch(self, batch: Batch) -> None:
        super()._apply_batch(batch)
        if (
            self._total_elements_added - self._persist_data.total_elements_added
            >= self._sync_threshold
        ):
            self._persist()

    @trace_method(
        "PersistentLocalHnswSegment._write_records", OpenTelemetryGranularity.ALL
    )
    @override
    def _write_records(self, records: Sequence[EmbeddingRecord]) -> None:
        """Add a batch of embeddings to the index"""
        if not self._running:
            raise RuntimeError("Cannot add embeddings to stopped component")
        with WriteRWLock(self._lock):
            for record in records:
                if record["embedding"] is not None:
                    self._ensure_index(len(records), len(record["embedding"]))
                if not self._index_initialized:
                    # If the index is not initialized here, it means that we have
                    # not yet added any records to the index. So we can just
                    # ignore the record since it was a delete.
                    continue
                self._brute_force_index = cast(BruteForceIndex, self._brute_force_index)

                self._max_seq_id = max(self._max_seq_id, record["seq_id"])
                id = record["id"]
                op = record["operation"]
                exists_in_index = self._id_to_label.get(
                    id, None
                ) is not None or self._brute_force_index.has_id(id)
                exists_in_bf_index = self._brute_force_index.has_id(id)

                if op == Operation.DELETE:
                    if exists_in_index:
                        self._curr_batch.apply(record)
                        if exists_in_bf_index:
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
                            self._curr_batch.apply(record, not exists_in_index)
                            self._brute_force_index.upsert([record])
                        else:
                            logger.warning(f"Add of existing embedding ID: {id}")
                elif op == Operation.UPSERT:
                    if record["embedding"] is not None:
                        self._curr_batch.apply(record, exists_in_index)
                        self._brute_force_index.upsert([record])
                if len(self._curr_batch) >= self._batch_size:
                    self._apply_batch(self._curr_batch)
                    self._curr_batch = Batch()
                    self._brute_force_index.clear()

    @override
    def count(self) -> int:
        return (
            len(self._id_to_label)
            + self._curr_batch.add_count
            - self._curr_batch.delete_count
        )

    @trace_method(
        "PersistentLocalHnswSegment.get_vectors", OpenTelemetryGranularity.ALL
    )
    @override
    def get_vectors(
        self, ids: Optional[Sequence[str]] = None
    ) -> Sequence[VectorEmbeddingRecord]:
        """Get the embeddings from the HNSW index and layered brute force
        batch index."""

        ids_hnsw: Set[str] = set()
        ids_bf: Set[str] = set()

        if self._index is not None:
            ids_hnsw = set(self._id_to_label.keys())
        if self._brute_force_index is not None:
            ids_bf = set(self._curr_batch.get_written_ids())

        target_ids = ids or list(ids_hnsw.union(ids_bf))
        self._brute_force_index = cast(BruteForceIndex, self._brute_force_index)
        hnsw_labels = []

        results: List[Optional[VectorEmbeddingRecord]] = []
        id_to_index: Dict[str, int] = {}
        for i, id in enumerate(target_ids):
            if id in ids_bf:
                results.append(self._brute_force_index.get_vectors([id])[0])
            elif id in ids_hnsw and id not in self._curr_batch._deleted_ids:
                hnsw_labels.append(self._id_to_label[id])
                # Placeholder for hnsw results to be filled in down below so we
                # can batch the hnsw get() call
                results.append(None)
            id_to_index[id] = i

        if len(hnsw_labels) > 0 and self._index is not None:
            vectors = cast(Sequence[Vector], self._index.get_items(hnsw_labels))

            for label, vector in zip(hnsw_labels, vectors):
                id = self._label_to_id[label]
                seq_id = self._id_to_seq_id[id]
                results[id_to_index[id]] = VectorEmbeddingRecord(
                    id=id, seq_id=seq_id, embedding=vector
                )

        return results  # type: ignore ## Python can't cast List with Optional to List with VectorEmbeddingRecord

    @trace_method(
        "PersistentLocalHnswSegment.query_vectors", OpenTelemetryGranularity.ALL
    )
    @override
    def query_vectors(
        self, query: VectorQuery
    ) -> Sequence[Sequence[VectorQueryResult]]:
        if self._index is None and self._brute_force_index is None:
            return [[] for _ in range(len(query["vectors"]))]

        k = query["k"]
        if k > self.count():
            logger.warning(
                f"Number of requested results {k} is greater than number of elements in index {self.count()}, updating n_results = {self.count()}"
            )
            k = self.count()

        # Overquery by updated and deleted elements layered on the index because they may
        # hide the real nearest neighbors in the hnsw index
        hnsw_k = k + self._curr_batch.update_count + self._curr_batch.delete_count
        if hnsw_k > len(self._id_to_label):
            hnsw_k = len(self._id_to_label)
        hnsw_query = VectorQuery(
            vectors=query["vectors"],
            k=hnsw_k,
            allowed_ids=query["allowed_ids"],
            include_embeddings=query["include_embeddings"],
            options=query["options"],
        )

        # For each query vector, we want to take the top k results from the
        # combined results of the brute force and hnsw index
        results: List[List[VectorQueryResult]] = []
        self._brute_force_index = cast(BruteForceIndex, self._brute_force_index)
        with ReadRWLock(self._lock):
            bf_results = self._brute_force_index.query(query)
            hnsw_results = super().query_vectors(hnsw_query)
            for i in range(len(query["vectors"])):
                # Merge results into a single list of size k
                bf_pointer: int = 0
                hnsw_pointer: int = 0
                curr_bf_result: Sequence[VectorQueryResult] = bf_results[i]
                curr_hnsw_result: Sequence[VectorQueryResult] = hnsw_results[i]
                curr_results: List[VectorQueryResult] = []
                # In the case where filters cause the number of results to be less than k,
                # we set k to be the number of results
                total_results = len(curr_bf_result) + len(curr_hnsw_result)
                if total_results == 0:
                    results.append([])
                else:
                    while len(curr_results) < min(k, total_results):
                        if bf_pointer < len(curr_bf_result) and hnsw_pointer < len(
                            curr_hnsw_result
                        ):
                            bf_dist = curr_bf_result[bf_pointer]["distance"]
                            hnsw_dist = curr_hnsw_result[hnsw_pointer]["distance"]
                            if bf_dist <= hnsw_dist:
                                curr_results.append(curr_bf_result[bf_pointer])
                                bf_pointer += 1
                            else:
                                id = curr_hnsw_result[hnsw_pointer]["id"]
                                # Only add the hnsw result if it is not in the brute force index
                                # as updated or deleted
                                if not self._brute_force_index.has_id(
                                    id
                                ) and not self._curr_batch.is_deleted(id):
                                    curr_results.append(curr_hnsw_result[hnsw_pointer])
                                hnsw_pointer += 1
                        else:
                            break
                    remaining = min(k, total_results) - len(curr_results)
                    if remaining > 0 and hnsw_pointer < len(curr_hnsw_result):
                        for i in range(
                            hnsw_pointer,
                            min(len(curr_hnsw_result), hnsw_pointer + remaining + 1),
                        ):
                            id = curr_hnsw_result[i]["id"]
                            if not self._brute_force_index.has_id(
                                id
                            ) and not self._curr_batch.is_deleted(id):
                                curr_results.append(curr_hnsw_result[i])
                    elif remaining > 0 and bf_pointer < len(curr_bf_result):
                        curr_results.extend(
                            curr_bf_result[bf_pointer : bf_pointer + remaining]
                        )
                    results.append(curr_results)
            return results

    @trace_method(
        "PersistentLocalHnswSegment.reset_state", OpenTelemetryGranularity.ALL
    )
    @override
    def reset_state(self) -> None:
        if self._allow_reset:
            data_path = self._get_storage_folder()
            if os.path.exists(data_path):
                self.close_persistent_index()
                shutil.rmtree(data_path, ignore_errors=True)

    @trace_method("PersistentLocalHnswSegment.delete", OpenTelemetryGranularity.ALL)
    @override
    def delete(self) -> None:
        data_path = self._get_storage_folder()
        if os.path.exists(data_path):
            self.close_persistent_index()
            shutil.rmtree(data_path, ignore_errors=False)

    @staticmethod
    def get_file_handle_count() -> int:
        """Return how many file handles are used by the index"""
        hnswlib_count = hnswlib.Index.file_handle_count
        hnswlib_count = cast(int, hnswlib_count)
        # One extra for the metadata file
        return hnswlib_count + 1  # type: ignore

    def open_persistent_index(self) -> None:
        """Open the persistent index"""
        if self._index is not None:
            self._index.open_file_handles()

    def close_persistent_index(self) -> None:
        """Close the persistent index"""
        if self._index is not None:
            self._index.close_file_handles()
