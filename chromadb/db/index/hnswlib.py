import os
import pickle
import time
from typing import Dict, List, Optional, Set, Tuple, Union, cast

from chromadb.api.types import Embeddings, IndexMetadata
import hnswlib
from chromadb.config import Settings
from chromadb.db.index import Index
from chromadb.errors import (
    InvalidDimensionException,
)
import logging
import re
from uuid import UUID
import multiprocessing

logger = logging.getLogger(__name__)


valid_params = {
    "hnsw:space": r"^(l2|cosine|ip)$",
    "hnsw:construction_ef": r"^\d+$",
    "hnsw:search_ef": r"^\d+$",
    "hnsw:M": r"^\d+$",
    "hnsw:num_threads": r"^\d+$",
    "hnsw:resize_factor": r"^\d+(\.\d+)?$",
}

DEFAULT_CAPACITY = 1000


class HnswParams:
    space: str
    construction_ef: int
    search_ef: int
    M: int
    num_threads: int
    resize_factor: float

    def __init__(self, metadata: Dict[str, str]):
        metadata = metadata or {}

        # Convert all values to strings for future compatibility.
        metadata = {k: str(v) for k, v in metadata.items()}

        for param, value in metadata.items():
            if param.startswith("hnsw:"):
                if param not in valid_params:
                    raise ValueError(f"Unknown HNSW parameter: {param}")
                if not re.match(valid_params[param], value):
                    raise ValueError(
                        f"Invalid value for HNSW parameter: {param} = {value}"
                    )

        self.space = metadata.get("hnsw:space", "l2")
        self.construction_ef = int(metadata.get("hnsw:construction_ef", 100))
        self.search_ef = int(metadata.get("hnsw:search_ef", 10))
        self.M = int(metadata.get("hnsw:M", 16))
        self.num_threads = int(
            metadata.get("hnsw:num_threads", multiprocessing.cpu_count())
        )
        self.resize_factor = float(metadata.get("hnsw:resize_factor", 1.2))


def hexid(id: Union[str, UUID]) -> str:
    """Backwards compatibility for old indexes which called uuid.hex on UUID ids"""
    return id.hex if isinstance(id, UUID) else id


def delete_all_indexes(settings: Settings) -> None:
    if os.path.exists(f"{settings.persist_directory}/index"):
        for file in os.listdir(f"{settings.persist_directory}/index"):
            os.remove(f"{settings.persist_directory}/index/{file}")


class Hnswlib(Index):
    _id: str
    _index: hnswlib.Index
    _index_metadata: IndexMetadata
    _params: HnswParams
    _id_to_label: Dict[str, int]
    _label_to_id: Dict[int, UUID]

    def __init__(
        self,
        id: str,
        settings: Settings,
        metadata: Dict[str, str],
        number_elements: int,
    ):
        self._save_folder = settings.persist_directory + "/index"
        self._params = HnswParams(metadata)
        self._id = id
        self._index = None
        # Mapping of IDs to HNSW integer labels
        self._id_to_label = {}
        self._label_to_id = {}

        self._load(number_elements)

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
        self._index_metadata = {
            "dimensionality": dimensionality,
            "curr_elements": 0,
            "total_elements_added": 0,
            "time_created": time.time(),
        }
        self._save()

    def _check_dimensionality(self, data: Embeddings) -> None:
        """Assert that the given data matches the index dimensionality"""
        dim = len(data[0])
        idx_dim = self._index.dim
        if dim != idx_dim:
            raise InvalidDimensionException(
                f"Dimensionality of ({dim}) does not match index dimensionality ({idx_dim})"
            )

    def add(
        self, ids: List[UUID], embeddings: Embeddings, update: bool = False
    ) -> None:
        """Add or update embeddings to the index"""

        dim = len(embeddings[0])

        if self._index is None:
            self._init_index(dim)
        # Calling init_index will ensure the index is not none, so we can safely cast
        self._index = cast(hnswlib.Index, self._index)

        # Check dimensionality
        self._check_dimensionality(embeddings)

        labels = []
        for id in ids:
            if hexid(id) in self._id_to_label:
                if update:
                    labels.append(self._id_to_label[hexid(id)])
                else:
                    raise ValueError(f"ID {id} already exists in index")
            else:
                self._index_metadata["total_elements_added"] += 1
                self._index_metadata["curr_elements"] += 1
                next_label = self._index_metadata["total_elements_added"]
                self._id_to_label[hexid(id)] = next_label
                self._label_to_id[next_label] = id
                labels.append(next_label)

        if (
            self._index_metadata["total_elements_added"]
            > self._index.get_max_elements()
        ):
            new_size = int(
                max(
                    self._index_metadata["total_elements_added"]
                    * self._params.resize_factor,
                    DEFAULT_CAPACITY,
                )
            )
            self._index.resize_index(new_size)

        self._index.add_items(embeddings, labels)
        self._save()

    def delete(self) -> None:
        # delete files, dont throw error if they dont exist
        try:
            os.remove(f"{self._save_folder}/id_to_uuid_{self._id}.pkl")
            os.remove(f"{self._save_folder}/uuid_to_id_{self._id}.pkl")
            os.remove(f"{self._save_folder}/index_{self._id}.bin")
            os.remove(f"{self._save_folder}/index_metadata_{self._id}.pkl")
        except Exception:
            pass

        self._index = None
        self._collection_uuid = None
        self._id_to_label = {}
        self._label_to_id = {}

    def delete_from_index(self, ids: List[UUID]) -> None:
        if self._index is not None:
            for id in ids:
                label = self._id_to_label[hexid(id)]
                self._index.mark_deleted(label)
                del self._label_to_id[label]
                del self._id_to_label[hexid(id)]
                self._index_metadata["curr_elements"] -= 1

        self._save()

    def _save(self) -> None:
        # create the directory if it doesn't exist
        if not os.path.exists(f"{self._save_folder}"):
            os.makedirs(f"{self._save_folder}")

        if self._index is None:
            return
        self._index.save_index(f"{self._save_folder}/index_{self._id}.bin")

        # pickle the mappers
        # Use old filenames for backwards compatibility
        with open(f"{self._save_folder}/id_to_uuid_{self._id}.pkl", "wb") as f:
            pickle.dump(self._label_to_id, f, pickle.HIGHEST_PROTOCOL)
        with open(f"{self._save_folder}/uuid_to_id_{self._id}.pkl", "wb") as f:
            pickle.dump(self._id_to_label, f, pickle.HIGHEST_PROTOCOL)
        with open(f"{self._save_folder}/index_metadata_{self._id}.pkl", "wb") as f:
            pickle.dump(self._index_metadata, f, pickle.HIGHEST_PROTOCOL)

        logger.debug(f"Index saved to {self._save_folder}/index.bin")

    def _exists(self) -> None:
        return

    def _load(self, curr_elements: int) -> None:
        if not os.path.exists(f"{self._save_folder}/index_{self._id}.bin"):
            return

        # unpickle the mappers
        with open(f"{self._save_folder}/id_to_uuid_{self._id}.pkl", "rb") as f:
            self._label_to_id = pickle.load(f)
        with open(f"{self._save_folder}/uuid_to_id_{self._id}.pkl", "rb") as f:
            self._id_to_label = pickle.load(f)
        with open(f"{self._save_folder}/index_metadata_{self._id}.pkl", "rb") as f:
            self._index_metadata = pickle.load(f)

        self._index_metadata["curr_elements"] = curr_elements
        # Backwards compatability with versions that don't have curr_elements or total_elements_added
        if "total_elements_added" not in self._index_metadata:
            self._index_metadata["total_elements_added"] = self._index_metadata[
                "elements"
            ]

        p = hnswlib.Index(
            space=self._params.space, dim=self._index_metadata["dimensionality"]
        )
        self._index = p
        self._index.load_index(
            f"{self._save_folder}/index_{self._id}.bin",
            max_elements=int(
                max(curr_elements * self._params.resize_factor, DEFAULT_CAPACITY)
            ),
        )
        self._index.set_ef(self._params.search_ef)
        self._index.set_num_threads(self._params.num_threads)

    def get_nearest_neighbors(
        self, query: Embeddings, k: int, ids: Optional[List[UUID]] = None
    ) -> Tuple[List[List[UUID]], List[List[float]]]:
        # The only case where the index is none is if no elements have been added
        # We don't save the index until at least one element has been added
        # And so there is also nothing at load time for persisted indexes
        # In the case where no elements have been added, we return empty
        if self._index is None:
            return [[] for _ in range(len(query))], [[] for _ in range(len(query))]

        # Check dimensionality
        self._check_dimensionality(query)

        # Check Number of requested results
        if k > self._index_metadata["curr_elements"]:
            logger.warning(
                f"Number of requested results {k} is greater than number of elements in index {self._index_metadata['curr_elements']}, updating n_results = {self._index_metadata['curr_elements']}"
            )
            k = self._index_metadata["curr_elements"]

        s2 = time.time()
        # get ids from uuids as a set, if they are available
        labels: Set[int] = set()
        if ids is not None:
            labels = {self._id_to_label[hexid(id)] for id in ids}
            if len(labels) < k:
                k = len(labels)

        filter_function = None
        if len(labels) != 0:
            filter_function = lambda label: label in labels  # NOQA: E731

        logger.debug(f"time to pre process our knn query: {time.time() - s2}")

        s3 = time.time()
        database_labels, distances = self._index.knn_query(
            query, k=k, filter=filter_function
        )
        distances = distances.tolist()
        distances = cast(List[List[float]], distances)
        logger.debug(f"time to run knn query: {time.time() - s3}")

        return_ids = [
            [self._label_to_id[label] for label in labels] for labels in database_labels
        ]
        return return_ids, distances
