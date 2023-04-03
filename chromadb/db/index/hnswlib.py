import os
import pickle
import time
from typing import Dict
from chromadb.api.types import IndexMetadata
import hnswlib
from chromadb.db.index import Index
from chromadb.errors import NoIndexException, InvalidDimensionException, NotEnoughElementsException
import logging
import re
from uuid import UUID

logger = logging.getLogger(__name__)


valid_params = {
    "hnsw:space": r"^(l2|cosine|ip)$",
    "hnsw:construction_ef": r"^\d+$",
    "hnsw:search_ef": r"^\d+$",
    "hnsw:M": r"^\d+$",
    "hnsw:num_threads": r"^\d+$",
    "hnsw:resize_factor": r"^\d+(\.\d+)?$",
}


class HnswParams:

    space: str
    construction_ef: int
    search_ef: int
    M: int
    num_threads: int
    resize_factor: float

    def __init__(self, metadata):

        metadata = metadata or {}

        # Convert all values to strings for future compatibility.
        metadata = {k: str(v) for k, v in metadata.items()}

        for param, value in metadata.items():
            if param.startswith("hnsw:"):
                if param not in valid_params:
                    raise ValueError(f"Unknown HNSW parameter: {param}")
                if not re.match(valid_params[param], value):
                    raise ValueError(f"Invalid value for HNSW parameter: {param} = {value}")

        self.space = metadata.get("hnsw:space", "l2")
        self.construction_ef = int(metadata.get("hnsw:construction_ef", 100))
        self.search_ef = int(metadata.get("hnsw:search_ef", 10))
        self.M = int(metadata.get("hnsw:M", 16))
        self.num_threads = int(metadata.get("hnsw:num_threads", 4))
        self.resize_factor = float(metadata.get("hnsw:resize_factor", 1.2))


def hexid(id):
    """Backwards compatibility for old indexes which called uuid.hex on UUID ids"""
    return id.hex if isinstance(id, UUID) else id


def delete_all_indexes(settings):
    if os.path.exists(f"{settings.persist_directory}/index"):
        for file in os.listdir(f"{settings.persist_directory}/index"):
            os.remove(f"{settings.persist_directory}/index/{file}")


class Hnswlib(Index):
    _id: str
    _index: hnswlib.Index
    _index_metadata: IndexMetadata
    _params: HnswParams
    _id_to_label: Dict[str, int]
    _label_to_id: Dict[int, str]

    def __init__(self, id, settings, metadata):
        self._save_folder = settings.persist_directory + "/index"
        self._params = HnswParams(metadata)
        self._id = id
        self._index = None
        # Mapping of IDs to HNSW integer labels
        self._id_to_label = {}
        self._label_to_id = {}

        self._load()

    def _init_index(self, dimensionality):
        # more comments available at the source: https://github.com/nmslib/hnswlib

        index = hnswlib.Index(
            space=self._params.space, dim=dimensionality
        )  # possible options are l2, cosine or ip
        index.init_index(
            max_elements=1000,
            ef_construction=self._params.construction_ef,
            M=self._params.M,
        )
        index.set_ef(self._params.search_ef)
        index.set_num_threads(self._params.num_threads)

        self._index = index
        self._index_metadata = {
            "dimensionality": dimensionality,
            "elements": 0,
            "time_created": time.time(),
        }
        self._save()

    def _check_dimensionality(self, data):
        """Assert that the given data matches the index dimensionality"""
        dim = len(data[0])
        idx_dim = self._index.dim
        if dim != idx_dim:
            raise InvalidDimensionException(
                f"Dimensionality of ({dim}) does not match index dimensionality ({idx_dim})"
            )

    def add(self, ids, embeddings, update=False):
        """Add or update embeddings to the index"""

        dim = len(embeddings[0])

        if self._index is None:
            self._init_index(dim)

        # Check dimensionality
        self._check_dimensionality(embeddings)

        labels = []
        for id in ids:
            if id in self._id_to_label:
                if update:
                    labels.append(self._id_to_label[hexid(id)])
                else:
                    raise ValueError(f"ID {id} already exists in index")
            else:
                self._index_metadata["elements"] += 1
                next_label = self._index_metadata["elements"]
                self._id_to_label[hexid(id)] = next_label
                self._label_to_id[next_label] = id
                labels.append(next_label)

        if self._index_metadata["elements"] > self._index.get_max_elements():
            new_size = max(self._index_metadata["elements"] * self._params.resize_factor, 1000)
            self._index.resize_index(int(new_size))

        self._index.add_items(embeddings, labels)
        self._save()

    def delete(self):
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

    def delete_from_index(self, ids):
        if self._index is not None:
            for id in ids:
                label = self._id_to_label[hexid(id)]
                self._index.mark_deleted(label)
                del self._label_to_id[label]
                del self._id_to_label[hexid(id)]

        self._save()

    def _save(self):
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

    def _exists(self):
        return

    def _load(self):

        if not os.path.exists(f"{self._save_folder}/index_{self._id}.bin"):
            return

        # unpickle the mappers
        with open(f"{self._save_folder}/id_to_uuid_{self._id}.pkl", "rb") as f:
            self._label_to_id = pickle.load(f)
        with open(f"{self._save_folder}/uuid_to_id_{self._id}.pkl", "rb") as f:
            self._id_to_label = pickle.load(f)
        with open(f"{self._save_folder}/index_metadata_{self._id}.pkl", "rb") as f:
            self._index_metadata = pickle.load(f)

        p = hnswlib.Index(space=self._params.space, dim=self._index_metadata["dimensionality"])
        self._index = p
        self._index.load_index(
            f"{self._save_folder}/index_{self._id}.bin",
            max_elements=self._index_metadata["elements"],
        )
        self._index.set_ef(self._params.search_ef)
        self._index.set_num_threads(self._params.num_threads)

    def get_nearest_neighbors(self, query, k, ids=None):

        if self._index is None:
            raise NoIndexException("Index not found, please create an instance before querying")

        # Check dimensionality
        self._check_dimensionality(query)

        if k > self._index_metadata["elements"]:
            raise NotEnoughElementsException(
                f"Number of requested results {k} cannot be greater than number of elements in index {self._index_metadata['elements']}"
            )

        s2 = time.time()
        # get ids from uuids as a set, if they are available
        labels = {}
        if ids is not None:
            labels = {self._id_to_label[hexid(id)] for id in ids}
            if len(labels) < k:
                k = len(labels)

        filter_function = None
        if len(labels) != 0:
            filter_function = lambda label: label in labels

        logger.debug(f"time to pre process our knn query: {time.time() - s2}")

        s3 = time.time()
        database_labels, distances = self._index.knn_query(query, k=k, filter=filter_function)
        logger.debug(f"time to run knn query: {time.time() - s3}")

        ids = [[self._label_to_id[label] for label in labels] for labels in database_labels]
        return ids, distances
