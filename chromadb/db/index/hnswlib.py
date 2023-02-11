import os
import pickle
import time
import uuid

import hnswlib
import numpy as np
from chromadb.db.index import Index
from chromadb.logger import logger
from chromadb.errors import NoIndexException


class Hnswlib(Index):

    _collection_uuid = None
    _index = None
    _index_metadata = {
        "dimensionality": None,
        "elements": None,
        "time_created": None,
    }

    _id_to_uuid = {}
    _uuid_to_id = {}

    def __init__(self, settings):
        self._save_folder = settings.persist_directory + "/index"

    def run(self, collection_uuid, uuids, embeddings, space="l2", ef=10, num_threads=4):

        # more comments available at the source: https://github.com/nmslib/hnswlib
        dimensionality = len(embeddings[0])
        for uuid, i in zip(uuids, range(len(uuids))):
            self._id_to_uuid[i] = uuid
            self._uuid_to_id[uuid.hex] = i

        index = hnswlib.Index(
            space=space, dim=dimensionality
        )  # possible options are l2, cosine or ip
        index.init_index(max_elements=len(embeddings), ef_construction=100, M=16)
        index.set_ef(ef)
        index.set_num_threads(num_threads)
        index.add_items(embeddings, range(len(uuids)))

        self._index = index
        self._collection_uuid = collection_uuid
        self._index_metadata = {
            "dimensionality": dimensionality,
            "elements": len(embeddings),
            "time_created": time.time(),
        }
        self._save()

    def add_incremental(self, collection_uuid, uuids, embeddings):
        if self._collection_uuid != collection_uuid:
            self._load(collection_uuid)

        if self._index is None:
            self.run(collection_uuid, uuids, embeddings)

        elif self._index is not None:

            current_elements = self._index_metadata["elements"]
            new_elements = len(uuids)

            self._index.resize_index(current_elements + new_elements)

            # first map the uuids to ids, offset by the current number of elements
            for uuid, i in zip(uuids, range(len(uuids))):
                offset = current_elements + i
                self._id_to_uuid[offset] = uuid
                self._uuid_to_id[uuid.hex] = offset

            # add the new elements to the index
            self._index.add_items(
                embeddings, range(current_elements, current_elements + new_elements)
            )

            # update the metadata
            self._index_metadata["elements"] += new_elements

        self._save()

    def delete(self, collection_uuid):
        # delete files, dont throw error if they dont exist
        try:
            os.remove(f"{self._save_folder}/id_to_uuid_{collection_uuid}.pkl")
            os.remove(f"{self._save_folder}/uuid_to_id_{collection_uuid}.pkl")
            os.remove(f"{self._save_folder}/index_metadata_{collection_uuid}.pkl")
            os.remove(f"{self._save_folder}/index_{collection_uuid}.bin")
        except:
            pass

        if self._collection_uuid == collection_uuid:
            self._index = None
            self._collection_uuid = None
            self._index_metadata = None
            self._id_to_uuid = {}
            self._uuid_to_id = {}

    def delete_from_index(self, collection_uuid, uuids):
        if self._collection_uuid != collection_uuid:
            self._load(collection_uuid)

        if self._index is not None:
            for uuid in uuids:
                self._index.mark_deleted(self._uuid_to_id[uuid.hex])
                del self._id_to_uuid[self._uuid_to_id[uuid.hex]]
                del self._uuid_to_id[uuid.hex]

        self._save()

    def _save(self):

        # create the directory if it doesn't exist
        if not os.path.exists(f"{self._save_folder}"):
            os.makedirs(f"{self._save_folder}")

        if self._index is None:
            return
        self._index.save_index(f"{self._save_folder}/index_{self._collection_uuid}.bin")

        # pickle the mappers
        with open(f"{self._save_folder}/id_to_uuid_{self._collection_uuid}.pkl", "wb") as f:
            pickle.dump(self._id_to_uuid, f, pickle.HIGHEST_PROTOCOL)
        with open(f"{self._save_folder}/uuid_to_id_{self._collection_uuid}.pkl", "wb") as f:
            pickle.dump(self._uuid_to_id, f, pickle.HIGHEST_PROTOCOL)
        with open(f"{self._save_folder}/index_metadata_{self._collection_uuid}.pkl", "wb") as f:
            pickle.dump(self._index_metadata, f, pickle.HIGHEST_PROTOCOL)

        logger.debug("Index saved to {self._save_folder}/index.bin")

    def _load(self, collection_uuid):
        # if we are calling load, we clearly need a different index than the one we have
        self._index = None

        # unpickle the mappers
        try:
            with open(f"{self._save_folder}/id_to_uuid_{collection_uuid}.pkl", "rb") as f:
                self._id_to_uuid = pickle.load(f)
            with open(f"{self._save_folder}/uuid_to_id_{collection_uuid}.pkl", "rb") as f:
                self._uuid_to_id = pickle.load(f)
            with open(f"{self._save_folder}/index_metadata_{collection_uuid}.pkl", "rb") as f:
                self._index_metadata = pickle.load(f)
            p = hnswlib.Index(space="l2", dim=self._index_metadata["dimensionality"])
            self._index = p
            self._index.load_index(
                f"{self._save_folder}/index_{collection_uuid}.bin",
                max_elements=self._index_metadata["elements"],
            )

            self._collection_uuid = collection_uuid
        except:
            logger.debug("Index not found")

    def has_index(self, collection_uuid):
        return os.path.isfile(f"{self._save_folder}/index_{collection_uuid}.bin")

    def get_nearest_neighbors(self, collection_uuid, query, k, uuids=None):

        if self._collection_uuid != collection_uuid:
            self._load(collection_uuid)

        if self._index is None:
            raise NoIndexException("Index not found, please create an instance before querying")

        s2 = time.time()
        # get ids from uuids as a set, if they are available
        ids = {}
        if uuids is not None:
            ids = {self._uuid_to_id[uuid.hex] for uuid in uuids}
            if len(ids) < k:
                k = len(ids)

        filter_function = None
        if len(ids) != 0:
            filter_function = lambda id: id in ids

        logger.debug(f"time to pre process our knn query: {time.time() - s2}")

        s3 = time.time()
        database_ids, distances = self._index.knn_query(query, k=k, filter=filter_function)
        logger.debug(f"time to run knn query: {time.time() - s3}")

        uuids = [[self._id_to_uuid[id] for id in ids] for ids in database_ids]
        return uuids, distances

    def reset(self):
        self._id_to_uuid = {}
        self._uuid_to_id = {}
        self._index = None
        self._collection_uuid = None

        if os.path.exists(f"{self._save_folder}"):
            for f in os.listdir(f"{self._save_folder}"):
                os.remove(os.path.join(f"{self._save_folder}", f))
        # recreate the directory
        if not os.path.exists(f"{self._save_folder}"):
            os.makedirs(f"{self._save_folder}")

    def delete_index(self, uuid):
        uuid = str(uuid)
        if self._collection_uuid == uuid:
            self._index = None
            self._collection_uuid = None
            self._index_metadata = None
            self._id_to_uuid = {}
            self._uuid_to_id = {}

        if os.path.exists(f"{self._save_folder}"):
            for f in os.listdir(f"{self._save_folder}"):
                if uuid in f:
                    os.remove(os.path.join(f"{self._save_folder}", f))
