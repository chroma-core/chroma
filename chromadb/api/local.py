import time
from typing import Dict, Optional
from chromadb.api import API
from chromadb.utils.sampling import score_and_store, get_sample
from chromadb.server.utils.telemetry.capture import Capture
from chromadb.api.models.Collection import Collection

import re

def is_valid_index_name(index_name):
    if len(index_name) < 3 or len(index_name) > 63:
        return False
    if not re.match("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", index_name):
        return False
    if ".." in index_name:
        return False
    if re.match("^[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}$", index_name):
        return False
    return True


class LocalAPI(API):

    def __init__(self, settings, db):
        self._db = db
        self._chroma_telemetry = Capture()
        self._chroma_telemetry.capture("server-start")

    def heartbeat(self):
        return int(1000 * time.time_ns())

    # create a new Collection object method as a factory
    def Collection(self, name):
        return Collection(self, name)

    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict] = None,
    ) -> int:
        if not is_valid_index_name(name):
            raise ValueError("Invalid index name: %s" % name)

        return self._db.create_collection(name, metadata)

    def get_collection(
        self,
        name: Optional[str] = None,
    ) -> int:
        return self._db.get_collection(name)

    def list_collections(self) -> int:
        return self._db.list_collections()

    def update_collection(
        self,
        name: str,
        metadata: Optional[Dict] = None,
    ) -> int:
        return self._db.update_collection(name, metadata)

    def delete_collection(
        self,
        name: str
    ) -> int:
        return self._db.delete_collection(name)

    

    def add(
        self,
        collection_name,
        embeddings,
        metadatas=None
    ):

        collection_name = collection_name or self.get_collection_name()
        number_of_embeddings = len(embeddings)

        if metadatas is None:
            metadatas = [{} for _ in range(number_of_embeddings)]

        # convert all metadatas values to strings : TODO: handle this better
        # this is currently here because clickhouse-driver does not support json
        for m in metadatas:
            for k, v in m.items():
                m[k] = str(v)

        collection_uuid = self.get_collection(collection_name).iloc[0].uuid
        added_uuids = self._db.add(collection_uuid, embedding=embeddings, metadata=metadatas)
        print("Added UUIDs: ", added_uuids)
        # self._db.add_incremental(collection_uuid, added_uuids, embeddings)

        return True


    def update(
        self,
        collection_name,
        embedding,
        metadata=None
    ):

        collection_name = collection_name or self.get_collection_name()
        number_of_embeddings = len(embedding)

        if metadata is None:
            metadata = [{} for _ in range(number_of_embeddings)]

        # convert all metadata values to strings : TODO: handle this better
        # this is currently here because clickhouse-driver does not support json
        for m in metadata:
            for k, v in m.items():
                m[k] = str(v)

        collection_uuid = self.get_collection(collection_name).iloc[0].uuid

        # find the uuids of the embeddings where the metadata matches
        # then update the embeddings for that 
        # then update the index position for that embedding
        for item in metadata:
            uuid = self._db.get_uuid(collection_uuid, item)
            if uuid is not None:
                self._db.update(collection_uuid, uuid, embedding, metadata)

        # added_uuids = self._db.add(collection_uuid, embedding, metadata)
        # self._db.add_incremental(collection_uuid, added_uuids, embedding)

        return True


    def fetch(self, where={}, sort=None, limit=None, offset=None, page=None, page_size=None):

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        return self._db.fetch(where, sort, limit, offset)

    def delete(self, where={}):

        where = self.where_with_collection_name(where)
        deleted_uuids = self._db.delete(where)
        return deleted_uuids

    def count(self, collection_name=None):

        collection_name = collection_name or self._collection_name
        return self._db.count(collection_name=collection_name)

    def reset(self):

        self._db.reset()
        return True

    def search(self, embedding, n_results=10, where={}):
        # collection_name should already be in where
        return self._db.get_nearest_neighbors(where, embedding, n_results)

    def raw_sql(self, raw_sql):

        return self._db.raw_sql(raw_sql)

    def create_index(self, collection_name=None):

        collection_name = collection_name or self.get_collection_name()
        collection_uuid = self.get_collection(collection_name).iloc[0].uuid

        self._db.create_index(
            collection_uuid=collection_uuid
        )
        return True
