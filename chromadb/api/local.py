import time
from typing import Dict, Optional
from chromadb.api import API
from chromadb.server.utils.telemetry.capture import Capture
from chromadb.api.models.Collection import Collection

import re

# mimics s3 bucket requirements for naming
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

    #
    # COLLECTION METHODS
    #
    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict] = None,
    ) -> Collection:
        if not is_valid_index_name(name):
            raise ValueError("Invalid index name: %s" % name)  # NIT: tell the user why

        self._db.create_collection(name, metadata)
        return Collection(self, name)

    def get_collection(
        self,
        name: str,
    ) -> Collection:
        self._db.get_collection(name)
        return Collection(self, name)

    def _get_collection_db(self, name: str) -> int:
        return self._db.get_collection(name)

    def list_collections(self) -> list:
        return self._db.list_collections()


    def modify(
        self,
        current_name: str,
        new_name: str = None,
        new_metadata: Optional[Dict] = None,
    ) -> int:
        # NIT: make sure we have a valid name like we do in create
        if new_name is not None:
            if not is_valid_index_name(new_name):
                raise ValueError("Invalid index name: %s" % new_name)

        self._db.update_collection(current_name, new_name, new_metadata)

    def delete_collection(self, name: str) -> int:
        return self._db.delete_collection(name)

    #
    # ITEM METHODS
    #
    def add(
        self,
        collection_name: str,
        embeddings,
        metadatas=None,
        documents=None,
        ids=None,
        increment_index=True,
    ):

        number_of_embeddings = len(embeddings)

        # fill in empty objects if not provided
        if metadatas is None:
            if isinstance(embeddings[0], list):
                metadatas = [{} for _ in range(number_of_embeddings)]
            else:
                metadatas = {}

        if ids is None:
            if isinstance(embeddings[0], list):
                ids = [None for _ in range(number_of_embeddings)]
            else:
                ids = None

        if documents is None:
            if isinstance(embeddings[0], list):
                documents = [None for _ in range(number_of_embeddings)]
            else:
                documents = None

        # convert all metadatas values to strings : TODO: handle this better
        # this is currently here because clickhouse-driver does not support json
        if isinstance(embeddings[0], list):
            for m in metadatas:
                for k, v in m.items():
                    m[k] = str(v)
        else:
            for k, v in metadatas.items():
                metadatas[k] = str(v)

        # convert to array for downstream processing
        if not isinstance(embeddings[0], list):
            embeddings = [embeddings]
            metadatas = [metadatas]
            documents = [documents]
            ids = [ids]

        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)
        added_uuids = self._db.add(
            collection_uuid, embedding=embeddings, metadata=metadatas, documents=documents, ids=ids
        )

        if increment_index:
            self._db.add_incremental(collection_uuid, added_uuids, embeddings)

        return True  # NIT: should this return the ids of the succesfully added items?

    def update(self, collection_name: str, embedding, metadata=None):

        number_of_embeddings = len(embedding)

        if metadata is None:
            metadata = [{} for _ in range(number_of_embeddings)]

        # convert all metadata values to strings : TODO: handle this better
        # this is currently here because clickhouse-driver does not support json
        for m in metadata:
            for k, v in m.items():
                m[k] = str(v)

        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)

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

    def get(
        self,
        collection_name,
        ids=None,
        where=None,
        sort=None,
        limit=None,
        offset=None,
        page=None,
        page_size=None,
    ):

        if where is None:
            where = {}

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        return self._db.get(
            collection_name=collection_name,
            ids=ids,
            where=where,
            sort=sort,
            limit=limit,
            offset=offset,
        )

    def delete(self, collection_name, ids=None, where=None):

        if where is None:
            where = {}

        deleted_uuids = self._db.delete(collection_name=collection_name, where=where, ids=ids)
        return deleted_uuids

    def count(self, collection_name):

        return self._db.count(collection_name=collection_name)

    def reset(self):

        self._db.reset()
        return True

    def query(self, collection_name, query_embeddings, n_results=10, where={}):

        return self._db.get_nearest_neighbors(
            collection_name=collection_name,
            where=where,
            embeddings=query_embeddings,
            n_results=n_results,
        )

    def raw_sql(self, raw_sql):

        return self._db.raw_sql(raw_sql)

    def create_index(self, collection_name):

        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)
        self._db.create_index(collection_uuid=collection_uuid)
        return True

    def peek(self, collection_name, n=10):

        return self.get(collection_name=collection_name, limit=n)
