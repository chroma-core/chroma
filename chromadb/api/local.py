import json
import time
from typing import Dict, Optional, Sequence, Callable
from chromadb.api import API
from chromadb.db import DB
from chromadb.api.types import GetResult, IDs, QueryResult
from chromadb.api.models.Collection import Collection

import re

# mimics s3 bucket requirements for naming
def is_valid_index_name(index_name):
    if len(index_name) < 3 or len(index_name) > 63:
        return False
    if not re.match("^[a-z0-9][a-z0-9._-]*[a-z0-9]$", index_name):
        return False
    if ".." in index_name:
        return False
    if re.match("^[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}$", index_name):
        return False
    return True


class LocalAPI(API):
    def __init__(self, settings, db: DB):
        self._db = db

    def heartbeat(self):
        return int(1000 * time.time_ns())

    #
    # COLLECTION METHODS
    #
    def create_collection(
        self,
        name: str,
        metadata: Optional[Dict] = None,
        embedding_function: Optional[Callable] = None,
    ) -> Collection:
        if not is_valid_index_name(name):
            raise ValueError("Invalid index name: %s" % name)  # NIT: tell the user why

        self._db.create_collection(name, metadata)
        return Collection(client=self, name=name, embedding_function=embedding_function)

    def get_collection(
        self,
        name: str,
    ) -> Collection:
        self._db.get_collection(name)
        return Collection(client=self, name=name)

    def _get_collection_db(self, name: str) -> int:
        return self._db.get_collection(name)

    def list_collections(self) -> Sequence[Collection]:
        collections = []
        db_collections = self._db.list_collections()
        for db_collection in db_collections:
            collections.append(Collection(client=self, name=db_collection[1]))
        return collections

    def modify(
        self,
        current_name: str,
        new_name: Optional[str] = None,
        new_metadata: Optional[Dict] = None,
    ):
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
    def _add(
        self,
        ids,
        collection_name: str,
        embeddings,
        metadatas=None,
        documents=None,
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

        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)
        added_uuids = self._db.add(
            collection_uuid, embedding=embeddings, metadata=metadatas, documents=documents, ids=ids
        )

        if increment_index:
            self._db.add_incremental(collection_uuid, added_uuids, embeddings)

        return True  # NIT: should this return the ids of the succesfully added items?

    def _update(
        self,
        collection_name: str,
        ids: IDs,
        embeddings=None,
        metadatas=None,
        documents=None,
    ):
        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)
        self._db.update(collection_uuid, ids, embeddings, metadatas, documents)

        return True

    def _db_result_to_get_result(self, db_result) -> GetResult:
        query_result = GetResult(embeddings=[], documents=[], ids=[], metadatas=[])
        for entry in db_result:
            query_result["embeddings"].append(entry[2])
            query_result["documents"].append(entry[3])
            query_result["ids"].append(entry[4])
            query_result["metadatas"].append(entry[5])
        return query_result

    def _get(
        self,
        collection_name,
        ids=None,
        where=None,
        sort=None,
        limit=None,
        offset=None,
        page=None,
        page_size=None,
        where_document=None
    ):

        if where is None:
            where = {}
        
        if where_document is None:
            where_document = {}

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        return self._db_result_to_get_result(
            self._db.get(
                collection_name=collection_name,
                ids=ids,
                where=where,
                sort=sort,
                limit=limit,
                offset=offset,
                where_document=where_document,
            )
        )

    def _delete(self, collection_name, ids=None, where=None, where_document=None):

        if where is None:
            where = {}

        deleted_uuids = self._db.delete(collection_name=collection_name, where=where, ids=ids, where_document=where_document)
        return deleted_uuids

    def _count(self, collection_name):

        return self._db.count(collection_name=collection_name)

    def reset(self):
        self._db.reset()
        return True

    def _query(self, collection_name, query_embeddings, n_results=10, where={}, where_document={}):
        uuids, distances = self._db.get_nearest_neighbors(
            collection_name=collection_name,
            where=where,
            where_document=where_document,
            embeddings=query_embeddings,
            n_results=n_results,
        )

        query_result = QueryResult(embeddings=[], documents=[], ids=[], metadatas=[], distances=[])
        for i in range(len(uuids)):
            embeddings = []
            documents = []
            ids = []
            metadatas = []
            db_result = self._db.get_by_ids(uuids[i])

            for entry in db_result:
                embeddings.append(entry[2])
                documents.append(entry[3])
                ids.append(entry[4])
                metadatas.append(json.loads(entry[5]))

            query_result["embeddings"].append(embeddings)
            query_result["documents"].append(documents)
            query_result["ids"].append(ids)
            query_result["metadatas"].append(metadatas)
            query_result["distances"].append(distances[i].tolist())

        return query_result

    def raw_sql(self, raw_sql):

        return self._db.raw_sql(raw_sql)

    def create_index(self, collection_name):

        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)
        self._db.create_index(collection_uuid=collection_uuid)
        return True

    def _peek(self, collection_name, n=10):
        return self._get(collection_name=collection_name, limit=n)

    def persist(self):
        self._db.persist()
        return True
