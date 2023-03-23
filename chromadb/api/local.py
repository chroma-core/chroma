import json
import uuid
import time
from typing import Dict, List, Literal, Optional, Sequence, Callable, Type, cast, TypedDict
from chromadb.api import API
from chromadb.db import DB
from chromadb.api.types import (
    Documents,
    Embedding,
    Embeddings,
    GetResult,
    IDs,
    Include,
    Metadatas,
    QueryResult,
    Where,
    WhereDocument,
    HnswIndexParams,
)
from chromadb.api.models.Collection import Collection

import re

from chromadb.db.index.hnswlib import DEFAULT_INDEX_PARAMS


# mimics s3 bucket requirements for naming
def check_index_name(index_name):
    msg = (
        "Expected collection name that "
        "(1) contains 3-63 characters, "
        "(2) starts and ends with an alphanumeric character, "
        "(3) otherwise contains only alphanumeric characters, underscores or hyphens (-), "
        "(4) contains no two consecutive periods (..) and "
        "(5) is not a valid IPv4 address, "
        f"got {index_name}"
    )
    if len(index_name) < 3 or len(index_name) > 63:
        raise ValueError(msg)
    if not re.match("^[a-z0-9][a-z0-9._-]*[a-z0-9]$", index_name):
        raise ValueError(msg)
    if ".." in index_name:
        raise ValueError(msg)
    if re.match("^[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}$", index_name):
        raise ValueError(msg)


def check_hnsw_index_params(index_params) -> HnswIndexParams:
    if index_params is None:
        # return default
        return DEFAULT_INDEX_PARAMS

    if index_params["space"] not in ["l2", "cosine", "ip"]:
        raise ValueError(f"Expected space to be l2, cosine, or ip, got {index_params['space']}")
    # default ef and M are 100 and 16
    # set if not set
    if "ef" not in index_params:
        index_params["ef"] = 100
    if "M" not in index_params:
        index_params["M"] = 16

    if index_params["ef"] < 0:
        raise ValueError(f"Expected ef to be >= 0, got {index_params['ef']}")
    if index_params["M"] < 0:
        raise ValueError(f"Expected M to be >= 0, got {index_params['M']}")

    return index_params


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
        get_or_create: bool = False,
        index_params: Optional[HnswIndexParams] = None,
    ) -> Collection:
        check_index_name(name)
        index_params = check_hnsw_index_params(index_params)

        res = self._db.create_collection(name, metadata, get_or_create, index_params)
        return Collection(
            client=self,
            name=name,
            embedding_function=embedding_function,
            metadata=res[0][2],
            index_params=res[0][3],
        )

    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[Dict] = None,
        embedding_function: Optional[Callable] = None,
        index_params: Optional[HnswIndexParams] = None,
    ) -> Collection:
        return self.create_collection(
            name, metadata, embedding_function, get_or_create=True, index_params=index_params
        )

    def get_collection(
        self,
        name: str,
        embedding_function: Optional[Callable] = None,
    ) -> Collection:
        res = self._db.get_collection(name)
        if len(res) == 0:
            raise ValueError(f"Collection {name} does not exist")
        return Collection(
            client=self, name=name, embedding_function=embedding_function, metadata=res[0][2]
        )

    def list_collections(self) -> Sequence[Collection]:
        collections = []
        db_collections = self._db.list_collections()
        for db_collection in db_collections:
            collections.append(
                Collection(client=self, name=db_collection[1], metadata=db_collection[2])
            )
        return collections

    def _modify(
        self,
        current_name: str,
        new_name: Optional[str] = None,
        new_metadata: Optional[Dict] = None,
        new_index_params: Optional[HnswIndexParams] = None,
    ):
        if new_name is not None:
            check_index_name(new_name)
        if new_index_params is not None:
            new_index_params = check_hnsw_index_params(new_index_params)

        self._db.update_collection(current_name, new_name, new_metadata, new_index_params)

    def delete_collection(self, name: str):
        return self._db.delete_collection(name)

    #
    # ITEM METHODS
    #
    def _add(
        self,
        ids,
        collection_name: str,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ):
        collection = self._db.get_fields_from_collection_name(
            collection_name, fields=["uuid", "index_params"]
        )
        collection_uuid = collection[0]
        index_params = collection[1]

        added_uuids = self._db.add(
            collection_uuid,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            ids=ids,
        )

        if increment_index:
            self._db.add_incremental(collection_uuid, added_uuids, embeddings, index_params)

        return True  # NIT: should this return the ids of the succesfully added items?

    def _update(
        self,
        collection_name: str,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ):
        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)
        self._db.update(collection_uuid, ids, embeddings, metadatas, documents)

        return True

    def _get(
        self,
        collection_name: str,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        where_document: Optional[WhereDocument] = {},
        include: Include = ["embeddings", "metadatas", "documents"],
    ):
        if where is None:
            where = {}

        if where_document is None:
            where_document = {}

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        include_embeddings = "embeddings" in include
        include_documents = "documents" in include
        include_metadatas = "metadatas" in include

        # Remove plural from include since db columns are singular
        db_columns = [column[:-1] for column in include] + ["id"]
        column_index = {column_name: index for index, column_name in enumerate(db_columns)}

        db_result = self._db.get(
            collection_name=collection_name,
            ids=ids,
            where=where,
            sort=sort,
            limit=limit,
            offset=offset,
            where_document=where_document,
            columns=db_columns,
        )

        get_result = GetResult(
            ids=[],
            embeddings=[] if include_embeddings else None,
            documents=[] if include_documents else None,
            metadatas=[] if include_metadatas else None,
        )

        for entry in db_result:
            if include_embeddings:
                cast(List, get_result["embeddings"]).append(entry[column_index["embedding"]])
            if include_documents:
                cast(List, get_result["documents"]).append(entry[column_index["document"]])
            if include_metadatas:
                cast(List, get_result["metadatas"]).append(entry[column_index["metadata"]])
            get_result["ids"].append(entry[column_index["id"]])
        return get_result

    def _delete(self, collection_name, ids=None, where=None, where_document=None):
        if where is None:
            where = {}

        if where_document is None:
            where_document = {}

        deleted_uuids = self._db.delete(
            collection_name=collection_name, where=where, ids=ids, where_document=where_document
        )
        return deleted_uuids

    def _count(self, collection_name):
        return self._db.count(collection_name=collection_name)

    def reset(self):
        self._db.reset()
        return True

    def _query(
        self,
        collection_name,
        query_embeddings,
        n_results=10,
        where={},
        where_document={},
        include: Include = ["documents", "metadatas", "distances"],
    ):
        uuids, distances = self._db.get_nearest_neighbors(
            collection_name=collection_name,
            where=where,
            where_document=where_document,
            embeddings=query_embeddings,
            n_results=n_results,
        )

        include_embeddings = "embeddings" in include
        include_documents = "documents" in include
        include_metadatas = "metadatas" in include
        include_distances = "distances" in include

        query_result = QueryResult(
            ids=[],
            embeddings=[] if include_embeddings else None,
            documents=[] if include_documents else None,
            metadatas=[] if include_metadatas else None,
            distances=[] if include_distances else None,
        )
        for i in range(len(uuids)):
            embeddings = []
            documents = []
            ids = []
            metadatas = []
            # Remove plural from include since db columns are singular
            db_columns = [column[:-1] for column in include if column != "distances"] + ["id"]
            column_index = {column_name: index for index, column_name in enumerate(db_columns)}
            db_result = self._db.get_by_ids(uuids[i], columns=db_columns)

            for entry in db_result:
                if include_embeddings:
                    embeddings.append(entry[column_index["embedding"]])
                if include_documents:
                    documents.append(entry[column_index["document"]])
                if include_metadatas:
                    metadatas.append(
                        json.loads(entry[column_index["metadata"]])
                        if entry[column_index["metadata"]]
                        else None
                    )
                ids.append(entry[column_index["id"]])

            if include_embeddings:
                cast(List, query_result["embeddings"]).append(embeddings)
            if include_documents:
                cast(List, query_result["documents"]).append(documents)
            if include_metadatas:
                cast(List, query_result["metadatas"]).append(metadatas)
            if include_distances:
                cast(List, query_result["distances"]).append(distances[i].tolist())
            query_result["ids"].append(ids)

        return query_result

    def raw_sql(self, raw_sql):
        return self._db.raw_sql(raw_sql)

    def create_index(self, collection_name: str):
        collection_uuid = self._db.get_collection_uuid_from_name(collection_name)
        self._db.create_index(collection_uuid=collection_uuid)
        return True

    def _peek(self, collection_name, n=10):
        return self._get(
            collection_name=collection_name,
            limit=n,
            include=["embeddings", "documents", "metadatas"],
        )

    def persist(self):
        self._db.persist()
        return True
