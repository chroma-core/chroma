from abc import abstractmethod
from chromadb.api.types import Documents, Embeddings, IDs, Metadatas, Where, WhereDocument
from chromadb.db import DB
from chromadb.db.index.hnswlib import Hnswlib, delete_all_indexes
from chromadb.errors import (
    NoDatapointsException,
)
import uuid
import numpy.typing as npt
import json
from typing import Dict, Optional, Sequence, List, Tuple, cast
import logging

logger = logging.getLogger(__name__)

COLLECTION_TABLE_SCHEMA = [{"uuid": "UUID"}, {"name": "String"}, {"metadata": "String"}]

EMBEDDING_TABLE_SCHEMA = [
    {"collection_uuid": "UUID"},
    {"uuid": "UUID"},
    {"embedding": "Array(Float64)"},
    {"document": "Nullable(String)"},
    {"id": "Nullable(String)"},
    {"metadata": "Nullable(String)"},
]


def db_array_schema_to_clickhouse_schema(table_schema):
    return_str = ""
    for element in table_schema:
        for k, v in element.items():
            return_str += f"{k} {v}, "
    return return_str


def db_schema_to_keys() -> List[str]:
    keys = []
    for element in EMBEDDING_TABLE_SCHEMA:
        keys.append(list(element.keys())[0])
    return keys


class CommonDBHandler(DB):
    #
    #  INIT METHODS
    #
    @abstractmethod
    def __init__(self, settings):
        self._settings = settings

    @abstractmethod
    def _create_table_collections(self):
        pass

    @abstractmethod
    def _create_table_embeddings(self):
        pass

    index_cache = {}

    def _index(self, collection_id):
        """Retrieve an HNSW index instance for the given collection"""

        if collection_id not in self.index_cache:
            coll = self.get_collection_by_id(collection_id)
            collection_metadata = coll[2]
            index = Hnswlib(collection_id, self._settings, collection_metadata)
            self.index_cache[collection_id] = index

        return self.index_cache[collection_id]

    def _delete_index(self, collection_id):
        """Delete an index from the cache"""
        index = self._index(collection_id)
        index.delete()
        del self.index_cache[collection_id]

    #
    #  UTILITY METHODS
    #
    def _create_where_clause(
        self,
        collection_uuid: str,
        ids: Optional[List[str]] = None,
        where: Where = {},
        where_document: WhereDocument = {},
    ):
        where_clauses: List[str] = []
        self._format_where(where, where_clauses)
        if len(where_document) > 0:
            where_document_clauses = []
            self._format_where_document(where_document, where_document_clauses)
            where_clauses.extend(where_document_clauses)

        if ids is not None:
            where_clauses.append(f" id IN {tuple(ids)}")

        where_clauses.append(f"collection_uuid = '{collection_uuid}'")
        where_str = " AND ".join(where_clauses)
        where_str = f"WHERE {where_str}"
        return where_str

    #
    #  COLLECTION METHODS
    #

    @abstractmethod
    def get_collection_by_id(self, collection_uuid: str) -> List:
        pass

    #
    #  ITEM METHODS
    #
    @abstractmethod
    def _update(
        self,
        collection_uuid,
        ids: IDs,
        embeddings: Optional[Embeddings],
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
    ):
        pass

    def update(
        self,
        collection_uuid,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ):
        # Verify all IDs exist
        existing_items = self.get(collection_uuid=collection_uuid, ids=ids)
        if len(existing_items) != len(ids):
            raise ValueError(f"Could not find {len(ids) - len(existing_items)} items for update")

        # Update the db
        self._update(collection_uuid, ids, embeddings, metadatas, documents)

        # Update the index
        if embeddings is not None:
            update_uuids = [x[1] for x in existing_items]
            index = self._index(collection_uuid)
            index.add(update_uuids, embeddings, update=True)

    @abstractmethod
    def _format_where(self, where, result):
        pass

    @abstractmethod
    def _format_where_document(self, where_document, results):
        pass

    @abstractmethod
    def _get(self, where, columns: Optional[List] = None) -> List:
        pass

    def get(
        self,
        where: Where = {},
        collection_name: Optional[str] = None,
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: WhereDocument = {},
        columns: Optional[List[str]] = None,
    ) -> Sequence:
        if collection_name is None and collection_uuid is None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        where_str = self._create_where_clause(
            # collection_uuid must be defined at this point, cast it for typechecker
            cast(str, collection_uuid),
            ids=ids,
            where=where,
            where_document=where_document,
        )

        if sort is not None:
            where_str += f" ORDER BY {sort}"
        else:
            where_str += " ORDER BY collection_uuid"  # stable ordering

        if limit is not None or isinstance(limit, int):
            where_str += f" LIMIT {limit}"

        if offset is not None or isinstance(offset, int):
            where_str += f" OFFSET {offset}"

        val = self._get(where=where_str, columns=columns)

        return val

    @abstractmethod
    def _delete(self, where_str: Optional[str] = None) -> List:
        pass

    def delete(
        self,
        where: Where = {},
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        where_document: WhereDocument = {},
    ) -> List:
        where_str = self._create_where_clause(
            # collection_uuid must be defined at this point, cast it for typechecker
            cast(str, collection_uuid),
            ids=ids,
            where=where,
            where_document=where_document,
        )

        deleted_uuids = self._delete(where_str)

        index = self._index(collection_uuid)
        index.delete_from_index(deleted_uuids)

        return deleted_uuids

    def get_nearest_neighbors(
        self,
        where: Where,
        where_document: WhereDocument,
        embeddings: Embeddings,
        n_results: int,
        collection_name=None,
        collection_uuid=None,
    ) -> Tuple[List[List[uuid.UUID]], npt.NDArray]:

        # Either the collection name or the collection uuid must be provided
        if collection_name is None and collection_uuid is None:
            raise TypeError("Arguments collection_name and collection_uuid cannot both be None")

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        if len(where) != 0 or len(where_document) != 0:
            results = self.get(
                collection_uuid=collection_uuid, where=where, where_document=where_document
            )

            if len(results) > 0:
                ids = [x[1] for x in results]
            else:
                raise NoDatapointsException(
                    f"No datapoints found for the supplied filter {json.dumps(where)}"
                )
        else:
            ids = None

        index = self._index(collection_uuid)
        uuids, distances = index.get_nearest_neighbors(embeddings, n_results, ids)

        return uuids, distances

    def create_index(self, collection_uuid: str):
        """Create an index for a collection_uuid and optionally scoped to a dataset.
        Args:
            collection_uuid (str): The collection_uuid to create an index for
            dataset (str, optional): The dataset to scope the index to. Defaults to None.
        Returns:
            None
        """
        get = self.get(collection_uuid=collection_uuid)

        uuids = [x[1] for x in get]
        embeddings = [x[2] for x in get]

        index = self._index(collection_uuid)
        index.add(uuids, embeddings)

    def add_incremental(self, collection_uuid, uuids, embeddings):
        index = self._index(collection_uuid)
        index.add(uuids, embeddings)

    def reset_indexes(self):
        delete_all_indexes(self._settings)
        self.index_cache = {}

    @abstractmethod
    def _reset(self):
        pass

    def reset(self):
        self._reset()
        self._create_table_collections()
        self._create_table_embeddings()

        self.reset_indexes()
