from typing import Any, Dict, List, Sequence, Optional, Tuple, cast
import uuid
from uuid import UUID
import numpy.typing as npt
import psycopg2 as pg

from chromadb.db.index.pgvector import Pgvector, delete_all_indexes

from psycopg2.extensions import cursor, connection
import json

from pypika import Query, Table

from chromadb.api.types import (
    Embeddings,
    Documents,
    IDs,
    Metadatas,
    Metadata,
    Where,
    WhereDocument,
)
from overrides import override

from chromadb.db import DB
from chromadb.config import System

import logging

logger = logging.getLogger(__name__)


class Postgres(DB):
    def __init__(self, system: System):
        super().__init__(system)
        self._settings = system.settings

        self._settings.require("postgres_username")
        self._settings.require("postgres_password")
        self._settings.require("postgres_hostname")
        self._settings.require("postgres_port")
        self._settings.require("postgres_databasename")

        self._init_conn()

    def _init_conn(self) -> None:
        self._conn = pg.connect(
            user=self._settings.postgres_username,
            password=self._settings.postgres_password,
            host=self._settings.postgres_hostname,
            port=self._settings.postgres_port,
            database=self._settings.postgres_databasename,
        )
        with self._conn.cursor() as curs:
            self._create_extensions(curs)
            self._create_table_collections(curs)
            self._create_table_embeddings_with_vector_size(curs, 5)
        self._conn.commit()

    def _get_conn(self) -> connection:
        if self._conn is None:
            self._init_conn()
        return self._conn

    def _create_extensions(self, cursor: cursor) -> None:
        cursor.execute("""CREATE EXTENSION IF NOT EXISTS vector;""")

    def _create_table_collections(self, cursor: cursor) -> None:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS collections (
   uuid UUID PRIMARY KEY,
   name TEXT NOT NULL,
   metadata JSONB);"""
        )

    def _create_table_embeddings_with_vector_size(
        self, cursor: cursor, size: int
    ) -> None:
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS embeddings{str(size)} (
   collection_uuid UUID NOT NULL,
   uuid UUID PRIMARY KEY,
   embedding VECTOR({str(size)}) NOT NULL,
   document TEXT NOT NULL,
   id TEXT,
   metadata TEXT
);"""
        )

    #
    # UTILITY FUNCTIONS
    #

    def _execute_query(self, query: str) -> None:
        with self._get_conn().cursor() as curs:
            curs.execute(query)
        self._conn.commit()

    # def _execute_query_with_response(self, query: str) -> list[tuple[Any, ...]]:
    def _execute_query_with_response(self, query: str):  # type: ignore
        with self._get_conn().cursor() as curs:
            curs.execute(query)
            res = curs.fetchall()
        self._conn.commit()
        return res

    # def _create_where_clause(
    #     self,
    #     collection_uuid: str,
    #     ids: Optional[List[str]] = None,
    #     where: Where = {},
    #     where_document: WhereDocument = {},
    # ):
    #     where_clauses: List[str] = []
    #     self._format_where(where, where_clauses)
    #     if len(where_document) > 0:
    #         where_document_clauses = []
    #         self._format_where_document(where_document, where_document_clauses)
    #         where_clauses.extend(where_document_clauses)

    #     if ids is not None:
    #         where_clauses.append(f" id IN {tuple(ids)}")

    #     where_clauses.append(f"collection_uuid = '{collection_uuid}'")
    #     where_str = " AND ".join(where_clauses)
    #     where_str = f"WHERE {where_str}"
    #     return where_str

    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[Metadata] = None,
        get_or_create: bool = False,
    ) -> Sequence:  # type: ignore
        # poor man's unique constraint
        # dupe_check = self.get_collection(name)

        # if len(dupe_check) > 0:
        #     if get_or_create:
        #         if dupe_check[0][2] != metadata:
        #             self.update_collection(
        #                 dupe_check[0][0], new_name=name, new_metadata=metadata
        #             )
        #             dupe_check = self.get_collection(name)
        #         logger.info(
        #             f"collection with name {name} already exists, returning existing \
        #                 collection"
        #         )
        #         return dupe_check
        #     else:
        #         raise ValueError(f"Collection with name {name} already exists")

        collection_uuid = uuid.uuid4()
        data_to_insert = [[collection_uuid, name, json.dumps(metadata)]]
        insert_query = (
            Query.into(Table("collections"))
            .columns("uuid", "name", "metadata")
            .insert(data_to_insert[0][0], data_to_insert[0][1], data_to_insert[0][2])
        )
        self._execute_query(str(insert_query))
        return [[collection_uuid, name, metadata]]

    @override
    def get_collection(self, name: str) -> Sequence[Any]:
        query = f"SELECT * FROM collections WHERE name = '{name}'"
        res = self._execute_query_with_response(query)
        # json.loads for metadata not needed, psycopg2 does it automatically
        return [[x[0], x[1], x[2]] for x in res]

    def get_collection_by_id(self, collection_uuid: UUID) -> Sequence[Any]:
        query = f"SELECT * FROM collections WHERE uuid = '{collection_uuid}'"
        res = self._execute_query_with_response(query)
        # json.loads for metadata not needed, psycopg2 does it automatically
        return [[x[0], x[1], x[2]] for x in res]

    @override
    def list_collections(self) -> Sequence:  # type: ignore
        query = "SELECT * FROM collections"
        res = self._execute_query_with_response(query)
        return [[x[0], x[1], x[2]] for x in res]

    @override
    def update_collection(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[Metadata] = None,
    ) -> None:
        raise NotImplementedError

    @override
    def delete_collection(self, name: str) -> None:
        collection_uuid = self.get_collection_uuid_from_name(name)
        query = (
            f"SELECT uuid FROM embeddings WHERE collection_uuid = '{collection_uuid}'"
        )
        self._execute_query(query)

        if self.index_cache.get(collection_uuid) is not None:
            self._delete_index(collection_uuid)

        query = f"DELETE FROM collections WHERE name = '{name}'"
        self._execute_query(query)
        raise NotImplementedError

    def _delete_index(self, collection_id: UUID) -> None:
        """Delete an index from the cache"""
        index = self._index(collection_id)
        index.delete()
        del self.index_cache[collection_id]

    def reset_indexes(self) -> None:
        delete_all_indexes(self._settings)
        self.index_cache = {}

    @override
    def get_collection_uuid_from_name(self, collection_name: str) -> UUID:
        query = f"SELECT uuid FROM collections WHERE name = '{collection_name}'"
        return cast(UUID, self._execute_query_with_response(query)[0][0])

    @override
    def add(
        self,
        collection_uuid: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
        ids: List[str],
    ) -> List[UUID]:
        embeddings_table = f"embeddings{len(embeddings[0])}"
        data_to_insert = [
            [
                collection_uuid,
                uuid.uuid4(),
                embedding,
                json.dumps(metadatas[i]) if metadatas else None,
                documents[i] if documents else None,
                ids[i],
            ]
            for i, embedding in enumerate(embeddings)
        ]
        # TODO: use bulk insert down the line rather than looping
        queries = [
            Query.into(Table(embeddings_table))
            .columns(
                "collection_uuid", "uuid", "embedding", "metadata", "document", "id"
            )
            .insert(data[0], data[1], data[2], data[3], data[4], data[5])
            for data in data_to_insert
        ]
        insert_query = ""
        for query in queries:
            insert_query += str(query) + ";"
        self._execute_query(insert_query)

        return [x[1] for x in data_to_insert]  # type: ignore

    @override
    def add_incremental(
        self, collection_uuid: UUID, ids: List[UUID], embeddings: Embeddings
    ) -> None:
        raise NotImplementedError

    def _add_where_clause(
        self,
        query: Query,
        collection_uuid: Optional[UUID],
        ids: Optional[List[str]] = None,
        where: Where = {},
        where_document: WhereDocument = {},
    ) -> Query:
        return query

    @override
    def get(
        self,
        where: Where = {},
        collection_name: Optional[str] = None,
        collection_uuid: Optional[UUID] = None,
        ids: Optional[IDs] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: WhereDocument = {},
        columns: Optional[List[str]] = None,
    ) -> Sequence[Any]:
        if collection_name is None and collection_uuid is None:
            raise TypeError(
                "Arguments collection_name and collection_uuid cannot both be None"
            )

        if collection_name is not None:
            collection_uuid = self.get_collection_uuid_from_name(collection_name)

        get_query = Query.from_(Table("collections")).select("*")

        # get_query: Query = self._add_where_clause(
        #     get_query,
        #     collection_uuid=collection_uuid,
        #     ids=ids,
        #     where=where,
        #     where_document=where_document,
        # )

        if sort is not None:
            get_query.orderby(sort)
        else:
            get_query.orderby("collection_uuid")  # stable ordering

        if limit is not None or isinstance(limit, int):
            get_query.limit(limit)

        if offset is not None or isinstance(offset, int):
            get_query.offset(offset)

        res = self._execute_query_with_response(str(get_query))
        return [[x[0], x[1], x[2]] for x in res]

    @override
    def update(
        self,
        collection_uuid: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ) -> bool:
        raise NotImplementedError

    @override
    def count(self, collection_id: UUID) -> int:
        raise NotImplementedError

    @override
    def delete(
        self,
        where: Where = {},
        collection_uuid: Optional[UUID] = None,
        ids: Optional[IDs] = None,
        where_document: WhereDocument = {},
    ) -> List[str]:
        raise NotImplementedError

    @override
    def reset(self) -> None:
        raise NotImplementedError

    @override
    def get_nearest_neighbors(
        self,
        collection_uuid: UUID,
        where: Where = {},
        embeddings: Optional[Embeddings] = None,
        n_results: int = 10,
        where_document: WhereDocument = {},
    ) -> Tuple[List[List[UUID]], npt.NDArray[Any]]:
        raise NotImplementedError

    @override
    def get_by_ids(
        self, uuids: List[UUID], columns: Optional[List[str]] = None
    ) -> Sequence:  # type: ignore
        raise NotImplementedError

    @override
    def raw_sql(self, raw_sql):  # type: ignore
        raise NotImplementedError

    @override
    def create_index(self, collection_uuid: UUID) -> None:
        raise NotImplementedError

    # TODO: implement this cache on the DB level
    # to offload state from the server
    index_cache: Dict[UUID, Pgvector] = {}

    def _index(self, collection_id: UUID) -> Pgvector:
        """Retrieve an Pgvector index instance for the given collection"""

        if collection_id not in self.index_cache:
            coll = self.get_collection_by_id(collection_id)
            collection_metadata = coll[2]
            index = Pgvector(
                collection_id,
                self._settings,
                collection_metadata,
                self._conn,
                5
                # self.count(collection_id),
            )
            self.index_cache[collection_id] = index

        return self.index_cache[collection_id]

    @override
    def persist(self) -> None:
        raise NotImplementedError(
            "Postgres is a persistent database, this method is not needed"
        )
