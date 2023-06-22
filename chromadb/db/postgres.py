from typing import List, Sequence, Optional, Tuple, cast
import uuid
from uuid import UUID
import numpy.typing as npt
import psycopg2 as pg
from psycopg2.extras import Json
from psycopg2.extensions import cursor, connection
import json

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
            self._create_table_embeddings_with_vector_size(curs, 100)
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

    def _execute_query_with_response(self, query: str) -> List[Tuple]:
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

        insert_query = f"""INSERT INTO collections (uuid, name, metadata) VALUES {data_to_insert[0][0], 
        data_to_insert[0][1], data_to_insert[0][2]}"""
        self._execute_query(insert_query)
        return [[collection_uuid, name, metadata]]

    @override
    def get_collection(self, name: str) -> Sequence:
        query = f"SELECT * FROM collections WHERE name = '{name}'"
        res = self._execute_query_with_response(query)
        # json.loads for metadata not needed, psycopg2 does it automatically
        return [[x[0], x[1], x[2]] for x in res]

    @override
    def list_collections(self) -> Sequence:  # type: ignore
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    @override
    def add_incremental(
        self, collection_uuid: UUID, ids: List[UUID], embeddings: Embeddings
    ) -> None:
        raise NotImplementedError

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
    ) -> Sequence:
        if collection_name is None and collection_uuid is None:
            raise TypeError(
                "Arguments collection_name and collection_uuid cannot both be None"
            )

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
    ) -> Tuple[List[List[UUID]], npt.NDArray]:
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
    def create_index(self, collection_uuid: UUID):
        raise NotImplementedError

    @override
    def persist(self) -> None:
        raise NotImplementedError(
            "Postgres is a persistent database, this method is not needed"
        )
