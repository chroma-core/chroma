from typing import List, Sequence, Optional, Tuple
import uuid
from uuid import UUID
import numpy.typing as npt
import psycopg2 as pg
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
            self._create_table_collections(curs)
            self._create_table_embeddings_with_vector_size(curs, 100)

    def _get_conn(self) -> pg.connection:
        if self._conn is None:
            self._init_conn()
        return self._conn

    def _create_table_collections(self, cursor: pg.cursor) -> None:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS collections (
                uuid UUID PRIMARY KEY,
                name TEXT,
                metadata TEXT
        )"""
        )

    def _create_table_embeddings_with_vector_size(
        self, cursor: pg.cursor, size: int
    ) -> None:
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS collections{str(size)} (
                collection_uuid UUID NOT NULL,
                uuid UUID PRIMARY KEY,
                embedding VECTOR({str(size)}) NOT NULL,
                document TEXT NOT NULL,
                id TEXT
                metadata TEXT
        )"""
        )

    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[Metadata] = None,
        get_or_create: bool = False,
    ) -> Sequence:  # type: ignore
        # poor man's unique constraint
        dupe_check = self.get_collection(name)

        if len(dupe_check) > 0:
            if get_or_create:
                if dupe_check[0][2] != metadata:
                    self.update_collection(
                        dupe_check[0][0], new_name=name, new_metadata=metadata
                    )
                    dupe_check = self.get_collection(name)
                logger.info(
                    f"collection with name {name} already exists, returning existing \
                        collection"
                )
                return dupe_check
            else:
                raise ValueError(f"Collection with name {name} already exists")

        collection_uuid = uuid.uuid4()
        data_to_insert = [[collection_uuid, name, json.dumps(metadata)]]

        with self._get_conn().cursor() as curr:
            curr.execute(
                f"""INSERT INTO collections (uuid, name, metadata) VALUES {data_to_insert[0][0],data_to_insert[0][1], data_to_insert[0][1]}"""
            )
        return [[collection_uuid, name, metadata]]

    @override
    def get_collection(self, name: str) -> Sequence:
        if self._conn is not None:
            with self._conn.cursor() as curs:
                curs.execute(
                    f"""
            SELECT * FROM collections WHERE name = '{name}'
            """
                )
                res = curs.fetchall()
            # json.loads the metadata
            return [[x[0], x[1], json.loads(x[2])] for x in res]
        raise ValueError("Postgres connection not found")

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
        raise NotImplementedError

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
    ) -> Sequence:  # type: ignore
        raise NotImplementedError

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
    def create_index(self, collection_uuid: UUID):  # type: ignore
        raise NotImplementedError

    @override
    def persist(self) -> None:
        raise NotImplementedError(
            "Postgres is a persistent database, this method is not needed"
        )
