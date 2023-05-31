from chromadb.api import API
from chromadb.config import System
from chromadb.db.system import SysDB
from chromadb.segment import SegmentManager
from chromadb.telemetry import Telemetry
from chromadb.api.models.Collection import Collection
from chromadb import __version__

from chromadb.api.types import (
    CollectionMetadata,
    EmbeddingFunction,
    IDs,
    Embeddings,
    Metadatas,
    Documents,
    Where,
    WhereDocument,
    Include,
    GetResult,
    QueryResult,
)

from typing import Optional, Sequence
from overrides import override
from uuid import UUID
import pandas as pd
import time
import logging

logger = logging.getLogger(__name__)


class SegmentAPI(API):
    """API implementation utilizing the new segment-based internal architecture"""

    _sysdb: SysDB
    _manager: SegmentManager
    _telemetry_client: Telemetry

    def __init__(self, system: System):
        super().__init__(system)
        self._sysdb = self.require(SysDB)
        self._manager = self.require(SegmentManager)
        self._telemetry_client = self.require(Telemetry)

    @override
    def heartbeat(self) -> int:
        return int(1000 * time.time_ns())

    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = None,
        get_or_create: bool = False,
    ) -> Collection:
        raise NotImplementedError()

    @override
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = None,
    ) -> Collection:
        raise NotImplementedError()

    @override
    def get_collection(
        self,
        name: str,
        embedding_function: Optional[EmbeddingFunction] = None,
    ) -> Collection:
        raise NotImplementedError()

    @override
    def list_collections(self) -> Sequence[Collection]:
        raise NotImplementedError()

    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        raise NotImplementedError()

    @override
    def delete_collection(self, name: str) -> None:
        raise NotImplementedError()

    @override
    def _add(
        self,
        ids: IDs,
        collection_id: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ) -> bool:
        raise NotImplementedError()

    @override
    def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ) -> bool:
        raise NotImplementedError()

    @override
    def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        increment_index: bool = True,
    ) -> bool:
        raise NotImplementedError()

    @override
    def _get(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        where_document: Optional[WhereDocument] = {},
        include: Include = ["embeddings", "metadatas", "documents"],
    ) -> GetResult:
        raise NotImplementedError()

    @override
    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
    ) -> IDs:
        raise NotImplementedError()

    @override
    def _count(self, collection_id: UUID) -> int:
        raise NotImplementedError()

    @override
    def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Where = {},
        where_document: WhereDocument = {},
        include: Include = ["documents", "metadatas", "distances"],
    ) -> QueryResult:
        raise NotImplementedError()

    @override
    def _peek(self, collection_id: UUID, n: int = 10) -> GetResult:
        raise NotImplementedError()

    @override
    def get_version(self) -> str:
        return __version__

    @override
    def raw_sql(self, sql: str) -> pd.DataFrame:
        raise NotImplementedError()

    @override
    def create_index(self, collection_name: str) -> bool:
        logger.warning(
            "Calling create_index is unnecessary, data is now automatically indexed"
        )
        return True

    @override
    def persist(self) -> bool:
        logger.warning(
            "Calling persist is unnecessary, data is now automatically indexed."
        )
        return True
