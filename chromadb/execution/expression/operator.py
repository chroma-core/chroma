from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from chromadb.api.types import Embeddings, IDs, Include, IncludeEnum
from chromadb.types import Collection, RequestVersionContext, Where, WhereDocument


@dataclass
class Scan:
    collection: Collection

    @property
    def version(self) -> RequestVersionContext:
        return RequestVersionContext(
            collection_version=self.collection.version,
            log_position=self.collection.log_position,
        )


@dataclass
class SegmentScan(Scan):
    knn_id: UUID
    metadata_id: UUID
    record_id: UUID


@dataclass
class Filter:
    user_ids: Optional[IDs] = None
    where: Optional[Where] = None
    where_document: Optional[WhereDocument] = None


@dataclass
class KNN:
    embeddings: Embeddings
    fetch: int


@dataclass
class Limit:
    skip: int = 0
    fetch: Optional[int] = None


@dataclass
class Projection:
    document: bool = False
    embedding: bool = False
    metadata: bool = False
    rank: bool = False
    uri: bool = False

    @property
    def included(self) -> Include:
        includes = list()
        if self.document:
            includes.append(IncludeEnum.documents)
        if self.embedding:
            includes.append(IncludeEnum.embeddings)
        if self.metadata:
            includes.append(IncludeEnum.metadatas)
        if self.rank:
            includes.append(IncludeEnum.distances)
        if self.uri:
            includes.append(IncludeEnum.uris)
        return includes
